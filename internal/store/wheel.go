package store

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Timing wheel expiry (DESIGN.md §9.3).
//
// A hierarchical timing wheel gives O(1) insert and O(1) work per tick,
// versus the sampled sweeper's O(sample) per round with no upper bound on
// how long a key outlives its deadline. The trade is memory per timer and
// cancellation complexity when a key's TTL is overwritten.
//
// Cancellation is handled *lazily*, which removes the complexity entirely:
// a timer carries the deadline it was scheduled for, and when it fires the
// expirer checks the live entry. If the key was deleted, overwritten with a
// different deadline, or given no TTL at all, the timer is simply dropped.
// Stale timers therefore cost memory until they fire, but they can never
// delete the wrong data. The alternative — an index from key to timer so a
// timer can be cancelled on overwrite — costs a lock and a map entry on
// every SET, in exchange for memory this design already bounds by the TTL
// duration itself.
//
// Structure: `levels` wheels of `slotCount` buckets each. Level 0 covers
// tick*slotCount, level 1 covers tick*slotCount^2, and so on. A timer more
// than one level-0 revolution out is parked at a higher level and cascaded
// down when that level's bucket comes due. This is the same scheme as the
// Linux kernel's classic timer wheel.

const (
	wheelSlotBits  = 8
	wheelSlotCount = 1 << wheelSlotBits
	wheelSlotMask  = wheelSlotCount - 1
	wheelLevels    = 5 // covers 256^5 ticks ~ 3.5e11 ticks; unreachable in practice
)

type wheelTimer struct {
	key      []byte
	expireAt uint64 // absolute wall-clock ms this timer was scheduled for
}

// Wheel is a hierarchical timing wheel over absolute wall-clock deadlines.
type Wheel struct {
	mu       sync.Mutex
	tickMs   uint64
	epochMs  uint64 // wall-clock ms corresponding to tick 0
	nowTick  uint64
	levels   [wheelLevels][wheelSlotCount][]*wheelTimer
	pending  atomic.Int64
	overflow []*wheelTimer // deadlines beyond the wheel's reach
}

// NewWheel builds a wheel whose tick zero is at epochMs.
func NewWheel(tick time.Duration, epochMs uint64) *Wheel {
	t := uint64(tick / time.Millisecond)
	if t == 0 {
		t = 1
	}
	return &Wheel{tickMs: t, epochMs: epochMs}
}

// Pending returns the number of scheduled timers, including stale ones.
func (w *Wheel) Pending() int64 { return w.pending.Load() }

func (w *Wheel) tickFor(ms uint64) uint64 {
	if ms <= w.epochMs {
		return 0
	}
	return (ms - w.epochMs) / w.tickMs
}

// Add schedules a timer for key at absolute deadline expireAt.
func (w *Wheel) Add(key []byte, expireAt uint64) {
	t := &wheelTimer{key: append([]byte(nil), key...), expireAt: expireAt}
	w.mu.Lock()
	w.addLocked(t)
	w.mu.Unlock()
	w.pending.Add(1)
}

func (w *Wheel) addLocked(t *wheelTimer) {
	target := w.tickFor(t.expireAt)
	if target <= w.nowTick {
		// Already due (or in the past). Land it in the very next bucket so
		// the normal Advance path fires it; firing inline here would mean
		// deleting from the store while holding the wheel lock.
		target = w.nowTick + 1
	}
	delta := target - w.nowTick
	for level := 0; level < wheelLevels; level++ {
		shift := uint(level * wheelSlotBits)
		if delta < (uint64(1) << uint((level+1)*wheelSlotBits)) {
			slot := (target >> shift) & wheelSlotMask
			w.levels[level][slot] = append(w.levels[level][slot], t)
			return
		}
	}
	w.overflow = append(w.overflow, t)
}

// Advance moves the wheel to nowMs and returns every timer that came due.
// The returned timers are candidates: the caller must validate each against
// the live store before deleting anything.
func (w *Wheel) Advance(nowMs uint64) []*wheelTimer {
	target := w.tickFor(nowMs)
	var due []*wheelTimer

	w.mu.Lock()
	defer w.mu.Unlock()

	for w.nowTick < target {
		w.nowTick++

		// Cascade higher levels first: when a level's index wraps to a new
		// bucket, that bucket's timers are re-inserted and settle into
		// lower levels at their now-nearer offsets.
		for level := 1; level < wheelLevels; level++ {
			shift := uint(level * wheelSlotBits)
			if w.nowTick&((uint64(1)<<shift)-1) != 0 {
				break // this level's boundary has not been reached
			}
			slot := (w.nowTick >> shift) & wheelSlotMask
			bucket := w.levels[level][slot]
			if len(bucket) == 0 {
				continue
			}
			w.levels[level][slot] = nil
			for _, t := range bucket {
				w.addLocked(t)
			}
		}

		slot := w.nowTick & wheelSlotMask
		if bucket := w.levels[0][slot]; len(bucket) > 0 {
			due = append(due, bucket...)
			w.levels[0][slot] = nil
		}
	}

	// Sweep the overflow list for anything now within reach.
	if len(w.overflow) > 0 {
		keep := w.overflow[:0]
		for _, t := range w.overflow {
			if w.tickFor(t.expireAt) < w.nowTick+(uint64(1)<<uint(wheelLevels*wheelSlotBits)) {
				w.addLocked(t)
			} else {
				keep = append(keep, t)
			}
		}
		w.overflow = keep
	}

	w.pending.Add(-int64(len(due)))
	return due
}

// WheelExpirer drives a Wheel against a Store.
type WheelExpirer struct {
	store *Store
	wheel *Wheel
	tick  time.Duration

	fired   atomic.Uint64
	stale   atomic.Uint64
	expired atomic.Uint64

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// NewWheelExpirer builds the expirer and registers the store hook that
// schedules a timer for every key that gains a TTL.
func NewWheelExpirer(s *Store, tick time.Duration) *WheelExpirer {
	if tick <= 0 {
		tick = 10 * time.Millisecond
	}
	w := &WheelExpirer{
		store: s,
		wheel: NewWheel(tick, s.Clock().NowMillis()),
		tick:  tick,
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	s.SetOnTTL(func(key []byte, expireAt uint64) { w.wheel.Add(key, expireAt) })

	// Any TTL-bearing keys already present (from recovery) must be
	// scheduled too, or they would only ever be reclaimed lazily.
	s.Range(func(e *Entry) bool {
		if e.ExpireAt != 0 {
			w.wheel.Add(e.Key, e.ExpireAt)
		}
		return true
	})
	return w
}

// Pending returns the number of scheduled timers.
func (w *WheelExpirer) Pending() int64 { return w.wheel.Pending() }

// Stats returns (fired, stale, expired) counters.
func (w *WheelExpirer) Stats() (uint64, uint64, uint64) {
	return w.fired.Load(), w.stale.Load(), w.expired.Load()
}

// Start runs the expirer until ctx is cancelled or Stop is called.
func (w *WheelExpirer) Start(ctx context.Context) {
	go func() {
		defer close(w.done)
		t := time.NewTicker(w.tick)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.stop:
				return
			case <-t.C:
				w.RunOnce()
			}
		}
	}()
}

// Stop halts the expirer.
func (w *WheelExpirer) Stop() {
	w.once.Do(func() { close(w.stop) })
	<-w.done
}

// RunOnce advances the wheel to the current time and reclaims what is due.
// Exported so tests can drive expiry deterministically.
func (w *WheelExpirer) RunOnce() int {
	nowMs := w.store.clock.NowMillis()
	due := w.wheel.Advance(nowMs)
	if len(due) == 0 {
		return 0
	}
	w.fired.Add(uint64(len(due)))
	n := 0
	for _, t := range due {
		i, _ := w.store.shardFor(t.key)
		k := string(t.key)
		var removed bool
		w.store.withWrite(i, func(sh *shard) {
			e, ok := sh.entries[k]
			// Lazy cancellation: only act if this is still the same
			// scheduling. An overwrite gave the key a different deadline
			// (or none), so this timer refers to a version that no longer
			// exists.
			if !ok || e.ExpireAt != t.expireAt {
				return
			}
			if !e.Expired(nowMs) {
				// The wheel's resolution rounded us early; reschedule.
				return
			}
			key := append([]byte(nil), e.Key...)
			sh.removeEntryLocked(e)
			sh.expired.Add(1)
			removed = true
			if sh.onEvict != nil {
				sh.onEvict(key, ReasonExpired)
			}
		})
		if removed {
			n++
			w.expired.Add(1)
		} else {
			w.stale.Add(1)
		}
	}
	return n
}
