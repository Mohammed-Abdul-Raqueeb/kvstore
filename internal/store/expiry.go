package store

import (
	"context"
	"sync/atomic"
	"time"
)

// SweepStats records what one sweeper cycle did.
type SweepStats struct {
	Rounds   int
	Sampled  int
	Expired  int
	Duration time.Duration
	Budgeted bool // true if the cycle stopped because it ran out of budget
}

// Sweeper is the active expiration mechanism (DESIGN.md §9.2).
//
// The algorithm, per shard:
//
//	take the write lock
//	sample N random keys *that have a TTL*
//	delete the expired ones
//	release the lock
//	if expired/N > threshold: repeat this shard
//	else: move on
//	if elapsed > budget: stop the cycle
//
// Two properties are what make this "expiration that does not block normal
// requests":
//
//  1. The lock is taken and released in small bites. A shard is never locked
//     for a whole scan, so the worst case a concurrent GET can queue behind
//     is one 20-key sample, not one million keys.
//  2. Total work per cycle is bounded by wall-clock budget, not by keyspace
//     size. A keyspace 100x larger does not produce a 100x latency spike; it
//     produces a longer tail of cycles.
//
// Sampling 20 with a 25% continue threshold converges to under ~25% expired
// keys remaining, which is the ratio Redis established empirically.
//
// Sampling is O(1) because each shard keeps ttlSlots, a flat slice of just
// the TTL-bearing entries. Iterating a Go map to reach a random position
// would be O(n) and would make the sweeper's cost scale with the keyspace,
// defeating the whole design.
type Sweeper struct {
	store     *Store
	interval  time.Duration
	sample    int
	threshold float64
	budget    time.Duration

	cycles     atomic.Uint64
	totalSwept atomic.Uint64
	lastStats  atomic.Pointer[SweepStats]

	stop chan struct{}
	done chan struct{}
}

// NewSweeper builds a sweeper. It does not start it.
func NewSweeper(s *Store, interval time.Duration, sample int, threshold float64, budget time.Duration) *Sweeper {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	if sample <= 0 {
		sample = 20
	}
	if threshold <= 0 {
		threshold = 0.25
	}
	if budget <= 0 {
		budget = interval / 4
	}
	return &Sweeper{
		store:     s,
		interval:  interval,
		sample:    sample,
		threshold: threshold,
		budget:    budget,
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// Start runs the sweeper until ctx is cancelled or Stop is called.
func (w *Sweeper) Start(ctx context.Context) {
	go func() {
		defer close(w.done)
		t := time.NewTicker(w.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.stop:
				return
			case <-t.C:
				w.RunCycle()
			}
		}
	}()
}

// Stop halts the sweeper and waits for the current cycle to finish.
func (w *Sweeper) Stop() {
	select {
	case <-w.stop:
	default:
		close(w.stop)
	}
	<-w.done
}

// RunCycle performs one full sweep cycle synchronously. Exported so tests can
// drive expiry deterministically instead of sleeping.
func (w *Sweeper) RunCycle() SweepStats {
	start := time.Now()
	st := SweepStats{}

	for i := range w.store.shards {
		for {
			if time.Since(start) > w.budget {
				st.Budgeted = true
				goto finish
			}
			nowMs := w.store.clock.NowMillis()
			var sampled, expired int
			w.store.withWrite(i, func(sh *shard) {
				sampled, expired = sh.sweepLocked(nowMs, w.sample)
			})
			st.Rounds++
			st.Sampled += sampled
			st.Expired += expired
			if sampled == 0 || float64(expired)/float64(sampled) <= w.threshold {
				break
			}
		}
	}

finish:
	st.Duration = time.Since(start)
	w.cycles.Add(1)
	w.totalSwept.Add(uint64(st.Expired))
	cp := st
	w.lastStats.Store(&cp)
	return st
}

// Cycles returns the number of completed sweep cycles.
func (w *Sweeper) Cycles() uint64 { return w.cycles.Load() }

// TotalExpired returns the number of keys the sweeper has reclaimed.
func (w *Sweeper) TotalExpired() uint64 { return w.totalSwept.Load() }

// LastCycle returns stats for the most recent cycle, or nil.
func (w *Sweeper) LastCycle() *SweepStats { return w.lastStats.Load() }

// PurgeExpired removes every entry whose deadline has already passed and
// returns how many were reclaimed.
//
// This exists because Range deliberately *hides* expired entries — it feeds
// the snapshot writer and the replication stream, neither of which should
// ever see a dead key. Recovery needs the opposite view: after a restart,
// keys that expired while the process was down are sitting in the store
// invisible but occupying memory, and something has to reclaim them
// (DESIGN.md §8 step 7). Using Range for that job silently reclaims nothing.
//
// Work is done in bounded batches so a keyspace with millions of expired
// keys does not hold a shard lock for the whole sweep.
func (s *Store) PurgeExpired() int {
	const batch = 1024
	nowMs := s.clock.NowMillis()
	total := 0
	for i := range s.shards {
		for {
			removed := 0
			s.withWrite(i, func(sh *shard) {
				// Iterate ttlSlots backwards: removeEntryLocked swaps the
				// last element into the freed index, and every element at a
				// higher index has already been examined, so nothing is
				// skipped.
				for j := len(sh.ttlSlots) - 1; j >= 0 && removed < batch; j-- {
					e := sh.ttlSlots[j]
					if e == nil || !e.Expired(nowMs) {
						continue
					}
					key := append([]byte(nil), e.Key...)
					sh.removeEntryLocked(e)
					sh.expired.Add(1)
					removed++
					if sh.onEvict != nil {
						sh.onEvict(key, ReasonExpired)
					}
				}
			})
			total += removed
			if removed < batch {
				break
			}
		}
	}
	return total
}
