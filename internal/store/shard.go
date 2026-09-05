package store

import (
	"sync"
	"sync/atomic"
)

// shard is the storage core. Every engine (sharded, global, actor) is built
// out of these; they differ only in how access to a shard is serialised.
//
// Methods whose names end in "Locked" assume the caller holds mu
// appropriately (write lock unless stated). The actor engine never takes mu
// at all, because a single goroutine owns the shard — it calls the same
// Locked methods, which is exactly why they are factored this way.
type shard struct {
	mu sync.RWMutex

	entries map[string]*Entry

	// slots holds every entry in the shard in an arbitrary order, so that a
	// uniformly random entry can be picked in O(1). Go maps deliberately
	// randomise iteration *start*, but reaching the k'th element is still
	// O(k), which is too slow for eviction sampling in the write path.
	//
	// Removal is swap-with-last, which is why Entry caches its own index.
	slots []*Entry

	// ttlSlots holds only entries with ExpireAt != 0. The expiry sweeper
	// samples exclusively from here, so a keyspace where 1% of keys have
	// TTLs costs the sweeper 1% of the work a whole-keyspace scan would.
	ttlSlots []*Entry

	// Exact-LRU intrusive list (head = most recently used). Populated only
	// when exactLRU is set. Kept in the tree so the read-throughput collapse
	// it causes can be measured rather than asserted (DESIGN.md §10).
	lruHead, lruTail *Entry
	exactLRU         bool

	// Logical byte accounting. Mutated under mu, published as an atomic so
	// STATS can read it without taking a lock.
	memBytes atomic.Int64
	budget   int64 // per-shard slice of the global limit; 0 = unlimited
	lowWater int64

	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
	expired   atomic.Uint64

	rng    splitmix64
	policy evictPolicy

	sampleK  int
	batchMax int

	// onEvict is invoked (without mu held by the callee's expectation — see
	// callers) for keys removed by eviction or active expiry, so the engine
	// can log a DELETE to the WAL and propagate it to replicas. Expiry is
	// *not* deterministic across nodes, so replicas must be told explicitly
	// rather than expiring on their own (DESIGN.md §9, "The replication
	// trap").
	onEvict func(key []byte, reason EvictReason)
}

// EvictReason distinguishes why a key was removed by the store itself.
type EvictReason uint8

const (
	ReasonExpired EvictReason = iota
	ReasonEvicted
)

type evictPolicy uint8

const (
	policyAllKeysLRU evictPolicy = iota
	policyVolatileLRU
	policyNoEviction
)

func newShard(sampleK, batchMax int, seed uint64, exactLRU bool, policy evictPolicy) *shard {
	return &shard{
		entries:  make(map[string]*Entry),
		exactLRU: exactLRU,
		rng:      splitmix64{state: seed | 1},
		policy:   policy,
		sampleK:  sampleK,
		batchMax: batchMax,
	}
}

// --- slot bookkeeping ------------------------------------------------------

func (s *shard) addSlotLocked(e *Entry) {
	e.slot = int32(len(s.slots))
	s.slots = append(s.slots, e)
	if e.ExpireAt != 0 {
		e.ttlSlot = int32(len(s.ttlSlots))
		s.ttlSlots = append(s.ttlSlots, e)
	} else {
		e.ttlSlot = -1
	}
}

func (s *shard) removeSlotLocked(e *Entry) {
	if i := e.slot; i >= 0 && int(i) < len(s.slots) {
		last := len(s.slots) - 1
		s.slots[i] = s.slots[last]
		s.slots[i].slot = i
		s.slots[last] = nil
		s.slots = s.slots[:last]
		e.slot = -1
	}
	s.removeTTLSlotLocked(e)
}

func (s *shard) removeTTLSlotLocked(e *Entry) {
	if i := e.ttlSlot; i >= 0 && int(i) < len(s.ttlSlots) {
		last := len(s.ttlSlots) - 1
		s.ttlSlots[i] = s.ttlSlots[last]
		s.ttlSlots[i].ttlSlot = i
		s.ttlSlots[last] = nil
		s.ttlSlots = s.ttlSlots[:last]
	}
	e.ttlSlot = -1
}

func (s *shard) addTTLSlotLocked(e *Entry) {
	if e.ttlSlot >= 0 {
		return
	}
	e.ttlSlot = int32(len(s.ttlSlots))
	s.ttlSlots = append(s.ttlSlots, e)
}

// --- exact LRU list --------------------------------------------------------

func (s *shard) lruPushFrontLocked(e *Entry) {
	e.prev = nil
	e.next = s.lruHead
	if s.lruHead != nil {
		s.lruHead.prev = e
	}
	s.lruHead = e
	if s.lruTail == nil {
		s.lruTail = e
	}
}

func (s *shard) lruRemoveLocked(e *Entry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else if s.lruHead == e {
		s.lruHead = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else if s.lruTail == e {
		s.lruTail = e.prev
	}
	e.prev, e.next = nil, nil
}

func (s *shard) lruMoveToFrontLocked(e *Entry) {
	if s.lruHead == e {
		return
	}
	s.lruRemoveLocked(e)
	s.lruPushFrontLocked(e)
}

// --- core operations -------------------------------------------------------

// getLocked looks up a key. The caller holds at least a read lock.
//
// Expired entries are reported as missing but are NOT deleted here: deletion
// needs the write lock, and upgrading mid-read is both impossible with
// sync.RWMutex and a good way to build a deadlock. The caller gets
// needsReap=true and performs the removal on a separate write-locked pass.
// That is lazy expiration (DESIGN.md §9.1).
func (s *shard) getLocked(key string, nowMs, nowMono uint64) (e *Entry, ok, needsReap bool) {
	e, ok = s.entries[key]
	if !ok {
		s.misses.Add(1)
		return nil, false, false
	}
	if e.Expired(nowMs) {
		s.misses.Add(1)
		return nil, false, true
	}
	e.touch(nowMono)
	s.hits.Add(1)
	return e, true, false
}

// setLocked inserts or overwrites. Key and value are copied.
//
// Returns false if the write could not be admitted under the memory policy,
// which the caller reports as status OOM.
func (s *shard) setLocked(key string, value []byte, expireAt, nowMono uint64) bool {
	cost := EntryCost(len(key), len(value))

	if old, exists := s.entries[key]; exists {
		// Overwrite in place. Accounting is a delta, not a remove-then-add,
		// so the counter never dips and eviction is never triggered
		// spuriously by replacing a value with a same-sized one.
		delta := cost - old.Size()
		if !s.admitLocked(delta, key) {
			return false
		}
		old.Value = append(old.Value[:0], value...)
		hadTTL := old.ExpireAt != 0
		old.ExpireAt = expireAt
		old.size = uint32(cost)
		old.touch(nowMono)
		switch {
		case expireAt != 0 && !hadTTL:
			s.addTTLSlotLocked(old)
		case expireAt == 0 && hadTTL:
			s.removeTTLSlotLocked(old)
		}
		if s.exactLRU {
			s.lruMoveToFrontLocked(old)
		}
		s.memBytes.Add(delta)
		return true
	}

	if !s.admitLocked(cost, key) {
		return false
	}
	e := &Entry{
		Key:      append([]byte(nil), key...),
		Value:    append([]byte(nil), value...),
		ExpireAt: expireAt,
		size:     uint32(cost),
		slot:     -1,
		ttlSlot:  -1,
	}
	e.touch(nowMono)
	s.entries[key] = e
	s.addSlotLocked(e)
	if s.exactLRU {
		s.lruPushFrontLocked(e)
	}
	s.memBytes.Add(cost)
	return true
}

// admitLocked enforces the memory limit for a write that would add delta
// bytes, evicting if the policy allows it.
func (s *shard) admitLocked(delta int64, protect string) bool {
	if s.budget <= 0 || delta <= 0 {
		return true
	}
	if s.memBytes.Load()+delta <= s.budget {
		return true
	}
	if s.policy == policyNoEviction {
		return false
	}
	// Evict down to the low-water mark, leaving room for this write.
	// Bounded per call (batchMax) so a single SET cannot turn into an
	// eviction storm that stalls the connection for seconds.
	target := s.lowWater - delta
	if target < 0 {
		target = 0
	}
	s.evictLocked(target, protect)
	return s.memBytes.Load()+delta <= s.budget
}

// evictLocked frees entries until memBytes <= target or the batch limit is
// hit or nothing is evictable.
//
// Policy: sampled LRU. Pick K entries at random, evict the one with the
// oldest lastAccess. K=5 lands within a few percent of exact LRU; K=10 is
// very close. The alternative — an exact intrusive list — forces every read
// to take a write lock, which is a far larger cost than the approximation
// error. See docs/DECISIONS.md ADR-006.
func (s *shard) evictLocked(target int64, protect string) int {
	if s.policy == policyNoEviction {
		return 0
	}
	n := 0
	for s.memBytes.Load() > target && n < s.batchMax {
		victim := s.pickVictimLocked(protect)
		if victim == nil {
			break
		}
		key := string(victim.Key)
		s.removeEntryLocked(victim)
		s.evictions.Add(1)
		n++
		if s.onEvict != nil {
			s.onEvict([]byte(key), ReasonEvicted)
		}
	}
	return n
}

// pickVictimLocked implements the sampling. With --exact-lru it returns the
// true tail of the intrusive list instead, so the two policies can be
// compared on identical workloads.
func (s *shard) pickVictimLocked(protect string) *Entry {
	if s.exactLRU {
		for e := s.lruTail; e != nil; e = e.prev {
			if string(e.Key) == protect {
				continue
			}
			if s.policy == policyVolatileLRU && e.ExpireAt == 0 {
				continue
			}
			return e
		}
		return nil
	}

	pool := s.slots
	if s.policy == policyVolatileLRU {
		pool = s.ttlSlots
	}
	if len(pool) == 0 {
		return nil
	}
	var best *Entry
	var bestAccess uint64
	for i := 0; i < s.sampleK; i++ {
		cand := pool[s.rng.intn(len(pool))]
		if cand == nil || string(cand.Key) == protect {
			continue
		}
		if a := cand.LastAccess(); best == nil || a < bestAccess {
			best, bestAccess = cand, a
		}
	}
	if best == nil && len(pool) > 0 {
		// Every sample was the protected key. Fall back to a linear probe so
		// that a one-key shard under pressure still makes progress.
		for _, cand := range pool {
			if cand != nil && string(cand.Key) != protect {
				return cand
			}
		}
	}
	return best
}

// removeEntryLocked deletes an entry and all of its index memberships.
func (s *shard) removeEntryLocked(e *Entry) {
	delete(s.entries, string(e.Key))
	s.removeSlotLocked(e)
	if s.exactLRU {
		s.lruRemoveLocked(e)
	}
	s.memBytes.Add(-e.Size())
}

func (s *shard) delLocked(key string) bool {
	e, ok := s.entries[key]
	if !ok {
		return false
	}
	s.removeEntryLocked(e)
	return true
}

// expireLocked sets a new absolute deadline on an existing key.
func (s *shard) expireLocked(key string, expireAt uint64) bool {
	e, ok := s.entries[key]
	if !ok {
		return false
	}
	hadTTL := e.ExpireAt != 0
	e.ExpireAt = expireAt
	switch {
	case expireAt != 0 && !hadTTL:
		s.addTTLSlotLocked(e)
	case expireAt == 0 && hadTTL:
		s.removeTTLSlotLocked(e)
	}
	return true
}

// sweepLocked performs one sampled expiry round over this shard and returns
// (sampled, expired). It is deliberately tiny: the caller loops, releasing
// the lock between rounds, so the shard is never locked for a whole scan.
func (s *shard) sweepLocked(nowMs uint64, sample int) (int, int) {
	if len(s.ttlSlots) == 0 {
		return 0, 0
	}
	if sample > len(s.ttlSlots) {
		sample = len(s.ttlSlots)
	}
	victims := make([]*Entry, 0, sample)
	for i := 0; i < sample; i++ {
		e := s.ttlSlots[s.rng.intn(len(s.ttlSlots))]
		if e != nil && e.Expired(nowMs) {
			victims = append(victims, e)
		}
	}
	for _, e := range victims {
		// A key can be sampled twice in one round; slot is set to -1 on
		// removal, so this check makes the second removal a no-op.
		if e.slot < 0 {
			continue
		}
		key := append([]byte(nil), e.Key...)
		s.removeEntryLocked(e)
		s.expired.Add(1)
		if s.onEvict != nil {
			s.onEvict(key, ReasonExpired)
		}
	}
	return sample, len(victims)
}

// reapLocked removes a specific key if (and only if) it is still expired.
// Used by the lazy path after a read observed an expired entry.
func (s *shard) reapLocked(key string, nowMs uint64) bool {
	e, ok := s.entries[key]
	if !ok || !e.Expired(nowMs) {
		return false
	}
	k := append([]byte(nil), e.Key...)
	s.removeEntryLocked(e)
	s.expired.Add(1)
	if s.onEvict != nil {
		s.onEvict(k, ReasonExpired)
	}
	return true
}

func (s *shard) flushLocked() {
	s.entries = make(map[string]*Entry)
	s.slots = s.slots[:0]
	s.ttlSlots = s.ttlSlots[:0]
	s.lruHead, s.lruTail = nil, nil
	s.memBytes.Store(0)
}
