package store

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raqueeb/kvstore/internal/config"
)

// ErrOOM is returned by Set when the write cannot be admitted under the
// configured memory limit and policy.
var ErrOOM = errors.New("out of memory: write would exceed max-memory under the current policy")

// Clock separates wall-clock time (used for absolute expiry deadlines, which
// must survive a restart) from monotonic time (used for access recency and
// every interval measurement). Tests inject a fake so TTL behaviour can be
// verified without sleeping.
type Clock interface {
	// NowMillis returns absolute wall-clock milliseconds since the epoch.
	NowMillis() uint64
	// MonoMillis returns monotonic milliseconds since the store started.
	MonoMillis() uint64
}

type realClock struct{ start time.Time }

// NewRealClock returns the production clock.
func NewRealClock() Clock { return &realClock{start: time.Now()} }

func (c *realClock) NowMillis() uint64  { return uint64(time.Now().UnixMilli()) }
func (c *realClock) MonoMillis() uint64 { return uint64(time.Since(c.start).Milliseconds()) }

// Options configures a Store.
type Options struct {
	Engine       config.StoreEngine
	Shards       int
	Seed         uint64
	MaxMemory    int64
	LowWater     int64
	Policy       config.EvictionPolicy
	ExactLRU     bool
	EvictSampleK int
	EvictBatch   int
	ActorQueue   int
	Clock        Clock
}

func (o *Options) withDefaults() {
	if o.Engine == "" {
		o.Engine = config.EngineSharded
	}
	if o.Engine == config.EngineGlobal {
		// The baseline is by definition a single lock over a single map.
		o.Shards = 1
	}
	if o.Shards <= 0 {
		o.Shards = 16
	}
	if o.Seed == 0 {
		o.Seed = NewSeed()
	}
	if o.EvictSampleK <= 0 {
		o.EvictSampleK = 5
	}
	if o.EvictBatch <= 0 {
		o.EvictBatch = 200
	}
	if o.ActorQueue <= 0 {
		o.ActorQueue = 1024
	}
	if o.Policy == "" {
		o.Policy = config.EvictAllKeysLRU
	}
	if o.Clock == nil {
		o.Clock = NewRealClock()
	}
	if o.MaxMemory > 0 && o.LowWater <= 0 {
		o.LowWater = int64(float64(o.MaxMemory) * 0.95)
	}
}

// Store is the public storage engine.
//
// The three engines share one shard implementation and differ only in how
// access is serialised:
//
//	sharded — RWMutex per shard. The default. Near-linear scaling for
//	          uniform keys; degrades under skew, which is measurable.
//	global  — one shard, one exclusive mutex, reads included. The baseline
//	          the sharded numbers are compared against.
//	actor   — one goroutine owns each shard and receives closures over a
//	          bounded channel. No locks at all, perfect per-shard ordering,
//	          at the cost of a channel round trip per operation.
//
// Keeping all three in the tree is what makes the Milestone 12 comparison a
// measurement rather than an opinion.
type Store struct {
	opts   Options
	shards []*shard
	mask   uint64
	seed   uint64
	clock  Clock

	mailboxes []chan func()
	actorWG   sync.WaitGroup
	closed    atomic.Bool

	cb atomic.Pointer[callbacks]
}

// callbacks are swapped atomically rather than guarded by a mutex: they are
// read on every mutation, and putting a shared RWMutex in the write path
// would reintroduce exactly the contention sharding exists to avoid.
type callbacks struct {
	onEvict func(key []byte, reason EvictReason)
	onTTL   func(key []byte, expireAt uint64)
}

func (s *Store) load() callbacks {
	if c := s.cb.Load(); c != nil {
		return *c
	}
	return callbacks{}
}

func (s *Store) update(mutate func(*callbacks)) {
	for {
		old := s.cb.Load()
		next := callbacks{}
		if old != nil {
			next = *old
		}
		mutate(&next)
		if s.cb.CompareAndSwap(old, &next) {
			return
		}
	}
}

// New builds a store.
func New(opts Options) *Store {
	opts.withDefaults()

	policy := policyAllKeysLRU
	switch opts.Policy {
	case config.EvictVolatile:
		policy = policyVolatileLRU
	case config.EvictNone:
		policy = policyNoEviction
	}

	s := &Store{
		opts:  opts,
		mask:  uint64(opts.Shards - 1),
		seed:  opts.Seed,
		clock: opts.Clock,
	}
	s.shards = make([]*shard, opts.Shards)
	for i := range s.shards {
		sh := newShard(opts.EvictSampleK, opts.EvictBatch, opts.Seed+uint64(i)*0x9E3779B97F4A7C15, opts.ExactLRU, policy)
		// The global limit is divided evenly across shards. A single global
		// atomic counter would be simpler to reason about but would put a
		// contended cache line in the write path of every shard, which is
		// exactly the contention sharding exists to remove. The cost of the
		// per-shard budget is that a badly skewed keyspace can evict from a
		// hot shard while cold shards are under budget; that is measured in
		// docs/BENCHMARKS.md and noted as a known limitation.
		if opts.MaxMemory > 0 {
			sh.budget = opts.MaxMemory / int64(opts.Shards)
			sh.lowWater = opts.LowWater / int64(opts.Shards)
			if sh.budget < 1 {
				sh.budget = 1
			}
		}
		sh.onEvict = s.fireEvict
		s.shards[i] = sh
	}

	if opts.Engine == config.EngineActor {
		s.mailboxes = make([]chan func(), opts.Shards)
		for i := range s.mailboxes {
			s.mailboxes[i] = make(chan func(), opts.ActorQueue)
			s.actorWG.Add(1)
			go s.runActor(s.mailboxes[i])
		}
	}
	return s
}

func (s *Store) runActor(mailbox chan func()) {
	defer s.actorWG.Done()
	for fn := range mailbox {
		fn()
	}
}

// SetOnEvict registers a callback fired when the store itself removes a key
// (active expiry or eviction). The engine uses it to append a DELETE to the
// WAL and to push an explicit delete to replicas — replicas must never
// expire keys on their own, because `now` differs between nodes.
//
// The callback is invoked with the shard lock held, so it must not block or
// perform I/O. The engine's implementation is a non-blocking queue push.
func (s *Store) SetOnEvict(fn func(key []byte, reason EvictReason)) {
	s.update(func(c *callbacks) { c.onEvict = fn })
}

// SetOnTTL registers a callback invoked whenever a key gains an absolute
// expiry deadline. The timing-wheel expirer uses it to schedule a timer;
// with the sampled sweeper it stays nil and costs one atomic load.
func (s *Store) SetOnTTL(fn func(key []byte, expireAt uint64)) {
	s.update(func(c *callbacks) { c.onTTL = fn })
}

func (s *Store) fireEvict(key []byte, reason EvictReason) {
	if fn := s.load().onEvict; fn != nil {
		fn(key, reason)
	}
}

func (s *Store) fireTTL(key []byte, expireAt uint64) {
	if expireAt == 0 {
		return
	}
	if fn := s.load().onTTL; fn != nil {
		fn(key, expireAt)
	}
}

// Clock exposes the store's clock so the engine and sweeper agree on time.
func (s *Store) Clock() Clock { return s.clock }

// Seed returns the hash seed in use (reported in STATS).
func (s *Store) Seed() uint64 { return s.seed }

// ShardCount returns the number of shards.
func (s *Store) ShardCount() int { return len(s.shards) }

// Engine returns the configured engine name.
func (s *Store) Engine() config.StoreEngine { return s.opts.Engine }

func (s *Store) shardFor(key []byte) (int, *shard) {
	if len(s.shards) == 1 {
		return 0, s.shards[0]
	}
	idx := int(Hash64(s.seed, key) & s.mask)
	return idx, s.shards[idx]
}

// ShardIndex exposes routing for the distribution test.
func (s *Store) ShardIndex(key []byte) int {
	i, _ := s.shardFor(key)
	return i
}

// withWrite runs fn with exclusive access to shard i.
func (s *Store) withWrite(i int, fn func(*shard)) {
	sh := s.shards[i]
	switch s.opts.Engine {
	case config.EngineActor:
		done := make(chan struct{})
		s.mailboxes[i] <- func() { fn(sh); close(done) }
		<-done
	default:
		// Both sharded and global take the exclusive lock for writes; they
		// differ on the read path.
		sh.mu.Lock()
		fn(sh)
		sh.mu.Unlock()
	}
}

// withRead runs fn with shared access to shard i.
func (s *Store) withRead(i int, fn func(*shard)) {
	sh := s.shards[i]
	switch s.opts.Engine {
	case config.EngineActor:
		done := make(chan struct{})
		s.mailboxes[i] <- func() { fn(sh); close(done) }
		<-done
	case config.EngineGlobal:
		// The baseline serialises reads too. That is the point of it.
		sh.mu.Lock()
		fn(sh)
		sh.mu.Unlock()
	default:
		sh.mu.RLock()
		fn(sh)
		sh.mu.RUnlock()
	}
}

// --- public operations -----------------------------------------------------

// Get returns a copy of the value for key.
//
// The returned slice is a fresh copy rather than a view into the entry. That
// costs an allocation per hit, and it is not negotiable: handing out a
// reference to a live entry means a concurrent SET can mutate the bytes a
// reader is in the middle of writing to a socket.
func (s *Store) Get(key []byte) ([]byte, bool) {
	nowMs, nowMono := s.clock.NowMillis(), s.clock.MonoMillis()
	i, _ := s.shardFor(key)
	k := string(key)

	var out []byte
	var ok, reap bool
	s.withRead(i, func(sh *shard) {
		e, found, needsReap := sh.getLocked(k, nowMs, nowMono)
		ok, reap = found, needsReap
		if found {
			out = append([]byte(nil), e.Value...)
		}
	})
	if reap {
		// Lazy expiration: the read saw a dead entry, so reclaim it on a
		// separate write-locked pass.
		s.withWrite(i, func(sh *shard) { sh.reapLocked(k, nowMs) })
	}
	return out, ok
}

// Set stores key=value with an absolute expiry deadline in wall-clock
// milliseconds (0 = no expiry).
func (s *Store) Set(key, value []byte, expireAt uint64) error {
	nowMono := s.clock.MonoMillis()
	i, _ := s.shardFor(key)
	k := string(key)

	var ok bool
	s.withWrite(i, func(sh *shard) { ok = sh.setLocked(k, value, expireAt, nowMono) })
	if !ok {
		return ErrOOM
	}
	// Registered outside the shard lock: the timing wheel has its own lock
	// and taking it under a shard lock would create a two-lock ordering we
	// would then have to maintain everywhere.
	s.fireTTL(key, expireAt)
	return nil
}

// Delete removes a key and reports whether it existed.
func (s *Store) Delete(key []byte) bool {
	nowMs := s.clock.NowMillis()
	i, _ := s.shardFor(key)
	k := string(key)
	var existed bool
	s.withWrite(i, func(sh *shard) {
		if e, ok := sh.entries[k]; ok && e.Expired(nowMs) {
			// Deleting an already-expired key reports "did not exist", which
			// is what a client that cannot see expired keys must observe.
			sh.removeEntryLocked(e)
			existed = false
			return
		}
		existed = sh.delLocked(k)
	})
	return existed
}

// Exists reports whether a live key is present.
func (s *Store) Exists(key []byte) bool {
	nowMs, nowMono := s.clock.NowMillis(), s.clock.MonoMillis()
	i, _ := s.shardFor(key)
	k := string(key)
	var ok, reap bool
	s.withRead(i, func(sh *shard) {
		_, found, needsReap := sh.getLocked(k, nowMs, nowMono)
		ok, reap = found, needsReap
	})
	if reap {
		s.withWrite(i, func(sh *shard) { sh.reapLocked(k, nowMs) })
	}
	return ok
}

// Expire sets an absolute deadline on an existing key. expireAt of 0 clears
// the TTL. Returns false if the key does not exist or is already expired.
func (s *Store) Expire(key []byte, expireAt uint64) bool {
	nowMs := s.clock.NowMillis()
	i, _ := s.shardFor(key)
	k := string(key)
	var ok bool
	s.withWrite(i, func(sh *shard) {
		if e, exists := sh.entries[k]; exists && e.Expired(nowMs) {
			sh.reapLocked(k, nowMs)
			ok = false
			return
		}
		ok = sh.expireLocked(k, expireAt)
	})
	if ok {
		s.fireTTL(key, expireAt)
	}
	return ok
}

// TTL returns the remaining lifetime in milliseconds.
// Returns (-1, true) for a key with no expiry, and (0, false) if missing.
func (s *Store) TTL(key []byte) (int64, bool) {
	nowMs, nowMono := s.clock.NowMillis(), s.clock.MonoMillis()
	i, _ := s.shardFor(key)
	k := string(key)
	var ms int64
	var ok, reap bool
	s.withRead(i, func(sh *shard) {
		e, found, needsReap := sh.getLocked(k, nowMs, nowMono)
		ok, reap = found, needsReap
		if found {
			if e.ExpireAt == 0 {
				ms = -1
			} else {
				ms = int64(e.ExpireAt) - int64(nowMs)
				if ms < 0 {
					ms = 0
				}
			}
		}
	})
	if reap {
		s.withWrite(i, func(sh *shard) { sh.reapLocked(k, nowMs) })
	}
	return ms, ok
}

// Keys returns up to limit live keys matching prefix. It is a debugging and
// inspection aid, not a hot path: it scans, and it says so.
func (s *Store) Keys(prefix []byte, limit int) [][]byte {
	if limit <= 0 {
		limit = 100
	}
	nowMs := s.clock.NowMillis()
	out := make([][]byte, 0, min(limit, 1024))
	for i := range s.shards {
		if len(out) >= limit {
			break
		}
		s.withRead(i, func(sh *shard) {
			for _, e := range sh.slots {
				if len(out) >= limit {
					return
				}
				if e == nil || e.Expired(nowMs) {
					continue
				}
				if len(prefix) > 0 && !hasPrefix(e.Key, prefix) {
					continue
				}
				out = append(out, append([]byte(nil), e.Key...))
			}
		})
	}
	return out
}

func hasPrefix(b, p []byte) bool {
	if len(b) < len(p) {
		return false
	}
	for i := range p {
		if b[i] != p[i] {
			return false
		}
	}
	return true
}

// Flush removes every key.
func (s *Store) Flush() {
	for i := range s.shards {
		s.withWrite(i, func(sh *shard) { sh.flushLocked() })
	}
}

// Len returns the number of entries, including any that are expired but not
// yet reaped.
func (s *Store) Len() int {
	n := 0
	for i := range s.shards {
		s.withRead(i, func(sh *shard) { n += len(sh.entries) })
	}
	return n
}

// MemoryBytes returns the logical accounted size of the keyspace.
func (s *Store) MemoryBytes() int64 {
	var n int64
	for _, sh := range s.shards {
		n += sh.memBytes.Load()
	}
	return n
}

// Range iterates every live entry, calling fn with a private copy.
//
// Entries are copied out in bounded batches and fn is invoked with no shard
// lock held. This is the rule from DESIGN.md §6: the snapshot writer and the
// replication stream both consume Range and both do I/O, and holding a shard
// lock across a disk write is how you build a lock convoy.
func (s *Store) Range(fn func(*Entry) bool) {
	const batchSize = 256
	nowMs := s.clock.NowMillis()
	for i := range s.shards {
		offset := 0
		for {
			batch := make([]*Entry, 0, batchSize)
			var total int
			s.withRead(i, func(sh *shard) {
				total = len(sh.slots)
				for j := offset; j < total && len(batch) < batchSize; j++ {
					e := sh.slots[j]
					if e == nil || e.Expired(nowMs) {
						continue
					}
					batch = append(batch, e.Clone())
				}
			})
			scanned := min(batchSize, total-offset)
			if scanned <= 0 {
				break
			}
			offset += scanned
			for _, e := range batch {
				if !fn(e) {
					return
				}
			}
			if offset >= total {
				break
			}
		}
	}
}

// Close shuts down actor goroutines. It is idempotent.
func (s *Store) Close() {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	if s.opts.Engine == config.EngineActor {
		for _, mb := range s.mailboxes {
			close(mb)
		}
		s.actorWG.Wait()
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Txn is exclusive access to the one shard that owns a key. It exists so the
// engine can reserve a WAL LSN and update memory as a single atomic step
// with respect to other writers of the same key.
//
// That atomicity is required for correctness, not tidiness. If the LSN were
// assigned outside the lock, two concurrent SETs to one key could be written
// to the log in one order and applied to memory in the other; replay would
// then produce a different value than the live store had. Holding the shard
// lock across both steps makes log order and memory order agree per key.
//
// A Txn must not block or perform I/O. The WAL submit it wraps is a
// guaranteed-non-blocking channel send, because the engine holds a
// backpressure slot before it ever takes the lock. See DESIGN.md §7.
type Txn struct {
	sh      *shard
	nowMs   uint64
	nowMono uint64
}

// Set inserts or overwrites, returning false if the memory policy rejects it.
func (t Txn) Set(key, value []byte, expireAt uint64) bool {
	return t.sh.setLocked(string(key), value, expireAt, t.nowMono)
}

// Delete removes a key, reporting whether a live key was present.
func (t Txn) Delete(key []byte) bool {
	k := string(key)
	if e, ok := t.sh.entries[k]; ok && e.Expired(t.nowMs) {
		t.sh.removeEntryLocked(e)
		return false
	}
	return t.sh.delLocked(k)
}

// Expire sets an absolute deadline on an existing live key.
func (t Txn) Expire(key []byte, expireAt uint64) bool {
	k := string(key)
	if e, ok := t.sh.entries[k]; ok && e.Expired(t.nowMs) {
		t.sh.reapLocked(k, t.nowMs)
		return false
	}
	return t.sh.expireLocked(k, expireAt)
}

// Exists reports whether a live key is present, without counting a hit.
func (t Txn) Exists(key []byte) bool {
	e, ok := t.sh.entries[string(key)]
	return ok && !e.Expired(t.nowMs)
}

// WithKey runs fn with exclusive access to the shard owning key.
func (s *Store) WithKey(key []byte, fn func(Txn)) {
	nowMs, nowMono := s.clock.NowMillis(), s.clock.MonoMillis()
	i, _ := s.shardFor(key)
	s.withWrite(i, func(sh *shard) {
		fn(Txn{sh: sh, nowMs: nowMs, nowMono: nowMono})
	})
}
