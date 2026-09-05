package store

import (
	"runtime"

	"github.com/raqueeb/kvstore/internal/config"
)

// Stats is a point-in-time view of the store, surfaced by the STATS command.
type Stats struct {
	Engine      config.StoreEngine `json:"engine"`
	Shards      int                `json:"shards"`
	HashSeed    uint64             `json:"hash_seed"`
	Keys        int                `json:"keys"`
	KeysWithTTL int                `json:"keys_with_ttl"`

	// LogicalBytes is our own accounting: sum of EntryCost over live entries.
	LogicalBytes int64 `json:"logical_bytes"`
	MaxMemory    int64 `json:"max_memory"`
	LowWater     int64 `json:"low_water"`

	// RSSBytes is the real resident set size from the OS, and HeapAlloc is
	// what the Go runtime thinks it has allocated. Both are exposed
	// alongside LogicalBytes precisely so the gap is visible at runtime:
	// allocator fragmentation, GC headroom, and per-object rounding all live
	// in that gap. DESIGN.md §10 asks for the ratio to be known rather than
	// assumed; see TestMemoryAccountingCalibration.
	RSSBytes       int64 `json:"rss_bytes"`
	HeapAllocBytes int64 `json:"heap_alloc_bytes"`
	HeapSysBytes   int64 `json:"heap_sys_bytes"`

	Hits      uint64 `json:"hits"`
	Misses    uint64 `json:"misses"`
	Evictions uint64 `json:"evictions"`
	Expired   uint64 `json:"expired"`

	// Per-shard breakdown, which is how key-distribution skew becomes
	// visible instead of being averaged away.
	ShardKeys  []int   `json:"shard_keys"`
	ShardBytes []int64 `json:"shard_bytes"`
}

// AccountingRatio returns RSS / LogicalBytes, the calibration number.
// Returns 0 when either side is unavailable.
func (s Stats) AccountingRatio() float64 {
	if s.LogicalBytes <= 0 || s.RSSBytes <= 0 {
		return 0
	}
	return float64(s.RSSBytes) / float64(s.LogicalBytes)
}

// Stats collects a snapshot of store counters.
func (s *Store) Stats() Stats {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	st := Stats{
		Engine:         s.opts.Engine,
		Shards:         len(s.shards),
		HashSeed:       s.seed,
		MaxMemory:      s.opts.MaxMemory,
		LowWater:       s.opts.LowWater,
		RSSBytes:       ResidentBytes(),
		HeapAllocBytes: int64(ms.HeapAlloc),
		HeapSysBytes:   int64(ms.HeapSys),
		ShardKeys:      make([]int, len(s.shards)),
		ShardBytes:     make([]int64, len(s.shards)),
	}

	for i := range s.shards {
		sh := s.shards[i]
		s.withRead(i, func(sh *shard) {
			st.ShardKeys[i] = len(sh.entries)
			st.Keys += len(sh.entries)
			st.KeysWithTTL += len(sh.ttlSlots)
		})
		b := sh.memBytes.Load()
		st.ShardBytes[i] = b
		st.LogicalBytes += b
		st.Hits += sh.hits.Load()
		st.Misses += sh.misses.Load()
		st.Evictions += sh.evictions.Load()
		st.Expired += sh.expired.Load()
	}
	return st
}

// SetMemoryLimit changes the limit at runtime, redistributing the per-shard
// budget. Used by tests and by kvbench when sweeping the limit dimension.
func (s *Store) SetMemoryLimit(maxMemory int64, lowWaterRatio float64) {
	s.opts.MaxMemory = maxMemory
	s.opts.LowWater = int64(float64(maxMemory) * lowWaterRatio)
	n := int64(len(s.shards))
	for i := range s.shards {
		sh := s.shards[i]
		s.withWrite(i, func(sh *shard) {
			if maxMemory <= 0 {
				sh.budget, sh.lowWater = 0, 0
				return
			}
			sh.budget = maxMemory / n
			sh.lowWater = s.opts.LowWater / n
			if sh.budget < 1 {
				sh.budget = 1
			}
		})
		_ = sh
	}
}

// EvictToLimit forces eviction across all shards down to the low-water mark.
// The background controller calls this; it is also how tests assert that
// memory stabilises.
func (s *Store) EvictToLimit() int {
	total := 0
	for i := range s.shards {
		s.withWrite(i, func(sh *shard) {
			if sh.budget <= 0 || sh.memBytes.Load() <= sh.budget {
				return
			}
			total += sh.evictLocked(sh.lowWater, "")
		})
	}
	return total
}
