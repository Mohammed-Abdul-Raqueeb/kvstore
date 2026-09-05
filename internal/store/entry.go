package store

import (
	"sync/atomic"
	"unsafe"
)

// Entry is one stored key/value pair plus its metadata.
//
// Field notes:
//
//   - Key and Value are owned copies. The server hands us sub-slices of a
//     reusable read buffer; storing those directly is mistake #7 in
//     DESIGN.md §15 ("the next read silently rewrites your stored value").
//     Copying on insert is the fix, and it is done in exactly one place:
//     shard.set.
//
//   - ExpireAt is absolute wall-clock milliseconds, 0 meaning "never". It is
//     absolute because it has to survive a restart and still mean the same
//     instant (DESIGN.md §9, "Clock discipline"). Everything that measures
//     an *interval* — sweeper cadence, deadlines, benchmarks — uses the
//     monotonic clock instead.
//
//   - lastAccess is a monotonic millisecond stamp updated with an atomic
//     store on every read. Atomic, not lock-protected, because a GET holds
//     only a read lock and upgrading to a write lock just to record an
//     access timestamp would serialise all reads. This is the mechanism that
//     makes sampled LRU cheap and exact LRU expensive.
type Entry struct {
	Key      []byte
	Value    []byte
	ExpireAt uint64

	lastAccess atomic.Uint64

	// size caches the accounted cost so eviction never has to recompute it.
	size uint32

	// slot is this entry's index in shard.slots, the flat slice that makes
	// uniform random sampling O(1). -1 when not present.
	slot int32

	// ttlSlot is the index in shard.ttlSlots, which holds only entries that
	// have a TTL. The expiry sweeper samples from this slice, so a keyspace
	// with few TTLs costs the sweeper almost nothing. -1 when not present.
	ttlSlot int32

	// Intrusive doubly-linked list pointers, used only when the store is
	// configured with --exact-lru. They are nil otherwise, and the whole
	// point of the default configuration is that they stay nil.
	prev, next *Entry
}

// entryStructSize is the fixed per-Entry overhead in bytes: the struct
// itself, not counting the bytes its slices point at.
const entryStructSize = int(unsafe.Sizeof(Entry{}))

// mapOverhead is the measured per-key cost of Go's map implementation: the
// bucket slot, the key header, the pointer, and the amortised cost of the
// bucket array being at most ~81% full before it doubles.
//
// This is an estimate. DESIGN.md §10 is explicit that being off by a known
// margin is fine and being off unknowingly is not, so
// TestMemoryAccountingTracksRSS in memory_test.go calibrates the counter
// against real RSS and the ratio is reported in docs/BENCHMARKS.md.
const mapOverhead = 64

// EntryCost returns the accounted byte cost of a key/value pair.
//
//	cost = key_len + value_len + sizeof(Entry) + MAP_OVERHEAD
func EntryCost(keyLen, valueLen int) int64 {
	return int64(keyLen) + int64(valueLen) + int64(entryStructSize) + mapOverhead
}

// Size returns the accounted cost of this entry.
func (e *Entry) Size() int64 { return int64(e.size) }

// Expired reports whether the entry has an expiry that has passed at nowMs.
func (e *Entry) Expired(nowMs uint64) bool {
	return e.ExpireAt != 0 && e.ExpireAt <= nowMs
}

// touch records an access at monotonic time nowMono. Safe under a read lock.
func (e *Entry) touch(nowMono uint64) { e.lastAccess.Store(nowMono) }

// LastAccess returns the monotonic millisecond stamp of the last read.
func (e *Entry) LastAccess() uint64 { return e.lastAccess.Load() }

// Clone returns a deep copy, used when handing entries to a snapshot writer
// or a replication stream without holding the shard lock across I/O.
func (e *Entry) Clone() *Entry {
	c := &Entry{
		Key:      append([]byte(nil), e.Key...),
		Value:    append([]byte(nil), e.Value...),
		ExpireAt: e.ExpireAt,
		size:     e.size,
		slot:     -1,
		ttlSlot:  -1,
	}
	c.lastAccess.Store(e.lastAccess.Load())
	return c
}
