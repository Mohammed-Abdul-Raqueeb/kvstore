package store

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/raqueeb/kvstore/internal/config"
)

func allEngines() []config.StoreEngine {
	return []config.StoreEngine{config.EngineSharded, config.EngineGlobal, config.EngineActor}
}

func newTestStore(t *testing.T, engine config.StoreEngine, mutate func(*Options)) (*Store, *fakeClock) {
	t.Helper()
	clk := newFakeClock(1_000_000)
	opts := Options{Engine: engine, Shards: 8, Seed: 0xABCDEF, Clock: clk}
	if mutate != nil {
		mutate(&opts)
	}
	s := New(opts)
	t.Cleanup(s.Close)
	return s, clk
}

func TestBasicOperationsAcrossEngines(t *testing.T) {
	for _, eng := range allEngines() {
		t.Run(string(eng), func(t *testing.T) {
			s, _ := newTestStore(t, eng, nil)

			if _, ok := s.Get([]byte("missing")); ok {
				t.Fatal("empty store returned a value")
			}
			if err := s.Set([]byte("a"), []byte("1"), 0); err != nil {
				t.Fatal(err)
			}
			v, ok := s.Get([]byte("a"))
			if !ok || string(v) != "1" {
				t.Fatalf("Get = %q, %v", v, ok)
			}
			if !s.Exists([]byte("a")) {
				t.Fatal("Exists false for present key")
			}
			// Overwrite.
			if err := s.Set([]byte("a"), []byte("22"), 0); err != nil {
				t.Fatal(err)
			}
			v, _ = s.Get([]byte("a"))
			if string(v) != "22" {
				t.Fatalf("overwrite failed: %q", v)
			}
			if !s.Delete([]byte("a")) {
				t.Fatal("Delete returned false for present key")
			}
			if s.Delete([]byte("a")) {
				t.Fatal("second Delete returned true")
			}
			if s.Len() != 0 {
				t.Fatalf("Len = %d, want 0", s.Len())
			}
		})
	}
}

func TestKeysAreArbitraryBytes(t *testing.T) {
	s, _ := newTestStore(t, config.EngineSharded, nil)
	keys := [][]byte{
		{}, {0x00}, {0x00, 0x00}, []byte("with\nnewline"),
		{0xFF, 0xFE, 0xFD}, // invalid UTF-8
		[]byte("with\x00embedded\x00nuls"),
	}
	for i, k := range keys {
		val := []byte(fmt.Sprintf("v%d", i))
		if err := s.Set(k, val, 0); err != nil {
			t.Fatalf("key %x: %v", k, err)
		}
	}
	for i, k := range keys {
		v, ok := s.Get(k)
		if !ok || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("key %x: got %q ok=%v", k, v, ok)
		}
	}
	if s.Len() != len(keys) {
		t.Fatalf("Len = %d, want %d", s.Len(), len(keys))
	}
}

func TestStoreCopiesKeyAndValue(t *testing.T) {
	// This is mistake #7 in DESIGN.md §15: the server hands the store
	// sub-slices of a reusable read buffer. If the store retains them, the
	// next read rewrites stored data.
	s, _ := newTestStore(t, config.EngineSharded, nil)
	buf := []byte("keyvalue")
	key, val := buf[0:3], buf[3:8]
	if err := s.Set(key, val, 0); err != nil {
		t.Fatal(err)
	}
	// Simulate the next socket read overwriting the buffer.
	copy(buf, "XXXXXXXX")

	got, ok := s.Get([]byte("key"))
	if !ok {
		t.Fatal("key vanished after the buffer was reused")
	}
	if string(got) != "value" {
		t.Fatalf("value was aliased to the read buffer: got %q", got)
	}
}

func TestGetReturnsCopyNotAlias(t *testing.T) {
	s, _ := newTestStore(t, config.EngineSharded, nil)
	_ = s.Set([]byte("k"), []byte("original"), 0)
	v, _ := s.Get([]byte("k"))
	copy(v, "MUTATED!")
	again, _ := s.Get([]byte("k"))
	if string(again) != "original" {
		t.Fatalf("Get handed out a live reference: %q", again)
	}
}

func TestTTLLazyExpiration(t *testing.T) {
	for _, eng := range allEngines() {
		t.Run(string(eng), func(t *testing.T) {
			s, clk := newTestStore(t, eng, nil)
			now := clk.NowMillis()

			_ = s.Set([]byte("soon"), []byte("v"), now+100)
			_ = s.Set([]byte("never"), []byte("v"), 0)

			ms, ok := s.TTL([]byte("soon"))
			if !ok || ms != 100 {
				t.Fatalf("TTL = %d, %v; want 100, true", ms, ok)
			}
			ms, ok = s.TTL([]byte("never"))
			if !ok || ms != -1 {
				t.Fatalf("TTL for no-expiry key = %d, %v; want -1, true", ms, ok)
			}

			clk.advance(99)
			if _, ok := s.Get([]byte("soon")); !ok {
				t.Fatal("key expired one millisecond early")
			}
			clk.advance(1) // now == expireAt, and expiry is inclusive
			if _, ok := s.Get([]byte("soon")); ok {
				t.Fatal("key did not expire at its deadline")
			}
			if s.Exists([]byte("soon")) {
				t.Fatal("Exists sees an expired key")
			}
			if _, ok := s.TTL([]byte("soon")); ok {
				t.Fatal("TTL sees an expired key")
			}
			// Lazy reap must actually reclaim it, not just hide it.
			if s.Len() != 1 {
				t.Fatalf("expired key was not reclaimed: Len = %d", s.Len())
			}
			if _, ok := s.Get([]byte("never")); !ok {
				t.Fatal("the key without a TTL was collateral damage")
			}
		})
	}
}

func TestExpireCommand(t *testing.T) {
	s, clk := newTestStore(t, config.EngineSharded, nil)
	now := clk.NowMillis()

	if s.Expire([]byte("nope"), now+1000) {
		t.Fatal("Expire on a missing key returned true")
	}
	_ = s.Set([]byte("k"), []byte("v"), 0)
	if !s.Expire([]byte("k"), now+50) {
		t.Fatal("Expire on a present key returned false")
	}
	if ms, _ := s.TTL([]byte("k")); ms != 50 {
		t.Fatalf("TTL after Expire = %d, want 50", ms)
	}
	// Clearing the TTL.
	if !s.Expire([]byte("k"), 0) {
		t.Fatal("clearing a TTL returned false")
	}
	if ms, _ := s.TTL([]byte("k")); ms != -1 {
		t.Fatalf("TTL after clearing = %d, want -1", ms)
	}
	clk.advance(1000)
	if _, ok := s.Get([]byte("k")); !ok {
		t.Fatal("key expired after its TTL was cleared")
	}
}

func TestSetOverwriteClearsTTLSlot(t *testing.T) {
	s, clk := newTestStore(t, config.EngineSharded, nil)
	now := clk.NowMillis()
	_ = s.Set([]byte("k"), []byte("v"), now+10)
	// Overwrite with no TTL.
	_ = s.Set([]byte("k"), []byte("v2"), 0)
	clk.advance(1000)
	if _, ok := s.Get([]byte("k")); !ok {
		t.Fatal("overwriting with no TTL failed to clear the old deadline")
	}
	st := s.Stats()
	if st.KeysWithTTL != 0 {
		t.Fatalf("KeysWithTTL = %d, want 0", st.KeysWithTTL)
	}
}

func TestSampledSweeperReclaimsMemory(t *testing.T) {
	s, clk := newTestStore(t, config.EngineSharded, nil)
	now := clk.NowMillis()

	const n = 5000
	for i := 0; i < n; i++ {
		_ = s.Set([]byte(fmt.Sprintf("k%05d", i)), bytes.Repeat([]byte("v"), 64), now+100)
	}
	// A few keys with no TTL must survive untouched.
	for i := 0; i < 10; i++ {
		_ = s.Set([]byte(fmt.Sprintf("perm%d", i)), []byte("v"), 0)
	}
	before := s.MemoryBytes()

	clk.advance(200)
	sw := NewSweeper(s, 10*time.Millisecond, 20, 0.25, time.Second)
	// The sweeper is probabilistic; the guarantee is convergence, not one
	// pass. Run cycles until it stops making progress.
	for i := 0; i < 200 && s.Len() > 10; i++ {
		if st := sw.RunCycle(); st.Expired == 0 && i > 5 {
			break
		}
	}

	if got := s.Len(); got > 10+n/20 {
		t.Fatalf("sweeper left %d keys, want close to 10 (started with %d)", got, n+10)
	}
	if s.MemoryBytes() >= before/2 {
		t.Fatalf("memory not reclaimed: %d -> %d", before, s.MemoryBytes())
	}
	for i := 0; i < 10; i++ {
		if !s.Exists([]byte(fmt.Sprintf("perm%d", i))) {
			t.Fatalf("sweeper deleted a key with no TTL")
		}
	}
}

func TestSweeperRespectsTimeBudget(t *testing.T) {
	s, clk := newTestStore(t, config.EngineSharded, func(o *Options) { o.Shards = 64 })
	now := clk.NowMillis()
	for i := 0; i < 50000; i++ {
		_ = s.Set([]byte(fmt.Sprintf("k%06d", i)), []byte("v"), now+10)
	}
	clk.advance(100)

	// A 1ms budget must produce a cycle that returns in roughly that time,
	// not one that scans 50k keys. This is the property that keeps the
	// sweeper from creating a latency cliff.
	sw := NewSweeper(s, 100*time.Millisecond, 20, 0.25, time.Millisecond)
	st := sw.RunCycle()
	if st.Duration > 100*time.Millisecond {
		t.Fatalf("cycle took %v with a 1ms budget", st.Duration)
	}
	if !st.Budgeted && st.Expired > 40000 {
		t.Fatalf("cycle expired %d keys without hitting the budget; it is not bounded", st.Expired)
	}
}

func TestTimingWheelExpiry(t *testing.T) {
	s, clk := newTestStore(t, config.EngineSharded, nil)
	now := clk.NowMillis()

	exp := NewWheelExpirer(s, 10*time.Millisecond)
	for i := 0; i < 500; i++ {
		_ = s.Set([]byte(fmt.Sprintf("k%03d", i)), []byte("v"), now+uint64(10+i))
	}
	_ = s.Set([]byte("permanent"), []byte("v"), 0)

	if exp.Pending() != 500 {
		t.Fatalf("wheel has %d timers, want 500", exp.Pending())
	}

	clk.advance(1000)
	exp.RunOnce()

	if s.Len() != 1 {
		t.Fatalf("wheel left %d keys, want 1", s.Len())
	}
	if !s.Exists([]byte("permanent")) {
		t.Fatal("wheel expired a key with no TTL")
	}
	_, _, expired := exp.Stats()
	if expired != 500 {
		t.Fatalf("wheel expired %d keys, want 500", expired)
	}
}

func TestTimingWheelLazyCancellation(t *testing.T) {
	s, clk := newTestStore(t, config.EngineSharded, nil)
	now := clk.NowMillis()
	exp := NewWheelExpirer(s, 10*time.Millisecond)

	_ = s.Set([]byte("k"), []byte("v1"), now+50)
	// Overwrite with a much later deadline. The first timer is now stale
	// and must be dropped rather than deleting live data.
	_ = s.Set([]byte("k"), []byte("v2"), now+100000)

	clk.advance(200)
	exp.RunOnce()

	v, ok := s.Get([]byte("k"))
	if !ok {
		t.Fatal("a stale timer deleted a key whose TTL had been extended")
	}
	if string(v) != "v2" {
		t.Fatalf("value = %q, want v2", v)
	}
	_, stale, _ := exp.Stats()
	if stale == 0 {
		t.Fatal("expected the stale timer to be counted")
	}
}

func TestTimingWheelCascades(t *testing.T) {
	// A deadline far enough out to be parked above level 0 must still fire.
	// Level 0 covers tick*256 = 2560ms here, so 60s requires cascading.
	s, clk := newTestStore(t, config.EngineSharded, nil)
	now := clk.NowMillis()
	exp := NewWheelExpirer(s, 10*time.Millisecond)
	_ = s.Set([]byte("far"), []byte("v"), now+60_000)

	// Advance in realistic increments so the wheel actually ticks through
	// its levels rather than jumping straight to the target.
	for i := 0; i < 700; i++ {
		clk.advance(100)
		exp.RunOnce()
	}
	if s.Exists([]byte("far")) {
		t.Fatal("a cascaded timer never fired")
	}
}

func TestHashDistributionIsUniform(t *testing.T) {
	// Chi-squared goodness-of-fit over 1M random keys (DESIGN.md §11).
	const (
		shards = 256
		n      = 1 << 20
	)
	seed := NewSeed()
	counts := make([]int, shards)
	var rng splitmix64
	rng.state = 12345
	key := make([]byte, 16)
	for i := 0; i < n; i++ {
		v := rng.next()
		for j := 0; j < 8; j++ {
			key[j] = byte(v >> (8 * j))
		}
		v2 := rng.next()
		for j := 0; j < 8; j++ {
			key[8+j] = byte(v2 >> (8 * j))
		}
		counts[Hash64(seed, key)&(shards-1)]++
	}

	expected := float64(n) / shards
	chi2 := 0.0
	for _, c := range counts {
		d := float64(c) - expected
		chi2 += d * d / expected
	}
	// 255 degrees of freedom: the 99.9th percentile of chi-squared is ~331.
	// A uniform hash lands well below that; a broken one lands in the
	// thousands.
	if chi2 > 331 {
		t.Fatalf("chi-squared = %.1f over %d shards; distribution is not uniform", chi2, shards)
	}
	t.Logf("chi-squared = %.1f (df=%d, threshold 331)", chi2, shards-1)
}

func TestHashIsSeeded(t *testing.T) {
	// Two different seeds must route at least some keys differently.
	// Without this, hash flooding is trivially reproducible offline.
	differing := 0
	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		if Hash64(1, k)&0xFF != Hash64(2, k)&0xFF {
			differing++
		}
	}
	if differing < 900 {
		t.Fatalf("only %d/1000 keys routed differently under a different seed", differing)
	}
}

func TestSequentialKeysSpreadAcrossShards(t *testing.T) {
	s, _ := newTestStore(t, config.EngineSharded, func(o *Options) { o.Shards = 16 })
	counts := make([]int, 16)
	for i := 0; i < 16000; i++ {
		counts[s.ShardIndex([]byte(fmt.Sprintf("user:%d", i)))]++
	}
	for i, c := range counts {
		if c < 700 || c > 1300 {
			t.Fatalf("shard %d got %d of 16000 sequential keys; expected ~1000", i, c)
		}
	}
}

func TestMemoryAccounting(t *testing.T) {
	s, _ := newTestStore(t, config.EngineSharded, nil)
	if s.MemoryBytes() != 0 {
		t.Fatalf("empty store accounts %d bytes", s.MemoryBytes())
	}
	key, val := []byte("key"), bytes.Repeat([]byte("v"), 1000)
	_ = s.Set(key, val, 0)

	want := EntryCost(len(key), len(val))
	if got := s.MemoryBytes(); got != want {
		t.Fatalf("accounted %d bytes, want %d", got, want)
	}

	// Overwriting with a larger value must adjust by the delta, not
	// double-count.
	bigger := bytes.Repeat([]byte("v"), 2000)
	_ = s.Set(key, bigger, 0)
	want = EntryCost(len(key), len(bigger))
	if got := s.MemoryBytes(); got != want {
		t.Fatalf("after overwrite accounted %d bytes, want %d", got, want)
	}

	s.Delete(key)
	if got := s.MemoryBytes(); got != 0 {
		t.Fatalf("after delete accounted %d bytes, want 0", got)
	}
}

func TestMemoryAccountingSurvivesRandomOps(t *testing.T) {
	s, _ := newTestStore(t, config.EngineSharded, nil)
	var rng splitmix64
	rng.state = 99
	live := map[string]int{}
	for i := 0; i < 20000; i++ {
		k := fmt.Sprintf("k%d", rng.intn(500))
		switch rng.intn(3) {
		case 0, 1:
			vlen := rng.intn(200)
			_ = s.Set([]byte(k), bytes.Repeat([]byte("x"), vlen), 0)
			live[k] = vlen
		case 2:
			s.Delete([]byte(k))
			delete(live, k)
		}
	}
	var want int64
	for k, vlen := range live {
		want += EntryCost(len(k), vlen)
	}
	if got := s.MemoryBytes(); got != want {
		t.Fatalf("accounting drifted: got %d, want %d (%d live keys)", got, want, len(live))
	}
}

func TestNoEvictionPolicyReturnsOOM(t *testing.T) {
	s, _ := newTestStore(t, config.EngineSharded, func(o *Options) {
		o.Shards = 1
		o.MaxMemory = 8 * 1024
		o.Policy = config.EvictNone
	})
	var lastErr error
	n := 0
	for i := 0; i < 1000; i++ {
		err := s.Set([]byte(fmt.Sprintf("k%04d", i)), bytes.Repeat([]byte("v"), 100), 0)
		if err != nil {
			lastErr = err
			break
		}
		n++
	}
	if lastErr != ErrOOM {
		t.Fatalf("noeviction did not return ErrOOM (accepted %d writes, err=%v)", n, lastErr)
	}
	// Reads must always still work.
	if _, ok := s.Get([]byte("k0000")); !ok {
		t.Fatal("reads must succeed under OOM")
	}
	if s.MemoryBytes() > 8*1024 {
		t.Fatalf("noeviction exceeded the limit: %d", s.MemoryBytes())
	}
}

func TestEvictionKeepsMemoryUnderLimit(t *testing.T) {
	for _, exact := range []bool{false, true} {
		name := "sampled"
		if exact {
			name = "exact"
		}
		t.Run(name, func(t *testing.T) {
			const limit = 256 * 1024
			s, _ := newTestStore(t, config.EngineSharded, func(o *Options) {
				o.Shards = 4
				o.MaxMemory = limit
				o.LowWater = limit * 95 / 100
				o.ExactLRU = exact
			})
			for i := 0; i < 20000; i++ {
				if err := s.Set([]byte(fmt.Sprintf("k%06d", i)), bytes.Repeat([]byte("v"), 100), 0); err != nil {
					t.Fatalf("write %d rejected under allkeys-lru: %v", i, err)
				}
			}
			if got := s.MemoryBytes(); got > limit {
				t.Fatalf("memory %d exceeds limit %d", got, limit)
			}
			st := s.Stats()
			if st.Evictions == 0 {
				t.Fatal("no evictions happened")
			}
			if st.Keys == 0 {
				t.Fatal("eviction emptied the store")
			}
			t.Logf("%s LRU: %d keys, %d bytes, %d evictions", name, st.Keys, st.LogicalBytes, st.Evictions)
		})
	}
}

func TestSampledLRUEvictsColdKeys(t *testing.T) {
	// The approximation must actually approximate: keys that are read
	// constantly should survive, keys never touched again should not.
	const limit = 200 * 1024
	s, clk := newTestStore(t, config.EngineSharded, func(o *Options) {
		o.Shards = 1
		o.MaxMemory = limit
		o.LowWater = limit * 90 / 100
		o.EvictSampleK = 10
	})

	hot := make([][]byte, 20)
	for i := range hot {
		hot[i] = []byte(fmt.Sprintf("hot%02d", i))
		_ = s.Set(hot[i], bytes.Repeat([]byte("h"), 100), 0)
	}

	for i := 0; i < 5000; i++ {
		clk.advanceMonoOnly(1)
		_ = s.Set([]byte(fmt.Sprintf("cold%06d", i)), bytes.Repeat([]byte("c"), 100), 0)
		// Keep the hot set hot.
		if i%5 == 0 {
			for _, k := range hot {
				s.Get(k)
			}
		}
	}

	survived := 0
	for _, k := range hot {
		if s.Exists(k) {
			survived++
		}
	}
	if survived < 18 {
		t.Fatalf("sampled LRU evicted %d/20 hot keys; the approximation is not working", 20-survived)
	}
	t.Logf("hot keys surviving: %d/20", survived)
}

func TestVolatileLRUOnlyEvictsKeysWithTTL(t *testing.T) {
	const limit = 128 * 1024
	s, clk := newTestStore(t, config.EngineSharded, func(o *Options) {
		o.Shards = 1
		o.MaxMemory = limit
		o.LowWater = limit * 90 / 100
		o.Policy = config.EvictVolatile
	})
	now := clk.NowMillis()
	for i := 0; i < 50; i++ {
		_ = s.Set([]byte(fmt.Sprintf("perm%03d", i)), bytes.Repeat([]byte("p"), 100), 0)
	}
	for i := 0; i < 5000; i++ {
		_ = s.Set([]byte(fmt.Sprintf("vol%06d", i)), bytes.Repeat([]byte("v"), 100), now+1_000_000)
	}
	for i := 0; i < 50; i++ {
		if !s.Exists([]byte(fmt.Sprintf("perm%03d", i))) {
			t.Fatalf("volatile-lru evicted perm%03d, which has no TTL", i)
		}
	}
	if s.Stats().Evictions == 0 {
		t.Fatal("volatile-lru evicted nothing")
	}
}

func TestFlushAndStats(t *testing.T) {
	s, _ := newTestStore(t, config.EngineSharded, nil)
	for i := 0; i < 100; i++ {
		_ = s.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v"), 0)
	}
	s.Get([]byte("k0"))
	s.Get([]byte("nope"))

	st := s.Stats()
	if st.Keys != 100 {
		t.Fatalf("Keys = %d", st.Keys)
	}
	if st.Hits == 0 || st.Misses == 0 {
		t.Fatalf("hit/miss counters not moving: %+v", st)
	}
	if len(st.ShardKeys) != s.ShardCount() {
		t.Fatalf("per-shard breakdown has %d entries, want %d", len(st.ShardKeys), s.ShardCount())
	}
	sum := 0
	for _, c := range st.ShardKeys {
		sum += c
	}
	if sum != 100 {
		t.Fatalf("per-shard key counts sum to %d, want 100", sum)
	}

	s.Flush()
	if s.Len() != 0 || s.MemoryBytes() != 0 {
		t.Fatalf("after Flush: %d keys, %d bytes", s.Len(), s.MemoryBytes())
	}
}

func TestRangeSeesEveryLiveEntry(t *testing.T) {
	s, clk := newTestStore(t, config.EngineSharded, nil)
	now := clk.NowMillis()
	want := map[string]string{}
	for i := 0; i < 1000; i++ {
		k, v := fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d", i)
		_ = s.Set([]byte(k), []byte(v), 0)
		want[k] = v
	}
	// Some expired keys that must NOT be visited.
	for i := 0; i < 100; i++ {
		_ = s.Set([]byte(fmt.Sprintf("dead%03d", i)), []byte("x"), now+1)
	}
	clk.advance(10)

	got := map[string]string{}
	s.Range(func(e *Entry) bool {
		got[string(e.Key)] = string(e.Value)
		return true
	})
	if len(got) != len(want) {
		t.Fatalf("Range visited %d entries, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("Range: %s = %q, want %q", k, got[k], v)
		}
	}
}

func TestRangeEarlyStop(t *testing.T) {
	s, _ := newTestStore(t, config.EngineSharded, nil)
	for i := 0; i < 1000; i++ {
		_ = s.Set([]byte(fmt.Sprintf("k%04d", i)), []byte("v"), 0)
	}
	n := 0
	s.Range(func(*Entry) bool { n++; return n < 10 })
	if n != 10 {
		t.Fatalf("Range visited %d entries after an early stop, want 10", n)
	}
}

func TestKeysPrefixAndLimit(t *testing.T) {
	s, _ := newTestStore(t, config.EngineSharded, nil)
	for i := 0; i < 100; i++ {
		_ = s.Set([]byte(fmt.Sprintf("user:%03d", i)), []byte("v"), 0)
		_ = s.Set([]byte(fmt.Sprintf("post:%03d", i)), []byte("v"), 0)
	}
	got := s.Keys([]byte("user:"), 1000)
	if len(got) != 100 {
		t.Fatalf("prefix scan returned %d keys, want 100", len(got))
	}
	for _, k := range got {
		if !bytes.HasPrefix(k, []byte("user:")) {
			t.Fatalf("prefix scan returned %q", k)
		}
	}
	if got := s.Keys(nil, 7); len(got) != 7 {
		t.Fatalf("limit ignored: got %d keys", len(got))
	}
}

// TestConcurrentAccess is the -race workhorse: overlapping keys, every
// operation, every engine.
func TestConcurrentAccess(t *testing.T) {
	for _, eng := range allEngines() {
		t.Run(string(eng), func(t *testing.T) {
			s, _ := newTestStore(t, eng, func(o *Options) { o.Shards = 8 })
			const (
				workers = 32
				ops     = 500
				keys    = 64 // deliberately fewer keys than workers
			)
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					var rng splitmix64
					rng.state = uint64(w) + 1
					for i := 0; i < ops; i++ {
						k := []byte(fmt.Sprintf("k%02d", rng.intn(keys)))
						switch rng.intn(6) {
						case 0, 1:
							_ = s.Set(k, []byte(fmt.Sprintf("w%d-i%d", w, i)), 0)
						case 2, 3:
							s.Get(k)
						case 4:
							s.Delete(k)
						case 5:
							s.Exists(k)
						}
					}
				}(w)
			}
			wg.Wait()
			// The invariant is just that we got here without a race or a
			// panic, and that the store is still coherent.
			st := s.Stats()
			if st.Keys != s.Len() {
				t.Fatalf("Stats.Keys = %d but Len = %d", st.Keys, s.Len())
			}
		})
	}
}

func TestConcurrentEvictionAndExpiry(t *testing.T) {
	const limit = 512 * 1024
	s, clk := newTestStore(t, config.EngineSharded, func(o *Options) {
		o.Shards = 8
		o.MaxMemory = limit
		o.LowWater = limit * 90 / 100
	})
	sw := NewSweeper(s, 5*time.Millisecond, 20, 0.25, 10*time.Millisecond)

	var wg sync.WaitGroup
	var sweeperWG sync.WaitGroup
	stop := make(chan struct{})

	sweeperWG.Add(1)
	go func() {
		defer sweeperWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				sw.RunCycle()
				clk.advance(1)
				// Pace the sweeper. In production a ticker does this; an
				// unpaced sweeper loop holds shard write locks back to back
				// and starves the very writers this test is trying to
				// exercise, which measures the harness rather than the store.
				time.Sleep(time.Millisecond)
			}
		}
	}()

	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var rng splitmix64
			rng.state = uint64(w) + 7
			for i := 0; i < 750; i++ {
				k := []byte(fmt.Sprintf("k%d-%d", w, rng.intn(500)))
				exp := uint64(0)
				if rng.intn(2) == 0 {
					exp = clk.NowMillis() + uint64(rng.intn(50))
				}
				_ = s.Set(k, bytes.Repeat([]byte("v"), 64), exp)
				s.Get(k)
			}
		}(w)
	}
	// Writers first, then stop the sweeper. Waiting on a single WaitGroup
	// that also contains the sweeper deadlocks: the sweeper only exits on
	// close(stop), which comes after the wait.
	wg.Wait()
	close(stop)
	sweeperWG.Wait()

	if got := s.MemoryBytes(); got > limit {
		t.Fatalf("memory %d exceeded limit %d under concurrent load", got, limit)
	}
	if got, want := s.MemoryBytes(), int64(0); got < want {
		t.Fatalf("accounting went negative: %d", got)
	}
}

func TestAccountingNeverGoesNegative(t *testing.T) {
	s, clk := newTestStore(t, config.EngineSharded, nil)
	now := clk.NowMillis()
	for i := 0; i < 500; i++ {
		_ = s.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v"), now+1)
	}
	clk.advance(5)
	// Reap the same keys via every path that can remove them.
	sw := NewSweeper(s, time.Millisecond, 20, 0.25, time.Second)
	for i := 0; i < 50; i++ {
		sw.RunCycle()
	}
	for i := 0; i < 500; i++ {
		s.Get([]byte(fmt.Sprintf("k%d", i)))
		s.Delete([]byte(fmt.Sprintf("k%d", i)))
	}
	if got := s.MemoryBytes(); got != 0 {
		t.Fatalf("memory accounting = %d after everything expired, want 0", got)
	}
}

func TestOnEvictCallbackFires(t *testing.T) {
	s, clk := newTestStore(t, config.EngineSharded, func(o *Options) {
		o.Shards = 1
		o.MaxMemory = 64 * 1024
		o.LowWater = 60 * 1024
	})
	var mu sync.Mutex
	reasons := map[EvictReason]int{}
	s.SetOnEvict(func(key []byte, r EvictReason) {
		mu.Lock()
		reasons[r]++
		mu.Unlock()
	})

	now := clk.NowMillis()
	for i := 0; i < 2000; i++ {
		_ = s.Set([]byte(fmt.Sprintf("k%05d", i)), bytes.Repeat([]byte("v"), 100), now+10)
	}
	clk.advance(100)
	NewSweeper(s, time.Millisecond, 20, 0.25, time.Second).RunCycle()

	mu.Lock()
	defer mu.Unlock()
	if reasons[ReasonEvicted] == 0 {
		t.Fatal("no eviction callbacks fired")
	}
	if reasons[ReasonExpired] == 0 {
		t.Fatal("no expiry callbacks fired")
	}
}

func TestEntryCostIsStable(t *testing.T) {
	// If Entry grows, the accounting constant silently changes meaning.
	// This test exists to make that a deliberate decision rather than a
	// surprise in the memory report.
	got := EntryCost(0, 0)
	want := int64(entryStructSize + mapOverhead)
	if got != want {
		t.Fatalf("EntryCost(0,0) = %d, want %d", got, want)
	}
	if entryStructSize > 128 {
		t.Fatalf("Entry has grown to %d bytes; update mapOverhead calibration in docs", entryStructSize)
	}
}

func TestSplitmixIsUniform(t *testing.T) {
	var rng splitmix64
	rng.state = 7
	const buckets = 16
	counts := make([]int, buckets)
	for i := 0; i < 160000; i++ {
		counts[rng.intn(buckets)]++
	}
	for i, c := range counts {
		if math.Abs(float64(c)-10000) > 500 {
			t.Fatalf("bucket %d got %d, expected ~10000", i, c)
		}
	}
}
