package differential

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/raqueeb/kvstore/internal/config"
	"github.com/raqueeb/kvstore/internal/store"
	"github.com/raqueeb/kvstore/test/harness"
)

// Model-based differential testing (DESIGN.md §11).
//
// The real store is sharded, has a TTL index, memory accounting, an eviction
// sampler and three interchangeable concurrency engines. The reference is a
// map behind one mutex. Random operation sequences are run against both and
// every observable result is compared.
//
// This is the test that catches what unit tests structurally cannot: shard
// routing bugs, TTL boundary conditions, accounting drift after a specific
// sequence of overwrites and deletes. A failing sequence is shrunk to a
// minimal reproduction before being reported, because a 5000-operation
// failure is not a bug report, it is a haystack.

type opKind int

const (
	opGet opKind = iota
	opSet
	opDelete
	opExists
	opExpire
	opTTL
	opAdvanceClock
	opCount
)

func (o opKind) String() string {
	return [...]string{"GET", "SET", "DELETE", "EXISTS", "EXPIRE", "TTL", "ADVANCE"}[o]
}

type operation struct {
	kind    opKind
	key     string
	value   string
	ttlMs   uint64
	advance uint64
}

func (o operation) String() string {
	switch o.kind {
	case opSet:
		return fmt.Sprintf("SET %q %q ttl=%d", o.key, o.value, o.ttlMs)
	case opExpire:
		return fmt.Sprintf("EXPIRE %q ttl=%d", o.key, o.ttlMs)
	case opAdvanceClock:
		return fmt.Sprintf("ADVANCE %dms", o.advance)
	default:
		return fmt.Sprintf("%s %q", o.kind, o.key)
	}
}

// testClock is shared by both implementations so "now" can never differ
// between them — otherwise a TTL comparison would be racing the wall clock
// and the test would be flaky for reasons that have nothing to do with the
// store.
type testClock struct {
	wall uint64
	mono uint64
}

func (c *testClock) NowMillis() uint64  { return c.wall }
func (c *testClock) MonoMillis() uint64 { return c.mono }
func (c *testClock) advance(ms uint64)  { c.wall += ms; c.mono += ms }

func generateOps(rng *rand.Rand, n, keyspace int) []operation {
	ops := make([]operation, n)
	for i := range ops {
		k := fmt.Sprintf("k%03d", rng.Intn(keyspace))
		op := operation{kind: opKind(rng.Intn(int(opCount))), key: k}
		switch op.kind {
		case opSet:
			op.value = strings.Repeat(string(rune('a'+rng.Intn(26))), rng.Intn(40))
			// Mostly no TTL, sometimes a short one, occasionally one that is
			// already in the past — that last case is where off-by-one
			// expiry bugs live.
			switch rng.Intn(10) {
			case 0, 1:
				op.ttlMs = uint64(rng.Intn(200) + 1)
			case 2:
				op.ttlMs = 1
			}
		case opExpire:
			if rng.Intn(4) == 0 {
				op.ttlMs = 0 // clear the TTL
			} else {
				op.ttlMs = uint64(rng.Intn(200) + 1)
			}
		case opAdvanceClock:
			op.advance = uint64(rng.Intn(60))
		}
		ops[i] = op
	}
	return ops
}

// runSequence executes ops against both implementations and returns the
// first divergence, or "" if they agreed throughout.
func runSequence(engine config.StoreEngine, shards int, ops []operation) string {
	clk := &testClock{wall: 1_000_000_000}
	real := store.New(store.Options{
		Engine: engine,
		Shards: shards,
		Seed:   0x5EED,
		Clock:  clk,
	})
	defer real.Close()
	ref := store.NewReference(clk)

	for i, op := range ops {
		switch op.kind {
		case opAdvanceClock:
			clk.advance(op.advance)

		case opSet:
			var expireAt uint64
			if op.ttlMs > 0 {
				expireAt = clk.NowMillis() + op.ttlMs
			}
			if err := real.Set([]byte(op.key), []byte(op.value), expireAt); err != nil {
				return fmt.Sprintf("op %d (%s): real store returned an error with no memory limit set: %v", i, op, err)
			}
			ref.Set([]byte(op.key), []byte(op.value), expireAt)

		case opGet:
			gotV, gotOK := real.Get([]byte(op.key))
			wantV, wantOK := ref.Get([]byte(op.key))
			if gotOK != wantOK {
				return fmt.Sprintf("op %d (%s): found=%v, reference says %v", i, op, gotOK, wantOK)
			}
			if gotOK && !bytes.Equal(gotV, wantV) {
				return fmt.Sprintf("op %d (%s): value %q, reference says %q", i, op, gotV, wantV)
			}

		case opDelete:
			got := real.Delete([]byte(op.key))
			want := ref.Delete([]byte(op.key))
			if got != want {
				return fmt.Sprintf("op %d (%s): existed=%v, reference says %v", i, op, got, want)
			}

		case opExists:
			got := real.Exists([]byte(op.key))
			want := ref.Exists([]byte(op.key))
			if got != want {
				return fmt.Sprintf("op %d (%s): exists=%v, reference says %v", i, op, got, want)
			}

		case opExpire:
			var expireAt uint64
			if op.ttlMs > 0 {
				expireAt = clk.NowMillis() + op.ttlMs
			}
			got := real.Expire([]byte(op.key), expireAt)
			want := ref.Expire([]byte(op.key), expireAt)
			if got != want {
				return fmt.Sprintf("op %d (%s): expire=%v, reference says %v", i, op, got, want)
			}

		case opTTL:
			gotMs, gotOK := real.TTL([]byte(op.key))
			wantMs, wantOK := ref.TTL([]byte(op.key))
			if gotOK != wantOK {
				return fmt.Sprintf("op %d (%s): found=%v, reference says %v", i, op, gotOK, wantOK)
			}
			if gotOK && gotMs != wantMs {
				return fmt.Sprintf("op %d (%s): ttl=%d, reference says %d", i, op, gotMs, wantMs)
			}
		}
	}

	// Final state comparison: key count must agree exactly. This is what
	// catches an entry that was hidden correctly but never reclaimed.
	if got, want := real.Len(), ref.Len(); got != want {
		// Len() on the real store counts expired-but-unreaped entries, so
		// purge first to compare like with like.
		real.PurgeExpired()
		if got2 := real.Len(); got2 != want {
			return fmt.Sprintf("final key count %d (%d before purge), reference says %d", got2, got, want)
		}
	}
	return ""
}

// shrink reduces a failing sequence to a minimal one that still fails, by
// repeatedly trying to delete each operation. Simple delta debugging: slow
// in theory, instant at these sizes, and it turns an unreadable failure into
// a reproduction you can paste into a unit test.
func shrink(engine config.StoreEngine, shards int, ops []operation) []operation {
	current := ops
	changed := true
	for changed && len(current) > 1 {
		changed = false
		for i := 0; i < len(current); i++ {
			candidate := make([]operation, 0, len(current)-1)
			candidate = append(candidate, current[:i]...)
			candidate = append(candidate, current[i+1:]...)
			if runSequence(engine, shards, candidate) != "" {
				current = candidate
				changed = true
				break
			}
		}
	}
	return current
}

func TestDifferentialAgainstReference(t *testing.T) {
	engines := []config.StoreEngine{config.EngineSharded, config.EngineGlobal, config.EngineActor}
	shardCounts := []int{1, 4, 16}

	seed := time.Now().UnixNano()
	if testing.Short() {
		seed = 1 // deterministic in short mode
	}
	t.Logf("random seed: %d (rerun with this seed to reproduce)", seed)

	iterations := 200
	opsPerRun := 500
	if testing.Short() {
		iterations, opsPerRun = 20, 100
	}

	for _, eng := range engines {
		for _, shards := range shardCounts {
			if eng == config.EngineGlobal && shards != 1 {
				continue // the global engine is single-shard by definition
			}
			t.Run(fmt.Sprintf("%s/shards=%d", eng, shards), func(t *testing.T) {
				rng := rand.New(rand.NewSource(seed))
				for iter := 0; iter < iterations; iter++ {
					ops := generateOps(rng, opsPerRun, 40)
					if failure := runSequence(eng, shards, ops); failure != "" {
						minimal := shrink(eng, shards, ops)
						var sb strings.Builder
						fmt.Fprintf(&sb, "divergence from the reference implementation\n")
						fmt.Fprintf(&sb, "  engine: %s, shards: %d, iteration: %d\n", eng, shards, iter)
						fmt.Fprintf(&sb, "  failure: %s\n", failure)
						fmt.Fprintf(&sb, "  shrunk from %d to %d operations:\n", len(ops), len(minimal))
						for i, op := range minimal {
							fmt.Fprintf(&sb, "    %2d. %s\n", i, op)
						}
						t.Fatal(sb.String())
					}
				}
			})
		}
	}
}

// TestDifferentialThroughTheNetwork runs the same idea end-to-end: a real
// server over a real socket versus the reference. It is slower and uses the
// wall clock, so TTLs are kept generous, but it covers the whole stack —
// protocol codec, dispatcher, engine and store — rather than the store
// alone.
func TestDifferentialThroughTheNetwork(t *testing.T) {
	cfg := harness.DefaultConfig(t.TempDir())
	cfg.Fsync = "no"
	s := harness.Start(t, cfg)
	c := s.Client(t)

	ref := make(map[string]string)
	rng := rand.New(rand.NewSource(42))

	n := 5000
	if testing.Short() {
		n = 500
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%03d", rng.Intn(100))
		switch rng.Intn(5) {
		case 0, 1:
			val := fmt.Sprintf("v%d-%d", i, rng.Intn(1000))
			if err := c.Set([]byte(key), []byte(val), 0); err != nil {
				t.Fatalf("op %d SET: %v", i, err)
			}
			ref[key] = val

		case 2:
			got, err := c.Get([]byte(key))
			want, present := ref[key]
			if present {
				if err != nil {
					t.Fatalf("op %d GET %q: %v (reference has it)", i, key, err)
				}
				if string(got) != want {
					t.Fatalf("op %d GET %q = %q, reference says %q", i, key, got, want)
				}
			} else if err == nil {
				t.Fatalf("op %d GET %q returned %q, reference has no such key", i, key, got)
			}

		case 3:
			existed, err := c.Delete([]byte(key))
			if err != nil {
				t.Fatalf("op %d DELETE: %v", i, err)
			}
			_, present := ref[key]
			if existed != present {
				t.Fatalf("op %d DELETE %q: existed=%v, reference says %v", i, key, existed, present)
			}
			delete(ref, key)

		case 4:
			got, err := c.Exists([]byte(key))
			if err != nil {
				t.Fatalf("op %d EXISTS: %v", i, err)
			}
			_, want := ref[key]
			if got != want {
				t.Fatalf("op %d EXISTS %q = %v, reference says %v", i, key, got, want)
			}
		}
	}

	// Final full comparison.
	keys, err := c.Keys(nil, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != len(ref) {
		t.Fatalf("server holds %d keys, reference holds %d", len(keys), len(ref))
	}
	for _, k := range keys {
		want, ok := ref[string(k)]
		if !ok {
			t.Fatalf("server has key %q that the reference does not", k)
		}
		got, err := c.Get(k)
		if err != nil || string(got) != want {
			t.Fatalf("final check %q = %q (%v), reference says %q", k, got, err, want)
		}
	}
}

// TestDifferentialAcrossRestart checks that a restart is state-preserving:
// the keyspace after recovery must equal the keyspace before shutdown.
func TestDifferentialAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := harness.DefaultConfig(dir)
	cfg.Fsync = config.FsyncAlways

	ref := make(map[string]string)
	rng := rand.New(rand.NewSource(7))

	s := harness.Start(t, cfg)
	c := s.Client(t)
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("k%04d", rng.Intn(500))
		if rng.Intn(4) == 0 {
			if _, err := c.Delete([]byte(key)); err != nil {
				t.Fatal(err)
			}
			delete(ref, key)
			continue
		}
		val := fmt.Sprintf("v%d", i)
		if err := c.Set([]byte(key), []byte(val), 0); err != nil {
			t.Fatal(err)
		}
		ref[key] = val
	}
	s.Stop()

	// Reopen on the same directory.
	cfg2 := cfg
	s2 := harness.Start(t, cfg2)
	c2 := s2.Client(t)

	keys, err := c2.Keys(nil, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != len(ref) {
		t.Fatalf("after restart the server holds %d keys, expected %d", len(keys), len(ref))
	}
	for k, want := range ref {
		got, err := c2.Get([]byte(k))
		if err != nil {
			t.Fatalf("key %q lost across restart: %v", k, err)
		}
		if string(got) != want {
			t.Fatalf("key %q = %q after restart, want %q", k, got, want)
		}
	}
}
