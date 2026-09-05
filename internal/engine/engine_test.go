package engine

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/raqueeb/kvstore/internal/config"
	"github.com/raqueeb/kvstore/internal/wal"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testConfig(dir string) config.Config {
	c := config.Default()
	c.DataDir = dir
	c.Shards = 4
	c.Fsync = config.FsyncAlways
	c.SegmentSize = 1 << 20
	c.GroupCommitMax = 64
	c.GroupCommitWait = time.Millisecond
	c.SnapshotInterval = 0
	c.Expiry = config.ExpirySampled
	c.NodeID = "test-node"
	return c
}

func openEngine(t *testing.T, cfg config.Config) *Engine {
	t.Helper()
	e, err := Open(cfg, quietLogger())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return e
}

func TestEngineBasicDurability(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)

	e := openEngine(t, cfg)
	for i := 0; i < 500; i++ {
		if err := e.Set([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%04d", i)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.Delete([]byte("k0000")); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: every acknowledged write must be back.
	e2 := openEngine(t, cfg)
	defer e2.Close()

	if _, ok := e2.Get([]byte("k0000")); ok {
		t.Fatal("a deleted key came back after restart")
	}
	for i := 1; i < 500; i++ {
		v, ok := e2.Get([]byte(fmt.Sprintf("k%04d", i)))
		if !ok {
			t.Fatalf("k%04d lost across restart", i)
		}
		if string(v) != fmt.Sprintf("v%04d", i) {
			t.Fatalf("k%04d = %q after restart", i, v)
		}
	}
	rep := e2.Recovery()
	if rep.WALApplied != 501 {
		t.Fatalf("replayed %d records, want 501", rep.WALApplied)
	}
	if rep.Truncated {
		t.Fatalf("clean shutdown produced a truncation: %s", rep.TruncateReason)
	}
}

func TestEngineLSNContinuesAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)

	e := openEngine(t, cfg)
	for i := 0; i < 100; i++ {
		e.Set([]byte("k"), []byte("v"), 0)
	}
	firstLast := e.LastLSN()
	e.Close()

	e2 := openEngine(t, cfg)
	defer e2.Close()
	if got := e2.LastLSN(); got != firstLast {
		t.Fatalf("LastLSN after restart = %d, want %d", got, firstLast)
	}
	e2.Set([]byte("k2"), []byte("v"), 0)
	if got := e2.LastLSN(); got != firstLast+1 {
		t.Fatalf("next LSN = %d, want %d", got, firstLast+1)
	}
}

func TestEngineTTLSurvivesRestartAndExpiresOnLoad(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)

	e := openEngine(t, cfg)
	// Long TTL: must survive.
	if err := e.Set([]byte("long"), []byte("v"), 3_600_000); err != nil {
		t.Fatal(err)
	}
	// Very short TTL: must be dropped on load.
	if err := e.Set([]byte("short"), []byte("v"), 30); err != nil {
		t.Fatal(err)
	}
	e.Close()

	time.Sleep(60 * time.Millisecond)

	e2 := openEngine(t, cfg)
	defer e2.Close()

	if _, ok := e2.Get([]byte("long")); !ok {
		t.Fatal("a key with a long TTL did not survive restart")
	}
	ms, ok := e2.TTL([]byte("long"))
	if !ok || ms <= 0 || ms > 3_600_000 {
		t.Fatalf("TTL after restart = %d (absolute deadlines must survive)", ms)
	}
	if _, ok := e2.Get([]byte("short")); ok {
		t.Fatal("a key that expired while the process was down came back")
	}
	if e2.Recovery().ExpiredOnLoad != 1 {
		t.Fatalf("ExpiredOnLoad = %d, want 1", e2.Recovery().ExpiredOnLoad)
	}
}

func TestEngineSnapshotTruncatesWAL(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.SegmentSize = 8 << 10 // force many segments

	e := openEngine(t, cfg)
	for i := 0; i < 2000; i++ {
		if err := e.Set([]byte(fmt.Sprintf("k%05d", i)), bytes.Repeat([]byte("v"), 64), 0); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := wal.ListSegments(cfg.WALDir())
	if len(before) < 3 {
		t.Fatalf("only %d segments; test needs more", len(before))
	}

	res, err := e.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if res.Entries != 2000 {
		t.Fatalf("snapshot holds %d entries, want 2000", res.Entries)
	}
	if res.SegmentsRemoved == 0 {
		t.Fatal("snapshot did not truncate any WAL segments")
	}
	after, _ := wal.ListSegments(cfg.WALDir())
	if len(after) >= len(before) {
		t.Fatalf("segments went from %d to %d", len(before), len(after))
	}
	e.Close()

	// Recovery must now come mostly from the snapshot.
	e2 := openEngine(t, cfg)
	defer e2.Close()
	if e2.Store().Len() != 2000 {
		t.Fatalf("after snapshot recovery: %d keys, want 2000", e2.Store().Len())
	}
	rep := e2.Recovery()
	if rep.SnapshotEntries != 2000 {
		t.Fatalf("recovery did not load the snapshot: %+v", rep)
	}
	if rep.WALApplied > 10 {
		t.Fatalf("replayed %d WAL records after a full snapshot; snapshots are not bounding recovery", rep.WALApplied)
	}
	for i := 0; i < 2000; i += 137 {
		if _, ok := e2.Get([]byte(fmt.Sprintf("k%05d", i))); !ok {
			t.Fatalf("k%05d missing after snapshot recovery", i)
		}
	}
}

func TestEngineSnapshotThenMoreWritesThenRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)

	e := openEngine(t, cfg)
	for i := 0; i < 500; i++ {
		e.Set([]byte(fmt.Sprintf("pre%04d", i)), []byte("v"), 0)
	}
	if _, err := e.Snapshot(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		e.Set([]byte(fmt.Sprintf("post%04d", i)), []byte("v"), 0)
	}
	// Overwrite something that was in the snapshot.
	e.Set([]byte("pre0000"), []byte("updated"), 0)
	e.Close()

	e2 := openEngine(t, cfg)
	defer e2.Close()
	if e2.Store().Len() != 1000 {
		t.Fatalf("%d keys after recovery, want 1000", e2.Store().Len())
	}
	v, ok := e2.Get([]byte("pre0000"))
	if !ok || string(v) != "updated" {
		t.Fatalf("post-snapshot overwrite lost: %q ok=%v", v, ok)
	}
	for i := 0; i < 500; i += 61 {
		if _, ok := e2.Get([]byte(fmt.Sprintf("post%04d", i))); !ok {
			t.Fatalf("post%04d lost", i)
		}
	}
}

func TestEngineDataDirLock(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)

	e := openEngine(t, cfg)
	defer e.Close()

	// A second engine on the same directory must be refused.
	if e2, err := Open(cfg, quietLogger()); err == nil {
		e2.Close()
		t.Fatal("two engines opened the same data directory")
	}
}

func TestEngineStaleLockIsBroken(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// A lock file from a PID that cannot be running.
	if err := os.WriteFile(cfg.LockPath(), []byte("999999999\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	e, err := Open(cfg, quietLogger())
	if err != nil {
		t.Fatalf("a stale lock must not block startup: %v", err)
	}
	e.Close()
}

func TestEngineLockReleasedOnClose(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	e := openEngine(t, cfg)
	e.Close()
	if _, err := os.Stat(cfg.LockPath()); !os.IsNotExist(err) {
		t.Fatal("lock file survived a clean Close")
	}
	e2 := openEngine(t, cfg)
	e2.Close()
}

func TestEngineReadOnlyRejectsWrites(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, testConfig(dir))
	defer e.Close()

	e.Set([]byte("k"), []byte("v"), 0)
	e.SetReadOnly(true)

	if err := e.Set([]byte("k2"), []byte("v"), 0); err != ErrReadOnly {
		t.Fatalf("Set on a read-only engine: %v, want ErrReadOnly", err)
	}
	if _, err := e.Delete([]byte("k")); err != ErrReadOnly {
		t.Fatalf("Delete on a read-only engine: %v", err)
	}
	if _, err := e.Expire([]byte("k"), 100); err != ErrReadOnly {
		t.Fatalf("Expire on a read-only engine: %v", err)
	}
	// Reads still work.
	if _, ok := e.Get([]byte("k")); !ok {
		t.Fatal("a read-only engine must still serve reads")
	}
}

func TestEngineOOMDoesNotWriteToWAL(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.Shards = 1
	cfg.MaxMemory = 16 << 10
	cfg.Policy = config.EvictNone

	e := openEngine(t, cfg)
	accepted := 0
	for i := 0; i < 5000; i++ {
		err := e.Set([]byte(fmt.Sprintf("k%05d", i)), bytes.Repeat([]byte("v"), 100), 0)
		if err == ErrOOM {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		accepted++
	}
	if accepted == 0 || accepted == 5000 {
		t.Fatalf("accepted %d writes; expected the limit to bite partway", accepted)
	}
	lsn := e.LastLSN()
	// Further rejected writes must not consume LSNs: a rejected write is not
	// in the log, so it must not be in the LSN sequence either.
	for i := 0; i < 50; i++ {
		if err := e.Set([]byte(fmt.Sprintf("z%05d", i)), bytes.Repeat([]byte("v"), 100), 0); err != ErrOOM {
			t.Fatalf("expected ErrOOM, got %v", err)
		}
	}
	if e.LastLSN() != lsn {
		t.Fatalf("rejected writes consumed LSNs: %d -> %d", lsn, e.LastLSN())
	}
	e.Close()

	// And the recovered store must match what was acknowledged.
	e2 := openEngine(t, cfg)
	defer e2.Close()
	if e2.Store().Len() != accepted {
		t.Fatalf("recovered %d keys, acknowledged %d", e2.Store().Len(), accepted)
	}
}

func TestEngineConcurrentWritesAreDurable(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.Fsync = config.FsyncAlways

	e := openEngine(t, cfg)

	const writers, perWriter = 16, 200
	var mu sync.Mutex
	acked := map[string]string{}

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				k := fmt.Sprintf("w%02d-k%04d", w, i)
				v := fmt.Sprintf("val-%d-%d", w, i)
				if err := e.Set([]byte(k), []byte(v), 0); err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				acked[k] = v
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	e.Close()

	e2 := openEngine(t, cfg)
	defer e2.Close()
	for k, v := range acked {
		got, ok := e2.Get([]byte(k))
		if !ok {
			t.Fatalf("acknowledged key %q missing after restart", k)
		}
		if string(got) != v {
			t.Fatalf("key %q = %q after restart, want %q", k, got, v)
		}
	}
	if e2.Store().Len() != len(acked) {
		t.Fatalf("recovered %d keys, acknowledged %d", e2.Store().Len(), len(acked))
	}
}

func TestEngineConcurrentSameKeyOrdering(t *testing.T) {
	// Concurrent writers to ONE key. Whatever the final in-memory value is,
	// recovery must produce the same one — that is the property the
	// LSN-under-shard-lock ordering exists to guarantee.
	dir := t.TempDir()
	cfg := testConfig(dir)

	e := openEngine(t, cfg)
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				e.Set([]byte("contended"), []byte(fmt.Sprintf("%d-%d", w, i)), 0)
			}
		}(w)
	}
	wg.Wait()

	inMemory, _ := e.Get([]byte("contended"))
	final := string(inMemory)
	e.Close()

	e2 := openEngine(t, cfg)
	defer e2.Close()
	recovered, ok := e2.Get([]byte("contended"))
	if !ok {
		t.Fatal("key missing after restart")
	}
	if string(recovered) != final {
		t.Fatalf("memory had %q but the log replays to %q: log order and memory order disagree",
			final, recovered)
	}
}

func TestEngineExpiryPropagatesToWAL(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.SweepInterval = 5 * time.Millisecond
	cfg.SweepBudget = 5 * time.Millisecond

	e := openEngine(t, cfg)
	for i := 0; i < 200; i++ {
		if err := e.Set([]byte(fmt.Sprintf("k%03d", i)), []byte("v"), 20); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	for i := 0; i < 30; i++ {
		e.RunSweepCycle()
	}
	if n := e.Store().Len(); n > 20 {
		t.Fatalf("sweeper left %d of 200 expired keys", n)
	}
	e.Close()

	e2 := openEngine(t, cfg)
	defer e2.Close()
	if n := e2.Store().Len(); n != 0 {
		t.Fatalf("%d expired keys came back after restart", n)
	}
}

func TestEngineFlush(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	e := openEngine(t, cfg)
	for i := 0; i < 200; i++ {
		e.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v"), 0)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	if e.Store().Len() != 0 {
		t.Fatal("Flush left keys behind")
	}
	e.Close()

	e2 := openEngine(t, cfg)
	defer e2.Close()
	if n := e2.Store().Len(); n != 0 {
		t.Fatalf("%d keys came back after a flush and restart", n)
	}
}

func TestEngineStatsJSON(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, testConfig(dir))
	defer e.Close()

	e.Set([]byte("k"), []byte("v"), 0)
	e.Get([]byte("k"))
	e.Get([]byte("nope"))

	b, err := e.StatsJSON()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"\"store\"", "\"wal\"", "\"recovery\"", "\"expiry\"", "logical_bytes", "durable_lsn"} {
		if !bytes.Contains(b, []byte(want)) {
			t.Fatalf("STATS JSON missing %s:\n%s", want, b)
		}
	}
	st := e.Stats()
	if st.Store.Hits == 0 || st.Store.Misses == 0 {
		t.Fatalf("counters not populated: %+v", st.Store)
	}
	if st.WAL.Records == 0 {
		t.Fatal("WAL record counter is zero after a write")
	}
}

func TestEngineRecoversFromTornTail(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)

	e := openEngine(t, cfg)
	for i := 0; i < 200; i++ {
		e.Set([]byte(fmt.Sprintf("k%03d", i)), []byte("v"), 0)
	}
	e.Close()

	// Chop the tail, simulating a crash mid-write.
	segs, _ := wal.ListSegments(cfg.WALDir())
	last := segs[len(segs)-1]
	if err := os.Truncate(last.Path, last.Size-9); err != nil {
		t.Fatal(err)
	}

	e2 := openEngine(t, cfg)
	defer e2.Close()
	if !e2.Recovery().Truncated {
		t.Fatal("torn tail was not reported")
	}
	if n := e2.Store().Len(); n != 199 {
		t.Fatalf("%d keys after torn-tail recovery, want 199", n)
	}
	// The engine must be fully writable afterwards.
	if err := e2.Set([]byte("after"), []byte("recovery"), 0); err != nil {
		t.Fatal(err)
	}
	if _, ok := e2.Get([]byte("after")); !ok {
		t.Fatal("write after torn-tail recovery did not land")
	}
}

func TestEngineRefusesCorruptedLog(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)

	e := openEngine(t, cfg)
	for i := 0; i < 200; i++ {
		e.Set([]byte(fmt.Sprintf("k%03d", i)), []byte("value-payload"), 0)
	}
	e.Close()

	segs, _ := wal.ListSegments(cfg.WALDir())
	data, _ := os.ReadFile(segs[0].Path)
	data[len(data)/2] ^= 0xFF
	os.WriteFile(segs[0].Path, data, 0o644)

	if e2, err := Open(cfg, quietLogger()); err == nil {
		e2.Close()
		t.Fatal("engine started on a corrupted log without --unsafe-truncate")
	}

	cfg.UnsafeTruncate = true
	e3, err := Open(cfg, quietLogger())
	if err != nil {
		t.Fatalf("--unsafe-truncate should allow startup: %v", err)
	}
	e3.Close()
}

func TestEngineSnapshotSurvivesCrashDuringWrite(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)

	e := openEngine(t, cfg)
	for i := 0; i < 300; i++ {
		e.Set([]byte(fmt.Sprintf("k%03d", i)), []byte("v"), 0)
	}
	if _, err := e.Snapshot(); err != nil {
		t.Fatal(err)
	}
	e.Close()

	// Simulate a crash mid-snapshot: a .tmp file left behind. The good
	// snapshot must still load and the .tmp must be ignored.
	tmp := filepath.Join(cfg.SnapshotDir(), "0000000000009999.snap.tmp")
	if err := os.WriteFile(tmp, []byte("garbage-half-written-snapshot"), 0o644); err != nil {
		t.Fatal(err)
	}

	e2 := openEngine(t, cfg)
	defer e2.Close()
	if n := e2.Store().Len(); n != 300 {
		t.Fatalf("%d keys recovered, want 300 — a stray .tmp broke recovery", n)
	}
}

func TestEngineSubscribeFeedsReplicas(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, testConfig(dir))
	defer e.Close()

	feed, cancel := e.Subscribe(64)
	defer cancel()

	if err := e.Set([]byte("k"), []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	select {
	case rec := <-feed:
		if rec.Type != wal.RecSet || string(rec.Key) != "k" || string(rec.Value) != "v" {
			t.Fatalf("unexpected record on the feed: %+v", rec)
		}
		if rec.LSN == 0 {
			t.Fatal("replicated record has no LSN")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no record reached the replica feed")
	}
}

func TestEngineSlowReplicaIsDroppedNotBlocking(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, testConfig(dir))
	defer e.Close()

	feed, cancel := e.Subscribe(4) // deliberately tiny
	defer cancel()

	// Write far more than the feed can hold, without ever reading it. The
	// primary must not block.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			e.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v"), 0)
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a slow replica blocked the primary's write path")
	}

	// The feed should have been closed once it overflowed.
	drained := 0
	for range feed {
		drained++
	}
	t.Logf("slow feed drained %d records before being dropped", drained)
}

func BenchmarkEngineSet(b *testing.B) {
	for _, policy := range []config.FsyncPolicy{config.FsyncAlways, config.FsyncEverySec, config.FsyncNo} {
		b.Run(string(policy), func(b *testing.B) {
			dir := b.TempDir()
			cfg := testConfig(dir)
			cfg.Fsync = policy
			cfg.SegmentSize = 1 << 30
			e, err := Open(cfg, quietLogger())
			if err != nil {
				b.Fatal(err)
			}
			defer e.Close()
			val := bytes.Repeat([]byte("v"), 64)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					i++
					if err := e.Set([]byte(fmt.Sprintf("bench%d", i)), val, 0); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

func BenchmarkEngineGet(b *testing.B) {
	dir := b.TempDir()
	cfg := testConfig(dir)
	cfg.Fsync = config.FsyncNo
	e, err := Open(cfg, quietLogger())
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()
	for i := 0; i < 10000; i++ {
		e.Set([]byte(fmt.Sprintf("bench%d", i)), []byte("value"), 0)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			e.Get([]byte(fmt.Sprintf("bench%d", i%10000)))
		}
	})
}
