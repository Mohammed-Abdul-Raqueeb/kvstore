package crash

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/raqueeb/kvstore/internal/client"
	"github.com/raqueeb/kvstore/internal/wal"
	"github.com/raqueeb/kvstore/test/harness"
)

// The crash test suite (DESIGN.md §8, Milestone 8).
//
// The central claim of this project is "acknowledged writes survive a
// crash". This is the only code that actually tests it. Everything else
// tests that the pieces behave; this tests that the promise holds.
//
// Method: run a real server as a subprocess, have writers record every key
// the server said OK to, SIGKILL the process at a random moment, restart,
// and assert that every acked key is present with the right value and that
// nothing appeared that was never sent.
//
// A goroutine cannot be SIGKILLed, so this suite requires a subprocess. It
// skips itself if the Go toolchain is unavailable to build one.

const defaultIterations = 12

func iterations(t *testing.T) int {
	if testing.Short() {
		return 3
	}
	if v := os.Getenv("KV_CRASH_ITERATIONS"); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			return n
		}
	}
	return defaultIterations
}

// ackLog records writes the server acknowledged, fsynced as it goes.
//
// The log has to be durable for the same reason the WAL does: if the test
// process dies alongside the server, a buffered ack list would be worthless.
// More importantly it must be written AFTER the OK is received, never
// before — recording an intent and then asserting durability of the intent
// would test nothing.
type ackLog struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
	m  map[string]string
}

func newAckLog(t *testing.T, path string) *ackLog {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	return &ackLog{f: f, w: bufio.NewWriter(f), m: make(map[string]string)}
}

func (a *ackLog) record(key, value string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.m[key] = value
	fmt.Fprintf(a.w, "%s\t%s\n", key, value)
	a.w.Flush()
	a.f.Sync()
}

func (a *ackLog) snapshot() map[string]string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string]string, len(a.m))
	for k, v := range a.m {
		out[k] = v
	}
	return out
}

func (a *ackLog) close() { a.w.Flush(); a.f.Close() }

func TestCrashRecoveryAckedWritesSurvive(t *testing.T) {
	fsyncPolicies := []string{"always"}
	if !testing.Short() {
		// `everysec` is included deliberately: it is EXPECTED to lose up to
		// a second of acknowledged writes, so the assertion for it is
		// different. Running it here documents the difference rather than
		// pretending all three policies give the same guarantee.
		fsyncPolicies = append(fsyncPolicies, "everysec")
	}

	for _, policy := range fsyncPolicies {
		t.Run("fsync="+policy, func(t *testing.T) {
			n := iterations(t)
			for iter := 0; iter < n; iter++ {
				runCrashIteration(t, iter, policy)
			}
		})
	}
}

func runCrashIteration(t *testing.T, iter int, fsyncPolicy string) {
	dir := filepath.Join(t.TempDir(), fmt.Sprintf("iter%d", iter))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	srv := harness.StartSubprocess(t, dir, "--fsync", fsyncPolicy, "--log-level", "warn")
	acks := newAckLog(t, filepath.Join(dir, "acked.log"))
	defer acks.close()

	const writers = 6
	stop := make(chan struct{})
	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c, err := srv.Client()
			if err != nil {
				return
			}
			defer c.Close()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("w%d-k%06d", w, i)
				val := fmt.Sprintf("value-%d-%d", w, i)
				if err := c.Set([]byte(key), []byte(val), 0); err != nil {
					// The server died mid-write. That is the point of the
					// test; this write is simply not acknowledged.
					return
				}
				// Only now, after OK, is this write a durability claim.
				acks.record(key, val)
			}
		}(w)
	}

	// Kill at a random moment while writes are in flight.
	delay := time.Duration(200+rand.Intn(800)) * time.Millisecond
	time.Sleep(delay)
	srv.Kill()
	close(stop)
	wg.Wait()

	expected := acks.snapshot()
	if len(expected) == 0 {
		t.Fatalf("iteration %d: no writes were acknowledged before the kill; the test proved nothing", iter)
	}

	// Restart on the same data directory. The lock file is stale (the
	// process was SIGKILLed and never released it), so this also exercises
	// stale-lock detection.
	restarted := harness.StartSubprocess(t, dir, "--fsync", fsyncPolicy, "--log-level", "warn")
	defer restarted.Stop()

	c, err := restarted.Client()
	if err != nil {
		t.Fatalf("iteration %d: cannot connect after restart: %v\n%s", iter, err, restarted.Log())
	}
	defer c.Close()

	missing, wrong := 0, 0
	var firstMissing string
	for k, want := range expected {
		got, err := c.Get([]byte(k))
		if err == client.ErrNotFound {
			missing++
			if firstMissing == "" {
				firstMissing = k
			}
			continue
		}
		if err != nil {
			t.Fatalf("iteration %d: GET %q after restart: %v", iter, k, err)
		}
		if string(got) != want {
			wrong++
		}
	}

	// Wrong values are never acceptable under any policy: returning data
	// that was never written is worse than losing data.
	if wrong > 0 {
		t.Fatalf("iteration %d: %d keys came back with the WRONG value after recovery", iter, wrong)
	}

	switch fsyncPolicy {
	case "always":
		// The strict guarantee: nothing acknowledged may be lost.
		if missing > 0 {
			t.Fatalf("iteration %d (fsync=always): %d of %d acknowledged writes were LOST after kill -9 (first: %q)\nserver log:\n%s",
				iter, missing, len(expected), firstMissing, restarted.Log())
		}
	case "everysec":
		// IMPORTANT CAVEAT about what this test can and cannot prove:
		// SIGKILL destroys the process but NOT the OS page cache, so data
		// that reached write() but not fsync() still gets flushed by the
		// kernel afterwards. `everysec` therefore usually loses nothing
		// here. Only a power failure or a kernel panic loses page-cache
		// contents, and neither can be simulated from userspace.
		//
		// So: this suite proves `always` is durable against process death.
		// It does NOT prove `everysec` is durable against machine death —
		// nothing short of pulling the plug (or dm-flakey / a VM with
		// disabled write caching) can test that. The assertion below is
		// therefore a bound, not a guarantee of zero loss.
		lossRate := float64(missing) / float64(len(expected))
		if lossRate > 0.5 {
			t.Fatalf("iteration %d (fsync=everysec): lost %d of %d acked writes (%.0f%%); far more than one second's worth",
				iter, missing, len(expected), lossRate*100)
		}
		if missing > 0 {
			t.Logf("iteration %d (fsync=everysec): lost %d of %d acked writes (%.2f%%) — expected for this policy",
				iter, missing, len(expected), lossRate*100)
		}
	}

	// A torn tail is normal after kill -9. More than one warning would mean
	// we truncated in several places, which is not.
	log := restarted.Log()
	if n := strings.Count(log, "torn tail truncated"); n > 1 {
		t.Fatalf("iteration %d: recovery emitted %d torn-tail warnings; expected at most 1\n%s", iter, n, log)
	}

	// And the recovered server must be fully usable.
	if err := c.Set([]byte("post-recovery"), []byte("ok"), 0); err != nil {
		t.Fatalf("iteration %d: server not writable after recovery: %v", iter, err)
	}
	t.Logf("iteration %d (%s): %d acked writes, %d missing, killed after %v",
		iter, fsyncPolicy, len(expected), missing, delay)
}

// TestCrashDuringSnapshot kills the server while a snapshot is being written
// and asserts the previous good snapshot is still usable.
func TestCrashDuringSnapshot(t *testing.T) {
	dir := t.TempDir()
	srv := harness.StartSubprocess(t, dir, "--fsync", "always", "--log-level", "warn")

	c, err := srv.Client()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3000; i++ {
		if err := c.Set([]byte(fmt.Sprintf("k%05d", i)), []byte(strings.Repeat("v", 200)), 0); err != nil {
			t.Fatal(err)
		}
	}
	// A known-good snapshot first.
	if _, err := c.Snapshot(); err != nil {
		t.Fatal(err)
	}
	for i := 3000; i < 5000; i++ {
		if err := c.Set([]byte(fmt.Sprintf("k%05d", i)), []byte(strings.Repeat("v", 200)), 0); err != nil {
			t.Fatal(err)
		}
	}
	c.Close()

	// Now start a snapshot and kill mid-flight.
	go func() {
		c2, err := srv.Client()
		if err != nil {
			return
		}
		defer c2.Close()
		_, _ = c2.Snapshot()
	}()
	time.Sleep(time.Duration(2+rand.Intn(8)) * time.Millisecond)
	srv.Kill()

	restarted := harness.StartSubprocess(t, dir, "--fsync", "always", "--log-level", "warn")
	defer restarted.Stop()
	c3, err := restarted.Client()
	if err != nil {
		t.Fatalf("cannot start after a crash during snapshot: %v\n%s", err, restarted.Log())
	}
	defer c3.Close()

	// Every key written before the kill must still be there, whether it came
	// from the snapshot or the WAL.
	for i := 0; i < 5000; i += 97 {
		k := fmt.Sprintf("k%05d", i)
		if _, err := c3.Get([]byte(k)); err != nil {
			t.Fatalf("key %q lost after a crash during snapshot: %v\n%s", k, err, restarted.Log())
		}
	}
}

// TestDeliberateWALCorruption asserts the server either recovers cleanly or
// refuses to start — but never returns wrong data.
func TestDeliberateWALCorruption(t *testing.T) {
	cases := []struct {
		name        string
		corrupt     func(t *testing.T, walDir string)
		mustRefuse  bool
		description string
	}{
		{
			name: "truncate final segment at a random offset",
			corrupt: func(t *testing.T, walDir string) {
				seg := lastSegment(t, walDir)
				fi, _ := os.Stat(seg)
				cut := fi.Size() / 2
				if err := os.Truncate(seg, cut); err != nil {
					t.Fatal(err)
				}
			},
			mustRefuse:  false,
			description: "a torn tail is the expected crash artefact; recovery truncates and continues",
		},
		{
			name: "flip a bit in the final segment",
			corrupt: func(t *testing.T, walDir string) {
				seg := lastSegment(t, walDir)
				d, err := os.ReadFile(seg)
				if err != nil {
					t.Fatal(err)
				}
				d[len(d)/2] ^= 0x20
				os.WriteFile(seg, d, 0o644)
			},
			mustRefuse:  true,
			description: "a bad CRC on a complete record is corruption, not a torn tail",
		},
		{
			name: "append garbage to the final segment",
			corrupt: func(t *testing.T, walDir string) {
				seg := lastSegment(t, walDir)
				f, _ := os.OpenFile(seg, os.O_APPEND|os.O_WRONLY, 0o644)
				f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00})
				f.Close()
			},
			mustRefuse:  false,
			description: "trailing bytes too short to be a record are a torn tail",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			srv := harness.StartSubprocess(t, dir, "--fsync", "always", "--log-level", "warn")

			c, err := srv.Client()
			if err != nil {
				t.Fatal(err)
			}
			written := map[string]string{}
			for i := 0; i < 500; i++ {
				k, v := fmt.Sprintf("k%04d", i), fmt.Sprintf("value%04d", i)
				if err := c.Set([]byte(k), []byte(v), 0); err != nil {
					t.Fatal(err)
				}
				written[k] = v
			}
			c.Close()
			if err := srv.Stop(); err != nil {
				t.Fatal(err)
			}

			tc.corrupt(t, filepath.Join(dir, "wal"))

			// Try to restart. Either outcome is acceptable; wrong data is not.
			bin := harness.ServerBinary(t)
			port := harness.FreePort(t)
			out, startErr := tryStart(t, bin, dir, port)

			if tc.mustRefuse {
				if startErr == nil {
					t.Fatalf("server started on a corrupted log; it must refuse.\n%s\n(%s)", out, tc.description)
				}
				if !strings.Contains(out, "corrupt") && !strings.Contains(out, "unsafe-truncate") {
					t.Fatalf("server refused but without explaining why:\n%s", out)
				}
				t.Logf("correctly refused: %s", firstLine(out))
				return
			}

			if startErr != nil {
				t.Fatalf("server refused to start on a torn tail, which is a normal crash artefact:\n%s\n(%s)",
					out, tc.description)
			}

			// It started. Whatever survived must be CORRECT.
			restarted := harness.StartSubprocess(t, dir, "--fsync", "always", "--log-level", "warn")
			defer restarted.Stop()
			c2, err := restarted.Client()
			if err != nil {
				t.Fatal(err)
			}
			defer c2.Close()

			survivors, wrong := 0, 0
			for k, want := range written {
				got, err := c2.Get([]byte(k))
				if err == client.ErrNotFound {
					continue
				}
				if err != nil {
					t.Fatalf("GET %q: %v", k, err)
				}
				survivors++
				if string(got) != want {
					wrong++
				}
			}
			if wrong > 0 {
				t.Fatalf("%d keys returned WRONG values after recovering from corruption", wrong)
			}
			t.Logf("%d of %d keys survived, none wrong", survivors, len(written))
		})
	}
}

// tryStart launches the server and reports whether it stayed up.
func tryStart(t *testing.T, bin, dataDir string, port int) (string, error) {
	t.Helper()
	logPath := filepath.Join(dataDir, "trystart.log")
	f, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	cmd := startCmd(bin, fmt.Sprintf("127.0.0.1:%d", port), dataDir)
	cmd.Stdout = f
	cmd.Stderr = f
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	// Give it a moment to either bind or die.
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		out, _ := os.ReadFile(logPath)
		return string(out), fmt.Errorf("server exited: %v", err)
	case <-time.After(3 * time.Second):
		cmd.Process.Kill()
		<-done
		out, _ := os.ReadFile(logPath)
		return string(out), nil
	}
}

func lastSegment(t *testing.T, walDir string) string {
	t.Helper()
	segs, err := wal.ListSegments(walDir)
	if err != nil || len(segs) == 0 {
		t.Fatalf("no segments in %s (err=%v)", walDir, err)
	}
	return segs[len(segs)-1].Path
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
