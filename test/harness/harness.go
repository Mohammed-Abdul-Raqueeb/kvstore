// Package harness provides shared scaffolding for the integration, chaos,
// differential and crash test suites.
//
// Two flavours of server:
//
//   - InProcess: engine + server in this process, bound to port 0. Fast,
//     debuggable, works under -race. Used for everything except crash tests.
//   - Subprocess: a real kvserver binary that can be SIGKILLed. The only
//     honest way to test crash recovery — you cannot kill -9 a goroutine,
//     and anything that simulates a crash in-process is testing your
//     simulation rather than your durability.
package harness

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/raqueeb/kvstore/internal/client"
	"github.com/raqueeb/kvstore/internal/config"
	"github.com/raqueeb/kvstore/internal/engine"
	"github.com/raqueeb/kvstore/internal/server"
)

// QuietLogger discards output, keeping test logs readable.
func QuietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// DefaultConfig returns a config suited to tests: a temp data dir, a free
// port, and small enough limits that boundary behaviour is reachable.
func DefaultConfig(dir string) config.Config {
	c := config.Default()
	c.DataDir = dir
	c.Addr = "127.0.0.1:0"
	c.Shards = 8
	c.Fsync = config.FsyncAlways
	c.SegmentSize = 1 << 20
	c.GroupCommitMax = 64
	c.GroupCommitWait = time.Millisecond
	c.IdleTimeout = 30 * time.Second
	c.NodeID = "test"
	return c
}

// InProcess is a server running inside the test binary.
type InProcess struct {
	Cfg    config.Config
	Engine *engine.Engine
	Server *server.Server
	Addr   string

	closeOnce sync.Once
}

// Start builds and starts an in-process server.
func Start(t *testing.T, cfg config.Config) *InProcess {
	t.Helper()
	eng, err := engine.Open(cfg, QuietLogger())
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	srv := server.New(cfg, eng, QuietLogger())
	if err := srv.Start(); err != nil {
		eng.Close()
		t.Fatalf("server.Start: %v", err)
	}
	ip := &InProcess{Cfg: cfg, Engine: eng, Server: srv, Addr: srv.Addr().String()}
	t.Cleanup(ip.Stop)
	return ip
}

// StartDefault starts a server with the default test config in a temp dir.
func StartDefault(t *testing.T) *InProcess {
	t.Helper()
	return Start(t, DefaultConfig(t.TempDir()))
}

// Stop shuts the server and engine down.
func (s *InProcess) Stop() {
	s.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.Server.Shutdown(ctx)
		_ = s.Engine.Close()
	})
}

// Client dials the server and registers cleanup.
func (s *InProcess) Client(t *testing.T) *client.Client {
	t.Helper()
	c, err := client.DialWithOptions(client.Options{Addr: s.Addr, Timeout: 15 * time.Second})
	if err != nil {
		t.Fatalf("dial %s: %v", s.Addr, err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// RawConn opens a raw TCP connection, for tests that send malformed bytes.
func (s *InProcess) RawConn(t *testing.T) net.Conn {
	t.Helper()
	nc, err := net.DialTimeout("tcp", s.Addr, 10*time.Second)
	if err != nil {
		t.Fatalf("raw dial: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc
}

// --- subprocess servers ----------------------------------------------------

// Subprocess is a real kvserver process that can be killed.
type Subprocess struct {
	Cmd     *exec.Cmd
	Addr    string
	DataDir string
	LogPath string
	t       *testing.T

	waitOnce sync.Once
	waitErr  error
}

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

// ServerBinary builds cmd/kvserver once per test run and returns its path.
//
// Skips (rather than fails) when the Go toolchain is unavailable, so that a
// packaged test run without a compiler degrades gracefully instead of
// reporting a false failure.
func ServerBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		if _, err := exec.LookPath("go"); err != nil {
			buildErr = fmt.Errorf("go toolchain not in PATH: %w", err)
			return
		}
		dir, err := os.MkdirTemp("", "kvbin")
		if err != nil {
			buildErr = err
			return
		}
		out := filepath.Join(dir, "kvserver")
		if runtime.GOOS == "windows" {
			out += ".exe"
		}
		cmd := exec.Command("go", "build", "-o", out, "github.com/raqueeb/kvstore/cmd/kvserver")
		cmd.Dir = repoRoot()
		if b, err := cmd.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("go build kvserver: %v\n%s", err, b)
			return
		}
		binPath = out
	})
	if buildErr != nil {
		t.Skipf("cannot build kvserver: %v", buildErr)
	}
	return binPath
}

func repoRoot() string {
	// The test binary runs in its package directory; walk up to the module
	// root by looking for go.mod.
	dir, err := os.Getwd()
	if err != nil {
		return "."
	}
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	return "."
}

// FreePort asks the OS for an unused TCP port.
//
// There is an inherent race between closing this listener and the server
// binding it. Tests accept that: the alternative is passing port 0 to the
// subprocess and parsing its log, which couples the test to log formatting.
func FreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// StartSubprocess launches a real kvserver on a free port and waits for it
// to accept connections.
func StartSubprocess(t *testing.T, dataDir string, extraArgs ...string) *Subprocess {
	t.Helper()
	return StartSubprocessAt(t, dataDir, fmt.Sprintf("127.0.0.1:%d", FreePort(t)), extraArgs...)
}

// StartSubprocessAt launches a kvserver on a specific address. Used when a
// primary has to come back on the address its replicas already know.
func StartSubprocessAt(t *testing.T, dataDir, addr string, extraArgs ...string) *Subprocess {
	t.Helper()
	bin := ServerBinary(t)

	logPath := filepath.Join(dataDir, fmt.Sprintf("server-%d.log", time.Now().UnixNano()))
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}

	args := append([]string{"--addr", addr, "--data-dir", dataDir}, extraArgs...)
	cmd := exec.Command(bin, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatalf("start kvserver: %v", err)
	}

	s := &Subprocess{Cmd: cmd, Addr: addr, DataDir: dataDir, LogPath: logPath, t: t}
	if err := s.waitReady(20 * time.Second); err != nil {
		s.Kill()
		logFile.Close()
		t.Fatalf("kvserver did not become ready: %v\n%s", err, s.Log())
	}
	logFile.Close()
	return s
}

func (s *Subprocess) waitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := client.DialWithOptions(client.Options{Addr: s.Addr, Timeout: time.Second})
		if err == nil {
			perr := c.Ping()
			c.Close()
			if perr == nil {
				return nil
			}
		}
		if s.Cmd.ProcessState != nil && s.Cmd.ProcessState.Exited() {
			return fmt.Errorf("process exited early with %v", s.Cmd.ProcessState)
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("timed out after %v", timeout)
}

// wait waits for the process to exit, memoizing the result so repeated
// Kill/Stop calls only ever issue one real wait.
//
// Tests routinely kill and restart a server mid-test (see
// TestReplicaResumesAfterPrimaryRestart, which calls primary.Kill() directly
// and then relies on startPair's t.Cleanup(func() { primary.Stop() }) to
// also run against the very same, already-dead Subprocess) — so a second
// Kill/Stop on an already-reaped process is expected, not a bug in the
// tests. On POSIX a redundant os.Process.Wait() call just returns "no child
// processes" immediately, so it used to be harmless there. On Windows it is
// not: it re-issues WaitForSingleObject on a process handle value that the
// OS is free to recycle for an unrelated object the instant it's closed,
// and that call can then block forever waiting on whatever that handle now
// refers to — which is exactly the hang seen in CI (a 20 minute test
// timeout inside a second, redundant Stop()). Memoizing with sync.Once
// ensures the real wait syscall only ever runs once per process.
func (s *Subprocess) wait() error {
	s.waitOnce.Do(func() {
		_, s.waitErr = s.Cmd.Process.Wait()
	})
	return s.waitErr
}

// Kill terminates the process ungracefully — SIGKILL on Unix,
// TerminateProcess on Windows. Neither gives the server a chance to flush,
// which is exactly the point. Safe to call more than once, including after
// Stop.
func (s *Subprocess) Kill() {
	if s.Cmd.Process != nil {
		_ = s.Cmd.Process.Kill()
		_ = s.wait()
	}
}

// Stop terminates the process gracefully and waits for it to exit. Safe to
// call more than once, including after Kill.
func (s *Subprocess) Stop() error {
	if s.Cmd.Process == nil {
		return nil
	}
	if err := signalTerm(s.Cmd.Process); err != nil {
		s.Kill()
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- s.wait() }()
	select {
	case <-done:
		return nil
	case <-time.After(15 * time.Second):
		s.Kill()
		return fmt.Errorf("graceful stop timed out")
	}
}

// Client dials the subprocess server.
func (s *Subprocess) Client() (*client.Client, error) {
	return client.DialWithOptions(client.Options{Addr: s.Addr, Timeout: 15 * time.Second})
}

// Log returns the server's captured output.
func (s *Subprocess) Log() string {
	b, err := os.ReadFile(s.LogPath)
	if err != nil {
		return ""
	}
	return string(b)
}

// LogContains reports whether the server log mentions substr.
func (s *Subprocess) LogContains(substr string) bool {
	return strings.Contains(s.Log(), substr)
}
