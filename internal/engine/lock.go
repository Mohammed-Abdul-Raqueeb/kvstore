package engine

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// DirLock is an exclusive lock on a data directory.
//
// Two servers sharing one data directory silently corrupt each other: both
// assign LSNs from their own counter, both append to the same segment, and
// the resulting log is unrecoverable. This is step 1 of the recovery
// algorithm in DESIGN.md §8 for exactly that reason.
//
// The implementation is a PID file created with O_EXCL rather than flock(2)
// or LockFileEx, because those have no portable equivalent and this project
// must run on Windows. The tradeoff: an O_EXCL PID file cannot detect a
// process that died without cleaning up, so we add explicit staleness
// detection — if the recorded PID is not alive, the lock is broken and
// retaken. That matters here specifically because the crash-recovery test
// harness SIGKILLs the server and restarts it, which is precisely the
// "died without cleaning up" case.
//
// Known limitation: PID reuse. If the recorded PID has been recycled by an
// unrelated process, we will conservatively refuse to start. Refusing is the
// safe direction to be wrong in.
type DirLock struct {
	path string
	held bool
}

// AcquireDirLock takes the lock at path.
func AcquireDirLock(path string) (*DirLock, error) {
	l := &DirLock{path: path}
	if err := l.tryCreate(); err == nil {
		l.held = true
		return l, nil
	} else if !os.IsExist(err) {
		return nil, err
	}

	// The file exists. Decide whether it is live or stale.
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read lock file %s: %w", path, err)
	}
	pid, perr := strconv.Atoi(strings.TrimSpace(strings.SplitN(string(data), "\n", 2)[0]))
	// Note: a PID equal to our own counts as live. An earlier version
	// excluded it, which let one process open the same data directory
	// twice — exactly the corruption this lock exists to prevent.
	if perr == nil && pid > 0 && processAlive(pid) {
		return nil, fmt.Errorf("data directory is locked by a running process (pid %d); "+
			"if that is wrong, remove %s", pid, path)
	}

	// Stale (or unparseable). Break it and retake.
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("remove stale lock %s: %w", path, err)
	}
	if err := l.tryCreate(); err != nil {
		return nil, fmt.Errorf("acquire lock after clearing a stale one: %w", err)
	}
	l.held = true
	return l, nil
}

func (l *DirLock) tryCreate() error {
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := fmt.Fprintf(f, "%d\n", os.Getpid()); err != nil {
		return err
	}
	return f.Sync()
}

// Release removes the lock file. Safe to call more than once.
func (l *DirLock) Release() error {
	if l == nil || !l.held {
		return nil
	}
	l.held = false
	if err := os.Remove(l.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
