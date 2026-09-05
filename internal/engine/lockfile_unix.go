//go:build !windows

package engine

import "syscall"

// processAlive reports whether a process with the given PID exists.
//
// Signal 0 performs the permission and existence checks without delivering
// anything, which is the standard way to probe liveness on POSIX. EPERM
// means the process exists but belongs to another user, which still counts
// as alive — refusing to steal its lock is the correct behaviour.
func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true
	}
	return err == syscall.EPERM
}
