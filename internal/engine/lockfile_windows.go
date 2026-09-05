//go:build windows

package engine

import "os"

// processAlive reports whether a process with the given PID exists.
//
// On Windows os.FindProcess opens a real handle to the process and returns
// an error when no such process exists, so it is a genuine liveness check
// (unlike on Unix, where it always succeeds for any integer).
func processAlive(pid int) bool {
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	p.Release()
	return true
}
