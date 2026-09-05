//go:build !windows

package harness

import (
	"os"
	"syscall"
)

func signalTerm(p *os.Process) error { return p.Signal(syscall.SIGTERM) }

// SignalStop pauses a process without killing it (SIGSTOP).
//
// This is the case that breaks naive failure detectors: the process is
// alive, its TCP connections stay open, but it answers nothing. A detector
// that only checks "is the socket up" will call it healthy forever.
// Unix only — Windows has no equivalent that leaves sockets intact.
func SignalStop(p *os.Process) error { return p.Signal(syscall.SIGSTOP) }

// SignalCont resumes a paused process.
func SignalCont(p *os.Process) error { return p.Signal(syscall.SIGCONT) }

// SupportsPause reports whether SIGSTOP/SIGCONT are available.
func SupportsPause() bool { return true }
