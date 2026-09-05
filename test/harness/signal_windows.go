//go:build windows

package harness

import (
	"errors"
	"os"
)

// signalTerm has no true equivalent on Windows for a console-less child, so
// the caller falls back to Kill. Graceful-shutdown tests are therefore
// Linux/macOS only; the shutdown path itself is exercised by the in-process
// suite, which calls Server.Shutdown directly on every platform.
func signalTerm(p *os.Process) error { return errors.New("SIGTERM is not supported on Windows") }

// SignalStop is unavailable on Windows.
func SignalStop(p *os.Process) error { return errors.New("SIGSTOP is not supported on Windows") }

// SignalCont is unavailable on Windows.
func SignalCont(p *os.Process) error { return errors.New("SIGCONT is not supported on Windows") }

// SupportsPause reports whether SIGSTOP/SIGCONT are available.
func SupportsPause() bool { return false }
