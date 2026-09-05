//go:build !linux

package store

import "runtime"

// ResidentBytes has no portable equivalent to /proc/self/statm.
//
// On Windows and macOS this returns the Go runtime's own view of memory
// obtained from the OS (HeapSys + StackSys + other reserved spans), which is
// a reasonable proxy but is NOT the same number: it excludes the binary's
// text and data segments and anything allocated outside the Go heap, and it
// counts reserved-but-untouched address space that the OS has not actually
// backed with pages.
//
// The memory-accounting calibration in docs/BENCHMARKS.md is therefore
// reported from Linux only. This is one of the documented Linux-only pieces
// of tooling; the server itself runs unchanged.
func ResidentBytes() int64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return int64(ms.Sys - ms.HeapReleased)
}
