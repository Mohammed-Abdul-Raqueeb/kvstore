//go:build linux

package store

import (
	"os"
	"strconv"
	"strings"
)

// ResidentBytes returns the process resident set size from /proc/self/statm.
//
// Field 2 of statm is the resident page count. Multiplying by the page size
// gives RSS. This is the same source Project #3's process monitor used, and
// it is the honest number to compare the logical counter against: the Go
// runtime's HeapAlloc excludes stacks, the GC's own structures, and anything
// the allocator has reserved but not returned to the OS.
func ResidentBytes() int64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(b))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * int64(os.Getpagesize())
}
