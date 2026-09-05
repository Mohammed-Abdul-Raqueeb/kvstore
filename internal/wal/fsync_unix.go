//go:build !windows

package wal

import "os"

// SyncDir fsyncs a directory.
//
// This is not paranoia: creating a file and fsyncing its *contents* does not
// make the directory entry durable. After a crash the file can be absent
// even though every byte you wrote to it was synced, because the parent
// directory's metadata block was still in the page cache. Every new segment
// and every snapshot rename must be followed by a directory fsync.
//
// See DESIGN.md §15 mistake #9.
func SyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// DirSyncSupported reports whether SyncDir does anything real on this OS.
func DirSyncSupported() bool { return true }
