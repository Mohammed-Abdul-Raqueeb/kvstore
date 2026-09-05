//go:build windows

package wal

// SyncDir is a no-op on Windows.
//
// Windows has no equivalent of fsync(2) on a directory handle: CreateFile
// with FILE_FLAG_BACKUP_SEMANTICS can open a directory, but FlushFileBuffers
// on that handle returns ERROR_ACCESS_DENIED. NTFS does journal its own
// metadata, so a created file's directory entry is generally recoverable
// after a crash, but that is a filesystem property we are relying on rather
// than a guarantee we are enforcing.
//
// DELIBERATE DEVIATION FROM DESIGN.md §5, recorded in docs/DECISIONS.md
// (ADR-011): on Windows the durability guarantee for *segment creation* and
// *snapshot rename* is weaker than on Linux. Record contents are still
// fsynced normally via File.Sync, so the `always` policy still means "this
// record is on stable storage" — what is weakened is only the durability of
// the directory entry for a brand-new file.
func SyncDir(dir string) error { return nil }

// DirSyncSupported reports whether SyncDir does anything real on this OS.
func DirSyncSupported() bool { return false }
