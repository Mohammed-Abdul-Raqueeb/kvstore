package snapshot

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/raqueeb/kvstore/internal/wal"
)

// Snapshot file format. Little-endian, consistent with the wire protocol and
// the WAL.
//
//	header (32 bytes)
//	  0   8  magic "KVSNAP01"
//	  8   4  format version
//	  12  8  created_at_ms
//	  20  8  last_included_lsn
//	  28  4  reserved (must be 0)
//
//	entries (repeated, entry_count times)
//	  0   2  key_len:u16
//	  2   4  val_len:u32
//	  6   8  expire_at_ms:u64
//	  14  ..  key bytes
//	  ..  ..  value bytes
//
//	footer (20 bytes)
//	  0   8  magic "KVSNAPFT"
//	  8   8  entry_count:u64
//	  16  4  crc32c over bytes [0, footer_start)
//
// The CRC lives in the footer and covers everything before it, so a snapshot
// is only valid if it was written to completion. A half-written file has no
// footer at all and is rejected without needing any special "is this
// finished?" flag. That is what makes "pick the newest snapshot whose footer
// CRC validates" (DESIGN.md §8 step 2) a complete recovery rule.
const (
	HeaderLen = 32
	FooterLen = 20

	FormatVersion uint32 = 1

	entryHeaderLen = 14
	maxKeyLen      = 1<<16 - 1
	maxValLen      = 16 << 20

	snapshotExt = ".snap"
	tmpExt      = ".tmp"
)

var (
	headerMagic = []byte("KVSNAP01")
	footerMagic = []byte("KVSNAPFT")
	castagnoli  = crc32.MakeTable(crc32.Castagnoli)
)

// Entry is one key/value pair in a snapshot.
type Entry struct {
	Key        []byte
	Value      []byte
	ExpireAtMs uint64
}

// Name returns the file name for a snapshot at the given LSN. Zero-padded so
// lexical order matches numeric order.
func Name(lastIncludedLSN uint64) string {
	return fmt.Sprintf("%016d%s", lastIncludedLSN, snapshotExt)
}

// Writer streams entries into a snapshot file.
type Writer struct {
	dir       string
	tmpPath   string
	finalPath string
	f         *os.File
	buf       []byte
	crc       uint32
	count     uint64
	lsn       uint64
	written   int64
	closed    bool
}

// Create opens a new snapshot for writing.
//
// The file is written under a .tmp name and only renamed into place once it
// is complete and fsynced. Writing in place would mean a crash mid-write
// destroys the only good snapshot you had — mistake #10 in DESIGN.md §15.
func Create(dir string, lastIncludedLSN, createdAtMs uint64) (*Writer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	final := filepath.Join(dir, Name(lastIncludedLSN))
	tmp := final + tmpExt

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create snapshot tmp: %w", err)
	}

	w := &Writer{dir: dir, tmpPath: tmp, finalPath: final, f: f, lsn: lastIncludedLSN}

	hdr := make([]byte, HeaderLen)
	copy(hdr[0:8], headerMagic)
	binary.LittleEndian.PutUint32(hdr[8:12], FormatVersion)
	binary.LittleEndian.PutUint64(hdr[12:20], createdAtMs)
	binary.LittleEndian.PutUint64(hdr[20:28], lastIncludedLSN)
	if err := w.write(hdr); err != nil {
		w.abort()
		return nil, err
	}
	return w, nil
}

func (w *Writer) write(b []byte) error {
	n, err := w.f.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	w.crc = crc32.Update(w.crc, castagnoli, b)
	w.written += int64(n)
	return nil
}

// Add appends one entry.
func (w *Writer) Add(e Entry) error {
	if len(e.Key) > maxKeyLen {
		return fmt.Errorf("snapshot: key of %d bytes exceeds limit", len(e.Key))
	}
	if len(e.Value) > maxValLen {
		return fmt.Errorf("snapshot: value of %d bytes exceeds limit", len(e.Value))
	}
	w.buf = w.buf[:0]
	var hdr [entryHeaderLen]byte
	binary.LittleEndian.PutUint16(hdr[0:2], uint16(len(e.Key)))
	binary.LittleEndian.PutUint32(hdr[2:6], uint32(len(e.Value)))
	binary.LittleEndian.PutUint64(hdr[6:14], e.ExpireAtMs)
	w.buf = append(w.buf, hdr[:]...)
	w.buf = append(w.buf, e.Key...)
	w.buf = append(w.buf, e.Value...)
	if err := w.write(w.buf); err != nil {
		return err
	}
	w.count++
	return nil
}

// Commit writes the footer, fsyncs, renames into place, and fsyncs the
// directory. After it returns nil the snapshot is durable and loadable.
func (w *Writer) Commit() (string, error) {
	if w.closed {
		return "", fmt.Errorf("snapshot: already closed")
	}
	w.closed = true

	footer := make([]byte, FooterLen)
	copy(footer[0:8], footerMagic)
	binary.LittleEndian.PutUint64(footer[8:16], w.count)
	// The CRC covers everything up to (not including) the CRC field itself,
	// so fold in the footer's own magic and count first.
	crc := crc32.Update(w.crc, castagnoli, footer[0:16])
	binary.LittleEndian.PutUint32(footer[16:20], crc)

	if n, err := w.f.Write(footer); err != nil || n != len(footer) {
		w.abort()
		return "", fmt.Errorf("write snapshot footer: %w", err)
	}
	// Order matters and is not negotiable:
	//   1. fsync the temp file    — contents are on stable storage
	//   2. rename over the target — atomic on POSIX and on Windows
	//                               (MoveFileEx with REPLACE_EXISTING)
	//   3. fsync the directory    — the *name* is on stable storage
	// Skipping (3) means a crash can leave the new file's contents durable
	// but its directory entry missing.
	if err := w.f.Sync(); err != nil {
		w.abort()
		return "", fmt.Errorf("sync snapshot: %w", err)
	}
	if err := w.f.Close(); err != nil {
		os.Remove(w.tmpPath)
		return "", fmt.Errorf("close snapshot: %w", err)
	}
	if err := os.Rename(w.tmpPath, w.finalPath); err != nil {
		os.Remove(w.tmpPath)
		return "", fmt.Errorf("rename snapshot: %w", err)
	}
	if err := wal.SyncDir(w.dir); err != nil {
		return "", fmt.Errorf("sync snapshot dir: %w", err)
	}
	return w.finalPath, nil
}

// Abort discards a partially written snapshot.
func (w *Writer) Abort() {
	if w.closed {
		return
	}
	w.closed = true
	w.abort()
}

func (w *Writer) abort() {
	if w.f != nil {
		w.f.Close()
	}
	os.Remove(w.tmpPath)
}

// Count returns the number of entries written so far.
func (w *Writer) Count() uint64 { return w.count }

// Bytes returns the number of bytes written so far.
func (w *Writer) Bytes() int64 { return w.written }

// LastIncludedLSN returns the LSN this snapshot covers up to.
func (w *Writer) LastIncludedLSN() uint64 { return w.lsn }
