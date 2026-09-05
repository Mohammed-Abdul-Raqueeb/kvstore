package snapshot

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Errors from loading.
var (
	ErrBadMagic   = errors.New("snapshot: bad header magic")
	ErrBadVersion = errors.New("snapshot: unsupported format version")
	ErrIncomplete = errors.New("snapshot: file is shorter than header+footer")
	ErrBadFooter  = errors.New("snapshot: bad footer magic")
	ErrBadCRC     = errors.New("snapshot: CRC mismatch")
	ErrEntryCount = errors.New("snapshot: entry count does not match contents")
	ErrNoSnapshot = errors.New("snapshot: none available")
)

// Info identifies a snapshot file on disk.
type Info struct {
	Path            string
	LastIncludedLSN uint64
	Size            int64
}

// List returns every snapshot in dir, newest (highest LSN) first.
func List(dir string) ([]Info, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []Info
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), snapshotExt) {
			continue
		}
		numPart := strings.TrimSuffix(e.Name(), snapshotExt)
		lsn, err := strconv.ParseUint(numPart, 10, 64)
		if err != nil {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			return nil, err
		}
		out = append(out, Info{Path: filepath.Join(dir, e.Name()), LastIncludedLSN: lsn, Size: fi.Size()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LastIncludedLSN > out[j].LastIncludedLSN })
	return out, nil
}

// Header describes a snapshot's metadata.
type Header struct {
	Version         uint32
	CreatedAtMs     uint64
	LastIncludedLSN uint64
	EntryCount      uint64
}

// Load reads a snapshot, calling fn for each entry.
//
// The whole file is verified before ANY entry reaches fn. Streaming entries
// out while the CRC is still unknown would mean a corrupt snapshot could
// half-populate the store before being rejected, and the caller would then
// have to unwind a partial load. Verifying first costs one extra pass over
// the file and removes an entire class of recovery bug.
//
// fn receives slices aliasing an internal buffer; copy anything retained.
func Load(path string, fn func(Entry) error) (Header, error) {
	var hdr Header

	f, err := os.Open(path)
	if err != nil {
		return hdr, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return hdr, err
	}
	size := st.Size()
	if size < HeaderLen+FooterLen {
		return hdr, fmt.Errorf("%w: %d bytes", ErrIncomplete, size)
	}

	// Read and check the footer first: it tells us whether the file is
	// complete before we spend time on the body.
	footer := make([]byte, FooterLen)
	if _, err := f.ReadAt(footer, size-FooterLen); err != nil {
		return hdr, err
	}
	if !bytes.Equal(footer[0:8], footerMagic) {
		return hdr, ErrBadFooter
	}
	hdr.EntryCount = binary.LittleEndian.Uint64(footer[8:16])
	wantCRC := binary.LittleEndian.Uint32(footer[16:20])

	// Verification pass: CRC everything from byte 0 up to the CRC field.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return hdr, err
	}
	var crc uint32
	if err := crcRange(f, size-4, &crc); err != nil {
		return hdr, err
	}
	if crc != wantCRC {
		return hdr, fmt.Errorf("%w: computed %08x, stored %08x", ErrBadCRC, crc, wantCRC)
	}

	// Content pass.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return hdr, err
	}
	r := bufio.NewReaderSize(f, 1<<20)

	head := make([]byte, HeaderLen)
	if _, err := io.ReadFull(r, head); err != nil {
		return hdr, err
	}
	if !bytes.Equal(head[0:8], headerMagic) {
		return hdr, ErrBadMagic
	}
	hdr.Version = binary.LittleEndian.Uint32(head[8:12])
	if hdr.Version != FormatVersion {
		return hdr, fmt.Errorf("%w: %d", ErrBadVersion, hdr.Version)
	}
	hdr.CreatedAtMs = binary.LittleEndian.Uint64(head[12:20])
	hdr.LastIncludedLSN = binary.LittleEndian.Uint64(head[20:28])

	bodyLen := size - HeaderLen - FooterLen
	var consumed int64
	var eh [entryHeaderLen]byte
	buf := make([]byte, 0, 4096)
	var seen uint64

	for consumed < bodyLen {
		if bodyLen-consumed < entryHeaderLen {
			return hdr, fmt.Errorf("%w: %d trailing bytes", ErrIncomplete, bodyLen-consumed)
		}
		if _, err := io.ReadFull(r, eh[:]); err != nil {
			return hdr, err
		}
		keyLen := int(binary.LittleEndian.Uint16(eh[0:2]))
		valLen := int(binary.LittleEndian.Uint32(eh[2:6]))
		expireAt := binary.LittleEndian.Uint64(eh[6:14])

		// The CRC has already validated the whole file, so these lengths
		// are not hostile — but bound them anyway rather than trusting a
		// number to size an allocation.
		if valLen > maxValLen || keyLen > maxKeyLen {
			return hdr, fmt.Errorf("snapshot: implausible entry lengths key=%d val=%d", keyLen, valLen)
		}
		need := int64(keyLen + valLen)
		if consumed+entryHeaderLen+need > bodyLen {
			return hdr, fmt.Errorf("%w: entry overruns the body", ErrIncomplete)
		}
		if cap(buf) < keyLen+valLen {
			buf = make([]byte, keyLen+valLen)
		}
		buf = buf[:keyLen+valLen]
		if _, err := io.ReadFull(r, buf); err != nil {
			return hdr, err
		}
		if err := fn(Entry{
			Key:        buf[:keyLen:keyLen],
			Value:      buf[keyLen : keyLen+valLen : keyLen+valLen],
			ExpireAtMs: expireAt,
		}); err != nil {
			return hdr, err
		}
		seen++
		consumed += entryHeaderLen + need
	}

	if seen != hdr.EntryCount {
		return hdr, fmt.Errorf("%w: footer says %d, read %d", ErrEntryCount, hdr.EntryCount, seen)
	}
	return hdr, nil
}

func crcRange(f *os.File, n int64, out *uint32) error {
	buf := make([]byte, 1<<20)
	var done int64
	var crc uint32
	for done < n {
		toRead := int64(len(buf))
		if remaining := n - done; remaining < toRead {
			toRead = remaining
		}
		m, err := io.ReadFull(f, buf[:toRead])
		if err != nil {
			return err
		}
		crc = crc32.Update(crc, castagnoli, buf[:m])
		done += int64(m)
	}
	*out = crc
	return nil
}

// LoadNewestValid tries snapshots newest-first and returns the first that
// validates, along with its path.
//
// Falling back to an older snapshot rather than failing outright is the
// behaviour that makes "crash during snapshot writing" survivable: the
// half-written file is rejected and the previous good one is used. The
// atomic rename in Create means a half-written file should never appear
// under a .snap name at all, so this is belt and braces — but a snapshot is
// the thing standing between you and a ten-hour WAL replay, so belt and
// braces is warranted.
func LoadNewestValid(dir string, fn func(Entry) error) (Header, string, error) {
	snaps, err := List(dir)
	if err != nil {
		return Header{}, "", err
	}
	if len(snaps) == 0 {
		return Header{}, "", ErrNoSnapshot
	}
	var lastErr error
	for _, s := range snaps {
		hdr, err := Load(s.Path, fn)
		if err == nil {
			return hdr, s.Path, nil
		}
		lastErr = fmt.Errorf("%s: %w", s.Path, err)
	}
	return Header{}, "", lastErr
}

// Prune removes every snapshot except the newest keep. Old snapshots are
// only useful as a fallback; keeping them all turns the data directory into
// a slow leak.
func Prune(dir string, keep int) (int, error) {
	if keep < 1 {
		keep = 1
	}
	snaps, err := List(dir)
	if err != nil {
		return 0, err
	}
	removed := 0
	for i := keep; i < len(snaps); i++ {
		if err := os.Remove(snaps[i].Path); err != nil {
			return removed, err
		}
		removed++
	}
	// Also clear abandoned .tmp files from interrupted snapshots.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return removed, nil
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), tmpExt) {
			os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	return removed, nil
}
