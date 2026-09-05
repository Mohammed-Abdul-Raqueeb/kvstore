package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Segments are named by the LSN of their first record, zero-padded to 16
// digits so that lexical order equals numeric order. That means listing a
// directory and sorting the names gives the replay order without opening a
// single file — which matters when there are thousands of segments.
//
//	wal/0000000000000001.log
//	wal/0000000000065537.log
const (
	segmentExt    = ".log"
	segmentPrefix = ""
)

// SegmentName returns the file name for a segment starting at firstLSN.
func SegmentName(firstLSN uint64) string {
	return fmt.Sprintf("%s%016d%s", segmentPrefix, firstLSN, segmentExt)
}

// ParseSegmentName extracts the first LSN encoded in a segment file name.
func ParseSegmentName(name string) (uint64, bool) {
	base := filepath.Base(name)
	if !strings.HasSuffix(base, segmentExt) {
		return 0, false
	}
	numPart := strings.TrimSuffix(base, segmentExt)
	if len(numPart) != 16 {
		return 0, false
	}
	n, err := strconv.ParseUint(numPart, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// SegmentInfo describes one segment on disk.
type SegmentInfo struct {
	Path     string
	FirstLSN uint64
	Size     int64
}

// ListSegments returns every segment in dir, sorted by first LSN.
//
// Files that do not parse as segment names are ignored rather than treated
// as errors: a `.tmp` from an interrupted snapshot or an operator's stray
// copy should not prevent startup. Anything that *does* parse as a segment
// is trusted to be one and will be validated by the scanner.
func ListSegments(dir string) ([]SegmentInfo, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var segs []SegmentInfo
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		lsn, ok := ParseSegmentName(e.Name())
		if !ok {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		segs = append(segs, SegmentInfo{
			Path:     filepath.Join(dir, e.Name()),
			FirstLSN: lsn,
			Size:     info.Size(),
		})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].FirstLSN < segs[j].FirstLSN })
	return segs, nil
}

// segment is an open segment file being appended to.
type segment struct {
	f        *os.File
	path     string
	firstLSN uint64
	size     int64
}

// createSegment makes a new segment file, writes its SEGMENT_HDR record,
// fsyncs the file, and then fsyncs the containing directory so that the
// file's *existence* is durable and not just its contents.
func createSegment(dir string, firstLSN, nowMs uint64) (*segment, error) {
	path := filepath.Join(dir, SegmentName(firstLSN))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create segment %s: %w", path, err)
	}
	hdr := AppendRecord(nil, newSegmentHeader(firstLSN, nowMs))
	if _, err := f.Write(hdr); err != nil {
		f.Close()
		return nil, fmt.Errorf("write segment header: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, fmt.Errorf("sync segment header: %w", err)
	}
	if err := SyncDir(dir); err != nil {
		f.Close()
		return nil, fmt.Errorf("sync wal dir: %w", err)
	}
	return &segment{f: f, path: path, firstLSN: firstLSN, size: int64(len(hdr))}, nil
}

// openSegmentForAppend reopens an existing segment and positions at offset.
// Any bytes past offset (a torn tail already identified by recovery) are
// truncated away first.
func openSegmentForAppend(path string, firstLSN, offset int64) (*segment, error) {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() > offset {
		if err := f.Truncate(offset); err != nil {
			f.Close()
			return nil, fmt.Errorf("truncate torn tail: %w", err)
		}
		// Truncation must itself be durable, or a second crash re-exposes
		// the garbage we just removed. DESIGN.md §15 mistake #12.
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, fmt.Errorf("sync after truncate: %w", err)
		}
	}
	if _, err := f.Seek(offset, 0); err != nil {
		f.Close()
		return nil, err
	}
	return &segment{f: f, path: path, firstLSN: uint64(firstLSN), size: offset}, nil
}

func (s *segment) write(b []byte) error {
	n, err := s.f.Write(b)
	s.size += int64(n)
	if err != nil {
		return err
	}
	if n != len(b) {
		return fmt.Errorf("wal: short write %d of %d bytes", n, len(b))
	}
	return nil
}

func (s *segment) sync() error { return s.f.Sync() }

func (s *segment) close() error { return s.f.Close() }
