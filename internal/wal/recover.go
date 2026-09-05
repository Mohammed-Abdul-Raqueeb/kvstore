package wal

import (
	"errors"
	"fmt"
	"os"
)

// ErrCorruptMidLog is returned when corruption is found in a segment that is
// not the last one. See ReplayResult for why this is fatal by default.
var ErrCorruptMidLog = errors.New("wal: corruption in a non-final segment")

// ReplayResult reports what recovery did. Every field here ends up in a log
// line at startup, because "recovery silently did something" is not an
// acceptable operational story.
type ReplayResult struct {
	Segments        int
	Records         int
	Applied         int
	Skipped         int // records at or below the snapshot's last included LSN
	LastLSN         uint64
	NextLSN         uint64
	ResumePath      string
	ResumeOffset    int64
	Truncated       bool
	TruncatedAt     int64
	TruncatedPath   string
	TruncateReason  string
	UnsafeTruncated bool
}

// Replay reads every segment in dir in LSN order and calls apply for each
// record with LSN > minLSN.
//
// The algorithm is DESIGN.md §8 steps 3–6:
//
//	for each segment, for each record:
//	    validate length bound, file bound, CRC, type
//	    skip if lsn <= minLSN (already in the snapshot)
//	    apply
//	on fault:
//	    last segment  -> truncate to last good offset, warn, continue startup
//	    other segment -> refuse to start, unless unsafeTruncate
//
// That distinction is the whole point. A torn tail is the expected
// consequence of a crash mid-write. Corruption in the middle of the log
// means something else went wrong, and continuing would silently discard
// every record after it.
//
// apply receives records whose Key/Value alias a reusable buffer; it must
// copy anything it retains. Recovery is single-threaded, so apply needs no
// locking of its own.
func Replay(dir string, minLSN uint64, unsafeTruncate bool, apply func(Record) error) (ReplayResult, error) {
	var res ReplayResult
	res.LastLSN = minLSN

	segs, err := ListSegments(dir)
	if err != nil {
		return res, fmt.Errorf("list segments: %w", err)
	}
	res.Segments = len(segs)
	if len(segs) == 0 {
		res.NextLSN = minLSN + 1
		return res, nil
	}

	for i, seg := range segs {
		isLast := i == len(segs)-1

		scan, err := ScanSegment(seg.Path, func(r Record) error {
			res.Records++
			if r.LSN <= minLSN {
				res.Skipped++
				return nil
			}
			if err := apply(r); err != nil {
				return err
			}
			res.Applied++
			if r.LSN > res.LastLSN {
				res.LastLSN = r.LSN
			}
			return nil
		})
		if err != nil {
			return res, fmt.Errorf("scan %s: %w", seg.Path, err)
		}

		if scan.Fault == FaultNone {
			res.ResumePath, res.ResumeOffset = seg.Path, scan.LastGoodOffset
			continue
		}

		// A fault in any segment but the last one means records that came
		// after it — in later segments — are already applied or about to be,
		// which would leave a hole in the middle of the log. Refuse.
		if !isLast {
			if !unsafeTruncate {
				return res, fmt.Errorf("%w: %s at offset %d: %v (rerun with --unsafe-truncate to discard everything from here on, WHICH LOSES DATA)",
					ErrCorruptMidLog, seg.Path, scan.FaultOffset, scan.FaultErr)
			}
			res.UnsafeTruncated = true
		} else if scan.Fault == FaultCorrupt && !unsafeTruncate {
			// Corruption (as opposed to a torn tail) even in the last
			// segment is not a normal crash artefact. A torn tail is a
			// partial record at the very end; a bad CRC on a *complete*
			// record means the bytes changed after they were written.
			return res, fmt.Errorf("%w: %s at offset %d: %v (rerun with --unsafe-truncate to discard from here on, WHICH LOSES DATA)",
				ErrCorruptMidLog, seg.Path, scan.FaultOffset, scan.FaultErr)
		}

		// Truncate the file back to the last record that checked out, and
		// fsync the truncation. Skipping that fsync means a second crash
		// re-exposes the garbage we just removed (mistake #12).
		if err := truncateSegment(seg.Path, scan.LastGoodOffset); err != nil {
			return res, fmt.Errorf("truncate %s: %w", seg.Path, err)
		}
		res.Truncated = true
		res.TruncatedAt = scan.LastGoodOffset
		res.TruncatedPath = seg.Path
		res.TruncateReason = fmt.Sprintf("%s: %v", scan.Fault, scan.FaultErr)
		res.ResumePath, res.ResumeOffset = seg.Path, scan.LastGoodOffset

		// Everything after a truncated segment is unreachable. Remove the
		// later segments so the next startup does not find a log with a
		// hole in it and refuse.
		for _, later := range segs[i+1:] {
			if err := os.Remove(later.Path); err != nil {
				return res, fmt.Errorf("remove orphaned segment %s: %w", later.Path, err)
			}
			res.Segments--
		}
		break
	}

	res.NextLSN = res.LastLSN + 1
	return res, nil
}

func truncateSegment(path string, offset int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(offset); err != nil {
		return err
	}
	return f.Sync()
}

// Verify scans every segment and reports the first fault, without applying
// anything. This backs `kvctl wal verify`.
func Verify(dir string) ([]ScanResult, error) {
	segs, err := ListSegments(dir)
	if err != nil {
		return nil, err
	}
	out := make([]ScanResult, 0, len(segs))
	for _, s := range segs {
		res, err := ScanSegment(s.Path, func(Record) error { return nil })
		if err != nil {
			return out, err
		}
		out = append(out, res)
	}
	return out, nil
}

// TruncateBelow deletes every segment whose records are entirely at or below
// lastIncludedLSN. Called after a snapshot is durable, which is what turns
// recovery from O(all writes ever) into O(writes since the last snapshot).
//
// A segment is only removed when the NEXT segment starts at or below
// lastIncludedLSN+1, because a segment's own first LSN tells us where it
// starts but not where it ends. The final segment is never removed: it is
// the one being appended to.
func TruncateBelow(dir string, lastIncludedLSN uint64) (int, error) {
	segs, err := ListSegments(dir)
	if err != nil {
		return 0, err
	}
	removed := 0
	for i := 0; i+1 < len(segs); i++ {
		if segs[i+1].FirstLSN > lastIncludedLSN+1 {
			// The next segment starts after the snapshot point, so this
			// segment may still contain records we need.
			break
		}
		if err := os.Remove(segs[i].Path); err != nil {
			return removed, err
		}
		removed++
	}
	if removed > 0 {
		if err := SyncDir(dir); err != nil {
			return removed, err
		}
	}
	return removed, nil
}
