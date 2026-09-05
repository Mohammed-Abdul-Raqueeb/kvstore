package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
)

// Fault classifies why a scan stopped early.
type Fault uint8

const (
	FaultNone Fault = iota
	// FaultTornTail: the file ends in the middle of a record. This is the
	// EXPECTED failure. It means the process died between write() and the
	// record being complete on disk, which is exactly the scenario a WAL is
	// designed for.
	FaultTornTail
	// FaultCorrupt: a record is present and complete but does not check
	// out — bad CRC, impossible length, unknown type. The disk lied, or
	// somebody edited the file. Silently dropping data here would be data
	// loss disguised as resilience.
	FaultCorrupt
)

func (f Fault) String() string {
	switch f {
	case FaultNone:
		return "none"
	case FaultTornTail:
		return "torn-tail"
	case FaultCorrupt:
		return "corrupt"
	default:
		return "unknown"
	}
}

// ScanResult describes how a single segment scan ended.
type ScanResult struct {
	Path           string
	FirstLSN       uint64
	LastLSN        uint64
	Records        int
	LastGoodOffset int64
	FileSize       int64
	Fault          Fault
	FaultOffset    int64
	FaultErr       error
}

// Healthy reports whether the segment was read to the end with no fault.
func (r ScanResult) Healthy() bool { return r.Fault == FaultNone }

// ScanSegment reads one segment, calling fn for every valid record in order.
//
// fn receives a Record whose Key and Value alias the read buffer; it must
// copy anything it retains. Returning an error from fn aborts the scan and
// that error is returned.
//
// The scan never returns an error for a torn tail or for corruption; those
// are reported in the ScanResult so the caller can apply the policy from
// DESIGN.md §8 step 6 (truncate the last segment, refuse for anything
// earlier). Errors are reserved for I/O failures and for fn.
func ScanSegment(path string, fn func(Record) error) (ScanResult, error) {
	res := ScanResult{Path: path}

	f, err := os.Open(path)
	if err != nil {
		return res, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return res, err
	}
	res.FileSize = st.Size()

	r := bufio.NewReaderSize(f, 1<<20)

	// hdr is reused across records; the payload buffer grows to the largest
	// record seen and is then reused, so a scan of a million small records
	// allocates once.
	var hdr [recHeaderLen]byte
	payload := make([]byte, 0, 4096)

	var offset int64
	first := true

	for {
		remaining := res.FileSize - offset
		if remaining == 0 {
			break // clean end of file
		}
		if remaining < recHeaderLen {
			res.Fault, res.FaultOffset = FaultTornTail, offset
			res.FaultErr = fmt.Errorf("%w: %d trailing bytes, less than a %d-byte record header",
				ErrTornTail, remaining, recHeaderLen)
			break
		}
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			res.Fault, res.FaultOffset = FaultTornTail, offset
			res.FaultErr = fmt.Errorf("%w: reading header: %v", ErrTornTail, err)
			break
		}

		length := uint32(hdr[4]) | uint32(hdr[5])<<8 | uint32(hdr[6])<<16 | uint32(hdr[7])<<24

		// Bound before allocating. This is the same discipline as the wire
		// protocol: a length field read off a disk that may have lied to us
		// is exactly as hostile as one read off a socket.
		if length > MaxRecordLen {
			res.Fault, res.FaultOffset = FaultCorrupt, offset
			res.FaultErr = fmt.Errorf("%w: %d at offset %d", ErrLengthTooBig, length, offset)
			break
		}
		if int64(recHeaderLen)+int64(length) > remaining {
			res.Fault, res.FaultOffset = FaultTornTail, offset
			res.FaultErr = fmt.Errorf("%w: record needs %d bytes, %d remain in file",
				ErrTornTail, recHeaderLen+int(length), remaining)
			break
		}

		if cap(payload) < int(length) {
			payload = make([]byte, length)
		}
		payload = payload[:length]
		if _, err := io.ReadFull(r, payload); err != nil {
			res.Fault, res.FaultOffset = FaultTornTail, offset
			res.FaultErr = fmt.Errorf("%w: reading payload: %v", ErrTornTail, err)
			break
		}

		// Reassemble and decode through the single decoder so the scanner
		// and the inspector can never disagree about what a record means.
		full := make([]byte, 0, recHeaderLen+len(payload))
		full = append(full, hdr[:]...)
		full = append(full, payload...)
		rec, n, derr := DecodeRecord(full, remaining)
		if derr != nil {
			switch {
			case errors.Is(derr, ErrTornTail):
				res.Fault = FaultTornTail
			default:
				res.Fault = FaultCorrupt
			}
			res.FaultOffset, res.FaultErr = offset, derr
			break
		}

		if first {
			if _, err := parseSegmentHeader(rec); err != nil {
				res.Fault, res.FaultOffset = FaultCorrupt, offset
				res.FaultErr = err
				break
			}
			res.FirstLSN = rec.LSN
			first = false
		} else {
			if err := fn(rec); err != nil {
				return res, err
			}
			res.Records++
			res.LastLSN = rec.LSN
		}

		offset += int64(n)
		res.LastGoodOffset = offset
	}

	if first && res.Fault == FaultNone {
		// Zero-length segment: the file was created but the header write did
		// not land. Treat as a torn tail at offset 0 so the caller truncates
		// rather than refusing to start.
		res.Fault, res.FaultOffset = FaultTornTail, 0
		res.FaultErr = errors.New("wal: segment has no SEGMENT_HDR record")
	}
	return res, nil
}
