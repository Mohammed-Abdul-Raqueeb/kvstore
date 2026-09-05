package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// WAL record format (DESIGN.md §5). Little-endian throughout, matching the
// wire protocol — mixing endianness between two binary formats in one
// codebase is a classic and very hard-to-find bug.
//
//	offset  size  field
//	0       4     crc32c    CRC over bytes [8, 8+length)
//	4       4     length    payload byte count
//	8       ...   payload
//
// Payload: a fixed 32-byte header followed by key and value bytes.
//
//	0     8   lsn:u64
//	8     8   created_at_ms:u64      wall clock, for the inspector
//	16    1   rec_type:u8            1=SET 2=DEL 3=EXPIRE 4=SEGMENT_HDR
//	17    1   flags:u8
//	18    2   key_len:u16
//	20    4   val_len:u32
//	24    8   expire_at_ms:u64       absolute; 0 = no expiry
//	32    ..  key bytes
//	..    ..  value bytes
//
// Total record = 8 + 32 + key_len + val_len.
const (
	recHeaderLen     = 8  // crc32c + length
	payloadHeaderLen = 32 // the fixed part of the payload

	// MaxRecordLen bounds the payload. See the note on the CRC below for why
	// this bound is load-bearing rather than merely defensive.
	MaxRecordLen = payloadHeaderLen + (1 << 16) + (16 << 20) + 64

	maxKeyLen = 1<<16 - 1
	maxValLen = 16 << 20
)

// castagnoli is CRC32C, the same polynomial LevelDB, RocksDB and the SCTP
// standard use. crc32.Castagnoli gets a hardware-accelerated implementation
// on amd64 and arm64 via the runtime's CRC32 intrinsics, so checksumming is
// not the bottleneck in group commit.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// RecordType identifies what a record does on replay.
type RecordType uint8

const (
	RecSet        RecordType = 1
	RecDelete     RecordType = 2
	RecExpire     RecordType = 3
	RecSegmentHdr RecordType = 4
)

func (t RecordType) String() string {
	switch t {
	case RecSet:
		return "SET"
	case RecDelete:
		return "DEL"
	case RecExpire:
		return "EXPIRE"
	case RecSegmentHdr:
		return "SEGMENT_HDR"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", uint8(t))
	}
}

// Valid reports whether the type is one we know how to replay. An unknown
// type in an otherwise CRC-valid record means the file was written by a
// newer version, which is a hard error rather than something to skip:
// skipping it would silently drop a mutation.
func (t RecordType) Valid() bool {
	return t >= RecSet && t <= RecSegmentHdr
}

// Record is one durable mutation.
type Record struct {
	LSN         uint64
	CreatedAtMs uint64
	Type        RecordType
	Flags       uint8
	Key         []byte
	Value       []byte
	ExpireAtMs  uint64
}

// EncodedLen returns the on-disk size of the record in bytes.
func (r Record) EncodedLen() int {
	return recHeaderLen + payloadHeaderLen + len(r.Key) + len(r.Value)
}

// Errors from decoding.
var (
	ErrShortRecord   = errors.New("wal: buffer shorter than a record header")
	ErrLengthTooBig  = errors.New("wal: declared record length exceeds MaxRecordLen")
	ErrTornTail      = errors.New("wal: record extends past end of file (torn tail)")
	ErrBadCRC        = errors.New("wal: CRC mismatch")
	ErrBadPayload    = errors.New("wal: payload shorter than its declared fields")
	ErrBadRecordType = errors.New("wal: unknown record type")
)

// AppendRecord serialises r onto dst and returns the extended slice.
func AppendRecord(dst []byte, r Record) []byte {
	payloadLen := payloadHeaderLen + len(r.Key) + len(r.Value)

	start := len(dst)
	dst = append(dst, make([]byte, recHeaderLen+payloadHeaderLen)...)
	p := dst[start+recHeaderLen:]

	binary.LittleEndian.PutUint64(p[0:8], r.LSN)
	binary.LittleEndian.PutUint64(p[8:16], r.CreatedAtMs)
	p[16] = byte(r.Type)
	p[17] = r.Flags
	binary.LittleEndian.PutUint16(p[18:20], uint16(len(r.Key)))
	binary.LittleEndian.PutUint32(p[20:24], uint32(len(r.Value)))
	binary.LittleEndian.PutUint64(p[24:32], r.ExpireAtMs)

	dst = append(dst, r.Key...)
	dst = append(dst, r.Value...)

	// The CRC covers the payload only, so it can be computed after the
	// payload is fully assembled.
	crc := crc32.Checksum(dst[start+recHeaderLen:start+recHeaderLen+payloadLen], castagnoli)
	binary.LittleEndian.PutUint32(dst[start:start+4], crc)
	binary.LittleEndian.PutUint32(dst[start+4:start+8], uint32(payloadLen))
	return dst
}

// DecodeRecord parses one record from the front of buf.
//
// fileRemaining is the number of bytes left in the file from the start of
// this record, which is what lets us tell a torn tail from a corrupt length
// field. Pass len(buf) if the whole file is in memory.
//
// # Why the CRC does not cover the length field
//
// It cannot: you have to trust `length` before you know how many bytes to
// checksum. This is a genuine chicken-and-egg problem and LevelDB solves it
// the same way. The defence is bounds validation instead of a checksum:
//
//	length > MaxRecordLen           -> corruption; no legal record is this big
//	offset + 8 + length > file_size -> torn tail; the write did not complete
//
// A corrupted length that survives both checks will make us read the wrong
// number of bytes, and the CRC over those bytes will then fail. So the
// length field is protected transitively: a bad length either fails a bound
// or produces a payload that fails its own checksum. The one case this does
// not catch is a corrupted length that happens to land on a byte range whose
// CRC32C matches the stored CRC, which is a 2^-32 event per corrupted
// record and is the same residual risk every log-structured store accepts.
func DecodeRecord(buf []byte, fileRemaining int64) (Record, int, error) {
	if len(buf) < recHeaderLen {
		return Record{}, 0, ErrShortRecord
	}
	crc := binary.LittleEndian.Uint32(buf[0:4])
	length := binary.LittleEndian.Uint32(buf[4:8])

	if length > MaxRecordLen {
		return Record{}, 0, fmt.Errorf("%w: %d", ErrLengthTooBig, length)
	}
	total := recHeaderLen + int(length)
	if int64(total) > fileRemaining {
		return Record{}, 0, ErrTornTail
	}
	if len(buf) < total {
		return Record{}, 0, ErrTornTail
	}
	payload := buf[recHeaderLen:total]
	if crc32.Checksum(payload, castagnoli) != crc {
		return Record{}, 0, ErrBadCRC
	}
	if len(payload) < payloadHeaderLen {
		return Record{}, 0, ErrBadPayload
	}

	var r Record
	r.LSN = binary.LittleEndian.Uint64(payload[0:8])
	r.CreatedAtMs = binary.LittleEndian.Uint64(payload[8:16])
	r.Type = RecordType(payload[16])
	r.Flags = payload[17]
	keyLen := int(binary.LittleEndian.Uint16(payload[18:20]))
	valLen := int(binary.LittleEndian.Uint32(payload[20:24]))
	r.ExpireAtMs = binary.LittleEndian.Uint64(payload[24:32])

	// The CRC has already passed, so these lengths are trustworthy — but
	// check them anyway. A CRC-valid record can still be internally
	// inconsistent if it was produced by a buggy writer, and a panic during
	// recovery is the worst possible failure mode.
	if valLen > maxValLen || keyLen > maxKeyLen {
		return Record{}, 0, fmt.Errorf("%w: key=%d val=%d", ErrBadPayload, keyLen, valLen)
	}
	if payloadHeaderLen+keyLen+valLen != len(payload) {
		return Record{}, 0, fmt.Errorf("%w: header says %d+%d bytes, payload has %d",
			ErrBadPayload, keyLen, valLen, len(payload)-payloadHeaderLen)
	}
	if !r.Type.Valid() {
		return Record{}, 0, fmt.Errorf("%w: %d", ErrBadRecordType, payload[16])
	}

	r.Key = payload[payloadHeaderLen : payloadHeaderLen+keyLen : payloadHeaderLen+keyLen]
	r.Value = payload[payloadHeaderLen+keyLen : payloadHeaderLen+keyLen+valLen : payloadHeaderLen+keyLen+valLen]
	return r, total, nil
}

// Clone returns a deep copy. DecodeRecord returns slices aliasing the read
// buffer, so anything retained past the next read must be cloned.
func (r Record) Clone() Record {
	c := r
	c.Key = append([]byte(nil), r.Key...)
	c.Value = append([]byte(nil), r.Value...)
	return c
}

// segmentMagic identifies a SEGMENT_HDR record's key. Every segment file
// begins with one, so a stray file in the WAL directory is recognisably not
// a segment and a segment's first LSN can be read without scanning it.
var segmentMagic = []byte("KVWALSEG")

const segmentFormatVersion uint32 = 1

// newSegmentHeader builds the SEGMENT_HDR record that opens a segment.
func newSegmentHeader(firstLSN, nowMs uint64) Record {
	val := make([]byte, 4)
	binary.LittleEndian.PutUint32(val, segmentFormatVersion)
	return Record{
		LSN:         firstLSN,
		CreatedAtMs: nowMs,
		Type:        RecSegmentHdr,
		Key:         segmentMagic,
		Value:       val,
	}
}

// parseSegmentHeader validates a SEGMENT_HDR record and returns its format
// version.
func parseSegmentHeader(r Record) (uint32, error) {
	if r.Type != RecSegmentHdr {
		return 0, fmt.Errorf("wal: first record is %s, not SEGMENT_HDR", r.Type)
	}
	if string(r.Key) != string(segmentMagic) {
		return 0, fmt.Errorf("wal: bad segment magic %q", r.Key)
	}
	if len(r.Value) != 4 {
		return 0, fmt.Errorf("wal: segment header value is %d bytes, want 4", len(r.Value))
	}
	v := binary.LittleEndian.Uint32(r.Value)
	if v != segmentFormatVersion {
		return 0, fmt.Errorf("wal: segment format version %d, this build understands %d", v, segmentFormatVersion)
	}
	return v, nil
}
