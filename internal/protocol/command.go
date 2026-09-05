package protocol

import (
	"encoding/binary"
	"math"
)

// Command is a decoded request body.
//
// Key and Value are sub-slices of the body buffer passed to DecodeCommand.
// They alias it and are only valid until the caller reuses that buffer. This
// is deliberate: it makes the read path allocation-free. Every consumer that
// retains a key or value (i.e. the store) copies it.
type Command struct {
	Op    Opcode
	Key   []byte
	Value []byte

	// TTLMillis is 0 for "no TTL" on SET, and is the requested lifetime in
	// milliseconds otherwise. It is a *relative* duration on the wire; the
	// server converts it to an absolute deadline. Relative on the wire
	// avoids requiring clock agreement between client and server.
	TTLMillis uint64

	// Limit is used by KEYS.
	Limit uint32

	// Replication fields (phase 2).
	FromLSN  uint64
	NodeID   []byte
	NodePort uint16
}

// cursor is a bounds-checked reader over a body slice. Every read either
// succeeds fully or sets the error flag; no partial state escapes.
type cursor struct {
	b   []byte
	off int
	err error
}

func (c *cursor) fail() { c.err = ErrTruncated }

func (c *cursor) remaining() int { return len(c.b) - c.off }

func (c *cursor) u8() uint8 {
	if c.err != nil || c.remaining() < 1 {
		c.fail()
		return 0
	}
	v := c.b[c.off]
	c.off++
	return v
}

func (c *cursor) u16() uint16 {
	if c.err != nil || c.remaining() < 2 {
		c.fail()
		return 0
	}
	v := binary.LittleEndian.Uint16(c.b[c.off:])
	c.off += 2
	return v
}

func (c *cursor) u32() uint32 {
	if c.err != nil || c.remaining() < 4 {
		c.fail()
		return 0
	}
	v := binary.LittleEndian.Uint32(c.b[c.off:])
	c.off += 4
	return v
}

func (c *cursor) u64() uint64 {
	if c.err != nil || c.remaining() < 8 {
		c.fail()
		return 0
	}
	v := binary.LittleEndian.Uint64(c.b[c.off:])
	c.off += 8
	return v
}

// bytes returns a sub-slice of length n. n is always compared against the
// bytes actually available *before* the slice is taken, so a hostile length
// field can never produce an out-of-range slice or an oversized allocation
// (it produces no allocation at all).
func (c *cursor) bytes(n int) []byte {
	if c.err != nil || n < 0 || c.remaining() < n {
		c.fail()
		return nil
	}
	v := c.b[c.off : c.off+n : c.off+n]
	c.off += n
	return v
}

// done asserts the body was consumed exactly. Trailing bytes mean the client
// and server disagree about the format, which is a framing-level problem.
func (c *cursor) done() error {
	if c.err != nil {
		return c.err
	}
	if c.remaining() != 0 {
		return ErrTrailingBytes
	}
	return nil
}

// DecodeCommand parses a request body for the given opcode.
//
// Guarantees relied upon by the fuzz target:
//   - never panics for any (op, body) pair;
//   - allocates nothing proportional to any length field in body;
//   - every returned slice is a sub-slice of body.
func DecodeCommand(op Opcode, body []byte) (Command, error) {
	cmd := Command{Op: op}
	c := &cursor{b: body}

	switch op {
	case OpPing, OpStats, OpFlush, OpSnapshot:
		// Empty body. Trailing bytes are rejected by done().

	case OpGet, OpDelete, OpExists, OpTTL:
		cmd.Key = c.bytes(int(c.u16()))

	case OpSet:
		cmd.Key = c.bytes(int(c.u16()))
		vlen := c.u32()
		if vlen > MaxValueLen {
			return cmd, ErrValueTooLong
		}
		cmd.Value = c.bytes(int(vlen))
		cmd.TTLMillis = c.u64()

	case OpExpire:
		cmd.Key = c.bytes(int(c.u16()))
		cmd.TTLMillis = c.u64()

	case OpKeys:
		cmd.Key = c.bytes(int(c.u16())) // prefix
		cmd.Limit = c.u32()

	case OpReplConf:
		cmd.NodeID = c.bytes(int(c.u16()))
		cmd.NodePort = c.u16()

	case OpSync:
		cmd.FromLSN = c.u64()

	case OpReplAck:
		cmd.FromLSN = c.u64()

	case OpPromote:
		// Empty body.

	default:
		// An unknown opcode is a *semantic* error, not a framing error: the
		// frame was well formed and we know exactly where the next one
		// starts. The server answers BAD_REQUEST and keeps the connection.
		return cmd, ErrUnknownOpcode
	}

	if err := c.done(); err != nil {
		return cmd, err
	}
	if len(cmd.Key) > MaxKeyLen {
		return cmd, ErrKeyTooLong
	}
	return cmd, nil
}

// ErrUnknownOpcode is returned by DecodeCommand for an opcode this build
// does not implement. Unlike the other codec errors it does NOT wrap
// ErrProtocol, because the framing is intact.
var ErrUnknownOpcode = errUnknownOpcode{}

type errUnknownOpcode struct{}

func (errUnknownOpcode) Error() string { return "unknown opcode" }

// EncodeCommand appends the body for cmd to dst and returns it.
func EncodeCommand(dst []byte, cmd Command) []byte {
	switch cmd.Op {
	case OpPing, OpStats, OpFlush, OpSnapshot, OpPromote:
		// empty

	case OpGet, OpDelete, OpExists, OpTTL:
		dst = appendU16(dst, uint16(len(cmd.Key)))
		dst = append(dst, cmd.Key...)

	case OpSet:
		dst = appendU16(dst, uint16(len(cmd.Key)))
		dst = append(dst, cmd.Key...)
		dst = appendU32(dst, uint32(len(cmd.Value)))
		dst = append(dst, cmd.Value...)
		dst = appendU64(dst, cmd.TTLMillis)

	case OpExpire:
		dst = appendU16(dst, uint16(len(cmd.Key)))
		dst = append(dst, cmd.Key...)
		dst = appendU64(dst, cmd.TTLMillis)

	case OpKeys:
		dst = appendU16(dst, uint16(len(cmd.Key)))
		dst = append(dst, cmd.Key...)
		dst = appendU32(dst, cmd.Limit)

	case OpReplConf:
		dst = appendU16(dst, uint16(len(cmd.NodeID)))
		dst = append(dst, cmd.NodeID...)
		dst = appendU16(dst, cmd.NodePort)

	case OpSync, OpReplAck:
		dst = appendU64(dst, cmd.FromLSN)
	}
	return dst
}

// --- Response bodies -------------------------------------------------------

// EncodeValueBody encodes a GET response body: val_len:u32, value.
func EncodeValueBody(dst []byte, v []byte) []byte {
	dst = appendU32(dst, uint32(len(v)))
	return append(dst, v...)
}

// DecodeValueBody parses a GET response body.
func DecodeValueBody(body []byte) ([]byte, error) {
	c := &cursor{b: body}
	v := c.bytes(int(c.u32()))
	if err := c.done(); err != nil {
		return nil, err
	}
	return v, nil
}

// EncodeBoolBody encodes an EXISTS response body: 1 byte.
func EncodeBoolBody(dst []byte, b bool) []byte {
	if b {
		return append(dst, 1)
	}
	return append(dst, 0)
}

// DecodeBoolBody parses an EXISTS response body.
func DecodeBoolBody(body []byte) (bool, error) {
	c := &cursor{b: body}
	v := c.u8()
	if err := c.done(); err != nil {
		return false, err
	}
	return v != 0, nil
}

// EncodeTTLBody encodes a TTL response: remaining milliseconds as a signed
// 64-bit value. -1 means "exists, but has no expiry".
func EncodeTTLBody(dst []byte, ms int64) []byte {
	return appendU64(dst, uint64(ms))
}

// DecodeTTLBody parses a TTL response body.
func DecodeTTLBody(body []byte) (int64, error) {
	c := &cursor{b: body}
	v := c.u64()
	if err := c.done(); err != nil {
		return 0, err
	}
	return int64(v), nil
}

// EncodeKeysBody encodes a KEYS response: count:u32 then count * (len:u16, key).
func EncodeKeysBody(dst []byte, keys [][]byte) []byte {
	dst = appendU32(dst, uint32(len(keys)))
	for _, k := range keys {
		dst = appendU16(dst, uint16(len(k)))
		dst = append(dst, k...)
	}
	return dst
}

// DecodeKeysBody parses a KEYS response body. The returned slices alias body.
func DecodeKeysBody(body []byte) ([][]byte, error) {
	c := &cursor{b: body}
	n := c.u32()
	// Guard the count before allocating the slice header array: a forged
	// count of 2^32-1 must not allocate 4 billion slice headers. Each key
	// costs at least 2 bytes on the wire, so the count cannot legitimately
	// exceed remaining/2.
	if c.err != nil || uint64(n) > uint64(c.remaining()/2) {
		return nil, ErrTruncated
	}
	keys := make([][]byte, 0, n)
	for i := uint32(0); i < n; i++ {
		keys = append(keys, c.bytes(int(c.u16())))
	}
	if err := c.done(); err != nil {
		return nil, err
	}
	return keys, nil
}

// EncodeErrorBody encodes a human-readable message for an error response.
// It is advisory only; clients key off the status code.
func EncodeErrorBody(dst []byte, msg string) []byte {
	if len(msg) > 1024 {
		msg = msg[:1024]
	}
	return append(dst, msg...)
}

// --- little-endian appenders ----------------------------------------------

func appendU16(dst []byte, v uint16) []byte {
	return append(dst, byte(v), byte(v>>8))
}

func appendU32(dst []byte, v uint32) []byte {
	return append(dst, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

func appendU64(dst []byte, v uint64) []byte {
	return append(dst,
		byte(v), byte(v>>8), byte(v>>16), byte(v>>24),
		byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
}

// MaxTTLMillis bounds a requested TTL so that converting it to an absolute
// nanosecond deadline cannot overflow int64.
const MaxTTLMillis = uint64(math.MaxInt64/int64(1_000_000)) - 1
