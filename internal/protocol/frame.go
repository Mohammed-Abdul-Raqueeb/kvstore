package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Errors returned by the codec.
//
// ErrProtocol and everything wrapping it are fatal to the connection: they
// indicate the byte stream is no longer parseable. Semantic errors are not
// returned as Go errors at all — they become a Status on a well-formed
// response frame.
var (
	ErrProtocol      = errors.New("protocol error")
	ErrBadMagic      = fmt.Errorf("%w: bad magic", ErrProtocol)
	ErrBadVersion    = fmt.Errorf("%w: unsupported version", ErrProtocol)
	ErrReservedSet   = fmt.Errorf("%w: reserved field is non-zero", ErrProtocol)
	ErrUnknownFlags  = fmt.Errorf("%w: unknown flag bits set", ErrProtocol)
	ErrFrameTooLarge = fmt.Errorf("%w: body_len exceeds MaxFrameLen", ErrProtocol)
	ErrShortBody     = fmt.Errorf("%w: body shorter than declared", ErrProtocol)
	ErrTrailingBytes = fmt.Errorf("%w: trailing bytes after body", ErrProtocol)
	ErrTruncated     = fmt.Errorf("%w: truncated field", ErrProtocol)
	ErrKeyTooLong    = fmt.Errorf("%w: key exceeds MaxKeyLen", ErrProtocol)
	ErrValueTooLong  = fmt.Errorf("%w: value exceeds MaxValueLen", ErrProtocol)
)

// Header is the fixed 16-byte frame header. Little-endian throughout.
//
//	offset  size  field
//	0       2     magic       0x564B ("KV" little-endian)
//	2       1     version     0x01
//	3       1     opcode (request) / status (response)
//	4       2     flags       bit0 = no_reply
//	6       2     reserved    must be 0, and is validated as 0
//	8       4     request_id  echoed in the response; enables pipelining
//	12      4     body_len    bytes following this header
//
// Reserved is validated rather than ignored. Accepting garbage in a reserved
// field means you can never use it later: old servers would silently accept
// frames from new clients that they cannot actually interpret.
type Header struct {
	Version   uint8
	Code      uint8 // Opcode on a request, Status on a response
	Flags     uint16
	Reserved  uint16
	RequestID uint32
	BodyLen   uint32
}

// EncodeHeader writes h into dst, which must be at least HeaderLen bytes.
func EncodeHeader(dst []byte, h Header) {
	_ = dst[HeaderLen-1] // bounds check hint
	binary.LittleEndian.PutUint16(dst[0:2], Magic)
	dst[2] = h.Version
	dst[3] = h.Code
	binary.LittleEndian.PutUint16(dst[4:6], h.Flags)
	binary.LittleEndian.PutUint16(dst[6:8], h.Reserved)
	binary.LittleEndian.PutUint32(dst[8:12], h.RequestID)
	binary.LittleEndian.PutUint32(dst[12:16], h.BodyLen)
}

// DecodeHeader parses and validates a 16-byte header.
//
// It validates magic, version, the reserved field, unknown flag bits, and
// body_len — in that order — and it does so *without allocating anything*.
// Callers must not allocate a body buffer until this function has returned
// nil.
func DecodeHeader(src []byte) (Header, error) {
	if len(src) < HeaderLen {
		return Header{}, ErrTruncated
	}
	var h Header
	if binary.LittleEndian.Uint16(src[0:2]) != Magic {
		return h, ErrBadMagic
	}
	h.Version = src[2]
	if h.Version != Version {
		return h, ErrBadVersion
	}
	h.Code = src[3]
	h.Flags = binary.LittleEndian.Uint16(src[4:6])
	if h.Flags&^AllFlags != 0 {
		return h, ErrUnknownFlags
	}
	h.Reserved = binary.LittleEndian.Uint16(src[6:8])
	if h.Reserved != 0 {
		return h, ErrReservedSet
	}
	h.RequestID = binary.LittleEndian.Uint32(src[8:12])
	h.BodyLen = binary.LittleEndian.Uint32(src[12:16])
	if h.BodyLen > MaxFrameLen {
		// This is the one-packet OOM defence. We return before any caller
		// has a chance to size a buffer from BodyLen.
		return h, ErrFrameTooLarge
	}
	return h, nil
}

// Frame is a decoded header plus its body. Body aliases the buffer it was
// read from; ReadFrame documents the ownership rules.
type Frame struct {
	Header
	Body []byte
}

// Opcode returns the header code interpreted as a request opcode.
func (f Frame) Opcode() Opcode { return Opcode(f.Code) }

// Status returns the header code interpreted as a response status.
func (f Frame) Status() Status { return Status(f.Code) }

// NoReply reports whether the fire-and-forget flag is set.
func (f Frame) NoReply() bool { return f.Flags&FlagNoReply != 0 }

// ReadFrame reads exactly one frame from r.
//
// It implements the read_full loop demanded by DESIGN.md §4: a single Read
// returning fewer bytes than requested is normal on a TCP socket and is not
// an error. io.ReadFull provides that loop.
//
// buf is an optional scratch buffer. If it has enough capacity for the body
// it is reused, which is what keeps the steady-state request path
// allocation-free. THE RETURNED Frame.Body THEREFORE ALIASES buf. The caller
// must copy any bytes it intends to retain past the next ReadFrame call —
// this is mistake #7 in DESIGN.md §15, and it is why store.Set copies keys
// and values on insert.
//
// maxBody lets the server impose a stricter cap than MaxFrameLen. Pass 0 for
// the protocol default.
func ReadFrame(r io.Reader, hdrBuf []byte, buf []byte, maxBody uint32) (Frame, []byte, error) {
	if len(hdrBuf) < HeaderLen {
		hdrBuf = make([]byte, HeaderLen)
	}
	if _, err := io.ReadFull(r, hdrBuf[:HeaderLen]); err != nil {
		// io.EOF here means a clean close on a frame boundary, which is
		// normal. ErrUnexpectedEOF means the peer died mid-header.
		return Frame{}, buf, err
	}
	h, err := DecodeHeader(hdrBuf[:HeaderLen])
	if err != nil {
		return Frame{}, buf, err
	}
	if maxBody == 0 || maxBody > MaxFrameLen {
		maxBody = MaxFrameLen
	}
	if h.BodyLen > maxBody {
		return Frame{Header: h}, buf, ErrFrameTooLarge
	}
	if h.BodyLen == 0 {
		return Frame{Header: h, Body: nil}, buf, nil
	}
	// Only now, after the bound check, do we size a buffer from a
	// client-supplied length.
	if uint32(cap(buf)) < h.BodyLen {
		buf = make([]byte, h.BodyLen)
	}
	body := buf[:h.BodyLen]
	if _, err := io.ReadFull(r, body); err != nil {
		if err == io.EOF {
			// A clean EOF *inside* a body is a truncated frame, not a
			// clean close.
			err = io.ErrUnexpectedEOF
		}
		return Frame{Header: h}, buf, err
	}
	return Frame{Header: h, Body: body}, buf, nil
}

// WriteFrame serialises a frame into dst (appending) and returns the
// extended slice. Doing it this way lets the server build a whole pipeline
// batch in one buffer and issue a single write.
func WriteFrame(dst []byte, h Header, body []byte) []byte {
	h.BodyLen = uint32(len(body))
	var hdr [HeaderLen]byte
	EncodeHeader(hdr[:], h)
	dst = append(dst, hdr[:]...)
	dst = append(dst, body...)
	return dst
}

// WriteFull writes the whole of p to w, looping over short writes.
//
// io.Writer's contract already requires a full write or an error, and
// *net.TCPConn honours it, but this function exists so the short-write loop
// is explicit and so tests can drive it with a deliberately short-writing
// writer. Short writes are real on raw file descriptors and on any custom
// Writer in the pipeline.
func WriteFull(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if n > 0 {
			p = p[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
