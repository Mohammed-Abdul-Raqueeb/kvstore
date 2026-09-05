package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestHeaderRoundTrip(t *testing.T) {
	cases := []Header{
		{Version: Version, Code: byte(OpPing), Flags: 0, RequestID: 0, BodyLen: 0},
		{Version: Version, Code: byte(OpSet), Flags: FlagNoReply, RequestID: 1, BodyLen: 1},
		{Version: Version, Code: byte(OpGet), Flags: 0, RequestID: 0xDEADBEEF, BodyLen: MaxFrameLen},
		{Version: Version, Code: 0xFF, Flags: 0, RequestID: 0xFFFFFFFF, BodyLen: MaxFrameLen - 1},
	}
	for _, want := range cases {
		var buf [HeaderLen]byte
		EncodeHeader(buf[:], want)
		got, err := DecodeHeader(buf[:])
		if err != nil {
			t.Fatalf("DecodeHeader(%+v): %v", want, err)
		}
		if got != want {
			t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, want)
		}
	}
}

func TestHeaderIsLittleEndian(t *testing.T) {
	var buf [HeaderLen]byte
	EncodeHeader(buf[:], Header{Version: Version, Code: 0x03, RequestID: 0x01020304, BodyLen: 0x0A0B0C0D})
	// "KV": 'K'=0x4B at byte 0, 'V'=0x56 at byte 1.
	if buf[0] != 'K' || buf[1] != 'V' {
		t.Fatalf("magic bytes on the wire = %q, want \"KV\"", buf[0:2])
	}
	if !bytes.Equal(buf[8:12], []byte{0x04, 0x03, 0x02, 0x01}) {
		t.Fatalf("request_id not little-endian: %x", buf[8:12])
	}
	if !bytes.Equal(buf[12:16], []byte{0x0D, 0x0C, 0x0B, 0x0A}) {
		t.Fatalf("body_len not little-endian: %x", buf[12:16])
	}
}

func TestDecodeHeaderRejections(t *testing.T) {
	valid := func() []byte {
		b := make([]byte, HeaderLen)
		EncodeHeader(b, Header{Version: Version, Code: byte(OpPing)})
		return b
	}

	tests := []struct {
		name    string
		mutate  func([]byte) []byte
		wantErr error
	}{
		{"short buffer", func(b []byte) []byte { return b[:HeaderLen-1] }, ErrTruncated},
		{"bad magic", func(b []byte) []byte { b[0] ^= 0xFF; return b }, ErrBadMagic},
		{"bad version", func(b []byte) []byte { b[2] = 0x99; return b }, ErrBadVersion},
		{"reserved set", func(b []byte) []byte { b[6] = 1; return b }, ErrReservedSet},
		{"unknown flags", func(b []byte) []byte {
			binary.LittleEndian.PutUint16(b[4:6], 0x8000)
			return b
		}, ErrUnknownFlags},
		{"4GiB body_len", func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[12:16], 0xFFFFFFFF)
			return b
		}, ErrFrameTooLarge},
		{"body_len = MaxFrameLen+1", func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[12:16], MaxFrameLen+1)
			return b
		}, ErrFrameTooLarge},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeHeader(tc.mutate(valid()))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
			// Every rejection above must also be fatal to the connection.
			if tc.wantErr != ErrTruncated && !errors.Is(err, ErrProtocol) {
				t.Fatalf("error %v does not wrap ErrProtocol", err)
			}
		})
	}
}

func TestDecodeHeaderAcceptsExactlyMaxFrameLen(t *testing.T) {
	b := make([]byte, HeaderLen)
	EncodeHeader(b, Header{Version: Version, Code: byte(OpSet), BodyLen: MaxFrameLen})
	if _, err := DecodeHeader(b); err != nil {
		t.Fatalf("MaxFrameLen must be accepted, got %v", err)
	}
}

// byteAtATimeReader hands out one byte per Read call, which is exactly the
// TCP behaviour that breaks naive framing code.
type byteAtATimeReader struct {
	b []byte
	i int
}

func (r *byteAtATimeReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = r.b[r.i]
	r.i++
	return 1, nil
}

func TestReadFrameSurvivesByteAtATimeDelivery(t *testing.T) {
	body := EncodeCommand(nil, Command{Op: OpSet, Key: []byte("k"), Value: []byte("v"), TTLMillis: 5})
	wire := WriteFrame(nil, Header{Version: Version, Code: byte(OpSet), RequestID: 42}, body)

	f, _, err := ReadFrame(&byteAtATimeReader{b: wire}, nil, nil, 0)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if f.RequestID != 42 || f.Opcode() != OpSet {
		t.Fatalf("unexpected header: %+v", f.Header)
	}
	cmd, err := DecodeCommand(f.Opcode(), f.Body)
	if err != nil {
		t.Fatalf("DecodeCommand: %v", err)
	}
	if string(cmd.Key) != "k" || string(cmd.Value) != "v" || cmd.TTLMillis != 5 {
		t.Fatalf("unexpected command: %+v", cmd)
	}
}

func TestReadFrameTwoFramesInOnePacket(t *testing.T) {
	var wire []byte
	for i := 0; i < 2; i++ {
		body := EncodeCommand(nil, Command{Op: OpGet, Key: []byte{byte('a' + i)}})
		wire = WriteFrame(wire, Header{Version: Version, Code: byte(OpGet), RequestID: uint32(i)}, body)
	}
	r := bytes.NewReader(wire)
	var buf []byte
	for i := 0; i < 2; i++ {
		f, nb, err := ReadFrame(r, nil, buf, 0)
		buf = nb
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if f.RequestID != uint32(i) {
			t.Fatalf("frame %d: request_id = %d", i, f.RequestID)
		}
		cmd, err := DecodeCommand(f.Opcode(), f.Body)
		if err != nil {
			t.Fatalf("frame %d decode: %v", i, err)
		}
		if cmd.Key[0] != byte('a'+i) {
			t.Fatalf("frame %d: key = %q", i, cmd.Key)
		}
	}
	if _, _, err := ReadFrame(r, nil, buf, 0); err != io.EOF {
		t.Fatalf("want clean EOF on frame boundary, got %v", err)
	}
}

func TestReadFrameDisconnectMidFrame(t *testing.T) {
	body := EncodeCommand(nil, Command{Op: OpGet, Key: []byte("hello")})
	wire := WriteFrame(nil, Header{Version: Version, Code: byte(OpGet)}, body)

	// Cut after the header but before the body: this is the "client
	// disconnects mid-frame" adversarial case.
	_, _, err := ReadFrame(bytes.NewReader(wire[:HeaderLen]), nil, nil, 0)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("got %v, want ErrUnexpectedEOF", err)
	}

	// Cut inside the header.
	_, _, err = ReadFrame(bytes.NewReader(wire[:HeaderLen-3]), nil, nil, 0)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("got %v, want ErrUnexpectedEOF", err)
	}
}

func TestReadFrameForgedLengthDoesNotAllocate(t *testing.T) {
	b := make([]byte, HeaderLen)
	EncodeHeader(b, Header{Version: Version, Code: byte(OpSet)})
	binary.LittleEndian.PutUint32(b[12:16], 0xFFFFFFFF) // 4 GiB

	allocs := testing.AllocsPerRun(50, func() {
		hdr := make([]byte, HeaderLen)
		_, _, err := ReadFrame(bytes.NewReader(b), hdr, nil, 0)
		if !errors.Is(err, ErrFrameTooLarge) {
			t.Fatalf("got %v, want ErrFrameTooLarge", err)
		}
	})
	// The only allocations permitted are the scratch header and the reader
	// itself; nothing sized by body_len.
	if allocs > 6 {
		t.Fatalf("forged length caused %v allocations per run", allocs)
	}
}

func TestReadFrameRespectsStricterServerLimit(t *testing.T) {
	body := make([]byte, 4096)
	wire := WriteFrame(nil, Header{Version: Version, Code: byte(OpPing)}, body)
	_, _, err := ReadFrame(bytes.NewReader(wire), nil, nil, 1024)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("got %v, want ErrFrameTooLarge", err)
	}
}

func TestReadFrameReusesScratchBuffer(t *testing.T) {
	body := make([]byte, 512)
	wire := WriteFrame(nil, Header{Version: Version, Code: byte(OpPing)}, body)

	buf := make([]byte, 0, 4096)
	before := cap(buf)
	f, buf2, err := ReadFrame(bytes.NewReader(wire), nil, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if cap(buf2) != before {
		t.Fatalf("scratch buffer was reallocated: cap %d -> %d", before, cap(buf2))
	}
	if len(f.Body) != 512 {
		t.Fatalf("body len = %d", len(f.Body))
	}
	// Body must alias the scratch buffer (documented behaviour that forces
	// consumers to copy). buf2 is returned with len 0, so reslice to reach
	// the backing array.
	if &f.Body[0] != &buf2[:1][0] {
		t.Fatal("body does not alias the scratch buffer")
	}
}

// shortWriter writes at most n bytes per call, to exercise WriteFull's loop.
type shortWriter struct {
	buf bytes.Buffer
	n   int
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) > w.n {
		p = p[:w.n]
	}
	return w.buf.Write(p)
}

func TestWriteFullLoopsOverShortWrites(t *testing.T) {
	data := bytes.Repeat([]byte("abcdefgh"), 1000)
	w := &shortWriter{n: 7}
	if err := WriteFull(w, data); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(w.buf.Bytes(), data) {
		t.Fatalf("short-write loop lost data: got %d bytes, want %d", w.buf.Len(), len(data))
	}
}

type stuckWriter struct{}

func (stuckWriter) Write(p []byte) (int, error) { return 0, nil }

func TestWriteFullDetectsNoProgress(t *testing.T) {
	if err := WriteFull(stuckWriter{}, []byte("x")); err != io.ErrShortWrite {
		t.Fatalf("want ErrShortWrite on a writer that never progresses")
	}
}
