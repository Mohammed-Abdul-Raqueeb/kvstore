package protocol

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"testing"
)

// FuzzDecodeFrame drives the full header+body read path with arbitrary
// bytes. The properties asserted here are exactly the ones DESIGN.md §11
// asks for:
//
//   - never panics;
//   - never allocates more than MaxFrameLen as a result of a length field;
//   - never returns a Frame whose Body points outside the input.
func FuzzDecodeFrame(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, HeaderLen))
	f.Add(WriteFrame(nil, Header{Version: Version, Code: byte(OpPing)}, nil))
	f.Add(WriteFrame(nil, Header{Version: Version, Code: byte(OpGet)},
		EncodeCommand(nil, Command{Op: OpGet, Key: []byte("k")})))
	f.Add(WriteFrame(nil, Header{Version: Version, Code: byte(OpSet)},
		EncodeCommand(nil, Command{Op: OpSet, Key: []byte("k"), Value: []byte("v"), TTLMillis: 1})))
	// A forged 4 GiB length with no body behind it.
	f.Add([]byte{'K', 'V', 0x01, 0x03, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)

		r := bytes.NewReader(data)
		var scratch []byte
		for i := 0; i < 8; i++ {
			frame, next, err := ReadFrame(r, nil, scratch, 0)
			scratch = next
			if err != nil {
				if errors.Is(err, ErrProtocol) || err == io.EOF || err == io.ErrUnexpectedEOF {
					return
				}
				t.Fatalf("unexpected error class from ReadFrame: %v", err)
			}
			if uint32(len(frame.Body)) != frame.BodyLen {
				t.Fatalf("body len %d != declared %d", len(frame.Body), frame.BodyLen)
			}
			if len(frame.Body) > MaxFrameLen {
				t.Fatalf("body exceeds MaxFrameLen: %d", len(frame.Body))
			}
			// The body must be a view into our scratch buffer, never a
			// fresh allocation sized by an unvalidated field.
			if len(frame.Body) > 0 && !sliceWithin(frame.Body, scratch) {
				t.Fatal("frame body escaped the scratch buffer")
			}
			// Decoding must also be total.
			cmd, derr := DecodeCommand(frame.Opcode(), frame.Body)
			if derr == nil {
				if len(cmd.Key) > MaxKeyLen {
					t.Fatalf("decoded key exceeds MaxKeyLen: %d", len(cmd.Key))
				}
				if len(cmd.Value) > MaxValueLen {
					t.Fatalf("decoded value exceeds MaxValueLen: %d", len(cmd.Value))
				}
			}
		}

		runtime.ReadMemStats(&after)
		if grew := after.TotalAlloc - before.TotalAlloc; grew > 8*MaxFrameLen {
			t.Fatalf("decoding %d bytes allocated %d bytes", len(data), grew)
		}
	})
}

// FuzzDecodeCommand fuzzes the body decoder directly across every opcode,
// which reaches states the frame fuzzer only hits by luck.
func FuzzDecodeCommand(f *testing.F) {
	f.Add(uint8(OpGet), []byte{1, 0, 'k'})
	f.Add(uint8(OpSet), EncodeCommand(nil, Command{Op: OpSet, Key: []byte("k"), Value: []byte("v")}))
	f.Add(uint8(OpExpire), EncodeCommand(nil, Command{Op: OpExpire, Key: []byte("k"), TTLMillis: 9}))
	f.Add(uint8(OpKeys), EncodeCommand(nil, Command{Op: OpKeys, Key: []byte("p"), Limit: 3}))
	f.Add(uint8(OpSync), []byte{0, 0, 0, 0, 0, 0, 0, 0})
	f.Add(uint8(0xEE), []byte{0xFF, 0xFF, 0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, op uint8, body []byte) {
		cmd, err := DecodeCommand(Opcode(op), body)
		if err != nil {
			return
		}
		if len(cmd.Key) > MaxKeyLen {
			t.Fatalf("key len %d > MaxKeyLen", len(cmd.Key))
		}
		if len(cmd.Value) > MaxValueLen {
			t.Fatalf("value len %d > MaxValueLen", len(cmd.Value))
		}
		// Every returned slice must be a view into body.
		if !sliceWithin(cmd.Key, body) || !sliceWithin(cmd.Value, body) || !sliceWithin(cmd.NodeID, body) {
			t.Fatal("decoded slice points outside the input body")
		}
		// Re-encoding a successfully decoded command must reproduce the
		// exact bytes: the codec is injective on valid input, which is what
		// lets the WAL and the replication stream share it.
		if got := EncodeCommand(nil, cmd); !bytes.Equal(got, body) {
			t.Fatalf("re-encode mismatch:\n got %x\nwant %x", got, body)
		}
	})
}

// FuzzDecodeResponse covers the client-side decoders, which parse bytes from
// a potentially hostile server.
func FuzzDecodeResponse(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0})
	f.Add(EncodeValueBody(nil, []byte("v")))
	f.Add(EncodeKeysBody(nil, [][]byte{[]byte("a"), []byte("b")}))

	f.Fuzz(func(t *testing.T, body []byte) {
		if v, err := DecodeValueBody(body); err == nil && !sliceWithin(v, body) {
			t.Fatal("value escapes body")
		}
		_, _ = DecodeBoolBody(body)
		_, _ = DecodeTTLBody(body)
		if ks, err := DecodeKeysBody(body); err == nil {
			for _, k := range ks {
				if !sliceWithin(k, body) {
					t.Fatal("key escapes body")
				}
			}
		}
	})
}
