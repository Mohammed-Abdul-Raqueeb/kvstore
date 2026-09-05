package protocol

import (
	"bytes"
	"errors"
	"testing"
)

func TestCommandRoundTripEveryOpcode(t *testing.T) {
	cmds := []Command{
		{Op: OpPing},
		{Op: OpStats},
		{Op: OpFlush},
		{Op: OpSnapshot},
		{Op: OpPromote},
		{Op: OpGet, Key: []byte("alpha")},
		{Op: OpDelete, Key: []byte("beta")},
		{Op: OpExists, Key: []byte("gamma")},
		{Op: OpTTL, Key: []byte("delta")},
		{Op: OpSet, Key: []byte("k"), Value: []byte("v"), TTLMillis: 0},
		{Op: OpSet, Key: []byte("k"), Value: []byte("v"), TTLMillis: 1234567},
		{Op: OpExpire, Key: []byte("k"), TTLMillis: 99},
		{Op: OpKeys, Key: []byte("pre"), Limit: 100},
		{Op: OpReplConf, NodeID: []byte("node-a"), NodePort: 7371},
		{Op: OpSync, FromLSN: 918273645},
		{Op: OpReplAck, FromLSN: 42},
		// Keys are arbitrary bytes: NUL, newline, invalid UTF-8.
		{Op: OpSet, Key: []byte{0x00, '\n', 0xFF, 0xFE}, Value: []byte{0x00, 0x01}},
	}
	for i, want := range cmds {
		body := EncodeCommand(nil, want)
		got, err := DecodeCommand(want.Op, body)
		if err != nil {
			t.Fatalf("case %d (%s): decode: %v", i, want.Op, err)
		}
		if got.Op != want.Op ||
			!bytes.Equal(got.Key, want.Key) ||
			!bytes.Equal(got.Value, want.Value) ||
			got.TTLMillis != want.TTLMillis ||
			got.Limit != want.Limit ||
			got.FromLSN != want.FromLSN ||
			!bytes.Equal(got.NodeID, want.NodeID) ||
			got.NodePort != want.NodePort {
			t.Fatalf("case %d (%s):\n got %+v\nwant %+v", i, want.Op, got, want)
		}
	}
}

func TestCommandBoundaryLengths(t *testing.T) {
	for _, klen := range []int{0, 1, 255, 256, MaxKeyLen - 1, MaxKeyLen} {
		key := bytes.Repeat([]byte("k"), klen)
		body := EncodeCommand(nil, Command{Op: OpGet, Key: key})
		got, err := DecodeCommand(OpGet, body)
		if err != nil {
			t.Fatalf("key len %d: %v", klen, err)
		}
		if len(got.Key) != klen {
			t.Fatalf("key len %d: decoded %d", klen, len(got.Key))
		}
	}

	for _, vlen := range []int{0, 1, 65535, 65536} {
		val := bytes.Repeat([]byte("v"), vlen)
		body := EncodeCommand(nil, Command{Op: OpSet, Key: []byte("k"), Value: val})
		got, err := DecodeCommand(OpSet, body)
		if err != nil {
			t.Fatalf("value len %d: %v", vlen, err)
		}
		if len(got.Value) != vlen {
			t.Fatalf("value len %d: decoded %d", vlen, len(got.Value))
		}
	}
}

func TestSetRejectsOversizedDeclaredValue(t *testing.T) {
	// Hand-forge a body whose val_len says MaxValueLen+1 while the actual
	// payload is two bytes. The decoder must reject on the declared length
	// alone, before it looks at what is actually there.
	body := appendU16(nil, 1)
	body = append(body, 'k')
	body = appendU32(body, MaxValueLen+1)
	body = append(body, 'x', 'y')

	_, err := DecodeCommand(OpSet, body)
	if !errors.Is(err, ErrValueTooLong) {
		t.Fatalf("got %v, want ErrValueTooLong", err)
	}
}

func TestDecodeRejectsTruncatedBodies(t *testing.T) {
	full := EncodeCommand(nil, Command{Op: OpSet, Key: []byte("key"), Value: []byte("value"), TTLMillis: 7})
	for cut := 0; cut < len(full); cut++ {
		_, err := DecodeCommand(OpSet, full[:cut])
		if err == nil {
			t.Fatalf("truncation at %d/%d accepted", cut, len(full))
		}
	}
	if _, err := DecodeCommand(OpSet, full); err != nil {
		t.Fatalf("full body rejected: %v", err)
	}
}

func TestDecodeRejectsTrailingBytes(t *testing.T) {
	body := EncodeCommand(nil, Command{Op: OpGet, Key: []byte("k")})
	_, err := DecodeCommand(OpGet, append(body, 0x00))
	if !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("got %v, want ErrTrailingBytes", err)
	}
}

func TestDecodeUnknownOpcodeIsNotFatal(t *testing.T) {
	_, err := DecodeCommand(Opcode(0x7E), nil)
	if !errors.Is(err, ErrUnknownOpcode) {
		t.Fatalf("got %v, want ErrUnknownOpcode", err)
	}
	// Crucially: an unknown opcode must NOT be a framing error, because the
	// frame boundary is still known. Connection survives.
	if errors.Is(err, ErrProtocol) {
		t.Fatal("unknown opcode must not wrap ErrProtocol")
	}
}

func TestDecodedSlicesAliasInput(t *testing.T) {
	body := EncodeCommand(nil, Command{Op: OpSet, Key: []byte("kk"), Value: []byte("vvvv")})
	cmd, err := DecodeCommand(OpSet, body)
	if err != nil {
		t.Fatal(err)
	}
	inBody := func(s []byte) bool {
		if len(s) == 0 {
			return true
		}
		return &s[0] == &body[2] || (&s[0] != nil && sliceWithin(s, body))
	}
	if !inBody(cmd.Key) || !inBody(cmd.Value) {
		t.Fatal("decoded slices must alias the input body, not copies")
	}
	// And they must be capacity-clamped so an append cannot scribble over
	// the neighbouring field.
	if cap(cmd.Key) != len(cmd.Key) {
		t.Fatalf("key cap %d != len %d; append would corrupt the value", cap(cmd.Key), len(cmd.Key))
	}
}

func sliceWithin(s, outer []byte) bool {
	if len(s) == 0 || len(outer) == 0 {
		return true
	}
	for i := range outer {
		if &outer[i] == &s[0] {
			return true
		}
	}
	return false
}

func TestResponseBodyCodecs(t *testing.T) {
	t.Run("value", func(t *testing.T) {
		for _, v := range [][]byte{nil, {}, []byte("x"), bytes.Repeat([]byte("y"), 100000)} {
			got, err := DecodeValueBody(EncodeValueBody(nil, v))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, v) {
				t.Fatalf("value round trip failed for len %d", len(v))
			}
		}
	})
	t.Run("bool", func(t *testing.T) {
		for _, b := range []bool{true, false} {
			got, err := DecodeBoolBody(EncodeBoolBody(nil, b))
			if err != nil || got != b {
				t.Fatalf("bool round trip: got %v err %v want %v", got, err, b)
			}
		}
	})
	t.Run("ttl", func(t *testing.T) {
		for _, ms := range []int64{-1, 0, 1, 1 << 40} {
			got, err := DecodeTTLBody(EncodeTTLBody(nil, ms))
			if err != nil || got != ms {
				t.Fatalf("ttl round trip: got %v err %v want %v", got, err, ms)
			}
		}
	})
	t.Run("keys", func(t *testing.T) {
		keys := [][]byte{[]byte("a"), []byte(""), []byte("longer-key"), {0x00, 0xFF}}
		got, err := DecodeKeysBody(EncodeKeysBody(nil, keys))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(keys) {
			t.Fatalf("got %d keys, want %d", len(got), len(keys))
		}
		for i := range keys {
			if !bytes.Equal(got[i], keys[i]) {
				t.Fatalf("key %d mismatch", i)
			}
		}
	})
}

func TestDecodeKeysBodyForgedCount(t *testing.T) {
	// count = 4 billion, body = 4 bytes. Must not try to allocate.
	body := appendU32(nil, 0xFFFFFFFF)
	if _, err := DecodeKeysBody(body); err == nil {
		t.Fatal("forged key count accepted")
	}
}
