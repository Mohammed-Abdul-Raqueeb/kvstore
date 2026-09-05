package protocol

// Wire-protocol limits.
//
// Every one of these is enforced *before* any allocation sized by a
// client-controlled length field. See DESIGN.md §4 ("Limits"):
//
//	"Never make([]byte, body_len) before validating body_len <= MAX_FRAME_LEN.
//	 A four-byte forged length field is otherwise a one-packet OOM kill."
//
// The values are package-level constants rather than config fields on
// purpose: the decoder is a pure function and must be testable and fuzzable
// without constructing a server. The server may impose *stricter* limits at
// runtime (see config.Config.MaxValueLen), never looser ones.
const (
	// MaxKeyLen is 64 KiB - 1. The u16 key_len field caps this naturally;
	// we state it explicitly so the check is visible at the call site.
	MaxKeyLen = 1<<16 - 1

	// MaxValueLen is 16 MiB.
	MaxValueLen = 16 << 20

	// MaxFrameLen bounds the body of a single frame. It is the largest
	// legal SET body (key + value + fixed fields) plus slack.
	//
	//	key_len:u16 + key + val_len:u32 + value + ttl_ms:u64
	MaxFrameLen = MaxValueLen + MaxKeyLen + 64

	// HeaderLen is the fixed frame header size in bytes.
	HeaderLen = 16

	// Magic is "KV" read as a little-endian u16: 'K'=0x4B at byte 0,
	// 'V'=0x56 at byte 1, so the u16 value is 0x564B.
	Magic uint16 = 0x564B

	// Version is the current protocol version.
	Version uint8 = 0x01
)

// Frame flag bits (header offset 4, u16, little-endian).
const (
	// FlagNoReply asks the server not to send a response for this request.
	// Fire-and-forget. Protocol errors are still reported (and still close
	// the connection) because the stream is unusable after one.
	FlagNoReply uint16 = 1 << 0
)

// AllFlags is the mask of every flag bit this version understands.
// Unknown flag bits are rejected so that they remain available for future
// versioning — the same reasoning as the reserved field.
const AllFlags = FlagNoReply
