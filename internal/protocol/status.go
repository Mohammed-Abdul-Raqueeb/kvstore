package protocol

// Status is the response counterpart to Opcode: header byte 3 on a response
// frame.
type Status uint8

const (
	StatusOK         Status = 0x00
	StatusNotFound   Status = 0x01
	StatusBadRequest Status = 0x02
	StatusTooLarge   Status = 0x03
	StatusOOM        Status = 0x04
	StatusInternal   Status = 0x05
	StatusReadOnly   Status = 0x06
	StatusNotLeader  Status = 0x07

	// StatusProtocolError is special: it means stream desynchronisation.
	// Once the framing is wrong there is no safe resynchronisation point,
	// so the server sends this and then closes. See Fatal().
	StatusProtocolError Status = 0x80
)

var statusNames = map[Status]string{
	StatusOK:            "OK",
	StatusNotFound:      "NOT_FOUND",
	StatusBadRequest:    "BAD_REQUEST",
	StatusTooLarge:      "TOO_LARGE",
	StatusOOM:           "OOM",
	StatusInternal:      "INTERNAL",
	StatusReadOnly:      "READ_ONLY",
	StatusNotLeader:     "NOT_LEADER",
	StatusProtocolError: "PROTOCOL_ERROR",
}

func (s Status) String() string {
	if n, ok := statusNames[s]; ok {
		return n
	}
	return "UNKNOWN_STATUS"
}

// Fatal reports whether the connection must be closed after sending this
// status.
//
// The distinction is the whole point of the status table in DESIGN.md §4: a
// semantic error (missing key, value too large, out of memory) leaves the
// byte stream perfectly framed, so the connection is still usable. A framing
// error means we no longer know where the next message starts, and guessing
// is worse than disconnecting.
func (s Status) Fatal() bool { return s == StatusProtocolError }

// OK reports whether the status indicates success.
func (s Status) OK() bool { return s == StatusOK }
