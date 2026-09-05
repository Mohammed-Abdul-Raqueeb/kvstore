package protocol

// Opcode identifies a request type. It occupies header byte 3 on a request
// frame; the same byte carries a Status on a response frame.
type Opcode uint8

const (
	OpPing     Opcode = 0x01
	OpGet      Opcode = 0x02
	OpSet      Opcode = 0x03
	OpDelete   Opcode = 0x04
	OpExists   Opcode = 0x05
	OpExpire   Opcode = 0x06
	OpStats    Opcode = 0x07
	OpTTL      Opcode = 0x08
	OpFlush    Opcode = 0x09
	OpSnapshot Opcode = 0x0A
	OpKeys     Opcode = 0x0B

	// Phase 2 (replication). Defined here so that the codec is a single
	// source of truth for the wire format, but only handled by the server
	// when clustering is enabled.
	OpReplConf Opcode = 0x10
	OpSync     Opcode = 0x11
	OpReplAck  Opcode = 0x12
	OpPromote  Opcode = 0x13
)

var opcodeNames = map[Opcode]string{
	OpPing:     "PING",
	OpGet:      "GET",
	OpSet:      "SET",
	OpDelete:   "DELETE",
	OpExists:   "EXISTS",
	OpExpire:   "EXPIRE",
	OpStats:    "STATS",
	OpTTL:      "TTL",
	OpFlush:    "FLUSH",
	OpSnapshot: "SNAPSHOT",
	OpKeys:     "KEYS",
	OpReplConf: "REPLCONF",
	OpSync:     "SYNC",
	OpReplAck:  "REPLACK",
	OpPromote:  "PROMOTE",
}

func (o Opcode) String() string {
	if n, ok := opcodeNames[o]; ok {
		return n
	}
	return "UNKNOWN"
}

// Valid reports whether the opcode is one this build understands.
func (o Opcode) Valid() bool {
	_, ok := opcodeNames[o]
	return ok
}

// IsMutation reports whether the opcode changes durable state and therefore
// must go through the WAL and be rejected on a read-only replica.
func (o Opcode) IsMutation() bool {
	switch o {
	case OpSet, OpDelete, OpExpire, OpFlush:
		return true
	default:
		return false
	}
}

// OpcodeByName maps a case-insensitive command name to an opcode. Used by
// kvctl; not part of the wire format.
func OpcodeByName(name string) (Opcode, bool) {
	for op, n := range opcodeNames {
		if equalFold(n, name) {
			return op, true
		}
	}
	return 0, false
}

func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if 'a' <= ca && ca <= 'z' {
			ca -= 32
		}
		if 'a' <= cb && cb <= 'z' {
			cb -= 32
		}
		if ca != cb {
			return false
		}
	}
	return true
}
