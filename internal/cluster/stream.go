package cluster

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/raqueeb/kvstore/internal/wal"
)

// The replication stream sits on top of a hijacked client connection. Once a
// SYNC frame has been accepted, the connection stops speaking the
// request/response protocol and becomes a one-way log feed with a
// low-frequency acknowledgement channel coming back.
//
// Message framing:
//
//	[1 byte type][payload]
//
// Payloads are self-delimiting: a WAL record carries its own length field,
// and the fixed-size messages are fixed size. Little-endian, like everything
// else here.
//
// Why ship the log and not the state (DESIGN.md interview question 22): the
// log is an ordered, idempotent-on-replay description of *changes*, so a
// replica that has fallen behind by N records needs exactly those N records
// to catch up, and it can resume from any point by LSN. Shipping state means
// either sending the whole keyspace every time or inventing a diff protocol,
// which is a worse version of the log you already have.
type msgType uint8

const (
	msgRecord    msgType = 1 // a WAL record to apply
	msgHeartbeat msgType = 2 // primary liveness + its current LSN
	msgFullBegin msgType = 3 // start of a full resync
	msgFullEnd   msgType = 4 // end of a full resync
	msgAck       msgType = 5 // replica -> primary: applied LSN
	msgReject    msgType = 6 // primary -> replica: refused, with a reason
)

func (m msgType) String() string {
	switch m {
	case msgRecord:
		return "RECORD"
	case msgHeartbeat:
		return "HEARTBEAT"
	case msgFullBegin:
		return "FULL_BEGIN"
	case msgFullEnd:
		return "FULL_END"
	case msgAck:
		return "ACK"
	case msgReject:
		return "REJECT"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", uint8(m))
	}
}

// maxStreamPayload bounds any single message so a hostile or broken peer
// cannot drive an unbounded allocation. Same discipline as the client
// protocol: a length off the wire is hostile until proven otherwise.
const maxStreamPayload = wal.MaxRecordLen + 64

// writeRecord frames one WAL record onto w.
func writeRecord(w io.Writer, buf []byte, rec wal.Record) ([]byte, error) {
	buf = append(buf[:0], byte(msgRecord))
	buf = wal.AppendRecord(buf, rec)
	_, err := w.Write(buf)
	return buf, err
}

// writeHeartbeat frames a heartbeat: primary LSN and epoch.
func writeHeartbeat(w io.Writer, lsn, epoch uint64) error {
	var b [17]byte
	b[0] = byte(msgHeartbeat)
	binary.LittleEndian.PutUint64(b[1:9], lsn)
	binary.LittleEndian.PutUint64(b[9:17], epoch)
	_, err := w.Write(b[:])
	return err
}

func writeLSNMsg(w io.Writer, t msgType, lsn uint64) error {
	var b [9]byte
	b[0] = byte(t)
	binary.LittleEndian.PutUint64(b[1:9], lsn)
	_, err := w.Write(b[:])
	return err
}

func writeReject(w io.Writer, reason string) error {
	if len(reason) > 1024 {
		reason = reason[:1024]
	}
	b := make([]byte, 3+len(reason))
	b[0] = byte(msgReject)
	binary.LittleEndian.PutUint16(b[1:3], uint16(len(reason)))
	copy(b[3:], reason)
	_, err := w.Write(b)
	return err
}

// streamMsg is a decoded stream message.
type streamMsg struct {
	Type   msgType
	Record wal.Record
	LSN    uint64
	Epoch  uint64
	Reason string
}

// readMsg reads one message. rec fields alias buf, which is reused.
func readMsg(r io.Reader, buf []byte) (streamMsg, []byte, error) {
	var t [1]byte
	if _, err := io.ReadFull(r, t[:]); err != nil {
		return streamMsg{}, buf, err
	}
	m := streamMsg{Type: msgType(t[0])}

	switch m.Type {
	case msgRecord:
		// Read the 8-byte record header first so we know the payload size
		// before allocating anything.
		var hdr [8]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return m, buf, err
		}
		length := binary.LittleEndian.Uint32(hdr[4:8])
		if length > maxStreamPayload {
			return m, buf, fmt.Errorf("replication: record length %d exceeds limit", length)
		}
		total := 8 + int(length)
		if cap(buf) < total {
			buf = make([]byte, total)
		}
		buf = buf[:total]
		copy(buf[:8], hdr[:])
		if _, err := io.ReadFull(r, buf[8:]); err != nil {
			return m, buf, err
		}
		rec, _, err := wal.DecodeRecord(buf, int64(total))
		if err != nil {
			return m, buf, fmt.Errorf("replication: %w", err)
		}
		m.Record = rec
		return m, buf, nil

	case msgHeartbeat:
		var b [16]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return m, buf, err
		}
		m.LSN = binary.LittleEndian.Uint64(b[0:8])
		m.Epoch = binary.LittleEndian.Uint64(b[8:16])
		return m, buf, nil

	case msgFullBegin, msgFullEnd, msgAck:
		var b [8]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return m, buf, err
		}
		m.LSN = binary.LittleEndian.Uint64(b[:])
		return m, buf, nil

	case msgReject:
		var b [2]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return m, buf, err
		}
		n := binary.LittleEndian.Uint16(b[:])
		reason := make([]byte, n)
		if _, err := io.ReadFull(r, reason); err != nil {
			return m, buf, err
		}
		m.Reason = string(reason)
		return m, buf, nil

	default:
		return m, buf, fmt.Errorf("replication: unknown stream message type %d", t[0])
	}
}
