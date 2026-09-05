package server

import (
	"errors"

	"github.com/raqueeb/kvstore/internal/engine"
	"github.com/raqueeb/kvstore/internal/protocol"
	"github.com/raqueeb/kvstore/internal/store"
)

// execute decodes one request frame, runs it against the engine, and appends
// the response frame to dst.
//
// It never returns a Go error: every outcome is a status on a well-formed
// response. The caller only closes the connection when the *status* says to
// (StatusProtocolError), which keeps the "is this fatal?" decision in one
// place instead of spread across every command.
func (s *Server) execute(frame protocol.Frame, dst []byte) []byte {
	op := frame.Opcode()

	cmd, err := protocol.DecodeCommand(op, frame.Body)
	if err != nil {
		if errors.Is(err, protocol.ErrUnknownOpcode) {
			// Well-framed, just not a command we know. The stream is still
			// synchronised, so the connection survives.
			return s.respondErr(dst, frame, protocol.StatusBadRequest, "unknown opcode")
		}
		if errors.Is(err, protocol.ErrValueTooLong) || errors.Is(err, protocol.ErrKeyTooLong) {
			return s.respondErr(dst, frame, protocol.StatusTooLarge, err.Error())
		}
		// Anything else from the body decoder means the body did not match
		// the length the header promised, which is a framing failure.
		return s.respondErr(dst, frame, protocol.StatusProtocolError, err.Error())
	}

	switch op {
	case protocol.OpPing:
		return s.respond(dst, frame, protocol.StatusOK, nil)

	case protocol.OpGet:
		if len(cmd.Key) == 0 {
			return s.respondErr(dst, frame, protocol.StatusBadRequest, "empty key")
		}
		v, ok := s.eng.Get(cmd.Key)
		if !ok {
			return s.respond(dst, frame, protocol.StatusNotFound, nil)
		}
		return s.respond(dst, frame, protocol.StatusOK, protocol.EncodeValueBody(nil, v))

	case protocol.OpSet:
		if len(cmd.Key) == 0 {
			return s.respondErr(dst, frame, protocol.StatusBadRequest, "empty key")
		}
		if len(cmd.Value) > s.cfg.MaxValueLen {
			return s.respondErr(dst, frame, protocol.StatusTooLarge, "value exceeds max-value-len")
		}
		switch err := s.eng.Set(cmd.Key, cmd.Value, cmd.TTLMillis); {
		case err == nil:
			return s.respond(dst, frame, protocol.StatusOK, nil)
		case errors.Is(err, store.ErrOOM):
			return s.respondErr(dst, frame, protocol.StatusOOM, "memory limit reached under the current policy")
		case errors.Is(err, engine.ErrReadOnly):
			return s.respondErr(dst, frame, protocol.StatusReadOnly, "this node is a read-only replica")
		default:
			s.log.Error("SET failed", "err", err)
			return s.respondErr(dst, frame, protocol.StatusInternal, err.Error())
		}

	case protocol.OpDelete:
		if len(cmd.Key) == 0 {
			return s.respondErr(dst, frame, protocol.StatusBadRequest, "empty key")
		}
		existed, err := s.eng.Delete(cmd.Key)
		if err != nil {
			return s.mutationError(dst, frame, err)
		}
		if !existed {
			return s.respond(dst, frame, protocol.StatusNotFound, nil)
		}
		return s.respond(dst, frame, protocol.StatusOK, nil)

	case protocol.OpExists:
		if len(cmd.Key) == 0 {
			return s.respondErr(dst, frame, protocol.StatusBadRequest, "empty key")
		}
		return s.respond(dst, frame, protocol.StatusOK,
			protocol.EncodeBoolBody(nil, s.eng.Exists(cmd.Key)))

	case protocol.OpExpire:
		if len(cmd.Key) == 0 {
			return s.respondErr(dst, frame, protocol.StatusBadRequest, "empty key")
		}
		ok, err := s.eng.Expire(cmd.Key, cmd.TTLMillis)
		if err != nil {
			return s.mutationError(dst, frame, err)
		}
		if !ok {
			return s.respond(dst, frame, protocol.StatusNotFound, nil)
		}
		return s.respond(dst, frame, protocol.StatusOK, nil)

	case protocol.OpTTL:
		if len(cmd.Key) == 0 {
			return s.respondErr(dst, frame, protocol.StatusBadRequest, "empty key")
		}
		ms, ok := s.eng.TTL(cmd.Key)
		if !ok {
			return s.respond(dst, frame, protocol.StatusNotFound, nil)
		}
		return s.respond(dst, frame, protocol.StatusOK, protocol.EncodeTTLBody(nil, ms))

	case protocol.OpKeys:
		limit := int(cmd.Limit)
		if limit <= 0 || limit > 10000 {
			limit = 10000
		}
		return s.respond(dst, frame, protocol.StatusOK,
			protocol.EncodeKeysBody(nil, s.eng.Keys(cmd.Key, limit)))

	case protocol.OpStats:
		b, err := s.statsJSON()
		if err != nil {
			return s.respondErr(dst, frame, protocol.StatusInternal, err.Error())
		}
		return s.respond(dst, frame, protocol.StatusOK, protocol.EncodeValueBody(nil, b))

	case protocol.OpFlush:
		if err := s.eng.Flush(); err != nil {
			return s.mutationError(dst, frame, err)
		}
		return s.respond(dst, frame, protocol.StatusOK, nil)

	case protocol.OpSnapshot:
		if s.eng.ReadOnly() {
			return s.respondErr(dst, frame, protocol.StatusReadOnly, "this node is a read-only replica")
		}
		res, err := s.eng.Snapshot()
		if err != nil {
			return s.respondErr(dst, frame, protocol.StatusInternal, err.Error())
		}
		return s.respond(dst, frame, protocol.StatusOK,
			protocol.EncodeValueBody(nil, []byte(res.Path)))

	case protocol.OpReplConf, protocol.OpSync, protocol.OpReplAck, protocol.OpPromote:
		// Replication opcodes are handled by the cluster layer, which hijacks
		// the connection before the normal dispatch loop sees them. Reaching
		// here means clustering is not enabled on this build or this node.
		if s.replHandler == nil {
			return s.respondErr(dst, frame, protocol.StatusBadRequest, "replication is not enabled on this node")
		}
		return s.respondErr(dst, frame, protocol.StatusInternal, "replication frame reached the normal dispatcher")

	default:
		return s.respondErr(dst, frame, protocol.StatusBadRequest, "unknown opcode")
	}
}

func (s *Server) mutationError(dst []byte, frame protocol.Frame, err error) []byte {
	switch {
	case errors.Is(err, engine.ErrReadOnly):
		return s.respondErr(dst, frame, protocol.StatusReadOnly, "this node is a read-only replica")
	case errors.Is(err, store.ErrOOM):
		return s.respondErr(dst, frame, protocol.StatusOOM, "memory limit reached")
	case errors.Is(err, engine.ErrClosed):
		return s.respondErr(dst, frame, protocol.StatusInternal, "server is shutting down")
	default:
		s.log.Error("mutation failed", "err", err)
		return s.respondErr(dst, frame, protocol.StatusInternal, err.Error())
	}
}

func (s *Server) respond(dst []byte, frame protocol.Frame, status protocol.Status, body []byte) []byte {
	return protocol.WriteFrame(dst, protocol.Header{
		Version:   protocol.Version,
		Code:      byte(status),
		RequestID: frame.RequestID,
	}, body)
}

func (s *Server) respondErr(dst []byte, frame protocol.Frame, status protocol.Status, msg string) []byte {
	return s.respond(dst, frame, status, protocol.EncodeErrorBody(nil, msg))
}
