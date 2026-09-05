package server

import (
	"bufio"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raqueeb/kvstore/internal/protocol"
)

// conn is one client connection.
//
// Structure: a reader goroutine decodes frames and produces responses; a
// writer goroutine drains a bounded queue onto the socket. Splitting them is
// what makes slow-client protection possible — with a single goroutine doing
// read-execute-write, a client that stops reading blocks the handler inside
// write(), and with the worker-pool architecture that would block a shared
// worker rather than just this connection.
type conn struct {
	id     uint64
	nc     net.Conn
	srv    *Server
	log    *slog.Logger
	reader *bufio.Reader

	// out carries encoded frames to the writer goroutine. Bounded: if it
	// fills, the client is not reading fast enough.
	out     chan []byte
	outSize atomic.Int64

	closeOnce sync.Once
	closed    chan struct{}
	writerWG  sync.WaitGroup

	requests atomic.Uint64
	hijacked atomic.Bool
	// closeReason is recorded for the log line rather than being guessed at
	// from an io error three layers up.
	closeReason atomic.Pointer[string]
}

var errOutputBufferFull = errors.New("client output buffer limit exceeded")

func newConn(id uint64, nc net.Conn, srv *Server) *conn {
	return &conn{
		id:     id,
		nc:     nc,
		srv:    srv,
		log:    srv.log.With("conn", id, "peer", nc.RemoteAddr().String()),
		reader: bufio.NewReaderSize(nc, 64<<10),
		out:    make(chan []byte, 64),
		closed: make(chan struct{}),
	}
}

// serve runs the read loop. It returns when the connection is finished.
func (c *conn) serve() {
	defer func() {
		// A hijacked connection now belongs to the cluster layer, which is
		// streaming on it. Closing it here would tear down replication.
		if !c.hijacked.Load() {
			c.close("read loop exited")
		}
	}()

	c.writerWG.Add(1)
	go c.writeLoop()

	var scratch []byte
	var respBuf []byte

	for {
		select {
		case <-c.closed:
			return
		case <-c.srv.ctx.Done():
			return
		default:
		}

		// A deadline on every read. Without one, a client that connects and
		// sends nothing holds a file descriptor forever; a few thousand of
		// those exhaust the ulimit and the server stops accepting anyone.
		if c.srv.cfg.IdleTimeout > 0 {
			_ = c.nc.SetReadDeadline(time.Now().Add(c.srv.cfg.IdleTimeout))
		}

		frame, next, err := protocol.ReadFrame(c.reader, nil, scratch, uint32(c.srv.maxFrame))
		scratch = next
		if err != nil {
			c.handleReadError(err, frame)
			return
		}
		c.requests.Add(1)
		c.srv.totalRequests.Add(1)

		// Replication opcodes may take the connection over entirely. The
		// buffered reader goes with it: it can already hold bytes the
		// cluster layer needs, and reading from the raw socket instead
		// would silently drop them.
		if isReplOpcode(frame.Opcode()) && c.srv.replHandler != nil {
			if c.srv.replHandler(c.nc, c.reader, frame) {
				c.hijacked.Store(true)
				c.srv.removeConn(c)
				close(c.closed) // stops the writer goroutine
				return
			}
			continue
		}

		respBuf = c.srv.execute(frame, respBuf[:0])

		if frame.NoReply() {
			// Fire-and-forget. The client explicitly asked not to be told,
			// so we skip the response entirely rather than sending one it
			// will not read.
			continue
		}
		if err := c.enqueue(respBuf); err != nil {
			c.close(err.Error())
			return
		}
	}
}

// handleReadError classifies why the read loop stopped and responds
// appropriately.
func (c *conn) handleReadError(err error, frame protocol.Frame) {
	switch {
	case err == io.EOF:
		// Clean close on a frame boundary. Not an error.
		c.close("client closed")

	case errors.Is(err, protocol.ErrProtocol):
		// Framing is broken. We no longer know where the next message
		// starts, so there is no safe way to continue: send the error and
		// close. This is the distinction the status table exists to make.
		c.srv.protocolErrors.Add(1)
		body := protocol.EncodeErrorBody(nil, err.Error())
		resp := protocol.WriteFrame(nil, protocol.Header{
			Version:   protocol.Version,
			Code:      byte(protocol.StatusProtocolError),
			RequestID: frame.RequestID,
		}, body)
		// Write directly with a short deadline rather than going through
		// the queue: we are about to close, and the queue may be backed up
		// precisely because this client is misbehaving.
		_ = c.nc.SetWriteDeadline(time.Now().Add(2 * time.Second))
		_ = protocol.WriteFull(c.nc, resp)
		c.log.Debug("protocol error; closing connection", "err", err)
		c.close("protocol error: " + err.Error())

	case isTimeout(err):
		c.srv.timeouts.Add(1)
		c.log.Debug("connection timed out")
		c.close("idle timeout")

	case err == io.ErrUnexpectedEOF:
		c.log.Debug("client disconnected mid-frame")
		c.close("disconnected mid-frame")

	default:
		c.log.Debug("read error", "err", err)
		c.close("read error: " + err.Error())
	}
}

// enqueue hands a response to the writer goroutine.
//
// The bounded queue is the slow-client defence. DESIGN.md §7 lists exactly
// three options when a response will not fit: block the handler (wrong — one
// slow client stalls shared resources), drop the response (wrong — it breaks
// the protocol contract), or close the connection with a logged reason.
// We close. This is what Redis does with client-output-buffer-limit.
func (c *conn) enqueue(resp []byte) error {
	if c.outSize.Load()+int64(len(resp)) > c.srv.cfg.OutputBufferLimit {
		c.srv.outputOverflows.Add(1)
		c.log.Warn("closing connection: output buffer limit exceeded",
			"limit", c.srv.cfg.OutputBufferLimit, "queued", c.outSize.Load())
		return errOutputBufferFull
	}
	// Copy: respBuf is reused by the read loop for the next request.
	cp := make([]byte, len(resp))
	copy(cp, resp)

	select {
	case c.out <- cp:
		c.outSize.Add(int64(len(cp)))
		return nil
	case <-c.closed:
		return errors.New("connection closed")
	default:
		c.srv.outputOverflows.Add(1)
		c.log.Warn("closing connection: output queue full")
		return errOutputBufferFull
	}
}

func (c *conn) writeLoop() {
	defer c.writerWG.Done()
	// Buffering here means a pipelined burst of small responses becomes one
	// syscall rather than one per response.
	bw := bufio.NewWriterSize(c.nc, 64<<10)

	flush := func() bool {
		if bw.Buffered() == 0 {
			return true
		}
		if c.srv.cfg.WriteTimeout > 0 {
			_ = c.nc.SetWriteDeadline(time.Now().Add(c.srv.cfg.WriteTimeout))
		}
		if err := bw.Flush(); err != nil {
			c.close("write error: " + err.Error())
			return false
		}
		return true
	}

	for {
		select {
		case <-c.closed:
			flush()
			return
		case b, ok := <-c.out:
			if !ok {
				flush()
				return
			}
			c.outSize.Add(-int64(len(b)))
			if _, err := bw.Write(b); err != nil {
				c.close("write error: " + err.Error())
				return
			}
			// Coalesce whatever else is already queued before flushing.
			drained := true
			for drained {
				select {
				case more, ok := <-c.out:
					if !ok {
						drained = false
						break
					}
					c.outSize.Add(-int64(len(more)))
					if _, err := bw.Write(more); err != nil {
						c.close("write error: " + err.Error())
						return
					}
				default:
					drained = false
				}
			}
			if !flush() {
				return
			}
		}
	}
}

func (c *conn) close(reason string) {
	c.closeOnce.Do(func() {
		c.closeReason.Store(&reason)
		close(c.closed)
		_ = c.nc.SetDeadline(time.Now().Add(time.Second))
		_ = c.nc.Close()
		c.srv.removeConn(c)
	})
}

// wait blocks until the writer goroutine has finished.
func (c *conn) wait() { c.writerWG.Wait() }

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func isReplOpcode(op protocol.Opcode) bool {
	switch op {
	case protocol.OpReplConf, protocol.OpSync, protocol.OpReplAck, protocol.OpPromote:
		return true
	default:
		return false
	}
}
