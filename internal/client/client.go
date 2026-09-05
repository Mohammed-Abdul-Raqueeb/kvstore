package client

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raqueeb/kvstore/internal/protocol"
)

// Errors returned by the client.
var (
	ErrNotFound = errors.New("key not found")
	ErrClosed   = errors.New("client is closed")
)

// StatusError carries a non-OK status from the server.
type StatusError struct {
	Status  protocol.Status
	Message string
}

func (e *StatusError) Error() string {
	if e.Message == "" {
		return e.Status.String()
	}
	return fmt.Sprintf("%s: %s", e.Status, e.Message)
}

// Client is a single connection to a kvstore server.
//
// It is safe for concurrent use: a mutex serialises request/response pairs.
// That is deliberate for the simple API — the protocol supports pipelining
// via request_id, and Pipeline() exposes it for callers (kvbench) that want
// depth, but the default path stays easy to reason about.
type Client struct {
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer

	mu      sync.Mutex
	nextID  atomic.Uint32
	scratch []byte
	reqBuf  []byte

	timeout time.Duration
	closed  atomic.Bool
}

// Options configures a client.
type Options struct {
	Addr    string
	Timeout time.Duration
}

// Dial connects to a server.
func Dial(addr string) (*Client, error) {
	return DialWithOptions(Options{Addr: addr, Timeout: 30 * time.Second})
}

// DialWithOptions connects with explicit options.
func DialWithOptions(o Options) (*Client, error) {
	if o.Timeout <= 0 {
		o.Timeout = 30 * time.Second
	}
	nc, err := net.DialTimeout("tcp", o.Addr, o.Timeout)
	if err != nil {
		return nil, err
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return &Client{
		conn:    nc,
		br:      bufio.NewReaderSize(nc, 64<<10),
		bw:      bufio.NewWriterSize(nc, 64<<10),
		timeout: o.Timeout,
	}, nil
}

// Close shuts the connection.
func (c *Client) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	return c.conn.Close()
}

// Conn exposes the raw connection, for adversarial tests that need to send
// malformed bytes.
func (c *Client) Conn() net.Conn { return c.conn }

// do sends one request and reads one response.
func (c *Client) do(op protocol.Opcode, cmd protocol.Command) (protocol.Frame, error) {
	if c.closed.Load() {
		return protocol.Frame{}, ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	id := c.nextID.Add(1)
	cmd.Op = op
	body := protocol.EncodeCommand(nil, cmd)
	c.reqBuf = protocol.WriteFrame(c.reqBuf[:0], protocol.Header{
		Version:   protocol.Version,
		Code:      byte(op),
		RequestID: id,
	}, body)

	if c.timeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
	}
	if err := protocol.WriteFull(c.bw, c.reqBuf); err != nil {
		return protocol.Frame{}, err
	}
	if err := c.bw.Flush(); err != nil {
		return protocol.Frame{}, err
	}

	if c.timeout > 0 {
		_ = c.conn.SetReadDeadline(time.Now().Add(c.timeout))
	}
	frame, next, err := protocol.ReadFrame(c.br, nil, c.scratch, 0)
	c.scratch = next
	if err != nil {
		return protocol.Frame{}, err
	}
	if frame.RequestID != id {
		return frame, fmt.Errorf("response request_id %d does not match request %d", frame.RequestID, id)
	}
	return frame, nil
}

func statusErr(f protocol.Frame) error {
	return &StatusError{Status: f.Status(), Message: string(f.Body)}
}

// Ping checks liveness.
func (c *Client) Ping() error {
	f, err := c.do(protocol.OpPing, protocol.Command{})
	if err != nil {
		return err
	}
	if !f.Status().OK() {
		return statusErr(f)
	}
	return nil
}

// Get fetches a value. Returns ErrNotFound if the key is absent or expired.
func (c *Client) Get(key []byte) ([]byte, error) {
	f, err := c.do(protocol.OpGet, protocol.Command{Key: key})
	if err != nil {
		return nil, err
	}
	switch f.Status() {
	case protocol.StatusOK:
		v, err := protocol.DecodeValueBody(f.Body)
		if err != nil {
			return nil, err
		}
		return append([]byte(nil), v...), nil
	case protocol.StatusNotFound:
		return nil, ErrNotFound
	default:
		return nil, statusErr(f)
	}
}

// Set stores a value with a relative TTL in milliseconds (0 = no expiry).
func (c *Client) Set(key, value []byte, ttlMillis uint64) error {
	f, err := c.do(protocol.OpSet, protocol.Command{Key: key, Value: value, TTLMillis: ttlMillis})
	if err != nil {
		return err
	}
	if !f.Status().OK() {
		return statusErr(f)
	}
	return nil
}

// Delete removes a key, reporting whether it existed.
func (c *Client) Delete(key []byte) (bool, error) {
	f, err := c.do(protocol.OpDelete, protocol.Command{Key: key})
	if err != nil {
		return false, err
	}
	switch f.Status() {
	case protocol.StatusOK:
		return true, nil
	case protocol.StatusNotFound:
		return false, nil
	default:
		return false, statusErr(f)
	}
}

// Exists reports whether a live key is present.
func (c *Client) Exists(key []byte) (bool, error) {
	f, err := c.do(protocol.OpExists, protocol.Command{Key: key})
	if err != nil {
		return false, err
	}
	if !f.Status().OK() {
		return false, statusErr(f)
	}
	return protocol.DecodeBoolBody(f.Body)
}

// Expire sets a relative TTL on an existing key.
func (c *Client) Expire(key []byte, ttlMillis uint64) (bool, error) {
	f, err := c.do(protocol.OpExpire, protocol.Command{Key: key, TTLMillis: ttlMillis})
	if err != nil {
		return false, err
	}
	switch f.Status() {
	case protocol.StatusOK:
		return true, nil
	case protocol.StatusNotFound:
		return false, nil
	default:
		return false, statusErr(f)
	}
}

// TTL returns the remaining lifetime in milliseconds (-1 = no expiry).
func (c *Client) TTL(key []byte) (int64, error) {
	f, err := c.do(protocol.OpTTL, protocol.Command{Key: key})
	if err != nil {
		return 0, err
	}
	switch f.Status() {
	case protocol.StatusOK:
		return protocol.DecodeTTLBody(f.Body)
	case protocol.StatusNotFound:
		return 0, ErrNotFound
	default:
		return 0, statusErr(f)
	}
}

// Keys returns up to limit keys matching prefix.
func (c *Client) Keys(prefix []byte, limit uint32) ([][]byte, error) {
	f, err := c.do(protocol.OpKeys, protocol.Command{Key: prefix, Limit: limit})
	if err != nil {
		return nil, err
	}
	if !f.Status().OK() {
		return nil, statusErr(f)
	}
	keys, err := protocol.DecodeKeysBody(f.Body)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(keys))
	for i, k := range keys {
		out[i] = append([]byte(nil), k...)
	}
	return out, nil
}

// Stats returns the server's STATS document as raw JSON.
func (c *Client) Stats() (json.RawMessage, error) {
	f, err := c.do(protocol.OpStats, protocol.Command{})
	if err != nil {
		return nil, err
	}
	if !f.Status().OK() {
		return nil, statusErr(f)
	}
	v, err := protocol.DecodeValueBody(f.Body)
	if err != nil {
		return nil, err
	}
	return append(json.RawMessage(nil), v...), nil
}

// Flush empties the keyspace.
func (c *Client) Flush() error {
	f, err := c.do(protocol.OpFlush, protocol.Command{})
	if err != nil {
		return err
	}
	if !f.Status().OK() {
		return statusErr(f)
	}
	return nil
}

// Snapshot triggers a snapshot and returns its path.
func (c *Client) Snapshot() (string, error) {
	f, err := c.do(protocol.OpSnapshot, protocol.Command{})
	if err != nil {
		return "", err
	}
	if !f.Status().OK() {
		return "", statusErr(f)
	}
	v, err := protocol.DecodeValueBody(f.Body)
	if err != nil {
		return "", err
	}
	return string(v), nil
}

// --- pipelining ------------------------------------------------------------

// Pipeline sends many requests before reading any responses.
//
// This is what request_id is for. Without it a client must wait a full
// round-trip per operation, and on a loopback that means you measure the
// scheduler rather than the server. Responses are matched back to requests
// by id, so they may legitimately arrive in any order.
type Pipeline struct {
	c    *Client
	reqs []pipeReq
	buf  []byte
}

type pipeReq struct {
	id  uint32
	op  protocol.Opcode
	cmd protocol.Command
}

// Pipeline starts a batch.
func (c *Client) Pipeline() *Pipeline { return &Pipeline{c: c} }

// Add queues one command.
func (p *Pipeline) Add(op protocol.Opcode, cmd protocol.Command) uint32 {
	id := p.c.nextID.Add(1)
	cmd.Op = op
	p.reqs = append(p.reqs, pipeReq{id: id, op: op, cmd: cmd})
	return id
}

// Result pairs a request id with its response.
type Result struct {
	ID     uint32
	Status protocol.Status
	Body   []byte
}

// Run flushes every queued request and collects all responses.
func (p *Pipeline) Run() ([]Result, error) {
	if len(p.reqs) == 0 {
		return nil, nil
	}
	c := p.c
	c.mu.Lock()
	defer c.mu.Unlock()

	p.buf = p.buf[:0]
	for _, r := range p.reqs {
		body := protocol.EncodeCommand(nil, r.cmd)
		p.buf = protocol.WriteFrame(p.buf, protocol.Header{
			Version:   protocol.Version,
			Code:      byte(r.op),
			RequestID: r.id,
		}, body)
	}
	if c.timeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
	}
	if err := protocol.WriteFull(c.bw, p.buf); err != nil {
		return nil, err
	}
	if err := c.bw.Flush(); err != nil {
		return nil, err
	}

	out := make([]Result, 0, len(p.reqs))
	var scratch []byte
	for i := 0; i < len(p.reqs); i++ {
		if c.timeout > 0 {
			_ = c.conn.SetReadDeadline(time.Now().Add(c.timeout))
		}
		f, next, err := protocol.ReadFrame(c.br, nil, scratch, 0)
		scratch = next
		if err != nil {
			return out, err
		}
		out = append(out, Result{
			ID:     f.RequestID,
			Status: f.Status(),
			Body:   append([]byte(nil), f.Body...),
		})
	}
	p.reqs = p.reqs[:0]
	return out, nil
}

// RawCommand sends a command and returns an error unless the status is OK.
// Used by the replication handshake, which needs the raw opcodes.
func (c *Client) RawCommand(op protocol.Opcode, cmd protocol.Command) error {
	f, err := c.do(op, cmd)
	if err != nil {
		return err
	}
	if !f.Status().OK() {
		return statusErr(f)
	}
	return nil
}

// Promote asks the server to promote itself from replica to primary.
func (c *Client) Promote() (string, error) {
	f, err := c.do(protocol.OpPromote, protocol.Command{})
	if err != nil {
		return "", err
	}
	if !f.Status().OK() {
		return "", statusErr(f)
	}
	return string(f.Body), nil
}
