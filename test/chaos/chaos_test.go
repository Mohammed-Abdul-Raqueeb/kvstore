package chaos

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/raqueeb/kvstore/internal/client"
	"github.com/raqueeb/kvstore/internal/protocol"
	"github.com/raqueeb/kvstore/test/harness"
)

// The contract these tests enforce: no input from the network, however
// hostile, may crash the server, wedge it, or make it allocate without
// bound. After every hostile client, a well-behaved client must still get
// correct service on a fresh connection.

func assertServerHealthy(t *testing.T, s *harness.InProcess) {
	t.Helper()
	c, err := client.DialWithOptions(client.Options{Addr: s.Addr, Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("server unreachable after hostile input: %v", err)
	}
	defer c.Close()
	if err := c.Ping(); err != nil {
		t.Fatalf("server unhealthy after hostile input: %v", err)
	}
	key := []byte(fmt.Sprintf("health-%d", time.Now().UnixNano()))
	if err := c.Set(key, []byte("ok"), 0); err != nil {
		t.Fatalf("server cannot serve writes after hostile input: %v", err)
	}
	v, err := c.Get(key)
	if err != nil || string(v) != "ok" {
		t.Fatalf("server returned wrong data after hostile input: %q %v", v, err)
	}
}

func header(op protocol.Opcode, id uint32, bodyLen uint32) []byte {
	b := make([]byte, protocol.HeaderLen)
	protocol.EncodeHeader(b, protocol.Header{
		Version:   protocol.Version,
		Code:      byte(op),
		RequestID: id,
		BodyLen:   bodyLen,
	})
	return b
}

func TestForgedBodyLengthDoesNotAllocate(t *testing.T) {
	s := harness.StartDefault(t)

	// A four-byte forged length field is a one-packet OOM if the server
	// sizes a buffer from it before validating.
	for _, forged := range []uint32{0xFFFFFFFF, 1 << 31, protocol.MaxFrameLen + 1, 1 << 28} {
		t.Run(fmt.Sprintf("len_%d", forged), func(t *testing.T) {
			nc := s.RawConn(t)
			h := header(protocol.OpSet, 1, 0)
			binary.LittleEndian.PutUint32(h[12:16], forged)
			if _, err := nc.Write(h); err != nil {
				t.Fatal(err)
			}
			// The server must answer PROTOCOL_ERROR and close, not wait for
			// four gigabytes that will never arrive.
			nc.SetReadDeadline(time.Now().Add(5 * time.Second))
			resp := make([]byte, protocol.HeaderLen)
			n, err := io.ReadFull(nc, resp)
			if err != nil && n == 0 {
				t.Fatalf("no response to a forged length: %v", err)
			}
			hdr, derr := protocol.DecodeHeader(resp)
			if derr != nil {
				t.Fatalf("malformed response: %v", derr)
			}
			if protocol.Status(hdr.Code) != protocol.StatusProtocolError {
				t.Fatalf("status = %s, want PROTOCOL_ERROR", protocol.Status(hdr.Code))
			}
		})
	}
	assertServerHealthy(t, s)
}

func TestRandomGarbageIsRejected(t *testing.T) {
	s := harness.StartDefault(t)

	payloads := [][]byte{
		{0x00},
		bytes.Repeat([]byte{0xFF}, 64),
		[]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"),
		[]byte("\x16\x03\x01\x02\x00\x01\x00\x01\xfc\x03\x03"), // a TLS ClientHello
		bytes.Repeat([]byte{0x4B, 0x56}, 100),                  // magic, then nonsense
		make([]byte, 4096),
	}
	for i, p := range payloads {
		t.Run(fmt.Sprintf("payload_%d", i), func(t *testing.T) {
			nc := s.RawConn(t)
			nc.SetDeadline(time.Now().Add(5 * time.Second))
			nc.Write(p)
			// Read whatever comes back; the only requirement is that the
			// connection ends rather than the server misbehaving.
			buf := make([]byte, 4096)
			for {
				if _, err := nc.Read(buf); err != nil {
					break
				}
			}
		})
	}
	assertServerHealthy(t, s)
}

func TestReservedFieldAndUnknownFlagsRejected(t *testing.T) {
	s := harness.StartDefault(t)

	t.Run("reserved non-zero", func(t *testing.T) {
		nc := s.RawConn(t)
		h := header(protocol.OpPing, 1, 0)
		binary.LittleEndian.PutUint16(h[6:8], 0x0001)
		nc.Write(h)
		expectStatus(t, nc, protocol.StatusProtocolError)
	})
	t.Run("unknown flag bit", func(t *testing.T) {
		nc := s.RawConn(t)
		h := header(protocol.OpPing, 1, 0)
		binary.LittleEndian.PutUint16(h[4:6], 0x8000)
		nc.Write(h)
		expectStatus(t, nc, protocol.StatusProtocolError)
	})
	t.Run("bad version", func(t *testing.T) {
		nc := s.RawConn(t)
		h := header(protocol.OpPing, 1, 0)
		h[2] = 0x42
		nc.Write(h)
		expectStatus(t, nc, protocol.StatusProtocolError)
	})
	t.Run("bad magic", func(t *testing.T) {
		nc := s.RawConn(t)
		h := header(protocol.OpPing, 1, 0)
		h[0] = 'X'
		nc.Write(h)
		expectStatus(t, nc, protocol.StatusProtocolError)
	})
	assertServerHealthy(t, s)
}

func expectStatus(t *testing.T, nc net.Conn, want protocol.Status) {
	t.Helper()
	nc.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp := make([]byte, protocol.HeaderLen)
	if _, err := io.ReadFull(nc, resp); err != nil {
		t.Fatalf("no response: %v", err)
	}
	hdr, err := protocol.DecodeHeader(resp)
	if err != nil {
		t.Fatalf("malformed response: %v", err)
	}
	// Drain the body. Error responses carry a message, and leaving it in
	// the socket desynchronises the next read on this connection.
	if hdr.BodyLen > 0 {
		if _, err := io.ReadFull(nc, make([]byte, hdr.BodyLen)); err != nil {
			t.Fatalf("reading response body: %v", err)
		}
	}
	if protocol.Status(hdr.Code) != want {
		t.Fatalf("status = %s, want %s", protocol.Status(hdr.Code), want)
	}
}

func TestUnknownOpcodeKeepsConnectionAlive(t *testing.T) {
	// A well-framed frame with an opcode we do not implement is a semantic
	// error, not a framing error. The stream is still synchronised, so the
	// connection MUST survive — this is the distinction that separates
	// BAD_REQUEST from PROTOCOL_ERROR.
	s := harness.StartDefault(t)
	nc := s.RawConn(t)

	nc.Write(header(protocol.Opcode(0x7E), 42, 0))
	expectStatus(t, nc, protocol.StatusBadRequest)

	// Same connection, valid command: must still work.
	body := protocol.EncodeCommand(nil, protocol.Command{Op: protocol.OpPing})
	frame := protocol.WriteFrame(nil, protocol.Header{
		Version: protocol.Version, Code: byte(protocol.OpPing), RequestID: 43,
	}, body)
	nc.Write(frame)
	expectStatus(t, nc, protocol.StatusOK)
}

func TestDisconnectMidFrame(t *testing.T) {
	s := harness.StartDefault(t)

	for i := 0; i < 50; i++ {
		nc, err := net.Dial("tcp", s.Addr)
		if err != nil {
			t.Fatal(err)
		}
		body := protocol.EncodeCommand(nil, protocol.Command{
			Op: protocol.OpSet, Key: []byte("k"), Value: bytes.Repeat([]byte("v"), 1000),
		})
		full := protocol.WriteFrame(nil, protocol.Header{
			Version: protocol.Version, Code: byte(protocol.OpSet),
		}, body)
		// Send the header and part of the body, then vanish.
		cut := protocol.HeaderLen + i*7%500
		if cut > len(full) {
			cut = len(full)
		}
		nc.Write(full[:cut])
		nc.Close()
	}
	assertServerHealthy(t, s)
}

func TestClientThatNeverSends(t *testing.T) {
	cfg := harness.DefaultConfig(t.TempDir())
	cfg.IdleTimeout = 300 * time.Millisecond
	s := harness.Start(t, cfg)

	nc, err := net.Dial("tcp", s.Addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	// The deadline must fire and close this connection. Without it, silent
	// connections accumulate until the process runs out of descriptors.
	nc.SetReadDeadline(time.Now().Add(10 * time.Second))
	buf := make([]byte, 1)
	start := time.Now()
	_, err = nc.Read(buf)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("server sent data to a client that never spoke")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("idle connection survived %v with a 300ms idle timeout", elapsed)
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("our own read timed out after %v; the server never closed the connection", elapsed)
	}
	assertServerHealthy(t, s)
}

func TestSlowClientIsDisconnectedNotBlocking(t *testing.T) {
	// A client that sends requests and never reads responses must be closed
	// once its output buffer fills, and must not slow anyone else down.
	cfg := harness.DefaultConfig(t.TempDir())
	cfg.OutputBufferLimit = 64 << 10
	cfg.Fsync = "no"
	s := harness.Start(t, cfg)

	good := s.Client(t)
	bigVal := bytes.Repeat([]byte("v"), 32<<10)
	if err := good.Set([]byte("big"), bigVal, 0); err != nil {
		t.Fatal(err)
	}

	// The rude client: pipeline many large GETs, never read.
	rude, err := net.Dial("tcp", s.Addr)
	if err != nil {
		t.Fatal(err)
	}
	defer rude.Close()

	body := protocol.EncodeCommand(nil, protocol.Command{Op: protocol.OpGet, Key: []byte("big")})
	var batch []byte
	for i := 0; i < 500; i++ {
		batch = protocol.WriteFrame(batch, protocol.Header{
			Version: protocol.Version, Code: byte(protocol.OpGet), RequestID: uint32(i),
		}, body)
	}
	rude.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, _ = rude.Write(batch)

	// Meanwhile a well-behaved client must stay responsive.
	deadline := time.Now().Add(5 * time.Second)
	ops := 0
	for time.Now().Before(deadline) && ops < 200 {
		start := time.Now()
		if err := good.Ping(); err != nil {
			t.Fatalf("well-behaved client broke while a slow client was misbehaving: %v", err)
		}
		if lat := time.Since(start); lat > 2*time.Second {
			t.Fatalf("a slow client added %v of latency for everyone else", lat)
		}
		ops++
	}
	if ops < 50 {
		t.Fatalf("only completed %d operations; the slow client is stalling the server", ops)
	}

	stats := s.Server.NetworkStats()
	t.Logf("output buffer overflows: %d, live conns: %d", stats.OutputOverflows, stats.LiveConns)
	assertServerHealthy(t, s)
}

func TestBoundaryKeyAndValueSizes(t *testing.T) {
	cfg := harness.DefaultConfig(t.TempDir())
	cfg.MaxValueLen = 1 << 20
	s := harness.Start(t, cfg)
	c := s.Client(t)

	t.Run("empty key rejected", func(t *testing.T) {
		err := c.Set([]byte{}, []byte("v"), 0)
		if err == nil {
			t.Fatal("empty key accepted")
		}
		var se *client.StatusError
		if !asStatus(err, &se) || se.Status != protocol.StatusBadRequest {
			t.Fatalf("got %v, want BAD_REQUEST", err)
		}
	})

	t.Run("max key length", func(t *testing.T) {
		key := bytes.Repeat([]byte("k"), protocol.MaxKeyLen)
		if err := c.Set(key, []byte("v"), 0); err != nil {
			t.Fatalf("a key of exactly MaxKeyLen was rejected: %v", err)
		}
		v, err := c.Get(key)
		if err != nil || string(v) != "v" {
			t.Fatalf("max-length key round trip failed: %q %v", v, err)
		}
	})

	t.Run("value over the server limit", func(t *testing.T) {
		val := bytes.Repeat([]byte("v"), (1<<20)+1)
		err := c.Set([]byte("toobig"), val, 0)
		if err == nil {
			t.Fatal("oversized value accepted")
		}
		var se *client.StatusError
		if !asStatus(err, &se) || se.Status != protocol.StatusTooLarge {
			t.Fatalf("got %v, want TOO_LARGE", err)
		}
	})

	t.Run("empty value allowed", func(t *testing.T) {
		if err := c.Set([]byte("emptyval"), []byte{}, 0); err != nil {
			t.Fatal(err)
		}
		v, err := c.Get([]byte("emptyval"))
		if err != nil || len(v) != 0 {
			t.Fatalf("empty value round trip: %q %v", v, err)
		}
	})

	assertServerHealthy(t, s)
}

func asStatus(err error, target **client.StatusError) bool {
	se, ok := err.(*client.StatusError)
	if ok {
		*target = se
	}
	return ok
}

func TestPipeliningManyRequestsInFlight(t *testing.T) {
	cfg := harness.DefaultConfig(t.TempDir())
	cfg.Fsync = "no"
	cfg.OutputBufferLimit = 16 << 20
	s := harness.Start(t, cfg)
	c := s.Client(t)

	const n = 10000
	p := c.Pipeline()
	ids := make(map[uint32]int, n)
	for i := 0; i < n; i++ {
		id := p.Add(protocol.OpSet, protocol.Command{
			Key:   []byte(fmt.Sprintf("pipe%05d", i)),
			Value: []byte(fmt.Sprintf("v%05d", i)),
		})
		ids[id] = i
	}
	results, err := p.Run()
	if err != nil {
		t.Fatalf("pipeline of %d requests failed: %v", n, err)
	}
	if len(results) != n {
		t.Fatalf("got %d responses, sent %d requests", len(results), n)
	}
	seen := make(map[uint32]bool, n)
	for _, r := range results {
		if _, ok := ids[r.ID]; !ok {
			t.Fatalf("response for request_id %d that was never sent", r.ID)
		}
		if seen[r.ID] {
			t.Fatalf("duplicate response for request_id %d", r.ID)
		}
		seen[r.ID] = true
		if r.Status != protocol.StatusOK {
			t.Fatalf("request %d failed: %s", r.ID, r.Status)
		}
	}

	// Spot-check that the values actually landed.
	for i := 0; i < n; i += 617 {
		v, err := c.Get([]byte(fmt.Sprintf("pipe%05d", i)))
		if err != nil || string(v) != fmt.Sprintf("v%05d", i) {
			t.Fatalf("pipelined write %d did not land: %q %v", i, v, err)
		}
	}
}

func TestConnectionChurn(t *testing.T) {
	cfg := harness.DefaultConfig(t.TempDir())
	cfg.Fsync = "no"
	s := harness.Start(t, cfg)

	const cycles = 1000
	for i := 0; i < cycles; i++ {
		c, err := client.DialWithOptions(client.Options{Addr: s.Addr, Timeout: 5 * time.Second})
		if err != nil {
			t.Fatalf("connect %d/%d failed: %v", i, cycles, err)
		}
		if err := c.Set([]byte(fmt.Sprintf("churn%d", i)), []byte("v"), 0); err != nil {
			c.Close()
			t.Fatalf("op on connection %d failed: %v", i, err)
		}
		c.Close()
	}

	// Give the server a moment to reap closed connections, then assert it
	// is not leaking them.
	deadline := time.Now().Add(5 * time.Second)
	var live int
	for time.Now().Before(deadline) {
		live = s.Server.NetworkStats().LiveConns
		if live <= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if live > 2 {
		t.Fatalf("%d connections still live after %d connect/disconnect cycles", live, cycles)
	}
	assertServerHealthy(t, s)
}

func TestManySimultaneousConnections(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high-connection-count test in -short mode")
	}
	cfg := harness.DefaultConfig(t.TempDir())
	cfg.Fsync = "no"
	cfg.MaxConns = 2000
	s := harness.Start(t, cfg)

	const n = 500 // enough to prove the point without exhausting CI ulimits
	var wg sync.WaitGroup
	errCh := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := client.DialWithOptions(client.Options{Addr: s.Addr, Timeout: 20 * time.Second})
			if err != nil {
				errCh <- fmt.Errorf("conn %d: %w", i, err)
				return
			}
			defer c.Close()
			for j := 0; j < 5; j++ {
				if err := c.Set([]byte(fmt.Sprintf("conn%d-%d", i, j)), []byte("v"), 0); err != nil {
					errCh <- fmt.Errorf("conn %d op %d: %w", i, j, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	failures := 0
	for err := range errCh {
		if failures < 5 {
			t.Errorf("%v", err)
		}
		failures++
	}
	if failures > 0 {
		t.Fatalf("%d of %d concurrent connections failed", failures, n)
	}
	assertServerHealthy(t, s)
}

func TestMaxConnsIsEnforced(t *testing.T) {
	cfg := harness.DefaultConfig(t.TempDir())
	cfg.MaxConns = 5
	cfg.Fsync = "no"
	s := harness.Start(t, cfg)

	var held []net.Conn
	defer func() {
		for _, c := range held {
			c.Close()
		}
	}()

	// Open more connections than the limit and keep them alive.
	for i := 0; i < 20; i++ {
		nc, err := net.DialTimeout("tcp", s.Addr, 2*time.Second)
		if err != nil {
			continue
		}
		held = append(held, nc)
	}
	time.Sleep(300 * time.Millisecond)

	stats := s.Server.NetworkStats()
	if stats.LiveConns > cfg.MaxConns {
		t.Fatalf("%d live connections exceeds max-conns %d", stats.LiveConns, cfg.MaxConns)
	}
	if stats.RejectedConns == 0 {
		t.Fatal("no connections were rejected despite exceeding the limit")
	}
	t.Logf("live=%d rejected=%d", stats.LiveConns, stats.RejectedConns)
}

func TestNoReplyFlag(t *testing.T) {
	cfg := harness.DefaultConfig(t.TempDir())
	cfg.Fsync = "no"
	s := harness.Start(t, cfg)
	nc := s.RawConn(t)

	// Fire-and-forget SET, then a normal PING. Only the PING should produce
	// a response; if the server answers the SET too, the client's framing
	// desynchronises.
	body := protocol.EncodeCommand(nil, protocol.Command{
		Op: protocol.OpSet, Key: []byte("noreply"), Value: []byte("v"),
	})
	frame := protocol.WriteFrame(nil, protocol.Header{
		Version: protocol.Version, Code: byte(protocol.OpSet),
		Flags: protocol.FlagNoReply, RequestID: 1,
	}, body)
	frame = protocol.WriteFrame(frame, protocol.Header{
		Version: protocol.Version, Code: byte(protocol.OpPing), RequestID: 2,
	}, nil)
	if _, err := nc.Write(frame); err != nil {
		t.Fatal(err)
	}

	nc.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp := make([]byte, protocol.HeaderLen)
	if _, err := io.ReadFull(nc, resp); err != nil {
		t.Fatal(err)
	}
	hdr, err := protocol.DecodeHeader(resp)
	if err != nil {
		t.Fatal(err)
	}
	if hdr.RequestID != 2 {
		t.Fatalf("first response has request_id %d; the no-reply SET was answered", hdr.RequestID)
	}

	// And the write must still have happened.
	c := s.Client(t)
	if v, err := c.Get([]byte("noreply")); err != nil || string(v) != "v" {
		t.Fatalf("no-reply write did not take effect: %q %v", v, err)
	}
}

func TestWorkerPoolModeHandlesSameChaos(t *testing.T) {
	cfg := harness.DefaultConfig(t.TempDir())
	cfg.ConnMode = "pool"
	cfg.Workers = 4
	cfg.Fsync = "no"
	s := harness.Start(t, cfg)

	// Malformed input.
	nc := s.RawConn(t)
	h := header(protocol.OpSet, 1, 0)
	binary.LittleEndian.PutUint32(h[12:16], 0xFFFFFFFF)
	nc.Write(h)
	expectStatus(t, nc, protocol.StatusProtocolError)

	// Concurrent well-behaved clients.
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := client.DialWithOptions(client.Options{Addr: s.Addr, Timeout: 15 * time.Second})
			if err != nil {
				t.Errorf("dial: %v", err)
				return
			}
			defer c.Close()
			for j := 0; j < 100; j++ {
				k := []byte(fmt.Sprintf("pool%d-%d", i, j))
				if err := c.Set(k, []byte("v"), 0); err != nil {
					t.Errorf("set: %v", err)
					return
				}
				if v, err := c.Get(k); err != nil || string(v) != "v" {
					t.Errorf("get: %q %v", v, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	assertServerHealthy(t, s)
}
