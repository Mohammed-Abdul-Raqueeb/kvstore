package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raqueeb/kvstore/internal/config"
	"github.com/raqueeb/kvstore/internal/engine"
	"github.com/raqueeb/kvstore/internal/protocol"
)

// ReplHandler lets the cluster layer take over a connection that sent a
// replication opcode.
//
// The buffered reader is handed over along with the socket because it may
// already hold bytes the cluster layer needs; re-reading from the raw
// net.Conn would silently drop them. Returning true means the connection has
// been hijacked and the server must not touch it again — in particular it
// must not close it.
type ReplHandler func(nc net.Conn, br *bufio.Reader, frame protocol.Frame) bool

// Server is the TCP front end.
type Server struct {
	cfg config.Config
	eng *engine.Engine
	log *slog.Logger

	ln     net.Listener
	textLn net.Listener

	maxFrame int

	mu     sync.Mutex
	conns  map[uint64]*conn
	nextID atomic.Uint64

	// Connection admission. A bounded number of live connections is the
	// difference between a server that degrades under load and one that
	// exhausts its file descriptors and stops accepting anybody, including
	// the operator trying to find out what is wrong.
	slots chan struct{}

	pool *workerPool

	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	started  atomic.Bool
	shutdown atomic.Bool

	replHandler ReplHandler
	clusterStat func() json.RawMessage

	totalConns      atomic.Uint64
	totalRequests   atomic.Uint64
	rejectedConns   atomic.Uint64
	protocolErrors  atomic.Uint64
	outputOverflows atomic.Uint64
	timeouts        atomic.Uint64
}

// New builds a server around an already-recovered engine.
func New(cfg config.Config, eng *engine.Engine, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())

	maxFrame := cfg.MaxValueLen + protocol.MaxKeyLen + 64
	if maxFrame > protocol.MaxFrameLen {
		maxFrame = protocol.MaxFrameLen
	}

	s := &Server{
		cfg:      cfg,
		eng:      eng,
		log:      logger,
		maxFrame: maxFrame,
		conns:    make(map[uint64]*conn),
		slots:    make(chan struct{}, cfg.MaxConns),
		ctx:      ctx,
		cancel:   cancel,
	}
	if cfg.ConnMode == config.ConnPool {
		s.pool = newWorkerPool(s, cfg.Workers, cfg.PoolQueueDepth)
	}
	return s
}

// SetReplHandler installs the cluster layer's connection hijack hook.
func (s *Server) SetReplHandler(h ReplHandler) { s.replHandler = h }

// SetClusterStats installs a callback supplying the cluster STATS section.
func (s *Server) SetClusterStats(fn func() json.RawMessage) { s.clusterStat = fn }

func (s *Server) statsJSON() ([]byte, error) {
	st := s.eng.Stats()
	wrapper := struct {
		engine.Stats
		Network NetworkStats `json:"network"`
	}{Stats: st, Network: s.NetworkStats()}
	if s.clusterStat != nil {
		wrapper.Cluster = s.clusterStat()
	}
	return json.Marshal(wrapper)
}

// NetworkStats reports connection-level counters.
type NetworkStats struct {
	Addr            string `json:"addr"`
	ConnMode        string `json:"conn_mode"`
	Workers         int    `json:"workers,omitempty"`
	LiveConns       int    `json:"live_connections"`
	MaxConns        int    `json:"max_connections"`
	TotalConns      uint64 `json:"total_connections"`
	RejectedConns   uint64 `json:"rejected_connections"`
	TotalRequests   uint64 `json:"total_requests"`
	ProtocolErrors  uint64 `json:"protocol_errors"`
	OutputOverflows uint64 `json:"output_buffer_overflows"`
	Timeouts        uint64 `json:"idle_timeouts"`
	MaxFrameBytes   int    `json:"max_frame_bytes"`
}

// NetworkStats returns a snapshot of connection counters.
func (s *Server) NetworkStats() NetworkStats {
	s.mu.Lock()
	live := len(s.conns)
	s.mu.Unlock()
	ns := NetworkStats{
		Addr:            s.cfg.Addr,
		ConnMode:        string(s.cfg.ConnMode),
		LiveConns:       live,
		MaxConns:        s.cfg.MaxConns,
		TotalConns:      s.totalConns.Load(),
		RejectedConns:   s.rejectedConns.Load(),
		TotalRequests:   s.totalRequests.Load(),
		ProtocolErrors:  s.protocolErrors.Load(),
		OutputOverflows: s.outputOverflows.Load(),
		Timeouts:        s.timeouts.Load(),
		MaxFrameBytes:   s.maxFrame,
	}
	if s.pool != nil {
		ns.Workers = s.cfg.Workers
	}
	return ns
}

// Start binds the listener and begins accepting.
//
// Called only after engine.Open has returned: the listener starts LAST, so a
// client can never reach a half-recovered store (DESIGN.md §3).
func (s *Server) Start() error {
	if !s.started.CompareAndSwap(false, true) {
		return fmt.Errorf("server already started")
	}
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.cfg.Addr, err)
	}
	s.ln = ln

	if s.pool != nil {
		s.pool.start()
	}

	s.wg.Add(1)
	go s.acceptLoop(ln)

	if s.cfg.TextAddr != "" {
		tln, err := net.Listen("tcp", s.cfg.TextAddr)
		if err != nil {
			ln.Close()
			return fmt.Errorf("listen on text port %s: %w", s.cfg.TextAddr, err)
		}
		s.textLn = tln
		s.wg.Add(1)
		go s.acceptTextLoop(tln)
		s.log.Info("debug text protocol listening", "addr", tln.Addr().String())
	}

	s.log.Info("listening",
		"addr", ln.Addr().String(),
		"conn_mode", s.cfg.ConnMode,
		"engine", s.cfg.Engine,
		"shards", s.cfg.Shards,
		"fsync", s.cfg.Fsync)
	return nil
}

// Addr returns the bound address, useful when the config used port 0.
func (s *Server) Addr() net.Addr {
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

func (s *Server) acceptLoop(ln net.Listener) {
	defer s.wg.Done()
	var tempDelay time.Duration

	for {
		nc, err := ln.Accept()
		if err != nil {
			if s.shutdown.Load() {
				return
			}
			// A transient accept error (EMFILE, ECONNABORTED) must not kill
			// the accept loop. Back off and retry, the way net/http does.
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if tempDelay > time.Second {
					tempDelay = time.Second
				}
				s.log.Warn("temporary accept error; backing off", "err", err, "delay", tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			s.log.Error("accept failed", "err", err)
			return
		}
		tempDelay = 0

		// Admission control before spawning anything. Unbounded goroutine
		// creation driven by an external party is a resource leak with a
		// remote trigger.
		select {
		case s.slots <- struct{}{}:
		default:
			s.rejectedConns.Add(1)
			s.log.Warn("connection rejected: max-conns reached", "max", s.cfg.MaxConns)
			nc.Close()
			continue
		}

		if tc, ok := nc.(*net.TCPConn); ok {
			// Nagle batches small writes, which is exactly wrong for a
			// request/response protocol: it adds up to 40ms of latency
			// waiting for more data that is not coming.
			_ = tc.SetNoDelay(true)
		}

		c := newConn(s.nextID.Add(1), nc, s)
		s.mu.Lock()
		s.conns[c.id] = c
		s.mu.Unlock()
		s.totalConns.Add(1)

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() { <-s.slots }()
			if s.pool != nil {
				s.pool.serve(c)
			} else {
				c.serve()
			}
			c.wait()
		}()
	}
}

func (s *Server) removeConn(c *conn) {
	s.mu.Lock()
	delete(s.conns, c.id)
	s.mu.Unlock()
}

// Shutdown stops accepting, closes live connections, and waits.
func (s *Server) Shutdown(ctx context.Context) error {
	if !s.shutdown.CompareAndSwap(false, true) {
		return nil
	}
	s.log.Info("shutting down listener")
	if s.ln != nil {
		s.ln.Close()
	}
	if s.textLn != nil {
		s.textLn.Close()
	}
	s.cancel()

	s.mu.Lock()
	live := make([]*conn, 0, len(s.conns))
	for _, c := range s.conns {
		live = append(live, c)
	}
	s.mu.Unlock()
	for _, c := range live {
		c.close("server shutting down")
	}

	if s.pool != nil {
		s.pool.stopPool()
	}

	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
