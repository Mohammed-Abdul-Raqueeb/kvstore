package server

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/raqueeb/kvstore/internal/protocol"
)

// workerPool is the second connection architecture (DESIGN.md §7B).
//
// Goroutine-per-connection scales fine in Go and is the default. This exists
// for two reasons, both worth measuring rather than asserting:
//
//  1. It demonstrates the resource-leak argument concretely — task creation
//     driven by an external party has to be bounded somewhere, and here the
//     bound is explicit rather than implicit in the runtime's scheduler.
//  2. It makes head-of-line blocking visible. A fixed set of workers pulling
//     from one queue means a slow request delays unrelated requests behind
//     it, and the latency histograms show exactly that. Goroutine-per-conn
//     does not have that property, and being able to show the difference in
//     kvbench output is the point of building both.
//
// The design: connections are still read by their own goroutine (framing is
// inherently per-connection and stateful), but the *execution* of each
// decoded request is handed to the pool. That is the honest version of
// "worker pool" for a stateful byte-stream protocol; distributing whole
// connections across a fixed set of event loops would be a different
// architecture with different, and worse, tail behaviour under skew.
type workerPool struct {
	srv     *Server
	workers int
	queue   chan *task

	wg   sync.WaitGroup
	once sync.Once
	stop chan struct{}

	queued    atomic.Uint64
	rejected  atomic.Uint64
	waitNanos atomic.Uint64
}

type task struct {
	c       *conn
	frame   protocol.Frame
	body    []byte // owned copy; frame.Body aliases the read buffer
	queued  time.Time
	done    chan struct{}
	respBuf []byte
}

var taskPool = sync.Pool{New: func() any { return &task{done: make(chan struct{}, 1)} }}

func newWorkerPool(s *Server, workers, depth int) *workerPool {
	if workers < 1 {
		workers = 1
	}
	if depth < 1 {
		depth = 1024
	}
	return &workerPool{
		srv:     s,
		workers: workers,
		queue:   make(chan *task, depth),
		stop:    make(chan struct{}),
	}
}

func (p *workerPool) start() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.run()
	}
}

func (p *workerPool) run() {
	defer p.wg.Done()
	for {
		select {
		case <-p.stop:
			return
		case t, ok := <-p.queue:
			if !ok {
				return
			}
			p.waitNanos.Add(uint64(time.Since(t.queued)))
			t.frame.Body = t.body
			t.respBuf = p.srv.execute(t.frame, t.respBuf[:0])
			t.done <- struct{}{}
		}
	}
}

func (p *workerPool) stopPool() {
	p.once.Do(func() { close(p.stop) })
	p.wg.Wait()
}

// serve runs a connection's read loop, submitting each decoded request to
// the pool instead of executing it inline.
func (p *workerPool) serve(c *conn) {
	defer func() {
		if !c.hijacked.Load() {
			c.close("read loop exited")
		}
	}()

	c.writerWG.Add(1)
	go c.writeLoop()

	var scratch []byte

	for {
		select {
		case <-c.closed:
			return
		case <-p.srv.ctx.Done():
			return
		default:
		}

		if p.srv.cfg.IdleTimeout > 0 {
			_ = c.nc.SetReadDeadline(time.Now().Add(p.srv.cfg.IdleTimeout))
		}

		frame, next, err := protocol.ReadFrame(c.reader, nil, scratch, uint32(p.srv.maxFrame))
		scratch = next
		if err != nil {
			c.handleReadError(err, frame)
			return
		}
		c.requests.Add(1)
		p.srv.totalRequests.Add(1)

		if isReplOpcode(frame.Opcode()) && p.srv.replHandler != nil {
			if p.srv.replHandler(c.nc, c.reader, frame) {
				c.hijacked.Store(true)
				p.srv.removeConn(c)
				close(c.closed)
				return
			}
			continue
		}

		t := taskPool.Get().(*task)
		t.c = c
		t.frame = frame
		// The body must be copied: the read buffer is reused as soon as this
		// loop comes round again, and the worker may not have run yet.
		t.body = append(t.body[:0], frame.Body...)
		t.queued = time.Now()

		select {
		case p.queue <- t:
			p.queued.Add(1)
		case <-c.closed:
			taskPool.Put(t)
			return
		case <-p.srv.ctx.Done():
			taskPool.Put(t)
			return
		}

		// Wait for this connection's request to complete before reading the
		// next one. This preserves per-connection ordering, which the
		// protocol's request_id makes optional in principle but which every
		// client in this repo relies on.
		select {
		case <-t.done:
		case <-c.closed:
			return
		}

		if !frame.NoReply() {
			if err := c.enqueue(t.respBuf); err != nil {
				taskPool.Put(t)
				c.close(err.Error())
				return
			}
		}
		taskPool.Put(t)
	}
}

// PoolStats reports queueing behaviour, which is where head-of-line blocking
// shows up numerically.
type PoolStats struct {
	Workers      int     `json:"workers"`
	QueueDepth   int     `json:"queue_depth"`
	QueueCap     int     `json:"queue_capacity"`
	Queued       uint64  `json:"tasks_queued"`
	Rejected     uint64  `json:"tasks_rejected"`
	AvgWaitMicro float64 `json:"avg_queue_wait_us"`
}

func (p *workerPool) stats() PoolStats {
	q := p.queued.Load()
	avg := 0.0
	if q > 0 {
		avg = float64(p.waitNanos.Load()) / float64(q) / 1000.0
	}
	return PoolStats{
		Workers:      p.workers,
		QueueDepth:   len(p.queue),
		QueueCap:     cap(p.queue),
		Queued:       q,
		Rejected:     p.rejected.Load(),
		AvgWaitMicro: avg,
	}
}
