package wal

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raqueeb/kvstore/internal/config"
)

// ErrClosed is returned by Submit after Close.
var ErrClosed = errors.New("wal: writer is closed")

// Options configures a Writer.
type Options struct {
	Dir             string
	Fsync           config.FsyncPolicy
	SegmentSize     int64
	GroupCommitMax  int
	GroupCommitWait time.Duration
	QueueDepth      int

	// StartLSN is the LSN the writer will assign to its next record. Set by
	// recovery to (last replayed LSN + 1).
	StartLSN uint64

	// ResumePath and ResumeOffset let the writer append to an existing
	// segment after recovery instead of starting a fresh one.
	ResumePath   string
	ResumeOffset int64

	// NowMs supplies wall-clock milliseconds; injectable for tests.
	NowMs func() uint64
}

func (o *Options) withDefaults() {
	if o.Fsync == "" {
		o.Fsync = config.FsyncEverySec
	}
	if o.SegmentSize <= 0 {
		o.SegmentSize = 64 << 20
	}
	if o.GroupCommitMax <= 0 {
		o.GroupCommitMax = 1024
	}
	if o.GroupCommitWait <= 0 {
		o.GroupCommitWait = 200 * time.Microsecond
	}
	if o.QueueDepth <= 0 {
		o.QueueDepth = 8192
	}
	if o.StartLSN == 0 {
		o.StartLSN = 1
	}
	if o.NowMs == nil {
		o.NowMs = func() uint64 { return uint64(time.Now().UnixMilli()) }
	}
}

// Commit is the handle returned by Submit. The caller applies the mutation
// to memory and then waits on this before acknowledging the client.
//
// That ordering is Option 2 from DESIGN.md §7: assign the LSN, apply to
// memory, ack only once fsync covers the LSN. Concurrent readers can
// therefore observe a value that is not yet durable — but no client was ever
// *told* the write succeeded, so no durability promise is broken. The
// anomaly window is exactly [apply, fsync completes].
type Commit struct {
	LSN uint64

	mu   sync.Mutex
	done bool
	err  error
	ch   chan struct{}
}

func newCommit(lsn uint64) *Commit {
	return &Commit{LSN: lsn, ch: make(chan struct{})}
}

func (c *Commit) finish(err error) {
	c.mu.Lock()
	if c.done {
		c.mu.Unlock()
		return
	}
	c.done, c.err = true, err
	c.mu.Unlock()
	close(c.ch)
}

// Wait blocks until the record is durable under the configured policy.
func (c *Commit) Wait() error {
	<-c.ch
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Done exposes the completion channel for callers that want to select on it.
func (c *Commit) Done() <-chan struct{} { return c.ch }

type submission struct {
	rec    Record
	commit *Commit
}

// Stats reports WAL activity, surfaced through the STATS command.
type Stats struct {
	Records        uint64  `json:"records"`
	Batches        uint64  `json:"batches"`
	Bytes          uint64  `json:"bytes"`
	Fsyncs         uint64  `json:"fsyncs"`
	Rotations      uint64  `json:"rotations"`
	LastLSN        uint64  `json:"last_lsn"`
	DurableLSN     uint64  `json:"durable_lsn"`
	AvgBatchSize   float64 `json:"avg_batch_size"`
	QueueDepth     int     `json:"queue_depth"`
	QueueCapacity  int     `json:"queue_capacity"`
	CurrentSegment string  `json:"current_segment"`
	FsyncPolicy    string  `json:"fsync_policy"`
	DirSyncSupport bool    `json:"dir_sync_supported"`
}

// Writer is the append-only log writer.
//
// Architecture (DESIGN.md §5, "Group commit"):
//
//	writers ──▶ MPSC channel ──▶ single WAL goroutine
//	                               drain up to N records or T microseconds
//	                               serialise into one contiguous buffer
//	                               ONE write()
//	                               ONE fsync()
//	                               signal every waiter in the batch
//
// One fsync amortised across N writes is the entire difference between a few
// hundred durable writes a second and a hundred thousand. Everything else in
// this file is bookkeeping around that loop.
type Writer struct {
	opts config.FsyncPolicy
	o    Options

	queue  chan submission
	seg    *segment
	dir    string
	closed atomic.Bool

	// lsnMu serialises LSN assignment *and* the channel send, so that the
	// order records arrive in the queue is exactly LSN order. Assigning the
	// LSN outside the send would let two goroutines interleave and write
	// LSNs out of order, which breaks the "sort by first LSN" recovery
	// assumption for no benefit.
	lsnMu   sync.Mutex
	nextLSN uint64

	durableLSN atomic.Uint64
	writtenLSN atomic.Uint64

	records   atomic.Uint64
	batches   atomic.Uint64
	bytes     atomic.Uint64
	fsyncs    atomic.Uint64
	rotations atomic.Uint64

	// pendingSync tracks whether there are written-but-unsynced bytes, so
	// the everysec ticker can skip the syscall when nothing changed.
	pendingSync atomic.Bool

	wg       sync.WaitGroup
	stopSync chan struct{}

	fatalMu sync.Mutex
	fatal   error
}

// Open creates or resumes a WAL in dir.
func Open(o Options) (*Writer, error) {
	o.withDefaults()
	if err := os.MkdirAll(o.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}

	w := &Writer{
		opts:     o.Fsync,
		o:        o,
		dir:      o.Dir,
		queue:    make(chan submission, o.QueueDepth),
		nextLSN:  o.StartLSN,
		stopSync: make(chan struct{}),
	}
	w.durableLSN.Store(o.StartLSN - 1)
	w.writtenLSN.Store(o.StartLSN - 1)

	var err error
	if o.ResumePath != "" {
		firstLSN, _ := ParseSegmentName(o.ResumePath)
		w.seg, err = openSegmentForAppend(o.ResumePath, int64(firstLSN), o.ResumeOffset)
	} else {
		w.seg, err = createSegment(o.Dir, o.StartLSN, o.NowMs())
	}
	if err != nil {
		return nil, err
	}

	w.wg.Add(1)
	go w.run()

	if o.Fsync == config.FsyncEverySec {
		w.wg.Add(1)
		go w.everySecLoop()
	}
	return w, nil
}

// NextLSN returns the LSN that will be assigned to the next record.
func (w *Writer) NextLSN() uint64 {
	w.lsnMu.Lock()
	defer w.lsnMu.Unlock()
	return w.nextLSN
}

// LastLSN returns the highest LSN assigned so far.
func (w *Writer) LastLSN() uint64 { return w.NextLSN() - 1 }

// DurableLSN returns the highest LSN known to be on stable storage.
func (w *Writer) DurableLSN() uint64 { return w.durableLSN.Load() }

// Submit assigns an LSN, queues the record, and returns immediately.
//
// It does NOT wait for durability; the caller applies the mutation to memory
// and then calls Commit.Wait before acknowledging the client.
func (w *Writer) Submit(r Record) (*Commit, error) {
	if w.closed.Load() {
		return nil, ErrClosed
	}
	if err := w.fatalErr(); err != nil {
		return nil, err
	}
	if len(r.Key) > maxKeyLen {
		return nil, fmt.Errorf("wal: key of %d bytes exceeds limit", len(r.Key))
	}
	if len(r.Value) > maxValLen {
		return nil, fmt.Errorf("wal: value of %d bytes exceeds limit", len(r.Value))
	}
	if !r.Type.Valid() || r.Type == RecSegmentHdr {
		return nil, fmt.Errorf("wal: cannot submit record type %s", r.Type)
	}

	w.lsnMu.Lock()
	if w.closed.Load() {
		w.lsnMu.Unlock()
		return nil, ErrClosed
	}
	r.LSN = w.nextLSN
	w.nextLSN++
	if r.CreatedAtMs == 0 {
		r.CreatedAtMs = w.o.NowMs()
	}
	c := newCommit(r.LSN)
	// The send happens under lsnMu so queue order == LSN order. If the queue
	// is full this blocks, which is backpressure reaching all the way to the
	// client: the right behaviour when the disk cannot keep up. The
	// alternative — dropping or buffering without bound — turns a slow disk
	// into either data loss or an OOM.
	w.queue <- submission{rec: r, commit: c}
	w.lsnMu.Unlock()
	return c, nil
}

// SubmitWait is Submit followed immediately by Wait, for callers with no
// memory-apply step in between (recovery-time writes, replication).
func (w *Writer) SubmitWait(r Record) (uint64, error) {
	c, err := w.Submit(r)
	if err != nil {
		return 0, err
	}
	return c.LSN, c.Wait()
}

// run is the single WAL goroutine.
func (w *Writer) run() {
	defer w.wg.Done()

	batch := make([]submission, 0, w.o.GroupCommitMax)
	buf := make([]byte, 0, 1<<16)

	for {
		first, ok := <-w.queue
		if !ok {
			return
		}
		batch = append(batch[:0], first)

		// Accumulate. Two triggers, whichever fires first:
		//
		//   size  — GroupCommitMax records, bounding memory and latency;
		//   time  — GroupCommitWait, bounding how long a lone writer waits
		//           for company that may never arrive.
		//
		// 200µs is chosen to be well under any realistic fsync (a consumer
		// NVMe fsync is 100µs–1ms; a spinning disk is 5–10ms), so the timer
		// costs nothing when the disk is the bottleneck, and small enough
		// that a single-client workload does not visibly stall. It is a
		// flag so the tradeoff can be measured rather than argued about.
		if w.o.GroupCommitMax > 1 {
			timer := time.NewTimer(w.o.GroupCommitWait)
		accumulate:
			for len(batch) < w.o.GroupCommitMax {
				select {
				case s, ok := <-w.queue:
					if !ok {
						break accumulate
					}
					batch = append(batch, s)
				case <-timer.C:
					break accumulate
				}
			}
			timer.Stop()
		}

		w.flushBatch(batch, &buf)
	}
}

func (w *Writer) flushBatch(batch []submission, buf *[]byte) {
	if len(batch) == 0 {
		return
	}

	// Rotate before serialising if this batch would overflow the segment.
	// Rotating mid-batch would split one group commit across two files and
	// two fsyncs for no benefit.
	if w.seg.size >= w.o.SegmentSize {
		if err := w.rotate(batch[0].rec.LSN); err != nil {
			w.failBatch(batch, err)
			return
		}
	}

	*buf = (*buf)[:0]
	for i := range batch {
		*buf = AppendRecord(*buf, batch[i].rec)
	}

	if err := w.seg.write(*buf); err != nil {
		w.failBatch(batch, err)
		return
	}
	w.bytes.Add(uint64(len(*buf)))
	w.records.Add(uint64(len(batch)))
	w.batches.Add(1)
	last := batch[len(batch)-1].rec.LSN
	w.writtenLSN.Store(last)

	switch w.opts {
	case config.FsyncAlways:
		if err := w.seg.sync(); err != nil {
			w.failBatch(batch, err)
			return
		}
		w.fsyncs.Add(1)
		w.durableLSN.Store(last)

	case config.FsyncEverySec, config.FsyncNo:
		// The bytes are in the page cache. We acknowledge now and let the
		// background ticker (everysec) or the OS writeback (no) make them
		// durable. This is the policy's entire point and its entire risk:
		// up to one second of acknowledged writes can be lost on a power
		// failure, and up to ~30s under `no`.
		w.pendingSync.Store(true)
		w.durableLSN.Store(last)
	}

	for i := range batch {
		batch[i].commit.finish(nil)
		batch[i] = submission{}
	}
}

func (w *Writer) failBatch(batch []submission, err error) {
	w.setFatal(err)
	for i := range batch {
		batch[i].commit.finish(err)
		batch[i] = submission{}
	}
}

func (w *Writer) rotate(firstLSN uint64) error {
	// Sync and close the old segment before the new one exists, so there is
	// never a moment where the newest data is only in the page cache while
	// a newer segment is already durable.
	if err := w.seg.sync(); err != nil {
		return err
	}
	if err := w.seg.close(); err != nil {
		return err
	}
	seg, err := createSegment(w.dir, firstLSN, w.o.NowMs())
	if err != nil {
		return err
	}
	w.seg = seg
	w.rotations.Add(1)
	w.pendingSync.Store(false)
	return nil
}

func (w *Writer) everySecLoop() {
	defer w.wg.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-w.stopSync:
			return
		case <-t.C:
			if !w.pendingSync.Swap(false) {
				continue
			}
			// Sync is issued from this goroutine while the writer goroutine
			// may be mid-write. That is safe: both operate on the same fd,
			// write(2) and fsync(2) are individually atomic with respect to
			// each other, and the worst case is that the sync covers a few
			// extra bytes.
			if err := w.seg.sync(); err != nil {
				w.setFatal(err)
				return
			}
			w.fsyncs.Add(1)
		}
	}
}

// Sync forces an fsync regardless of policy and returns once complete.
func (w *Writer) Sync() error {
	if err := w.fatalErr(); err != nil {
		return err
	}
	w.pendingSync.Store(false)
	if err := w.seg.sync(); err != nil {
		w.setFatal(err)
		return err
	}
	w.fsyncs.Add(1)
	w.durableLSN.Store(w.writtenLSN.Load())
	return nil
}

// Close drains the queue, syncs, and shuts the writer down. It is idempotent.
func (w *Writer) Close() error {
	if !w.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Take lsnMu so no Submit is mid-send when the channel closes; a send on
	// a closed channel panics, and "the server panicked during shutdown" is
	// a genuinely bad look for a durability story.
	w.lsnMu.Lock()
	close(w.queue)
	w.lsnMu.Unlock()

	close(w.stopSync)
	w.wg.Wait()

	err := w.seg.sync()
	if cerr := w.seg.close(); err == nil {
		err = cerr
	}
	if ferr := w.fatalErr(); ferr != nil {
		err = ferr
	}
	return err
}

func (w *Writer) setFatal(err error) {
	w.fatalMu.Lock()
	if w.fatal == nil {
		w.fatal = err
	}
	w.fatalMu.Unlock()
}

func (w *Writer) fatalErr() error {
	w.fatalMu.Lock()
	defer w.fatalMu.Unlock()
	return w.fatal
}

// Stats returns a snapshot of writer counters.
func (w *Writer) Stats() Stats {
	batches := w.batches.Load()
	records := w.records.Load()
	avg := 0.0
	if batches > 0 {
		avg = float64(records) / float64(batches)
	}
	return Stats{
		Records:        records,
		Batches:        batches,
		Bytes:          w.bytes.Load(),
		Fsyncs:         w.fsyncs.Load(),
		Rotations:      w.rotations.Load(),
		LastLSN:        w.LastLSN(),
		DurableLSN:     w.durableLSN.Load(),
		AvgBatchSize:   avg,
		QueueDepth:     len(w.queue),
		QueueCapacity:  cap(w.queue),
		CurrentSegment: w.seg.path,
		FsyncPolicy:    string(w.opts),
		DirSyncSupport: DirSyncSupported(),
	}
}
