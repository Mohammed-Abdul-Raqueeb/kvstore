package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/raqueeb/kvstore/internal/config"
	"github.com/raqueeb/kvstore/internal/snapshot"
	"github.com/raqueeb/kvstore/internal/store"
	"github.com/raqueeb/kvstore/internal/wal"
)

// --- read path -------------------------------------------------------------

// Get returns a copy of the value for key.
func (e *Engine) Get(key []byte) ([]byte, bool) { return e.store.Get(key) }

// Exists reports whether a live key is present.
func (e *Engine) Exists(key []byte) bool { return e.store.Exists(key) }

// TTL returns remaining lifetime in milliseconds (-1 = no expiry).
func (e *Engine) TTL(key []byte) (int64, bool) { return e.store.TTL(key) }

// Keys returns up to limit live keys with the given prefix.
func (e *Engine) Keys(prefix []byte, limit int) [][]byte { return e.store.Keys(prefix, limit) }

// --- write path ------------------------------------------------------------

// Set stores key=value, with ttlMillis as a relative lifetime (0 = none).
//
// The ordering is Option 2 from DESIGN.md §7:
//
//	take a backpressure slot        (bounds in-flight writes)
//	take the shard lock
//	  apply to memory
//	  reserve an LSN and queue the record   (guaranteed not to block)
//	release the shard lock
//	wait for the commit to become durable
//	only then acknowledge the client
//
// A crash between the apply and the fsync means a concurrent reader may have
// observed a value that does not survive the restart. Nobody was ever *told*
// the write succeeded, so no durability promise is broken. That window —
// between memory apply and fsync completion — is the exact anomaly this
// design accepts in exchange for keeping disk latency out of the shard lock.
func (e *Engine) Set(key, value []byte, ttlMillis uint64) error {
	if e.closed.Load() {
		return ErrClosed
	}
	if e.readOnly.Load() {
		return ErrReadOnly
	}
	expireAt := e.deadline(ttlMillis)

	rec := wal.Record{
		Type:       wal.RecSet,
		Key:        key,
		Value:      value,
		ExpireAtMs: expireAt,
	}
	return e.mutate(rec, func(t store.Txn) error {
		if !t.Set(key, value, expireAt) {
			return ErrOOM
		}
		return nil
	})
}

// Delete removes a key and reports whether it existed.
func (e *Engine) Delete(key []byte) (bool, error) {
	if e.closed.Load() {
		return false, ErrClosed
	}
	if e.readOnly.Load() {
		return false, ErrReadOnly
	}
	var existed bool
	err := e.mutate(wal.Record{Type: wal.RecDelete, Key: key}, func(t store.Txn) error {
		existed = t.Delete(key)
		return nil
	})
	return existed, err
}

// Expire sets a relative TTL on an existing key. ttlMillis of 0 clears it.
func (e *Engine) Expire(key []byte, ttlMillis uint64) (bool, error) {
	if e.closed.Load() {
		return false, ErrClosed
	}
	if e.readOnly.Load() {
		return false, ErrReadOnly
	}
	expireAt := e.deadline(ttlMillis)
	var ok bool
	err := e.mutate(wal.Record{Type: wal.RecExpire, Key: key, ExpireAtMs: expireAt}, func(t store.Txn) error {
		ok = t.Expire(key, expireAt)
		return nil
	})
	return ok, err
}

// deadline converts a relative TTL in milliseconds into an absolute
// wall-clock deadline, clamping so the arithmetic cannot overflow.
//
// Relative on the wire, absolute in storage: the client and server need not
// agree on the wall clock, but the stored deadline has to survive a restart
// and still mean the same instant.
func (e *Engine) deadline(ttlMillis uint64) uint64 {
	if ttlMillis == 0 {
		return 0
	}
	const maxTTL = uint64(1) << 50
	if ttlMillis > maxTTL {
		ttlMillis = maxTTL
	}
	return e.clock.NowMillis() + ttlMillis
}

// mutate is the shared write path: backpressure, atomic apply+reserve, then
// wait for durability.
func (e *Engine) mutate(rec wal.Record, apply func(store.Txn) error) error {
	// Backpressure is taken outside every lock. If the disk cannot keep up,
	// callers queue here — never inside a shard lock, where they would block
	// unrelated keys.
	select {
	case e.gate <- struct{}{}:
	case <-e.ctx.Done():
		return ErrClosed
	}
	defer func() { <-e.gate }()

	var commit *wal.Commit
	var applyErr, walErr error

	e.store.WithKey(rec.Key, func(t store.Txn) {
		if applyErr = apply(t); applyErr != nil {
			return
		}
		// The record is queued while the shard lock is held so that log
		// order matches memory order for this key. The send cannot block:
		// the gate above bounds in-flight mutations below the queue depth.
		commit, walErr = e.wal.Submit(rec)
	})

	if applyErr != nil {
		return applyErr
	}
	if walErr != nil {
		// Memory is now ahead of the log. That is the same anomaly the
		// design already accepts between apply and fsync, so the store is
		// not corrupt — but the client must not be told this succeeded.
		e.log.Error("WAL submit failed after memory apply", "err", walErr)
		return walErr
	}

	e.mutations.Add(1)
	e.fanOutToReplicas(rec, commit.LSN)

	// Durability wait. Under `everysec` and `no` this returns as soon as the
	// bytes are in the page cache, which is the whole point of those modes.
	return commit.Wait()
}

// Flush empties the keyspace. It is logged as a snapshot boundary rather
// than as N deletes: writing a million DELETE records to express "everything
// is gone" would be absurd, so FLUSH forces a snapshot of the (now empty)
// store and truncates the log below it.
func (e *Engine) Flush() error {
	if e.readOnly.Load() {
		return ErrReadOnly
	}
	e.store.Flush()
	_, err := e.Snapshot()
	return err
}

// --- eviction / expiry propagation ----------------------------------------

// onStoreEvict is called by the store while a shard lock is held, so it must
// never block. It hands the event to a queue and returns immediately.
func (e *Engine) onStoreEvict(key []byte, reason store.EvictReason) {
	if e.readOnly.Load() || e.closed.Load() {
		return
	}
	select {
	case e.evictQ <- evictEvent{key: append([]byte(nil), key...), reason: reason}:
	default:
		// The queue is full. Dropping is safe but not free, so it is
		// counted and reported in STATS.
		//
		// Why it is safe: for EXPIRY, the original SET record carries the
		// absolute deadline, so replay re-creates the key and recovery's
		// drop-expired pass removes it again — the recovered state is
		// identical. For EVICTION, replay re-inserts the key and the memory
		// limit evicts again; the recovered keyspace may differ in *which*
		// keys were evicted, but it is a valid state under the same policy.
		//
		// What it is not safe for: replicas, which would miss the delete.
		// A primary running with replicas attached should treat a non-zero
		// count here as a real problem.
		e.evictDrops.Add(1)
	}
}

func (e *Engine) drainEvictQueue() {
	defer e.evictWG.Done()
	for {
		select {
		case <-e.ctx.Done():
			// Drain what is already queued so a graceful shutdown does not
			// lose deletes that were about to be logged.
			for {
				select {
				case ev := <-e.evictQ:
					e.logEvict(ev)
				default:
					return
				}
			}
		case ev := <-e.evictQ:
			e.logEvict(ev)
		}
	}
}

func (e *Engine) logEvict(ev evictEvent) {
	rec := wal.Record{Type: wal.RecDelete, Key: ev.key}
	commit, err := e.wal.Submit(rec)
	if err != nil {
		return // shutting down
	}
	e.fanOutToReplicas(rec, commit.LSN)
}

// --- background controllers ------------------------------------------------

func (e *Engine) startBackground() {
	switch e.cfg.Expiry {
	case config.ExpirySampled:
		e.sweeper = store.NewSweeper(e.store, e.cfg.SweepInterval, e.cfg.SweepSample,
			e.cfg.SweepThreshold, e.cfg.SweepBudget)
		e.sweeper.Start(e.ctx)
	case config.ExpiryWheel:
		e.wheel = store.NewWheelExpirer(e.store, e.cfg.WheelTick)
		e.wheel.Start(e.ctx)
	case config.ExpiryLazy:
		// Nothing to start: expiry happens on access only.
	}

	// Eviction controller. Eviction also runs inline in the write path; this
	// background pass exists so that a store which goes over its limit and
	// then sees no writes still returns to the low-water mark.
	if e.cfg.MaxMemory > 0 && e.cfg.Policy != config.EvictNone {
		e.bgWG.Add(1)
		go func() {
			defer e.bgWG.Done()
			t := time.NewTicker(200 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-e.ctx.Done():
					return
				case <-t.C:
					e.store.EvictToLimit()
				}
			}
		}()
	}

	if e.cfg.SnapshotInterval > 0 {
		e.bgWG.Add(1)
		go func() {
			defer e.bgWG.Done()
			t := time.NewTicker(e.cfg.SnapshotInterval)
			defer t.Stop()
			for {
				select {
				case <-e.ctx.Done():
					return
				case <-t.C:
					since := e.mutations.Load()
					if since < e.cfg.SnapshotMinChanges {
						continue
					}
					if _, err := e.Snapshot(); err != nil {
						e.log.Error("background snapshot failed", "err", err)
					}
				}
			}
		}()
	}
}

// --- snapshots -------------------------------------------------------------

// SnapshotResult describes a completed snapshot.
type SnapshotResult struct {
	Path            string        `json:"path"`
	Entries         uint64        `json:"entries"`
	Bytes           int64         `json:"bytes"`
	LastIncludedLSN uint64        `json:"last_included_lsn"`
	SegmentsRemoved int           `json:"segments_removed"`
	Duration        time.Duration `json:"duration_ns"`
}

// Snapshot writes a point-in-time dump of the keyspace and truncates the WAL
// below it.
//
// The LSN is captured BEFORE iteration begins. Writes that land during the
// iteration may or may not appear in the file, but every one of them has an
// LSN above the recorded point, so replay applies them again afterwards.
// Taking the LSN after iteration would be the bug: a write that happened
// during the scan and missed the file would also be skipped on replay.
func (e *Engine) Snapshot() (SnapshotResult, error) {
	e.snapMu.Lock()
	defer e.snapMu.Unlock()

	start := time.Now()
	var res SnapshotResult

	lastLSN := e.wal.DurableLSN()
	w, err := snapshot.Create(e.cfg.SnapshotDir(), lastLSN, e.clock.NowMillis())
	if err != nil {
		return res, err
	}

	var writeErr error
	e.store.Range(func(en *store.Entry) bool {
		if err := w.Add(snapshot.Entry{Key: en.Key, Value: en.Value, ExpireAtMs: en.ExpireAt}); err != nil {
			writeErr = err
			return false
		}
		return true
	})
	if writeErr != nil {
		w.Abort()
		return res, writeErr
	}

	res.Entries = w.Count()
	res.Bytes = w.Bytes()
	path, err := w.Commit()
	if err != nil {
		return res, err
	}
	res.Path = path
	res.LastIncludedLSN = lastLSN
	e.lastSnapLSN.Store(lastLSN)
	e.snapshots.Add(1)
	e.mutations.Store(0)

	// Only after the snapshot is durable may the log below it be discarded.
	// Reversing these two steps loses data on a crash in between.
	removed, err := wal.TruncateBelow(e.cfg.WALDir(), lastLSN)
	if err != nil {
		e.log.Error("wal truncation after snapshot failed", "err", err)
	}
	res.SegmentsRemoved = removed

	if _, err := snapshot.Prune(e.cfg.SnapshotDir(), 2); err != nil {
		e.log.Warn("snapshot prune failed", "err", err)
	}

	res.Duration = time.Since(start)
	e.log.Info("snapshot written",
		"path", res.Path, "entries", res.Entries, "bytes", res.Bytes,
		"lsn", res.LastIncludedLSN, "segments_removed", removed, "duration", res.Duration)
	return res, nil
}

// --- replication support (used by internal/cluster) ------------------------

// Subscribe registers a live feed of records for a replica. Returns the feed
// and a cancel function. Records are dropped for a replica that cannot keep
// up, and the feed is closed — a slow replica must never stall the primary.
func (e *Engine) Subscribe(buffer int) (<-chan wal.Record, func()) {
	if buffer <= 0 {
		buffer = 4096
	}
	ch := make(chan wal.Record, buffer)
	id := e.replNext.Add(1)
	e.replMu.Lock()
	e.replFeeds[id] = ch
	e.replMu.Unlock()
	return ch, func() {
		e.replMu.Lock()
		if c, ok := e.replFeeds[id]; ok {
			delete(e.replFeeds, id)
			close(c)
		}
		e.replMu.Unlock()
	}
}

func (e *Engine) fanOutToReplicas(rec wal.Record, lsn uint64) {
	e.replMu.RLock()
	if len(e.replFeeds) == 0 {
		e.replMu.RUnlock()
		return
	}
	rec.LSN = lsn
	cp := rec.Clone()
	var stalled []int64
	for id, ch := range e.replFeeds {
		select {
		case ch <- cp:
		default:
			stalled = append(stalled, id)
		}
	}
	e.replMu.RUnlock()

	if len(stalled) > 0 {
		e.replMu.Lock()
		for _, id := range stalled {
			if c, ok := e.replFeeds[id]; ok {
				delete(e.replFeeds, id)
				close(c)
			}
		}
		e.replMu.Unlock()
		e.log.Warn("dropped replica feed: backlog full", "count", len(stalled))
	}
}

// ApplyReplicated applies a record received from a primary. Used only on a
// replica, which never writes to its own WAL from client traffic but does
// persist the replicated stream so it can resume after a restart.
func (e *Engine) ApplyReplicated(rec wal.Record) error {
	if err := e.applyRecord(rec); err != nil {
		return err
	}
	if _, err := e.wal.Submit(rec); err != nil {
		return err
	}
	e.mutations.Add(1)
	return nil
}

// SetReadOnly toggles replica mode.
func (e *Engine) SetReadOnly(ro bool) { e.readOnly.Store(ro) }

// ReadOnly reports whether writes are rejected.
func (e *Engine) ReadOnly() bool { return e.readOnly.Load() }

// LastLSN returns the highest assigned LSN.
func (e *Engine) LastLSN() uint64 { return e.wal.LastLSN() }

// DurableLSN returns the highest LSN known to be on stable storage.
func (e *Engine) DurableLSN() uint64 { return e.wal.DurableLSN() }

// Store exposes the underlying store, for tests and the benchmark harness.
func (e *Engine) Store() *store.Store { return e.store }

// Config returns the engine's configuration.
func (e *Engine) Config() config.Config { return e.cfg }

// Recovery returns the startup report.
func (e *Engine) Recovery() RecoveryReport { return e.recovery }

// --- shutdown --------------------------------------------------------------

// Close stops background work, flushes the WAL and releases the data lock.
func (e *Engine) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	if e.sweeper != nil {
		e.sweeper.Stop()
	}
	if e.wheel != nil {
		e.wheel.Stop()
	}
	e.cancel()
	e.bgWG.Wait()
	e.evictWG.Wait()

	if e.cfg.SnapshotOnShutdown {
		if _, err := e.Snapshot(); err != nil {
			e.log.Error("shutdown snapshot failed", "err", err)
		}
	}

	e.replMu.Lock()
	for id, ch := range e.replFeeds {
		delete(e.replFeeds, id)
		close(ch)
	}
	e.replMu.Unlock()

	var firstErr error
	if err := e.wal.Close(); err != nil {
		firstErr = fmt.Errorf("close wal: %w", err)
	}
	e.store.Close()
	if err := e.lock.Release(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("release lock: %w", err)
	}
	return firstErr
}

// ForceSync fsyncs the WAL regardless of policy.
func (e *Engine) ForceSync() error { return e.wal.Sync() }

// RunSweepCycle drives one expiry cycle synchronously. Test hook.
func (e *Engine) RunSweepCycle() {
	if e.sweeper != nil {
		e.sweeper.RunCycle()
	}
	if e.wheel != nil {
		e.wheel.RunOnce()
	}
}

// Ctx exposes the engine lifetime context.
func (e *Engine) Ctx() context.Context { return e.ctx }
