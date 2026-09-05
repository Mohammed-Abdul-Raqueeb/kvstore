package cluster

import (
	"bufio"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/raqueeb/kvstore/internal/protocol"
	"github.com/raqueeb/kvstore/internal/store"
	"github.com/raqueeb/kvstore/internal/wal"
)

// replicaConn is one attached replica, from the primary's point of view.
type replicaConn struct {
	id        int64
	nodeID    string
	addr      string
	conn      net.Conn
	connected time.Time

	ackedLSN  atomic.Uint64
	lastAckAt atomic.Int64 // unix nano
	sent      atomic.Uint64
	fullSync  atomic.Bool
	state     atomic.Int32 // peerState
}

// HandleConn is the server's replication hijack hook. It returns true when
// the connection has been taken over.
func (n *Node) HandleConn(nc net.Conn, br *bufio.Reader, frame protocol.Frame) bool {
	switch frame.Opcode() {
	case protocol.OpReplConf:
		return n.handleReplConf(nc, br, frame)
	case protocol.OpSync:
		return n.handleSync(nc, br, frame)
	case protocol.OpPromote:
		return n.handlePromote(nc, frame)
	default:
		return false
	}
}

func (n *Node) handleReplConf(nc net.Conn, br *bufio.Reader, frame protocol.Frame) bool {
	cmd, err := protocol.DecodeCommand(protocol.OpReplConf, frame.Body)
	if err != nil {
		writeStatus(nc, frame, protocol.StatusBadRequest, err.Error())
		return false
	}
	n.log.Info("replica handshake", "node_id", string(cmd.NodeID), "port", cmd.NodePort)
	// REPLCONF is a plain request/response exchange; the takeover happens on
	// the SYNC that follows, so hand the connection back to the dispatcher.
	writeStatus(nc, frame, protocol.StatusOK, "")
	return false
}

func (n *Node) handleSync(nc net.Conn, br *bufio.Reader, frame protocol.Frame) bool {
	cmd, err := protocol.DecodeCommand(protocol.OpSync, frame.Body)
	if err != nil {
		writeStatus(nc, frame, protocol.StatusBadRequest, err.Error())
		return false
	}
	if n.eng.ReadOnly() {
		// A replica cannot serve replication. Say so explicitly rather than
		// letting the peer hang waiting for records that will never come.
		writeStatus(nc, frame, protocol.StatusNotLeader, "this node is a replica, not a primary")
		return false
	}

	// Acknowledge the SYNC in the normal protocol, then switch this
	// connection over to the stream format.
	writeStatus(nc, frame, protocol.StatusOK, "")

	rc := &replicaConn{
		id:        n.nextReplID.Add(1),
		addr:      nc.RemoteAddr().String(),
		conn:      nc,
		connected: time.Now(),
	}
	rc.state.Store(int32(peerAlive))
	rc.lastAckAt.Store(time.Now().UnixNano())

	n.mu.Lock()
	n.replicas[rc.id] = rc
	n.mu.Unlock()

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.streamTo(rc, br, cmd.FromLSN)
	}()
	return true // hijacked
}

// streamTo feeds one replica: catch-up first, then the live tail.
func (n *Node) streamTo(rc *replicaConn, br *bufio.Reader, fromLSN uint64) {
	defer func() {
		n.mu.Lock()
		delete(n.replicas, rc.id)
		n.mu.Unlock()
		rc.conn.Close()
		n.log.Info("replica disconnected", "addr", rc.addr, "acked_lsn", rc.ackedLSN.Load())
	}()

	bw := bufio.NewWriterSize(rc.conn, 256<<10)

	// Subscribe to live records BEFORE reading the catch-up backlog. Doing
	// it the other way round leaves a window where a write lands after the
	// backlog scan and before the subscription, and that record is lost
	// forever — the replica would silently diverge. Overlap is fine because
	// applying a record twice is idempotent for SET and DELETE.
	feed, cancel := n.eng.Subscribe(n.cfg.ReplBacklog)
	defer cancel()

	// Read acknowledgements on a separate goroutine.
	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		var buf []byte
		for {
			msg, next, err := readMsg(br, buf)
			buf = next
			if err != nil {
				return
			}
			if msg.Type == msgAck {
				rc.ackedLSN.Store(msg.LSN)
				rc.lastAckAt.Store(time.Now().UnixNano())
				rc.state.Store(int32(peerAlive))
			}
		}
	}()

	if err := n.catchUp(rc, bw, fromLSN); err != nil {
		n.log.Warn("replica catch-up failed", "addr", rc.addr, "err", err)
		return
	}

	hb := time.NewTicker(n.cfg.HeartbeatInterval)
	defer hb.Stop()
	var recBuf []byte

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ackDone:
			return
		case rec, ok := <-feed:
			if !ok {
				// The engine dropped this feed because it fell too far
				// behind. Closing the connection forces the replica to
				// reconnect and resync, which is the correct recovery.
				n.log.Warn("replica feed overflowed; dropping connection", "addr", rc.addr)
				return
			}
			if rec.LSN <= rc.ackedLSN.Load() && rec.LSN <= fromLSN {
				continue
			}
			var err error
			recBuf, err = writeRecord(bw, recBuf, rec)
			if err != nil {
				return
			}
			rc.sent.Add(1)
			// Flush when the feed is momentarily empty, so a burst becomes
			// one syscall but a lone write is not delayed.
			if len(feed) == 0 {
				if err := bw.Flush(); err != nil {
					return
				}
			}
		case <-hb.C:
			if err := writeHeartbeat(bw, n.eng.LastLSN(), n.epoch.Load()); err != nil {
				return
			}
			if err := bw.Flush(); err != nil {
				return
			}
			n.checkReplicaLiveness(rc)
		}
	}
}

// catchUp brings a replica from fromLSN to the present.
//
// Two paths. If the primary still has WAL segments covering fromLSN, ship
// those records — cheap, and the replica keeps whatever it already had. If
// the log has been truncated past that point (because a snapshot subsumed
// it), the only correct answer is a full resync: we cannot manufacture
// records we no longer have. This is the "what if the primary truncated the
// WAL past its position" case from DESIGN.md interview question 23.
func (n *Node) catchUp(rc *replicaConn, bw *bufio.Writer, fromLSN uint64) error {
	segs, err := wal.ListSegments(n.cfg.WALDir())
	if err != nil {
		return err
	}
	haveFrom := uint64(0)
	if len(segs) > 0 {
		haveFrom = segs[0].FirstLSN
	}

	needFull := fromLSN == 0 || len(segs) == 0 || fromLSN+1 < haveFrom
	if needFull {
		return n.fullResync(rc, bw)
	}

	rc.fullSync.Store(false)
	var recBuf []byte
	sent := 0
	for _, seg := range segs {
		res, err := wal.ScanSegment(seg.Path, func(r wal.Record) error {
			if r.LSN <= fromLSN {
				return nil
			}
			var werr error
			recBuf, werr = writeRecord(bw, recBuf, r)
			if werr != nil {
				return werr
			}
			sent++
			return nil
		})
		if err != nil {
			return err
		}
		if res.Fault != wal.FaultNone {
			// A torn tail on the primary's own live segment is normal; stop
			// shipping there and let the live feed carry the rest.
			break
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	n.log.Info("replica partial resync complete", "addr", rc.addr, "from_lsn", fromLSN, "records", sent)
	return nil
}

// fullResync sends the entire keyspace as a stream of SET records.
//
// Expressing a snapshot as ordinary SET records rather than inventing a
// bulk-transfer format means the replica has exactly one apply path for
// everything it receives. Fewer code paths, fewer ways to diverge.
func (n *Node) fullResync(rc *replicaConn, bw *bufio.Writer) error {
	rc.fullSync.Store(true)
	lsn := n.eng.DurableLSN()
	if err := writeLSNMsg(bw, msgFullBegin, lsn); err != nil {
		return err
	}

	var recBuf []byte
	var sendErr error
	count := 0
	n.eng.Store().Range(func(e *store.Entry) bool {
		var err error
		recBuf, err = writeRecord(bw, recBuf, wal.Record{
			LSN:        lsn,
			Type:       wal.RecSet,
			Key:        e.Key,
			Value:      e.Value,
			ExpireAtMs: e.ExpireAt,
		})
		if err != nil {
			sendErr = err
			return false
		}
		count++
		return true
	})
	if sendErr != nil {
		return sendErr
	}
	if err := writeLSNMsg(bw, msgFullEnd, lsn); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	n.log.Info("replica full resync complete", "addr", rc.addr, "entries", count, "lsn", lsn)
	return nil
}

// checkReplicaLiveness applies the failure detector from the primary's side.
func (n *Node) checkReplicaLiveness(rc *replicaConn) {
	since := time.Since(time.Unix(0, rc.lastAckAt.Load()))
	switch {
	case since > n.cfg.FailureTimeout*2:
		if rc.state.Swap(int32(peerDead)) != int32(peerDead) {
			n.log.Warn("replica declared dead", "addr", rc.addr, "silent_for", since)
		}
	case since > n.cfg.FailureTimeout:
		if rc.state.Swap(int32(peerSuspect)) != int32(peerSuspect) {
			n.log.Warn("replica suspect", "addr", rc.addr, "silent_for", since)
		}
	}
}

func writeStatus(nc net.Conn, frame protocol.Frame, status protocol.Status, msg string) {
	var body []byte
	if msg != "" {
		body = protocol.EncodeErrorBody(nil, msg)
	}
	resp := protocol.WriteFrame(nil, protocol.Header{
		Version:   protocol.Version,
		Code:      byte(status),
		RequestID: frame.RequestID,
	}, body)
	_ = nc.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_ = protocol.WriteFull(nc, resp)
	_ = nc.SetWriteDeadline(time.Time{})
}

func (n *Node) handlePromote(nc net.Conn, frame protocol.Frame) bool {
	if err := n.Promote(); err != nil {
		writeStatus(nc, frame, protocol.StatusBadRequest, err.Error())
		return false
	}
	writeStatus(nc, frame, protocol.StatusOK, fmt.Sprintf("promoted to primary at epoch %d", n.epoch.Load()))
	return false
}
