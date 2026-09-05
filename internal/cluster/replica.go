package cluster

import (
	"bufio"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/raqueeb/kvstore/internal/client"
	"github.com/raqueeb/kvstore/internal/protocol"
	"github.com/raqueeb/kvstore/internal/wal"
)

// replicaState is the replica's view of its own replication.
type replicaState struct {
	appliedLSN   atomic.Uint64
	primaryLSN   atomic.Uint64
	lastMsgAt    atomic.Int64 // unix nano
	connected    atomic.Bool
	fullSyncs    atomic.Uint64
	partialSyncs atomic.Uint64
	applied      atomic.Uint64
	reconnects   atomic.Uint64
	peer         atomic.Int32
}

// runReplica maintains the connection to the primary, reconnecting forever.
func (n *Node) runReplica() {
	defer n.wg.Done()
	backoff := 100 * time.Millisecond
	const maxBackoff = 5 * time.Second

	for {
		select {
		case <-n.ctx.Done():
			return
		default:
		}

		err := n.replicateOnce()
		n.repl.connected.Store(false)
		if err != nil {
			n.log.Warn("replication link down", "primary", n.cfg.PrimaryAddr, "err", err)
		}

		select {
		case <-n.ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		n.repl.reconnects.Add(1)
	}
}

// replicateOnce runs one full connection lifetime against the primary.
func (n *Node) replicateOnce() error {
	c, err := client.DialWithOptions(client.Options{
		Addr:    n.cfg.PrimaryAddr,
		Timeout: 10 * time.Second,
	})
	if err != nil {
		return err
	}
	defer c.Close()

	// Handshake: identify ourselves, then ask for the stream from where we
	// left off. Resuming from our own applied LSN is what turns a brief
	// disconnect into a few records rather than a full keyspace transfer.
	from := n.eng.LastLSN()
	n.repl.appliedLSN.Store(from)

	if err := replConf(c, n.cfg.NodeID, 0); err != nil {
		return fmt.Errorf("REPLCONF: %w", err)
	}
	if err := syncFrom(c, from); err != nil {
		return fmt.Errorf("SYNC: %w", err)
	}

	nc := c.Conn()
	// The connection now speaks the stream format. Clear the deadline the
	// client library set: the stream is long-lived and quiet between
	// heartbeats, so a request-shaped deadline would kill it constantly.
	_ = nc.SetDeadline(time.Time{})

	br := bufio.NewReaderSize(nc, 256<<10)
	bw := bufio.NewWriterSize(nc, 4<<10)

	n.repl.connected.Store(true)
	n.repl.lastMsgAt.Store(time.Now().UnixNano())
	n.repl.peer.Store(int32(peerAlive))
	n.log.Info("replicating from primary", "primary", n.cfg.PrimaryAddr, "from_lsn", from)

	// Acknowledge periodically so the primary can measure our lag.
	ackStop := make(chan struct{})
	defer close(ackStop)
	go func() {
		t := time.NewTicker(n.cfg.HeartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-ackStop:
				return
			case <-n.ctx.Done():
				return
			case <-t.C:
				if err := writeLSNMsg(bw, msgAck, n.repl.appliedLSN.Load()); err != nil {
					return
				}
				if bw.Flush() != nil {
					return
				}
			}
		}
	}()

	// The failure detector. A read deadline slightly longer than the
	// configured timeout turns "the primary went silent" into an ordinary
	// read error, which is the only signal available to us.
	//
	// This cannot distinguish a crashed primary from a slow or partitioned
	// one — nothing can. That is the fundamental limitation, and the
	// consequence is that the timeout is a tradeoff, not a correct value:
	// too short and a GC pause on the primary triggers a spurious
	// disconnect; too long and real failures go unnoticed. See
	// docs/DECISIONS.md ADR-013.
	var buf []byte
	inFullSync := false

	for {
		select {
		case <-n.ctx.Done():
			return nil
		default:
		}

		_ = nc.SetReadDeadline(time.Now().Add(n.cfg.FailureTimeout))
		msg, next, err := readMsg(br, buf)
		buf = next
		if err != nil {
			if isTimeout(err) {
				n.repl.peer.Store(int32(peerDead))
				return fmt.Errorf("primary silent for %v", n.cfg.FailureTimeout)
			}
			return err
		}
		n.repl.lastMsgAt.Store(time.Now().UnixNano())
		n.repl.peer.Store(int32(peerAlive))

		switch msg.Type {
		case msgFullBegin:
			// A full resync replaces our state wholesale. Clearing first
			// matters: without it, keys the primary has since deleted would
			// survive on the replica forever.
			//
			// This goes through ResetForFullResync, not Flush: Flush is the
			// client-facing FLUSH command and refuses to run in read-only
			// mode, which previously forced this call site to flip ReadOnly
			// off for the duration of the reset so Flush's own guard would
			// let it through. That made "a replica never accepts a client
			// write" false for exactly the span of every full resync — the
			// same window TestReplicaRejectsWrites was hitting, since a
			// brand new replica does a full resync as part of its very
			// first connection. ReadOnly now stays true for the entire
			// resync; nothing here needs to touch it.
			n.log.Info("full resync starting", "primary_lsn", msg.LSN)
			if err := n.eng.ResetForFullResync(); err != nil {
				return err
			}
			inFullSync = true
			n.repl.fullSyncs.Add(1)

		case msgFullEnd:
			inFullSync = false
			n.repl.appliedLSN.Store(msg.LSN)
			n.repl.primaryLSN.Store(msg.LSN)
			n.log.Info("full resync complete", "lsn", msg.LSN)

		case msgRecord:
			if err := n.applyFromPrimary(msg.Record); err != nil {
				return fmt.Errorf("apply replicated record: %w", err)
			}
			n.repl.applied.Add(1)
			if !inFullSync && msg.Record.LSN > n.repl.appliedLSN.Load() {
				n.repl.appliedLSN.Store(msg.Record.LSN)
			}

		case msgHeartbeat:
			n.repl.primaryLSN.Store(msg.LSN)
			// Fencing: a primary at a lower epoch than one we have already
			// seen is a stale leader that came back. Refuse its stream.
			if msg.Epoch < n.epoch.Load() {
				return fmt.Errorf("primary epoch %d is behind ours (%d); refusing a stale leader",
					msg.Epoch, n.epoch.Load())
			}
			if msg.Epoch > n.epoch.Load() {
				n.epoch.Store(msg.Epoch)
			}

		case msgReject:
			return fmt.Errorf("primary rejected replication: %s", msg.Reason)

		default:
			return fmt.Errorf("unexpected stream message %s", msg.Type)
		}
	}
}

// applyFromPrimary writes a replicated record into the local engine.
//
// The replica applies the primary's records verbatim, including the DELETEs
// the primary generates when a key expires. It must NEVER expire keys on its
// own: expiry depends on `now`, nodes do not share a clock, and a replica
// that expired independently would return NOT_FOUND for a key the primary
// still serves. That is DESIGN.md §9, "The replication trap", and it is why
// a replica runs with active expiry disabled.
func (n *Node) applyFromPrimary(rec wal.Record) error {
	return n.eng.ApplyReplicated(rec)
}

func replConf(c *client.Client, nodeID string, port uint16) error {
	return c.RawCommand(protocol.OpReplConf, protocol.Command{
		NodeID:   []byte(nodeID),
		NodePort: port,
	})
}

func syncFrom(c *client.Client, fromLSN uint64) error {
	return c.RawCommand(protocol.OpSync, protocol.Command{FromLSN: fromLSN})
}

func isTimeout(err error) bool {
	var ne net.Error
	if ok := asNetError(err, &ne); ok {
		return ne.Timeout()
	}
	return false
}
