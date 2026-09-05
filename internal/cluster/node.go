package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raqueeb/kvstore/internal/config"
	"github.com/raqueeb/kvstore/internal/engine"
)

// peerState is the failure detector's view of a peer.
//
// Three states rather than two, because "I have not heard from you recently"
// and "I am confident you are gone" are different claims and conflating them
// produces detectors that flap. Suspect is the honest middle: something is
// wrong, but not enough time has passed to act on it.
type peerState int32

const (
	peerAlive peerState = iota
	peerSuspect
	peerDead
)

func (p peerState) String() string {
	switch p {
	case peerAlive:
		return "alive"
	case peerSuspect:
		return "suspect"
	case peerDead:
		return "dead"
	default:
		return "unknown"
	}
}

// Node is the replication layer for one server.
//
// SCOPE — read this before believing anything about consistency:
//
//   - Replication is ASYNCHRONOUS. A primary acknowledges a write once it is
//     durable locally, without waiting for any replica. A primary that dies
//     can therefore lose writes that no replica received. This is the same
//     guarantee Redis async replication gives.
//   - Leader election is NOT implemented. Promotion is a manual operation
//     (the PROMOTE command or `kvctl promote`). What IS implemented is
//     epoch fencing: promotion bumps an epoch, and a replica refuses a
//     stream from a primary whose epoch is lower than the highest it has
//     seen, so a returning old leader cannot quietly resume feeding stale
//     data.
//   - Consequently there is no split-brain *protection* beyond fencing, and
//     no quorum. Two nodes both promoted by an operator will both accept
//     writes. Automatic election with quorum voting is deliberately out of
//     scope; see docs/DECISIONS.md ADR-014 and the README's scope section.
//
// What this does provide: log shipping, resumable partial resync, full
// resync when the log has been truncated past the replica's position,
// heartbeats in both directions, three-state failure detection, and
// replication lag measured both as an LSN gap and as wall-clock delay.
type Node struct {
	cfg config.Config
	eng *engine.Engine
	log *slog.Logger

	epoch atomic.Uint64

	mu         sync.RWMutex
	replicas   map[int64]*replicaConn
	nextReplID atomic.Int64

	repl replicaState

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
}

// New builds a cluster node.
func New(cfg config.Config, eng *engine.Engine, logger *slog.Logger) *Node {
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		cfg:      cfg,
		eng:      eng,
		log:      logger.With("component", "cluster", "node", cfg.NodeID),
		replicas: make(map[int64]*replicaConn),
		ctx:      ctx,
		cancel:   cancel,
	}
	n.epoch.Store(1)
	return n
}

// Start begins replication work appropriate to the configured role.
func (n *Node) Start() {
	if n.cfg.Role == config.RoleReplica {
		n.eng.SetReadOnly(true)
		n.wg.Add(1)
		go n.runReplica()
		n.log.Info("started as replica", "primary", n.cfg.PrimaryAddr)
	} else {
		n.log.Info("started as primary")
	}
}

// Stop shuts the node down.
func (n *Node) Stop() {
	n.once.Do(func() {
		n.cancel()
		n.mu.Lock()
		for _, rc := range n.replicas {
			rc.conn.Close()
		}
		n.mu.Unlock()
		done := make(chan struct{})
		go func() { n.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			n.log.Warn("cluster shutdown timed out")
		}
	})
}

// ErrAlreadyPrimary is returned by Promote on a node that is already primary.
var ErrAlreadyPrimary = errors.New("cluster: node is already a primary")

// Promote turns a replica into a primary.
//
// This is a MANUAL operation, invoked by an operator or an external
// orchestrator. It bumps the epoch and stops read-only mode. The epoch bump
// is the fencing token: any replica that subsequently sees a heartbeat from
// the old primary at a lower epoch will refuse it.
//
// What this does not do: verify that a quorum agrees, or that this node has
// the most complete log. Promoting a replica that is behind loses whatever
// the old primary had and it did not. An orchestrator should compare
// replicas' applied LSNs (visible in STATS) before choosing one.
func (n *Node) Promote() error {
	if !n.eng.ReadOnly() {
		return ErrAlreadyPrimary
	}
	newEpoch := n.epoch.Add(1)
	n.eng.SetReadOnly(false)
	n.log.Warn("promoted to primary", "epoch", newEpoch,
		"applied_lsn", n.repl.appliedLSN.Load(),
		"note", "manual promotion; no quorum was consulted")
	return nil
}

// Epoch returns the current fencing epoch.
func (n *Node) Epoch() uint64 { return n.epoch.Load() }

// --- stats -----------------------------------------------------------------

// ReplicaInfo describes one attached replica from the primary's side.
type ReplicaInfo struct {
	Addr        string `json:"addr"`
	State       string `json:"state"`
	AckedLSN    uint64 `json:"acked_lsn"`
	LagRecords  int64  `json:"lag_records"`
	LagMillis   int64  `json:"lag_ms"`
	RecordsSent uint64 `json:"records_sent"`
	FullSync    bool   `json:"last_sync_was_full"`
	ConnectedMs int64  `json:"connected_for_ms"`
}

// Stats is the cluster section of the STATS document.
type Stats struct {
	Role   string `json:"role"`
	NodeID string `json:"node_id"`
	Epoch  uint64 `json:"epoch"`

	// Primary side.
	Replicas []ReplicaInfo `json:"replicas,omitempty"`

	// Replica side.
	PrimaryAddr      string `json:"primary_addr,omitempty"`
	Connected        bool   `json:"connected,omitempty"`
	PrimaryState     string `json:"primary_state,omitempty"`
	AppliedLSN       uint64 `json:"applied_lsn,omitempty"`
	PrimaryLSN       uint64 `json:"primary_lsn,omitempty"`
	LagRecords       int64  `json:"lag_records,omitempty"`
	LagMillis        int64  `json:"lag_ms,omitempty"`
	RecordsApplied   uint64 `json:"records_applied,omitempty"`
	FullResyncs      uint64 `json:"full_resyncs,omitempty"`
	PartialResyncs   uint64 `json:"partial_resyncs,omitempty"`
	Reconnects       uint64 `json:"reconnects,omitempty"`
	ConsistencyModel string `json:"consistency_model"`
}

// Stats collects cluster metrics.
func (n *Node) Stats() Stats {
	s := Stats{
		Role:   string(n.cfg.Role),
		NodeID: n.cfg.NodeID,
		Epoch:  n.epoch.Load(),
		// Stated in the metrics themselves so nobody has to infer it.
		ConsistencyModel: "asynchronous replication; reads on a replica may be stale; " +
			"no quorum, no automatic election, manual promotion with epoch fencing",
	}

	if n.cfg.Role == config.RoleReplica {
		s.PrimaryAddr = n.cfg.PrimaryAddr
		s.Connected = n.repl.connected.Load()
		s.PrimaryState = peerState(n.repl.peer.Load()).String()
		s.AppliedLSN = n.repl.appliedLSN.Load()
		s.PrimaryLSN = n.repl.primaryLSN.Load()
		s.LagRecords = int64(s.PrimaryLSN) - int64(s.AppliedLSN)
		if s.LagRecords < 0 {
			s.LagRecords = 0
		}
		if last := n.repl.lastMsgAt.Load(); last > 0 {
			s.LagMillis = time.Since(time.Unix(0, last)).Milliseconds()
		}
		s.RecordsApplied = n.repl.applied.Load()
		s.FullResyncs = n.repl.fullSyncs.Load()
		s.PartialResyncs = n.repl.partialSyncs.Load()
		s.Reconnects = n.repl.reconnects.Load()
		return s
	}

	primaryLSN := n.eng.LastLSN()
	n.mu.RLock()
	for _, rc := range n.replicas {
		acked := rc.ackedLSN.Load()
		lag := int64(primaryLSN) - int64(acked)
		if lag < 0 {
			lag = 0
		}
		s.Replicas = append(s.Replicas, ReplicaInfo{
			Addr:        rc.addr,
			State:       peerState(rc.state.Load()).String(),
			AckedLSN:    acked,
			LagRecords:  lag,
			LagMillis:   time.Since(time.Unix(0, rc.lastAckAt.Load())).Milliseconds(),
			RecordsSent: rc.sent.Load(),
			FullSync:    rc.fullSync.Load(),
			ConnectedMs: time.Since(rc.connected).Milliseconds(),
		})
	}
	n.mu.RUnlock()
	return s
}

// StatsJSON renders the cluster stats section.
func (n *Node) StatsJSON() json.RawMessage {
	b, err := json.Marshal(n.Stats())
	if err != nil {
		return json.RawMessage(`{"error":"cluster stats marshal failed"}`)
	}
	return b
}

// ReplicaCount returns the number of attached replicas.
func (n *Node) ReplicaCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.replicas)
}

// AppliedLSN returns this replica's applied LSN.
func (n *Node) AppliedLSN() uint64 { return n.repl.appliedLSN.Load() }

// asNetError is errors.As specialised for net.Error, kept here so replica.go
// does not need the errors import for one call.
func asNetError(err error, target *net.Error) bool {
	return errors.As(err, target)
}
