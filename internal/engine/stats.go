package engine

import (
	"encoding/json"
	"runtime"
	"time"

	"github.com/raqueeb/kvstore/internal/store"
	"github.com/raqueeb/kvstore/internal/wal"
)

// Stats is the full server view returned by the STATS command.
type Stats struct {
	Server   ServerStats     `json:"server"`
	Store    store.Stats     `json:"store"`
	WAL      wal.Stats       `json:"wal"`
	Expiry   ExpiryStats     `json:"expiry"`
	Recovery RecoveryReport  `json:"recovery"`
	Cluster  json.RawMessage `json:"cluster,omitempty"`
}

// ServerStats covers process-level and engine-level counters.
type ServerStats struct {
	Role            string `json:"role"`
	ReadOnly        bool   `json:"read_only"`
	NodeID          string `json:"node_id"`
	UptimeMs        int64  `json:"uptime_ms"`
	Goroutines      int    `json:"goroutines"`
	GoVersion       string `json:"go_version"`
	OS              string `json:"os"`
	Arch            string `json:"arch"`
	NumCPU          int    `json:"num_cpu"`
	Mutations       uint64 `json:"mutations"`
	Snapshots       uint64 `json:"snapshots"`
	LastSnapshotLSN uint64 `json:"last_snapshot_lsn"`
	EvictLogDrops   uint64 `json:"evict_log_drops"`
	InFlightWrites  int    `json:"in_flight_writes"`
	WriteGateCap    int    `json:"write_gate_capacity"`
}

// ExpiryStats reports the active expiration mechanism's behaviour.
type ExpiryStats struct {
	Mode           string `json:"mode"`
	Cycles         uint64 `json:"cycles,omitempty"`
	TotalExpired   uint64 `json:"total_expired,omitempty"`
	LastRounds     int    `json:"last_cycle_rounds,omitempty"`
	LastSampled    int    `json:"last_cycle_sampled,omitempty"`
	LastExpired    int    `json:"last_cycle_expired,omitempty"`
	LastDurationNs int64  `json:"last_cycle_duration_ns,omitempty"`
	LastBudgeted   bool   `json:"last_cycle_hit_budget,omitempty"`
	WheelPending   int64  `json:"wheel_pending_timers,omitempty"`
	WheelFired     uint64 `json:"wheel_fired,omitempty"`
	WheelStale     uint64 `json:"wheel_stale_timers,omitempty"`
}

var processStart = time.Now()

// Stats collects a snapshot of every subsystem.
func (e *Engine) Stats() Stats {
	s := Stats{
		Server: ServerStats{
			Role:            string(e.cfg.Role),
			ReadOnly:        e.readOnly.Load(),
			NodeID:          e.cfg.NodeID,
			UptimeMs:        time.Since(processStart).Milliseconds(),
			Goroutines:      runtime.NumGoroutine(),
			GoVersion:       runtime.Version(),
			OS:              runtime.GOOS,
			Arch:            runtime.GOARCH,
			NumCPU:          runtime.NumCPU(),
			Mutations:       e.mutations.Load(),
			Snapshots:       e.snapshots.Load(),
			LastSnapshotLSN: e.lastSnapLSN.Load(),
			EvictLogDrops:   e.evictDrops.Load(),
			InFlightWrites:  len(e.gate),
			WriteGateCap:    cap(e.gate),
		},
		Store:    e.store.Stats(),
		WAL:      e.wal.Stats(),
		Recovery: e.recovery,
	}

	s.Expiry.Mode = string(e.cfg.Expiry)
	if e.sweeper != nil {
		s.Expiry.Cycles = e.sweeper.Cycles()
		s.Expiry.TotalExpired = e.sweeper.TotalExpired()
		if last := e.sweeper.LastCycle(); last != nil {
			s.Expiry.LastRounds = last.Rounds
			s.Expiry.LastSampled = last.Sampled
			s.Expiry.LastExpired = last.Expired
			s.Expiry.LastDurationNs = last.Duration.Nanoseconds()
			s.Expiry.LastBudgeted = last.Budgeted
		}
	}
	if e.wheel != nil {
		fired, stale, expired := e.wheel.Stats()
		s.Expiry.WheelPending = e.wheel.Pending()
		s.Expiry.WheelFired = fired
		s.Expiry.WheelStale = stale
		s.Expiry.TotalExpired = expired
	}
	return s
}

// SetClusterStats installs a callback supplying the cluster section of STATS.
func (e *Engine) SetClusterStats(fn func() json.RawMessage) {
	e.clusterStats.Store(&fn)
}

// StatsJSON renders Stats as JSON, which is what goes on the wire.
func (e *Engine) StatsJSON() ([]byte, error) {
	s := e.Stats()
	if fnp := e.clusterStats.Load(); fnp != nil && *fnp != nil {
		s.Cluster = (*fnp)()
	}
	return json.Marshal(s)
}
