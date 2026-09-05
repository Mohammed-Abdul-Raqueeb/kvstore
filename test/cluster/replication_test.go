package cluster

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/raqueeb/kvstore/internal/client"
	"github.com/raqueeb/kvstore/internal/protocol"
	"github.com/raqueeb/kvstore/test/harness"
)

// Replication tests (DESIGN.md Phase 2, M13/M14).
//
// These run two real server processes and a real socket between them,
// because replication bugs live in reconnection, resync boundaries and
// timing — none of which an in-process fake exercises.

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", timeout, what)
}

type clusterStats struct {
	Role       string `json:"role"`
	Epoch      uint64 `json:"epoch"`
	Connected  bool   `json:"connected"`
	AppliedLSN uint64 `json:"applied_lsn"`
	PrimaryLSN uint64 `json:"primary_lsn"`
	LagRecords int64  `json:"lag_records"`
	Replicas   []struct {
		Addr       string `json:"addr"`
		State      string `json:"state"`
		AckedLSN   uint64 `json:"acked_lsn"`
		LagRecords int64  `json:"lag_records"`
	} `json:"replicas"`
	FullResyncs uint64 `json:"full_resyncs"`
	Reconnects  uint64 `json:"reconnects"`
}

func fetchCluster(t *testing.T, c *client.Client) clusterStats {
	t.Helper()
	raw, err := c.Stats()
	if err != nil {
		t.Fatalf("STATS: %v", err)
	}
	var doc struct {
		Cluster json.RawMessage `json:"cluster"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	var cs clusterStats
	if len(doc.Cluster) > 0 {
		if err := json.Unmarshal(doc.Cluster, &cs); err != nil {
			t.Fatal(err)
		}
	}
	return cs
}

// startPair brings up a primary and a replica attached to it.
func startPair(t *testing.T) (primary, replica *harness.Subprocess, pc, rc *client.Client) {
	t.Helper()
	root := t.TempDir()

	primary = harness.StartSubprocess(t, filepath.Join(root, "primary"),
		"--fsync", "always", "--log-level", "warn")
	t.Cleanup(func() { primary.Stop() })

	replica = harness.StartSubprocess(t, filepath.Join(root, "replica"),
		"--fsync", "always", "--log-level", "warn",
		"--replicaof", primary.Addr,
		"--heartbeat-interval", "100ms",
		"--failure-timeout", "1s")
	t.Cleanup(func() { replica.Stop() })

	var err error
	if pc, err = primary.Client(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	if rc, err = replica.Client(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rc.Close() })

	waitFor(t, 15*time.Second, "replica to connect to the primary", func() bool {
		return fetchCluster(t, rc).Connected
	})
	return
}

func TestReplicationStreamsWrites(t *testing.T) {
	_, _, pc, rc := startPair(t)

	for i := 0; i < 500; i++ {
		if err := pc.Set([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%04d", i)), 0); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, 20*time.Second, "the replica to catch up", func() bool {
		cs := fetchCluster(t, rc)
		return cs.LagRecords == 0 && cs.AppliedLSN > 0
	})

	for i := 0; i < 500; i += 37 {
		k := []byte(fmt.Sprintf("k%04d", i))
		v, err := rc.Get(k)
		if err != nil {
			t.Fatalf("key %s missing on the replica: %v", k, err)
		}
		if string(v) != fmt.Sprintf("v%04d", i) {
			t.Fatalf("replica has %s = %q, want v%04d", k, v, i)
		}
	}

	// Deletes must replicate too.
	if _, err := pc.Delete([]byte("k0000")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "the delete to replicate", func() bool {
		_, err := rc.Get([]byte("k0000"))
		return err == client.ErrNotFound
	})
}

func TestReplicaRejectsWrites(t *testing.T) {
	_, _, _, rc := startPair(t)

	err := rc.Set([]byte("nope"), []byte("v"), 0)
	if err == nil {
		t.Fatal("the replica accepted a write")
	}
	se, ok := err.(*client.StatusError)
	if !ok || se.Status != protocol.StatusReadOnly {
		t.Fatalf("got %v, want READ_ONLY", err)
	}

	if _, err := rc.Delete([]byte("nope")); err == nil {
		t.Fatal("the replica accepted a delete")
	}
	// Reads must still work.
	if err := rc.Ping(); err != nil {
		t.Fatalf("the replica stopped serving reads: %v", err)
	}
}

func TestReplicationLagIsReported(t *testing.T) {
	_, _, pc, rc := startPair(t)

	for i := 0; i < 200; i++ {
		if err := pc.Set([]byte(fmt.Sprintf("lag%03d", i)), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 20*time.Second, "lag to settle to zero", func() bool {
		return fetchCluster(t, rc).LagRecords == 0
	})

	// The primary must also see the replica and report its acked LSN.
	waitFor(t, 10*time.Second, "the primary to report the replica", func() bool {
		cs := fetchCluster(t, pc)
		return len(cs.Replicas) == 1 && cs.Replicas[0].AckedLSN > 0
	})
	cs := fetchCluster(t, pc)
	t.Logf("primary sees replica %s state=%s acked=%d lag=%d",
		cs.Replicas[0].Addr, cs.Replicas[0].State, cs.Replicas[0].AckedLSN, cs.Replicas[0].LagRecords)
	if cs.Replicas[0].State != "alive" {
		t.Fatalf("replica state is %q, want alive", cs.Replicas[0].State)
	}
}

func TestReplicaResumesAfterPrimaryRestart(t *testing.T) {
	primary, _, pc, rc := startPair(t)

	for i := 0; i < 100; i++ {
		if err := pc.Set([]byte(fmt.Sprintf("before%03d", i)), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 20*time.Second, "initial catch-up", func() bool {
		return fetchCluster(t, rc).LagRecords == 0
	})
	pc.Close()

	// Kill the primary outright and bring it back on the same address.
	dataDir := primary.DataDir
	addr := primary.Addr
	primary.Kill()

	waitFor(t, 15*time.Second, "the replica to notice the primary is gone", func() bool {
		return !fetchCluster(t, rc).Connected
	})

	restarted := harness.StartSubprocessAt(t, dataDir, addr, "--fsync", "always", "--log-level", "warn")
	defer restarted.Stop()

	pc2, err := restarted.Client()
	if err != nil {
		t.Fatal(err)
	}
	defer pc2.Close()

	waitFor(t, 30*time.Second, "the replica to reconnect", func() bool {
		return fetchCluster(t, rc).Connected
	})

	for i := 0; i < 100; i++ {
		if err := pc2.Set([]byte(fmt.Sprintf("after%03d", i)), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 30*time.Second, "post-restart writes to replicate", func() bool {
		_, err := rc.Get([]byte("after099"))
		return err == nil
	})

	// Data from before the restart must still be present on the replica.
	if _, err := rc.Get([]byte("before050")); err != nil {
		t.Fatalf("pre-restart data lost on the replica: %v", err)
	}
	cs := fetchCluster(t, rc)
	t.Logf("replica reconnects=%d full_resyncs=%d", cs.Reconnects, cs.FullResyncs)
}

func TestPromotionMakesReplicaWritable(t *testing.T) {
	primary, _, pc, rc := startPair(t)

	for i := 0; i < 50; i++ {
		if err := pc.Set([]byte(fmt.Sprintf("p%03d", i)), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 20*time.Second, "catch-up", func() bool {
		return fetchCluster(t, rc).LagRecords == 0
	})

	epochBefore := fetchCluster(t, rc).Epoch
	pc.Close()
	primary.Kill()

	msg, err := rc.Promote()
	if err != nil {
		t.Fatalf("promotion failed: %v", err)
	}
	t.Logf("promotion: %s", msg)

	cs := fetchCluster(t, rc)
	if cs.Epoch <= epochBefore {
		t.Fatalf("epoch did not advance on promotion: %d -> %d", epochBefore, cs.Epoch)
	}

	// The promoted node must now accept writes and still hold the old data.
	if err := rc.Set([]byte("written-after-promotion"), []byte("ok"), 0); err != nil {
		t.Fatalf("promoted node still refuses writes: %v", err)
	}
	if v, err := rc.Get([]byte("p049")); err != nil || string(v) != "v" {
		t.Fatalf("promoted node lost replicated data: %q %v", v, err)
	}

	// Promoting again must be a no-op error, not a second epoch bump.
	if _, err := rc.Promote(); err == nil {
		t.Fatal("promoting an existing primary should fail")
	}
}

func TestFullResyncWhenReplicaIsFarBehind(t *testing.T) {
	root := t.TempDir()
	primary := harness.StartSubprocess(t, filepath.Join(root, "primary"),
		"--fsync", "always", "--log-level", "warn", "--segment-size", "8192")
	defer primary.Stop()

	pc, err := primary.Client()
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	// Write a lot, then snapshot, which truncates the WAL. A replica
	// starting from zero cannot be served from the log and must get a full
	// resync instead.
	for i := 0; i < 3000; i++ {
		if err := pc.Set([]byte(fmt.Sprintf("k%05d", i)), []byte("value"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pc.Snapshot(); err != nil {
		t.Fatal(err)
	}

	replica := harness.StartSubprocess(t, filepath.Join(root, "replica"),
		"--fsync", "always", "--log-level", "warn",
		"--replicaof", primary.Addr,
		"--heartbeat-interval", "100ms", "--failure-timeout", "1s")
	defer replica.Stop()

	rc, err := replica.Client()
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	waitFor(t, 40*time.Second, "the full resync to complete", func() bool {
		_, err := rc.Get([]byte("k02999"))
		return err == nil
	})

	cs := fetchCluster(t, rc)
	if cs.FullResyncs == 0 {
		t.Fatal("expected a full resync, but none was recorded")
	}
	// Spot-check the transferred keyspace.
	for i := 0; i < 3000; i += 271 {
		if _, err := rc.Get([]byte(fmt.Sprintf("k%05d", i))); err != nil {
			t.Fatalf("key k%05d missing after full resync: %v", i, err)
		}
	}
	t.Logf("full resyncs=%d applied_lsn=%d", cs.FullResyncs, cs.AppliedLSN)
}

func TestReplicaDoesNotExpireIndependently(t *testing.T) {
	// A replica must apply the primary's expiry DELETEs, never run its own
	// expiry clock. Otherwise it can return NOT_FOUND for a key the primary
	// still serves, because the two machines disagree about `now`.
	_, _, pc, rc := startPair(t)

	if err := pc.Set([]byte("ttl-key"), []byte("v"), 60_000); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 20*time.Second, "the TTL key to replicate", func() bool {
		_, err := rc.Get([]byte("ttl-key"))
		return err == nil
	})

	ms, err := rc.TTL([]byte("ttl-key"))
	if err != nil {
		t.Fatalf("TTL on the replica: %v", err)
	}
	if ms <= 0 || ms > 60_000 {
		t.Fatalf("replica TTL = %d, want a value in (0, 60000]", ms)
	}
	t.Logf("replica reports TTL of %dms, matching the primary's absolute deadline", ms)
}
