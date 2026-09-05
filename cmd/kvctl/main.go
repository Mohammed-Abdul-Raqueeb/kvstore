package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/raqueeb/kvstore/internal/client"
	"github.com/raqueeb/kvstore/internal/config"
	"github.com/raqueeb/kvstore/internal/snapshot"
	"github.com/raqueeb/kvstore/internal/wal"
)

var Version = "dev"

const usage = `kvctl — client and log inspector for kvstore

CLIENT COMMANDS (need a running server; --addr defaults to 127.0.0.1:7379)
  kvctl ping
  kvctl get <key>
  kvctl set <key> <value> [--ttl 30s]
  kvctl del <key>
  kvctl exists <key>
  kvctl expire <key> <duration>
  kvctl ttl <key>
  kvctl keys [prefix] [--limit 100]
  kvctl stats [--raw]
  kvctl flush
  kvctl snapshot
  kvctl promote
  kvctl repl                       watch replication lag

WAL INSPECTOR (works offline, on a data directory)
  kvctl wal dump   --dir data/ [--limit 100] [--from-lsn N]
  kvctl wal verify --dir data/
  kvctl wal stats  --dir data/
  kvctl wal replay --dir data/ [--to-lsn N]
  kvctl snapshots  --dir data/

GLOBAL FLAGS
  --addr    server address (default 127.0.0.1:7379)
  --timeout request timeout (default 30s)
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, client.ErrNotFound) {
			// A missing key is not a program failure, but scripts want to
			// distinguish it. Exit 1 with nothing on stdout, like grep.
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "kvctl: %v\n", err)
		os.Exit(2)
	}
}

type opts struct {
	addr    string
	timeout time.Duration
	ttl     time.Duration
	limit   int
	dir     string
	fromLSN uint64
	toLSN   uint64
	raw     bool
	watch   time.Duration
}

func run(args []string) error {
	var o opts
	fs := flag.NewFlagSet("kvctl", flag.ContinueOnError)
	fs.StringVar(&o.addr, "addr", envOr("KV_ADDR", "127.0.0.1:7379"), "server address")
	fs.DurationVar(&o.timeout, "timeout", 30*time.Second, "request timeout")
	fs.DurationVar(&o.ttl, "ttl", 0, "TTL for set (e.g. 30s, 5m); 0 = no expiry")
	fs.IntVar(&o.limit, "limit", 100, "maximum results")
	fs.StringVar(&o.dir, "dir", "data", "data directory for offline commands")
	fs.Uint64Var(&o.fromLSN, "from-lsn", 0, "start at this LSN")
	fs.Uint64Var(&o.toLSN, "to-lsn", 0, "stop at this LSN (0 = no limit)")
	fs.BoolVar(&o.raw, "raw", false, "print raw JSON instead of a summary")
	fs.DurationVar(&o.watch, "watch", 0, "repeat every interval")
	fs.Usage = func() { fmt.Fprint(os.Stderr, usage) }

	// Flags may appear before or after the subcommand, so split positional
	// arguments out first.
	var positional []string
	var flags []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if strings.HasPrefix(a, "-") {
			flags = append(flags, a)
			// A flag that takes a value and was not written as --k=v
			// consumes the next argument.
			if !strings.Contains(a, "=") && takesValue(strings.TrimLeft(a, "-")) && i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
			continue
		}
		positional = append(positional, a)
	}
	if err := fs.Parse(flags); err != nil {
		return err
	}
	if len(positional) == 0 {
		fs.Usage()
		return errors.New("no command given")
	}

	cmd := strings.ToLower(positional[0])
	rest := positional[1:]

	switch cmd {
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil
	case "version":
		fmt.Println(Version)
		return nil
	case "wal":
		return walCommand(rest, o)
	case "snapshots":
		return snapshotsCommand(o)
	}
	return clientCommand(cmd, rest, o)
}

func takesValue(name string) bool {
	switch strings.SplitN(name, "=", 2)[0] {
	case "addr", "timeout", "ttl", "limit", "dir", "from-lsn", "to-lsn", "watch":
		return true
	}
	return false
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

// --- client commands -------------------------------------------------------

func clientCommand(cmd string, args []string, o opts) error {
	c, err := client.DialWithOptions(client.Options{Addr: o.addr, Timeout: o.timeout})
	if err != nil {
		return fmt.Errorf("connect to %s: %w", o.addr, err)
	}
	defer c.Close()

	switch cmd {
	case "ping":
		start := time.Now()
		if err := c.Ping(); err != nil {
			return err
		}
		fmt.Printf("PONG (%.2fms)\n", float64(time.Since(start).Microseconds())/1000)
		return nil

	case "get":
		if len(args) != 1 {
			return errors.New("usage: kvctl get <key>")
		}
		v, err := c.Get([]byte(args[0]))
		if err != nil {
			return err
		}
		os.Stdout.Write(v)
		if len(v) > 0 && v[len(v)-1] != '\n' {
			fmt.Println()
		}
		return nil

	case "set":
		if len(args) != 2 {
			return errors.New("usage: kvctl set <key> <value> [--ttl 30s]")
		}
		if err := c.Set([]byte(args[0]), []byte(args[1]), uint64(o.ttl.Milliseconds())); err != nil {
			return err
		}
		fmt.Println("OK")
		return nil

	case "del", "delete":
		if len(args) != 1 {
			return errors.New("usage: kvctl del <key>")
		}
		existed, err := c.Delete([]byte(args[0]))
		if err != nil {
			return err
		}
		if existed {
			fmt.Println("deleted")
		} else {
			fmt.Println("not found")
		}
		return nil

	case "exists":
		if len(args) != 1 {
			return errors.New("usage: kvctl exists <key>")
		}
		ok, err := c.Exists([]byte(args[0]))
		if err != nil {
			return err
		}
		fmt.Println(ok)
		return nil

	case "expire":
		if len(args) != 2 {
			return errors.New("usage: kvctl expire <key> <duration>")
		}
		d, err := time.ParseDuration(args[1])
		if err != nil {
			return fmt.Errorf("bad duration %q: %w", args[1], err)
		}
		ok, err := c.Expire([]byte(args[0]), uint64(d.Milliseconds()))
		if err != nil {
			return err
		}
		if !ok {
			fmt.Println("not found")
			return nil
		}
		fmt.Println("OK")
		return nil

	case "ttl":
		if len(args) != 1 {
			return errors.New("usage: kvctl ttl <key>")
		}
		ms, err := c.TTL([]byte(args[0]))
		if err != nil {
			return err
		}
		if ms < 0 {
			fmt.Println("no expiry")
		} else {
			fmt.Printf("%s (%dms)\n", time.Duration(ms)*time.Millisecond, ms)
		}
		return nil

	case "keys":
		prefix := ""
		if len(args) > 0 {
			prefix = args[0]
		}
		keys, err := c.Keys([]byte(prefix), uint32(o.limit))
		if err != nil {
			return err
		}
		sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
		for _, k := range keys {
			fmt.Printf("%s\n", k)
		}
		fmt.Fprintf(os.Stderr, "(%d keys)\n", len(keys))
		return nil

	case "stats":
		return statsCommand(c, o)

	case "flush":
		if err := c.Flush(); err != nil {
			return err
		}
		fmt.Println("OK")
		return nil

	case "snapshot":
		path, err := c.Snapshot()
		if err != nil {
			return err
		}
		fmt.Println(path)
		return nil

	case "promote":
		msg, err := c.Promote()
		if err != nil {
			return err
		}
		fmt.Println(msg)
		return nil

	case "repl":
		return replCommand(c, o)

	default:
		return fmt.Errorf("unknown command %q (try: kvctl help)", cmd)
	}
}

func statsCommand(c *client.Client, o opts) error {
	raw, err := c.Stats()
	if err != nil {
		return err
	}
	if o.raw {
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, raw, "", "  "); err != nil {
			os.Stdout.Write(raw)
			return nil
		}
		fmt.Println(pretty.String())
		return nil
	}

	var s statsDoc
	if err := json.Unmarshal(raw, &s); err != nil {
		os.Stdout.Write(raw)
		return nil
	}

	fmt.Printf("server    role=%s read_only=%v uptime=%s goroutines=%d\n",
		s.Server.Role, s.Server.ReadOnly,
		(time.Duration(s.Server.UptimeMs) * time.Millisecond).Round(time.Second),
		s.Server.Goroutines)
	fmt.Printf("store     engine=%s shards=%d keys=%d (ttl=%d)\n",
		s.Store.Engine, s.Store.Shards, s.Store.Keys, s.Store.KeysWithTTL)
	fmt.Printf("memory    logical=%s rss=%s ratio=%.2fx limit=%s\n",
		config.FormatBytes(s.Store.LogicalBytes),
		config.FormatBytes(s.Store.RSSBytes),
		ratio(s.Store.RSSBytes, s.Store.LogicalBytes),
		limitStr(s.Store.MaxMemory))
	total := s.Store.Hits + s.Store.Misses
	hitRate := 0.0
	if total > 0 {
		hitRate = 100 * float64(s.Store.Hits) / float64(total)
	}
	fmt.Printf("access    hits=%d misses=%d hit_rate=%.1f%% evictions=%d expired=%d\n",
		s.Store.Hits, s.Store.Misses, hitRate, s.Store.Evictions, s.Store.Expired)
	fmt.Printf("wal       policy=%s records=%d batches=%d avg_batch=%.1f fsyncs=%d\n",
		s.WAL.FsyncPolicy, s.WAL.Records, s.WAL.Batches, s.WAL.AvgBatchSize, s.WAL.Fsyncs)
	fmt.Printf("wal       last_lsn=%d durable_lsn=%d bytes=%s rotations=%d dir_fsync=%v\n",
		s.WAL.LastLSN, s.WAL.DurableLSN, config.FormatBytes(int64(s.WAL.Bytes)),
		s.WAL.Rotations, s.WAL.DirSyncSupport)
	fmt.Printf("expiry    mode=%s cycles=%d total_expired=%d\n",
		s.Expiry.Mode, s.Expiry.Cycles, s.Expiry.TotalExpired)
	fmt.Printf("recovery  keys=%d wal_applied=%d expired_on_load=%d duration=%s truncated=%v\n",
		s.Recovery.KeysLoaded, s.Recovery.WALApplied, s.Recovery.ExpiredOnLoad,
		time.Duration(s.Recovery.Duration).Round(time.Millisecond), s.Recovery.Truncated)
	fmt.Printf("network   conns=%d/%d total=%d requests=%d proto_errors=%d overflows=%d\n",
		s.Network.LiveConns, s.Network.MaxConns, s.Network.TotalConns,
		s.Network.TotalRequests, s.Network.ProtocolErrors, s.Network.OutputOverflows)

	if len(s.Cluster) > 0 {
		var cl clusterDoc
		if json.Unmarshal(s.Cluster, &cl) == nil && cl.Role != "" {
			fmt.Printf("cluster   role=%s epoch=%d\n", cl.Role, cl.Epoch)
			for _, r := range cl.Replicas {
				fmt.Printf("  replica %s state=%s acked_lsn=%d lag=%d records / %dms\n",
					r.Addr, r.State, r.AckedLSN, r.LagRecords, r.LagMillis)
			}
			if cl.PrimaryAddr != "" {
				fmt.Printf("  primary %s connected=%v state=%s applied_lsn=%d lag=%d records\n",
					cl.PrimaryAddr, cl.Connected, cl.PrimaryState, cl.AppliedLSN, cl.LagRecords)
			}
		}
	}
	return nil
}

func replCommand(c *client.Client, o opts) error {
	interval := o.watch
	if interval <= 0 {
		interval = time.Second
	}
	for {
		raw, err := c.Stats()
		if err != nil {
			return err
		}
		var s statsDoc
		if err := json.Unmarshal(raw, &s); err != nil {
			return err
		}
		if len(s.Cluster) == 0 {
			return errors.New("this server has no cluster information (clustering not enabled)")
		}
		var cl clusterDoc
		if err := json.Unmarshal(s.Cluster, &cl); err != nil {
			return err
		}
		ts := time.Now().Format("15:04:05")
		if cl.Role == "primary" {
			fmt.Printf("[%s] primary epoch=%d lsn=%d replicas=%d\n", ts, cl.Epoch, s.WAL.LastLSN, len(cl.Replicas))
			for _, r := range cl.Replicas {
				fmt.Printf("        %s  %-7s acked=%d  lag=%d records  %dms\n",
					r.Addr, r.State, r.AckedLSN, r.LagRecords, r.LagMillis)
			}
		} else {
			fmt.Printf("[%s] replica epoch=%d primary=%s connected=%v applied=%d primary_lsn=%d lag=%d\n",
				ts, cl.Epoch, cl.PrimaryAddr, cl.Connected, cl.AppliedLSN, cl.PrimaryLSN, cl.LagRecords)
		}
		if o.watch <= 0 {
			return nil
		}
		time.Sleep(interval)
	}
}

func ratio(a, b int64) float64 {
	if b <= 0 {
		return 0
	}
	return float64(a) / float64(b)
}

func limitStr(n int64) string {
	if n <= 0 {
		return "unlimited"
	}
	return config.FormatBytes(n)
}

// --- WAL inspector ---------------------------------------------------------

func walCommand(args []string, o opts) error {
	if len(args) == 0 {
		return errors.New("usage: kvctl wal <dump|verify|stats|replay> --dir <data-dir>")
	}
	walDir := config.Config{DataDir: o.dir}.WALDir()

	switch strings.ToLower(args[0]) {
	case "dump":
		return walDump(walDir, o)
	case "verify":
		return walVerify(walDir)
	case "stats":
		return walStats(walDir)
	case "replay":
		return walReplay(walDir, o)
	default:
		return fmt.Errorf("unknown wal subcommand %q", args[0])
	}
}

func walDump(dir string, o opts) error {
	segs, err := wal.ListSegments(dir)
	if err != nil {
		return err
	}
	if len(segs) == 0 {
		return fmt.Errorf("no WAL segments in %s", dir)
	}
	shown := 0
	for _, seg := range segs {
		fmt.Printf("== %s (first_lsn=%d, %s)\n", seg.Path, seg.FirstLSN, config.FormatBytes(seg.Size))
		res, err := wal.ScanSegment(seg.Path, func(r wal.Record) error {
			if r.LSN < o.fromLSN {
				return nil
			}
			if o.toLSN > 0 && r.LSN > o.toLSN {
				return nil
			}
			if o.limit > 0 && shown >= o.limit {
				return nil
			}
			shown++
			fmt.Printf("  lsn=%-8d %-11s key=%-24s val=%-6s expire_at=%s created=%s\n",
				r.LSN, r.Type, printable(r.Key, 24),
				config.FormatBytes(int64(len(r.Value))),
				msTime(r.ExpireAtMs), msTime(r.CreatedAtMs))
			return nil
		})
		if err != nil {
			return err
		}
		if res.Fault != wal.FaultNone {
			fmt.Printf("  !! %s at offset %d: %v\n", res.Fault, res.FaultOffset, res.FaultErr)
		}
	}
	if o.limit > 0 && shown >= o.limit {
		fmt.Printf("(output limited to %d records; use --limit 0 for all)\n", o.limit)
	}
	return nil
}

func walVerify(dir string) error {
	results, err := wal.Verify(dir)
	if err != nil {
		return err
	}
	if len(results) == 0 {
		fmt.Printf("no WAL segments in %s\n", dir)
		return nil
	}
	bad := 0
	for i, r := range results {
		status := "OK"
		if !r.Healthy() {
			status = strings.ToUpper(r.Fault.String())
			bad++
		}
		fmt.Printf("%-40s %-10s records=%-8d lsn=%d..%d size=%s\n",
			r.Path, status, r.Records, r.FirstLSN, r.LastLSN, config.FormatBytes(r.FileSize))
		if !r.Healthy() {
			fmt.Printf("    first bad offset: %d\n", r.FaultOffset)
			fmt.Printf("    reason:           %v\n", r.FaultErr)
			isLast := i == len(results)-1
			switch {
			case r.Fault == wal.FaultTornTail && isLast:
				fmt.Printf("    verdict:          EXPECTED — a torn tail on the final segment is a\n" +
					"                      normal crash artefact. Recovery will truncate it.\n")
			case isLast:
				fmt.Printf("    verdict:          CORRUPTION on the final segment. Recovery will refuse\n" +
					"                      to start without --unsafe-truncate.\n")
			default:
				fmt.Printf("    verdict:          CORRUPTION mid-log. Every record after this offset is\n" +
					"                      unreachable. Recovery will refuse to start.\n")
			}
		}
	}
	if bad > 0 {
		return fmt.Errorf("%d of %d segments failed verification", bad, len(results))
	}
	fmt.Printf("all %d segments verified\n", len(results))
	return nil
}

func walStats(dir string) error {
	segs, err := wal.ListSegments(dir)
	if err != nil {
		return err
	}
	if len(segs) == 0 {
		fmt.Printf("no WAL segments in %s\n", dir)
		return nil
	}
	byType := map[string]int{}
	var total, keyBytes, valBytes int
	var minLSN, maxLSN uint64
	var totalSize int64
	first := true

	for _, seg := range segs {
		totalSize += seg.Size
		if _, err := wal.ScanSegment(seg.Path, func(r wal.Record) error {
			byType[r.Type.String()]++
			total++
			keyBytes += len(r.Key)
			valBytes += len(r.Value)
			if first || r.LSN < minLSN {
				minLSN = r.LSN
			}
			if r.LSN > maxLSN {
				maxLSN = r.LSN
			}
			first = false
			return nil
		}); err != nil {
			return err
		}
	}

	fmt.Printf("segments      %d\n", len(segs))
	fmt.Printf("total size    %s\n", config.FormatBytes(totalSize))
	fmt.Printf("records       %d\n", total)
	fmt.Printf("lsn range     %d .. %d\n", minLSN, maxLSN)
	if total > 0 {
		fmt.Printf("avg record    %s\n", config.FormatBytes(totalSize/int64(total)))
	}
	fmt.Printf("key bytes     %s\n", config.FormatBytes(int64(keyBytes)))
	fmt.Printf("value bytes   %s\n", config.FormatBytes(int64(valBytes)))
	fmt.Println("by type:")
	types := make([]string, 0, len(byType))
	for t := range byType {
		types = append(types, t)
	}
	sort.Strings(types)
	for _, t := range types {
		fmt.Printf("  %-12s %d\n", t, byType[t])
	}
	if gaps := maxLSN - minLSN + 1; total > 0 && uint64(total) != gaps {
		fmt.Printf("\nnote: %d records span an LSN range of %d — the log is not dense.\n", total, gaps)
		fmt.Printf("      This is expected if a snapshot truncated older segments.\n")
	}
	return nil
}

// walReplay rebuilds the keyspace in memory and prints the result, without
// touching the server or the data directory. This is the tool for answering
// "what does this log actually say?" when recovery and reality disagree.
func walReplay(dir string, o opts) error {
	state := map[string][]byte{}
	expiry := map[string]uint64{}
	applied := 0

	res, err := wal.Replay(dir, o.fromLSN, true, func(r wal.Record) error {
		if o.toLSN > 0 && r.LSN > o.toLSN {
			return nil
		}
		switch r.Type {
		case wal.RecSet:
			state[string(r.Key)] = append([]byte(nil), r.Value...)
			if r.ExpireAtMs != 0 {
				expiry[string(r.Key)] = r.ExpireAtMs
			} else {
				delete(expiry, string(r.Key))
			}
		case wal.RecDelete:
			delete(state, string(r.Key))
			delete(expiry, string(r.Key))
		case wal.RecExpire:
			if _, ok := state[string(r.Key)]; ok {
				if r.ExpireAtMs == 0 {
					delete(expiry, string(r.Key))
				} else {
					expiry[string(r.Key)] = r.ExpireAtMs
				}
			}
		}
		applied++
		return nil
	})
	if err != nil {
		return err
	}

	fmt.Printf("replayed %d records from %d segments (skipped %d below lsn %d)\n",
		applied, res.Segments, res.Skipped, o.fromLSN)
	if res.Truncated {
		fmt.Printf("NOTE: replay stopped early at %s offset %d: %s\n",
			res.TruncatedPath, res.TruncatedAt, res.TruncateReason)
	}
	fmt.Printf("resulting keyspace: %d keys (%d with a TTL)\n\n", len(state), len(expiry))

	keys := make([]string, 0, len(state))
	for k := range state {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	limit := o.limit
	if limit <= 0 || limit > len(keys) {
		limit = len(keys)
	}
	for _, k := range keys[:limit] {
		exp := "-"
		if e, ok := expiry[k]; ok {
			exp = msTime(e)
		}
		fmt.Printf("  %-32s %-8s expire_at=%s\n",
			printable([]byte(k), 32), config.FormatBytes(int64(len(state[k]))), exp)
	}
	if limit < len(keys) {
		fmt.Printf("  ... and %d more (use --limit 0 for all)\n", len(keys)-limit)
	}
	return nil
}

func snapshotsCommand(o opts) error {
	dir := config.Config{DataDir: o.dir}.SnapshotDir()
	snaps, err := snapshot.List(dir)
	if err != nil {
		return err
	}
	if len(snaps) == 0 {
		fmt.Printf("no snapshots in %s\n", dir)
		return nil
	}
	for _, s := range snaps {
		count := 0
		hdr, err := snapshot.Load(s.Path, func(snapshot.Entry) error { count++; return nil })
		status := "OK"
		if err != nil {
			status = "INVALID: " + err.Error()
		}
		fmt.Printf("%-44s lsn=%-10d size=%-10s entries=%-8d created=%s  %s\n",
			s.Path, s.LastIncludedLSN, config.FormatBytes(s.Size), count,
			msTime(hdr.CreatedAtMs), status)
	}
	return nil
}

func printable(b []byte, max int) string {
	var sb strings.Builder
	for _, c := range b {
		if c >= 0x20 && c < 0x7F {
			sb.WriteByte(c)
		} else {
			sb.WriteString("\\x" + strconv.FormatUint(uint64(c), 16))
		}
		if sb.Len() >= max {
			return sb.String()[:max-3] + "..."
		}
	}
	return sb.String()
}

func msTime(ms uint64) string {
	if ms == 0 {
		return "-"
	}
	return time.UnixMilli(int64(ms)).Format("2006-01-02T15:04:05.000")
}

// --- STATS document shapes -------------------------------------------------

type statsDoc struct {
	Server struct {
		Role       string `json:"role"`
		ReadOnly   bool   `json:"read_only"`
		UptimeMs   int64  `json:"uptime_ms"`
		Goroutines int    `json:"goroutines"`
	} `json:"server"`
	Store struct {
		Engine       string `json:"engine"`
		Shards       int    `json:"shards"`
		Keys         int    `json:"keys"`
		KeysWithTTL  int    `json:"keys_with_ttl"`
		LogicalBytes int64  `json:"logical_bytes"`
		MaxMemory    int64  `json:"max_memory"`
		RSSBytes     int64  `json:"rss_bytes"`
		Hits         uint64 `json:"hits"`
		Misses       uint64 `json:"misses"`
		Evictions    uint64 `json:"evictions"`
		Expired      uint64 `json:"expired"`
	} `json:"store"`
	WAL struct {
		Records        uint64  `json:"records"`
		Batches        uint64  `json:"batches"`
		Bytes          uint64  `json:"bytes"`
		Fsyncs         uint64  `json:"fsyncs"`
		Rotations      uint64  `json:"rotations"`
		LastLSN        uint64  `json:"last_lsn"`
		DurableLSN     uint64  `json:"durable_lsn"`
		AvgBatchSize   float64 `json:"avg_batch_size"`
		FsyncPolicy    string  `json:"fsync_policy"`
		DirSyncSupport bool    `json:"dir_sync_supported"`
	} `json:"wal"`
	Expiry struct {
		Mode         string `json:"mode"`
		Cycles       uint64 `json:"cycles"`
		TotalExpired uint64 `json:"total_expired"`
	} `json:"expiry"`
	Recovery struct {
		KeysLoaded    int   `json:"keys_loaded"`
		WALApplied    int   `json:"wal_applied"`
		ExpiredOnLoad int   `json:"expired_on_load"`
		Duration      int64 `json:"duration_ns"`
		Truncated     bool  `json:"truncated"`
	} `json:"recovery"`
	Network struct {
		LiveConns       int    `json:"live_connections"`
		MaxConns        int    `json:"max_connections"`
		TotalConns      uint64 `json:"total_connections"`
		TotalRequests   uint64 `json:"total_requests"`
		ProtocolErrors  uint64 `json:"protocol_errors"`
		OutputOverflows uint64 `json:"output_buffer_overflows"`
	} `json:"network"`
	Cluster json.RawMessage `json:"cluster"`
}

type clusterDoc struct {
	Role     string `json:"role"`
	Epoch    uint64 `json:"epoch"`
	Replicas []struct {
		Addr       string `json:"addr"`
		State      string `json:"state"`
		AckedLSN   uint64 `json:"acked_lsn"`
		LagRecords int64  `json:"lag_records"`
		LagMillis  int64  `json:"lag_ms"`
	} `json:"replicas"`
	PrimaryAddr  string `json:"primary_addr"`
	Connected    bool   `json:"connected"`
	PrimaryState string `json:"primary_state"`
	AppliedLSN   uint64 `json:"applied_lsn"`
	PrimaryLSN   uint64 `json:"primary_lsn"`
	LagRecords   int64  `json:"lag_records"`
}
