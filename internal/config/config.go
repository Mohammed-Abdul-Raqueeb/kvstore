package config

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// StoreEngine selects the concurrency architecture of the store. All three
// are kept in the tree so the Milestone 12 benchmark comparison is real
// rather than theoretical (DESIGN.md §6, §12).
type StoreEngine string

const (
	EngineSharded StoreEngine = "sharded" // RWMutex per shard (default)
	EngineGlobal  StoreEngine = "global"  // one mutex, the baseline
	EngineActor   StoreEngine = "actor"   // goroutine per shard, no locks
)

// FsyncPolicy controls WAL durability (DESIGN.md §5).
type FsyncPolicy string

const (
	FsyncAlways   FsyncPolicy = "always"   // fsync before ack
	FsyncEverySec FsyncPolicy = "everysec" // background fsync each second
	FsyncNo       FsyncPolicy = "no"       // rely on the page cache
)

// EvictionPolicy controls behaviour at the memory limit (DESIGN.md §10).
type EvictionPolicy string

const (
	EvictAllKeysLRU EvictionPolicy = "allkeys-lru"
	EvictVolatile   EvictionPolicy = "volatile-lru"
	EvictNone       EvictionPolicy = "noeviction"
)

// ExpiryMode selects the active expiration mechanism (DESIGN.md §9).
type ExpiryMode string

const (
	ExpiryLazy    ExpiryMode = "lazy"    // 9a only: expire on access
	ExpirySampled ExpiryMode = "sampled" // 9b: sampled background sweeper
	ExpiryWheel   ExpiryMode = "wheel"   // 9c: hierarchical timing wheel
)

// ConnMode selects the connection-handling architecture (DESIGN.md §7).
type ConnMode string

const (
	ConnGoroutine ConnMode = "goroutine" // goroutine per connection
	ConnPool      ConnMode = "pool"      // fixed worker pool
)

// Role is the replication role (phase 2).
type Role string

const (
	RolePrimary Role = "primary"
	RoleReplica Role = "replica"
)

// Config is the complete server configuration. Every field is settable by
// flag; every flag has an environment-variable equivalent named KV_<FLAG>
// with dashes replaced by underscores.
type Config struct {
	// Network
	Addr              string
	TextAddr          string
	MaxConns          int
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	OutputBufferLimit int64
	MaxValueLen       int
	ConnMode          ConnMode
	Workers           int
	PoolQueueDepth    int

	// Storage
	DataDir    string
	Engine     StoreEngine
	Shards     int
	HashSeed   uint64
	ActorQueue int

	// Durability
	Fsync           FsyncPolicy
	SegmentSize     int64
	GroupCommitMax  int
	GroupCommitWait time.Duration
	WALQueueDepth   int
	UnsafeTruncate  bool

	// Snapshots
	SnapshotInterval   time.Duration
	SnapshotMinChanges uint64
	SnapshotOnShutdown bool

	// Memory / eviction
	MaxMemory     int64
	Policy        EvictionPolicy
	EvictSampleK  int
	ExactLRU      bool
	LowWaterRatio float64
	EvictBatchMax int

	// Expiry
	Expiry         ExpiryMode
	SweepInterval  time.Duration
	SweepSample    int
	SweepThreshold float64
	SweepBudget    time.Duration
	WheelTick      time.Duration

	// Replication (phase 2)
	Role              Role
	PrimaryAddr       string
	NodeID            string
	HeartbeatInterval time.Duration
	FailureTimeout    time.Duration
	ReplBacklog       int

	// Misc
	LogLevel string
}

// Default returns the default configuration.
func Default() Config {
	return Config{
		Addr:              "127.0.0.1:7379",
		TextAddr:          "",
		MaxConns:          10000,
		ReadTimeout:       5 * time.Minute,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       10 * time.Minute,
		OutputBufferLimit: 1 << 20,
		MaxValueLen:       16 << 20,
		ConnMode:          ConnGoroutine,
		Workers:           runtime.NumCPU() * 2,
		PoolQueueDepth:    4096,

		DataDir:    "data",
		Engine:     EngineSharded,
		Shards:     nextPow2(2 * runtime.NumCPU()),
		HashSeed:   0, // 0 means "generate a random seed at startup"
		ActorQueue: 1024,

		Fsync:           FsyncEverySec,
		SegmentSize:     64 << 20,
		GroupCommitMax:  1024,
		GroupCommitWait: 200 * time.Microsecond,
		WALQueueDepth:   8192,
		UnsafeTruncate:  false,

		SnapshotInterval:   0, // disabled unless set
		SnapshotMinChanges: 100000,
		SnapshotOnShutdown: false,

		MaxMemory:     0, // 0 = unlimited
		Policy:        EvictAllKeysLRU,
		EvictSampleK:  5,
		ExactLRU:      false,
		LowWaterRatio: 0.95,
		EvictBatchMax: 200,

		Expiry:         ExpirySampled,
		SweepInterval:  100 * time.Millisecond,
		SweepSample:    20,
		SweepThreshold: 0.25,
		SweepBudget:    25 * time.Millisecond,
		WheelTick:      10 * time.Millisecond,

		Role:              RolePrimary,
		PrimaryAddr:       "",
		NodeID:            "",
		HeartbeatInterval: 500 * time.Millisecond,
		FailureTimeout:    3 * time.Second,
		ReplBacklog:       4096,

		LogLevel: "info",
	}
}

// WALDir returns the directory holding WAL segments.
func (c Config) WALDir() string { return filepath.Join(c.DataDir, "wal") }

// SnapshotDir returns the directory holding snapshots.
func (c Config) SnapshotDir() string { return filepath.Join(c.DataDir, "snapshots") }

// LockPath returns the exclusive data-directory lock file (DESIGN.md §8 step 1).
func (c Config) LockPath() string { return filepath.Join(c.DataDir, "LOCK") }

// Validate checks the configuration for self-consistency. It is called
// before anything is opened, so a bad flag produces a clear error rather
// than a partially initialised server.
func (c *Config) Validate() error {
	if c.Shards <= 0 || c.Shards&(c.Shards-1) != 0 {
		return fmt.Errorf("shards must be a power of two, got %d", c.Shards)
	}
	switch c.Engine {
	case EngineSharded, EngineGlobal, EngineActor:
	default:
		return fmt.Errorf("unknown engine %q", c.Engine)
	}
	switch c.Fsync {
	case FsyncAlways, FsyncEverySec, FsyncNo:
	default:
		return fmt.Errorf("unknown fsync policy %q", c.Fsync)
	}
	switch c.Policy {
	case EvictAllKeysLRU, EvictVolatile, EvictNone:
	default:
		return fmt.Errorf("unknown max-memory-policy %q", c.Policy)
	}
	switch c.Expiry {
	case ExpiryLazy, ExpirySampled, ExpiryWheel:
	default:
		return fmt.Errorf("unknown expiry mode %q", c.Expiry)
	}
	switch c.ConnMode {
	case ConnGoroutine, ConnPool:
	default:
		return fmt.Errorf("unknown conn-mode %q", c.ConnMode)
	}
	switch c.Role {
	case RolePrimary, RoleReplica:
	default:
		return fmt.Errorf("unknown role %q", c.Role)
	}
	if c.Role == RoleReplica && c.PrimaryAddr == "" {
		return fmt.Errorf("--replicaof is required when --role=replica")
	}
	if c.EvictSampleK < 1 {
		return fmt.Errorf("evict-sample-k must be >= 1")
	}
	if c.LowWaterRatio <= 0 || c.LowWaterRatio > 1 {
		return fmt.Errorf("low-water-ratio must be in (0, 1]")
	}
	if c.MaxValueLen <= 0 || c.MaxValueLen > 16<<20 {
		return fmt.Errorf("max-value-len must be in (0, 16MiB]")
	}
	if c.SegmentSize < 4096 {
		return fmt.Errorf("segment-size must be at least 4096")
	}
	if c.Workers < 1 {
		return fmt.Errorf("workers must be >= 1")
	}
	if c.DataDir == "" {
		return fmt.Errorf("data-dir must not be empty")
	}
	return nil
}

// LowWaterMark returns the byte target eviction drains down to once the
// high-water mark (MaxMemory) is breached. Evicting to a lower mark rather
// than to exactly the limit prevents thrashing at the boundary.
func (c Config) LowWaterMark() int64 {
	if c.MaxMemory <= 0 {
		return 0
	}
	return int64(float64(c.MaxMemory) * c.LowWaterRatio)
}

// RegisterFlags binds every field to fs. Values already in c act as
// defaults, so callers can pre-seed from Default().
func (c *Config) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.Addr, "addr", c.Addr, "binary protocol listen address")
	fs.StringVar(&c.TextAddr, "text-addr", c.TextAddr, "optional line-protocol debug listener (empty = disabled)")
	fs.IntVar(&c.MaxConns, "max-conns", c.MaxConns, "maximum concurrent client connections")
	fs.DurationVar(&c.ReadTimeout, "read-timeout", c.ReadTimeout, "per-read socket deadline")
	fs.DurationVar(&c.WriteTimeout, "write-timeout", c.WriteTimeout, "per-write socket deadline")
	fs.DurationVar(&c.IdleTimeout, "idle-timeout", c.IdleTimeout, "close connections idle for this long")
	sizeVar(fs, &c.OutputBufferLimit, "output-buffer-limit", c.OutputBufferLimit, "per-connection output buffer cap; exceeding it closes the connection")
	intSizeVar(fs, &c.MaxValueLen, "max-value-len", c.MaxValueLen, "largest accepted value")
	stringEnumVar(fs, (*string)(&c.ConnMode), "conn-mode", string(c.ConnMode), "connection architecture: goroutine|pool")
	fs.IntVar(&c.Workers, "workers", c.Workers, "worker count for --conn-mode=pool")
	fs.IntVar(&c.PoolQueueDepth, "pool-queue", c.PoolQueueDepth, "worker pool queue depth")

	fs.StringVar(&c.DataDir, "data-dir", c.DataDir, "directory for WAL, snapshots and the lock file")
	stringEnumVar(fs, (*string)(&c.Engine), "engine", string(c.Engine), "store engine: sharded|global|actor")
	fs.IntVar(&c.Shards, "shards", c.Shards, "shard count (power of two)")
	fs.Uint64Var(&c.HashSeed, "hash-seed", c.HashSeed, "hash seed; 0 generates a random one at startup")
	fs.IntVar(&c.ActorQueue, "actor-queue", c.ActorQueue, "per-shard mailbox depth for --engine=actor")

	stringEnumVar(fs, (*string)(&c.Fsync), "fsync", string(c.Fsync), "WAL durability: always|everysec|no")
	sizeVar(fs, &c.SegmentSize, "segment-size", c.SegmentSize, "WAL segment rotation threshold")
	fs.IntVar(&c.GroupCommitMax, "group-commit-max", c.GroupCommitMax, "max records per group commit batch")
	fs.DurationVar(&c.GroupCommitWait, "group-commit-wait", c.GroupCommitWait, "max time to accumulate a group commit batch")
	fs.IntVar(&c.WALQueueDepth, "wal-queue", c.WALQueueDepth, "WAL submission queue depth")
	fs.BoolVar(&c.UnsafeTruncate, "unsafe-truncate", c.UnsafeTruncate, "allow startup to truncate mid-log corruption (DATA LOSS)")

	fs.DurationVar(&c.SnapshotInterval, "snapshot-interval", c.SnapshotInterval, "background snapshot cadence (0 = disabled)")
	fs.Uint64Var(&c.SnapshotMinChanges, "snapshot-min-changes", c.SnapshotMinChanges, "minimum mutations since last snapshot before another is taken")
	fs.BoolVar(&c.SnapshotOnShutdown, "snapshot-on-shutdown", c.SnapshotOnShutdown, "write a snapshot during graceful shutdown")

	sizeVar(fs, &c.MaxMemory, "max-memory", c.MaxMemory, "logical memory limit (0 = unlimited), e.g. 512MB")
	stringEnumVar(fs, (*string)(&c.Policy), "max-memory-policy", string(c.Policy), "allkeys-lru|volatile-lru|noeviction")
	fs.IntVar(&c.EvictSampleK, "evict-sample-k", c.EvictSampleK, "sampled-LRU sample size")
	fs.BoolVar(&c.ExactLRU, "exact-lru", c.ExactLRU, "use an exact intrusive LRU list instead of sampling (slower reads; for comparison)")
	fs.Float64Var(&c.LowWaterRatio, "low-water-ratio", c.LowWaterRatio, "evict down to this fraction of max-memory")
	fs.IntVar(&c.EvictBatchMax, "evict-batch-max", c.EvictBatchMax, "max entries evicted per write-path call")

	stringEnumVar(fs, (*string)(&c.Expiry), "expiry", string(c.Expiry), "active expiry mechanism: lazy|sampled|wheel")
	fs.DurationVar(&c.SweepInterval, "sweep-interval", c.SweepInterval, "expiry sweeper cadence")
	fs.IntVar(&c.SweepSample, "sweep-sample", c.SweepSample, "keys sampled per shard per sweep round")
	fs.Float64Var(&c.SweepThreshold, "sweep-threshold", c.SweepThreshold, "repeat a shard while this fraction of the sample was expired")
	fs.DurationVar(&c.SweepBudget, "sweep-budget", c.SweepBudget, "wall-clock budget for one full sweep cycle")
	fs.DurationVar(&c.WheelTick, "wheel-tick", c.WheelTick, "timing wheel tick duration for --expiry=wheel")

	stringEnumVar(fs, (*string)(&c.Role), "role", string(c.Role), "primary|replica")
	fs.StringVar(&c.PrimaryAddr, "replicaof", c.PrimaryAddr, "primary address to replicate from (implies --role=replica)")
	fs.StringVar(&c.NodeID, "node-id", c.NodeID, "stable node identifier; generated if empty")
	fs.DurationVar(&c.HeartbeatInterval, "heartbeat-interval", c.HeartbeatInterval, "replication heartbeat cadence")
	fs.DurationVar(&c.FailureTimeout, "failure-timeout", c.FailureTimeout, "declare a peer dead after this long without a heartbeat")
	fs.IntVar(&c.ReplBacklog, "repl-backlog", c.ReplBacklog, "per-replica record backlog before the replica is dropped")

	fs.StringVar(&c.LogLevel, "log-level", c.LogLevel, "debug|info|warn|error")
}

// ApplyEnv overlays KV_-prefixed environment variables for any flag that was
// not explicitly set on the command line. Explicit flags always win.
func (c *Config) ApplyEnv(fs *flag.FlagSet) error {
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })

	var firstErr error
	fs.VisitAll(func(f *flag.Flag) {
		if set[f.Name] || firstErr != nil {
			return
		}
		env := "KV_" + strings.ToUpper(strings.ReplaceAll(f.Name, "-", "_"))
		v, ok := os.LookupEnv(env)
		if !ok {
			return
		}
		if err := f.Value.Set(v); err != nil {
			firstErr = fmt.Errorf("%s=%q: %w", env, v, err)
		}
	})
	return firstErr
}

// Normalise applies cross-field defaults that depend on other fields.
func (c *Config) Normalise() {
	if c.PrimaryAddr != "" {
		c.Role = RoleReplica
	}
	if c.NodeID == "" {
		host, _ := os.Hostname()
		c.NodeID = fmt.Sprintf("%s-%s", host, strings.ReplaceAll(c.Addr, ":", "-"))
	}
}

func nextPow2(n int) int {
	if n < 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

// --- flag helpers ----------------------------------------------------------

type sizeValue struct{ p *int64 }

func (s sizeValue) String() string {
	if s.p == nil {
		return "0"
	}
	return FormatBytes(*s.p)
}

func (s sizeValue) Set(v string) error {
	n, err := ParseBytes(v)
	if err != nil {
		return err
	}
	*s.p = n
	return nil
}

func sizeVar(fs *flag.FlagSet, p *int64, name string, def int64, usage string) {
	*p = def
	fs.Var(sizeValue{p}, name, usage)
}

type intSizeValue struct{ p *int }

func (s intSizeValue) String() string {
	if s.p == nil {
		return "0"
	}
	return FormatBytes(int64(*s.p))
}

func (s intSizeValue) Set(v string) error {
	n, err := ParseBytes(v)
	if err != nil {
		return err
	}
	*s.p = int(n)
	return nil
}

func intSizeVar(fs *flag.FlagSet, p *int, name string, def int, usage string) {
	*p = def
	fs.Var(intSizeValue{p}, name, usage)
}

// stringEnumVar exists so typed string aliases (StoreEngine and friends) can
// be bound to a flag without losing their type.
func stringEnumVar(fs *flag.FlagSet, p *string, name, def, usage string) {
	*p = def
	fs.StringVar(p, name, def, usage)
}

// ParseBytes parses sizes like "512", "64KB", "16MiB", "2gb".
// KB/MB/GB are treated as binary multiples (1024-based), matching how every
// storage system in practice interprets them.
func ParseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	i := 0
	for i < len(s) && (s[i] == '-' || s[i] == '+' || (s[i] >= '0' && s[i] <= '9') || s[i] == '.') {
		i++
	}
	numPart, unitPart := s[:i], strings.ToLower(strings.TrimSpace(s[i:]))
	if numPart == "" {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	num, err := strconv.ParseFloat(numPart, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q: %w", s, err)
	}
	var mult float64 = 1
	switch unitPart {
	case "", "b":
		mult = 1
	case "k", "kb", "kib":
		mult = 1 << 10
	case "m", "mb", "mib":
		mult = 1 << 20
	case "g", "gb", "gib":
		mult = 1 << 30
	case "t", "tb", "tib":
		mult = 1 << 40
	default:
		return 0, fmt.Errorf("unknown size unit %q", unitPart)
	}
	return int64(num * mult), nil
}

// FormatBytes renders a byte count in the most readable binary unit.
func FormatBytes(n int64) string {
	abs := n
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs >= 1<<40:
		return fmt.Sprintf("%.2fTiB", float64(n)/(1<<40))
	case abs >= 1<<30:
		return fmt.Sprintf("%.2fGiB", float64(n)/(1<<30))
	case abs >= 1<<20:
		return fmt.Sprintf("%.2fMiB", float64(n)/(1<<20))
	case abs >= 1<<10:
		return fmt.Sprintf("%.2fKiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}
