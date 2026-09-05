# kvstore

A durable, concurrent key-value store built from scratch in Go: custom binary
TCP protocol, sharded in-memory storage engine, write-ahead log with group
commit, crash recovery, TTL expiry, memory limits with sampled-LRU eviction,
snapshots, and asynchronous replication.

No Redis, no RocksDB, no LevelDB, no embedded database. **Zero external Go
dependencies** — the standard library only.

```
                        ┌──────────────┐
   clients ── TCP ────► │ internal/    │
                        │  server      │  accept, framing, dispatch,
                        │              │  slow-client protection
                        └──────┬───────┘
                               │
                        ┌──────▼───────┐
                        │ internal/    │  the durable engine:
                        │  engine      │  recovery, write path, snapshots
                        └──┬────────┬──┘
                           │        │
              ┌────────────▼──┐  ┌──▼────────────┐
              │ internal/     │  │ internal/     │
              │  store        │  │  wal          │
              │               │  │               │
              │ sharded map   │  │ group commit  │
              │ TTL index     │  │ segments      │
              │ LRU sampling  │  │ CRC32C        │
              │ accounting    │  │ recovery      │
              └───────────────┘  └───────────────┘

   internal/protocol  — pure codec, knows nothing about sockets or storage
   internal/snapshot  — atomic point-in-time keyspace dumps
   internal/cluster   — Phase 2 replication (separate; see Scope)
```

`protocol`, `store` and `wal` have no knowledge of each other. Exactly one
package composes them.

## Quickstart

```bash
make build          # -> bin/kvserver, bin/kvctl, bin/kvbench

./bin/kvserver --data-dir ./data --fsync always
```

```bash
./bin/kvctl ping
./bin/kvctl set greeting "hello world"
./bin/kvctl get greeting
./bin/kvctl set session:42 token --ttl 30s
./bin/kvctl ttl session:42
./bin/kvctl keys session:
./bin/kvctl stats
```

Kill the server with `kill -9`, start it again, and your data is still there.
That is the point of the whole exercise, and `make test-crash` proves it 24
different ways.

## What it does

| Feature | Detail |
|---|---|
| **Binary protocol** | 16-byte little-endian header, explicit framing, pipelining via `request_id`, hard bounds validated before allocation |
| **Storage engine** | Sharded map with per-shard `RWMutex`; `global` and `actor` engines also built, for comparison |
| **Write-ahead log** | CRC32C records, segment rotation, group commit, three fsync policies |
| **Crash recovery** | Snapshot + WAL replay; distinguishes a torn tail from real corruption |
| **TTL** | Lazy expiry, plus a sampled sweeper *or* a hierarchical timing wheel |
| **Memory limits** | Sampled LRU (K=5), exact LRU, volatile-LRU, or `noeviction` |
| **Snapshots** | Atomic tmp→fsync→rename→fsync-dir, footer CRC over the whole file |
| **Replication** | Async log shipping, partial and full resync, lag metrics, epoch fencing |
| **Tooling** | `kvctl` client + offline WAL inspector, `kvbench` open/closed-loop load generator |

## Scope — what this is *not*

Stated up front rather than discovered later:

- **No automatic leader election.** Promotion is manual (`kvctl promote`).
  Epoch fencing prevents a returning old primary from feeding stale data, but
  two nodes both promoted by an operator will both accept writes. A
  half-correct election is worse than none — see `docs/DECISIONS.md` ADR-014.
- **Asynchronous replication only.** A primary acknowledges before any
  replica has the write, so a primary that dies can lose writes. Same
  guarantee as Redis async replication. Reported in the `consistency_model`
  field of every `STATS` response.
- **No transactions, no multi-key atomicity, no secondary indexes, no
  clustering/sharding across nodes.**
- **Multi-core scaling is unmeasured.** See `docs/BENCHMARKS.md`.

## Configuration

Every flag has an environment equivalent: `--max-memory` → `KV_MAX_MEMORY`.
An explicit flag always wins. `./bin/kvserver --help` lists all of them.

The ones that matter most:

```bash
--fsync always|everysec|no     # durability vs throughput
--max-memory 512MB             # 0 = unlimited
--max-memory-policy allkeys-lru|volatile-lru|noeviction
--engine sharded|global|actor  # concurrency architecture
--shards 16                    # power of two
--expiry sampled|wheel|lazy    # active expiration mechanism
--conn-mode goroutine|pool     # connection architecture
--replicaof host:port          # run as a replica
```

## Using kvctl

```bash
# Client (needs a running server)
kvctl ping
kvctl get <key>
kvctl set <key> <value> [--ttl 30s]
kvctl del <key>
kvctl exists <key>
kvctl expire <key> 5m
kvctl ttl <key>
kvctl keys [prefix] [--limit 100]
kvctl stats [--raw]
kvctl flush
kvctl snapshot
kvctl promote
kvctl repl --watch 1s          # live replication lag

# Offline log inspector — works on a data directory, no server needed
kvctl wal verify --dir data/   # per-segment health with a verdict
kvctl wal stats  --dir data/   # counts by type, LSN range, sizes
kvctl wal dump   --dir data/ --limit 50
kvctl wal replay --dir data/   # rebuild the keyspace offline and print it
kvctl snapshots  --dir data/   # list snapshots, validating each CRC
```

`kvctl wal replay` is the tool for when recovery and reality disagree: it
reconstructs state in memory without touching the server or the files.

## Replication

```bash
# Primary
./bin/kvserver --addr 127.0.0.1:7379 --data-dir ./data-primary

# Replica
./bin/kvserver --addr 127.0.0.1:7381 --data-dir ./data-replica \
    --replicaof 127.0.0.1:7379

./bin/kvctl --addr 127.0.0.1:7379 repl --watch 1s
```

A replica is read-only and rejects writes with `READ_ONLY`. It resumes from
its own applied LSN after a disconnect, falling back to a full resync only
when the primary's log no longer covers that point.

A replica **never expires keys on its own clock** — it applies the primary's
expiry deletes. Two machines do not share a clock, and a replica that expired
independently would return `NOT_FOUND` for a key the primary still serves.

## Testing

```bash
make test          # everything (several minutes)
make test-short    # fast subset
make test-race     # under the race detector
make test-crash    # SIGKILLs real server processes and verifies recovery
make test-chaos    # malformed frames, slow clients, connection churn
make test-diff     # model-based differential against a reference implementation
make test-cluster  # replication
make fuzz          # protocol decoders, 30s each
make bench         # Go micro-benchmarks
```

What each suite is *for*:

- **`test/crash`** — the only code that tests the central claim,
  "acknowledged writes survive a crash". Writers record every key the server
  said OK to, the process is SIGKILLed at a random moment, and after restart
  every acked key must be present and correct. You cannot SIGKILL a
  goroutine, so this spawns real processes.
- **`test/differential`** — random operation sequences run against both the
  real store and a twenty-line reference map, with every result compared.
  Failing sequences are automatically **shrunk** to a minimal reproduction.
- **`test/chaos`** — forged 4 GiB length fields, TLS handshakes sent to the
  KV port, clients that disconnect mid-frame, clients that never read their
  responses. After each one, a well-behaved client must still get correct
  service.
- **Fuzzing** — `FuzzDecodeFrame` asserts the decoder never panics, never
  allocates beyond `MaxFrameLen`, and never returns a slice pointing outside
  its input.

## Platform support

Builds and runs on **Linux, macOS and Windows**. Two things differ:

| | Linux/macOS | Windows |
|---|---|---|
| Directory fsync | real `fsync(2)` on the dir handle | **no-op** — no OS equivalent |
| RSS measurement | `/proc/self/statm` (Linux) | Go runtime approximation |
| `SIGSTOP` pause tests | available | skipped |
| Graceful `SIGTERM` in subprocess tests | available | falls back to kill |

The directory-fsync gap is real and documented in `docs/DECISIONS.md`
ADR-011. Record *contents* are still fsynced, so `--fsync always` still means
"on stable storage"; what is weakened is durability of the directory entry
for a brand-new file. `STATS` reports `wal.dir_sync_supported` so this is
visible at runtime.

## Documentation

| File | Contents |
|---|---|
| `docs/DESIGN.md` | the original design document this implements |
| `docs/PROTOCOL.md` | wire format, opcodes, statuses, framing rules |
| `docs/WAL.md` | record format, group commit, recovery algorithm, snapshots |
| `docs/DECISIONS.md` | every significant decision, its alternatives and its cost — including all deviations from the design |
| `docs/BENCHMARKS.md` | methodology, measured results, and what has *not* been measured |

## Repository layout

```
cmd/
  kvserver/     server binary
  kvctl/        client + offline WAL inspector
  kvbench/      load generator with a log-linear latency histogram
internal/
  protocol/     wire codec (pure functions, fuzzed)
  store/        sharded map, TTL index, eviction, reference implementation
  wal/          records, segments, group commit, recovery
  snapshot/     atomic snapshot write/read
  engine/       composition: recovery, write path, snapshots, data-dir lock
  server/       TCP accept, connection lifecycle, dispatch, worker pool
  cluster/      Phase 2 replication
  client/       reusable client with pipelining
  config/       flags, env overlay, validation
test/
  harness/      in-process and subprocess server scaffolding
  crash/        kill -9 durability suite
  chaos/        adversarial input and slow clients
  differential/ model-based testing with shrinking
  cluster/      replication integration
```
