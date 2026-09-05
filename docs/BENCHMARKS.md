# Benchmarks

## Read this before reading any number below

**The hardware these were measured on is not a server.** Everything here was
run on a single-vCPU Linux VM with the load generator and the server sharing
that one CPU. That has three consequences, and ignoring them would make every
number in this document a lie:

1. **There is no parallelism to exploit.** Any result comparing concurrency
   strategies measures overhead, not scalability. Sharding cannot win on one
   core because there is nothing to shard across.
2. **The client competes with the server for CPU.** Throughput figures are a
   floor, not a ceiling.
3. **`fsync` is going to a VM's virtual disk**, whose flush semantics are
   whatever the hypervisor decided. Durability *timings* here are indicative;
   durability *correctness* is established by the crash suite, not by these.

Numbers from a real multi-core machine with a real NVMe device would look
entirely different, and the sharding comparison in particular would be
expected to invert. The methodology below is the part worth keeping; the
figures are a worked example of applying it.

## Methodology

### Coordinated omission

`kvbench` has two modes and the distinction is the single most important
thing in the tool.

**Closed loop** (`--mode closed`): each connection sends its next request
only after the previous response arrives. When the server stalls for 500 ms,
every client politely stops sending. That stall is recorded as *one* slow
request instead of the hundreds that would have been issued during it. The
p99 comes out beautiful and completely fictional.

**Open loop** (`--mode open --rate N`): requests are issued on a schedule
fixed in advance, and latency is measured from the time a request was **due**,
not from when it was sent. A 500 ms stall then appears in every request due
during it — which is what a user behind a queue actually experiences.

The schedule is derived from a fixed start instant, never from "now" after
each request. Deriving the next deadline from the current time is exactly the
bug that reintroduces coordinated omission.

Use closed loop for maximum-throughput claims. Use open loop for any latency
claim. `kvbench` prints a warning after every closed-loop run saying so.

### Self-reported generator saturation

Open loop counts requests issued behind schedule. A non-zero `omitted` count
means the *client* could not keep up, so the latencies understate the
server's problems. `kvbench` prints a prominent warning rather than quietly
reporting optimistic numbers. On the single-CPU VM this fires above roughly
10k req/s, which is a fact about the test rig, not the server.

### Loopback floor

Every run first measures PING round-trip time, so there is a baseline to
distinguish "the server is slow" from "the kernel and the scheduler are
slow". On this VM: **p50 = 15.6 µs, p99 = 50.2 µs**. No measured latency
below that floor means anything.

### Latency histogram

Log-linear buckets with 64 sub-buckets per octave — worst-case relative error
about 1.6%, constant across the range, fixed memory, no reservoir sampling to
distort the tail. Per-worker histograms are merged at the end so the hot path
never touches shared state.

**Never report only the mean.** A service that is 1 ms at p50 and 4 s at
p99.9 has a lovely mean.

## Measured results

Single vCPU, Linux 6.18 x86_64, Go 1.22.2. 64-byte values, 95% GET / 5% SET
unless stated. Client and server on the same host.

### Throughput, closed loop

| Configuration | ops/sec | p50 | p99 | p99.9 |
|---|---|---|---|---|
| 16 conns, 95/5, `--fsync no` | 62,874 | 223 µs | 655 µs | 2.13 ms |

### Concurrency engine comparison

16 connections, 95/5, `--fsync no`, 16 shards:

| `--engine` | ops/sec | p50 | p99 | max |
|---|---|---|---|---|
| `sharded` | 60,690 | 227 µs | 688 µs | 8.29 ms |
| `global` | 63,172 | 223 µs | 639 µs | 4.16 ms |
| `actor` | 56,082 | 240 µs | 770 µs | 42.4 ms |

**This is the opposite of the result sharding exists to produce, and it is
the correct result for this hardware.** With one core there is no lock
contention to relieve, so the sharded engine's extra hashing and indirection
is pure overhead and the global mutex wins slightly. The actor engine pays a
channel round trip per operation and loses by ~11%, with a notably worse tail
(42 ms max) from scheduler interaction.

What this measurement *does* establish: the sharding machinery costs about
4% when it cannot help. What it cannot establish: whether it helps when it
can. That needs a multi-core host, and the honest statement is that this
build has not demonstrated it.

### Durability policy cost

32 connections, pure SET workload:

| `--fsync` | ops/sec | vs `no` |
|---|---|---|
| `always` | 19,096 | 58% |
| `everysec` | 32,010 | 98% |
| `no` | 32,743 | — |

`always` costs about 42% of write throughput here. On real hardware with a
slower fsync the gap widens considerably; group commit is what keeps it from
being a factor of a hundred.

### Group commit

Under 32 concurrent writers with `--fsync always`:

```
records=75158  batches=3751  avg_batch=20.0  fsyncs=3751
```

**20 records per fsync.** Without batching that would be 75,158 fsyncs
instead of 3,751 — a 20× reduction in the operation that dominates durable
write cost. The batch size self-tunes: it rises with concurrency and falls
to 1 for a single idle client, bounded by `--group-commit-max` (1024) and
`--group-commit-wait` (200 µs).

### Recovery time

200,000 keys, 64-byte values, 256,078 WAL records (30 MiB):

| Scenario | Recovery time | Records replayed |
|---|---|---|
| WAL only, no snapshot | 328 ms | 256,078 |
| After a snapshot | 282 ms | 0 applied (256,078 scanned) |

Roughly **780,000 records/sec** of replay throughput.

The snapshot barely helped, and the reason is worth stating: `TruncateBelow`
never removes the segment currently being appended to, and with the default
64 MiB segment size this entire 30 MiB log was one segment. So the snapshot
saved the *apply* work but not the *scan* work. Snapshots only bound recovery
once segments actually rotate — with `--segment-size 8MB` the same test
removes segments and recovery drops accordingly. This is a real limitation of
the interaction between snapshot cadence and segment size, not a measurement
artefact.

### Memory accounting calibration

`STATS` reports `logical_bytes` (our own accounting: `key + value +
sizeof(Entry) + 64`) alongside `rss_bytes` from `/proc/self/statm`. The ratio
is exposed at runtime by `kvctl stats` precisely so the gap is visible rather
than assumed.

At small key counts the ratio is meaningless — a nearly-empty store showed
9251× simply because the Go runtime's baseline RSS (5.8 MiB) dwarfs 657 bytes
of data. The ratio only becomes informative at scale, and reading it at low
key counts is a mistake the tool should probably warn about.

`ResidentBytes()` reads `/proc/self/statm` and is **Linux only**. On Windows
and macOS it falls back to the Go runtime's view of memory obtained from the
OS, which excludes the binary's text and data segments and counts reserved
address space the OS has not backed. The calibration figure is therefore only
meaningful on Linux.

## Reproducing

```bash
make build

# Terminal 1
./bin/kvserver --addr 127.0.0.1:7379 --data-dir /tmp/kvdata --fsync no

# Terminal 2 — maximum throughput
./bin/kvbench --conns 64 --workload 95/5 --duration 30s --warmup 5s

# Latency at a fixed offered load
./bin/kvbench --mode open --rate 50000 --conns 64 --duration 30s

# Skewed keys, where sharding stops helping
./bin/kvbench --dist zipfian --zipf-s 0.99 --conns 64

# Full sweep into a CSV
make bench-suite
```

Every CSV row records the client OS, architecture, CPU count and full
configuration, so a result is never separated from the conditions that
produced it.

## What has not been measured

Stated explicitly rather than left as an implied claim:

- **Multi-core scaling.** The central claim of the sharded design is
  untested on this hardware.
- **Real fsync latency.** A physical NVMe or spinning disk behaves nothing
  like a VM's virtual block device.
- **Large values.** All figures use 64-byte values.
- **Sustained multi-hour behaviour**, memory fragmentation over time, or GC
  pause distribution under long-running load.
- **Replication throughput and lag under load.** Replication correctness is
  covered by `test/cluster`; its performance is not characterised.
- **`everysec` durability under power loss.** See the caveat in
  `test/crash` — SIGKILL does not clear the OS page cache, so no userspace
  test can demonstrate this.
