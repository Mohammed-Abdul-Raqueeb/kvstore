# Distributed Key-Value Store — Design Document

Project #5. Single-node durable KV store first, replication second.

---

## 0. Language choice

Build it in **Go** unless you specifically want the Rust practice.

Reasoning: the hard parts of this project are protocol framing, durability, lock design, and measurement. Go lets you build three different concurrency architectures and benchmark them against each other in the time Rust would take you to build one. The comparison in Milestone 12 is the single most interview-valuable artifact here, and Go makes it cheap. Go also gives you `-race` for free, which will find real bugs in the sharded store.

Pick Rust instead if: you want precise memory accounting (Rust makes the memory-limit milestone genuinely honest rather than estimated), or you want `loom` for exhaustive concurrency model checking, or your previous four projects were all in Go and you want variety.

Everything below is language-neutral. Where it matters I note the Go and Rust variants.

**Do not** use a framework. `net` / `tokio::net`, `sync`, and a CRC library are the dependency budget. A CLI arg parser and an HDR histogram library for the benchmark tool are acceptable. Nothing else.

---

## 1. Complete architecture

Seven subsystems, deliberately separated so each can be tested in isolation:

| Subsystem | Responsibility | Knows about |
|---|---|---|
| **Protocol** | Encode/decode frames. Pure functions over byte slices. | Nothing. No sockets, no store. |
| **Server** | Accept loop, connection lifecycle, read/write buffers, deadlines, dispatch. | Protocol, Store, WAL |
| **Store** | Sharded in-memory map, TTL metadata, memory accounting, eviction. | Nothing external |
| **WAL** | Append-only log, group commit, fsync policy, segment rotation. | Nothing external |
| **Recovery** | Snapshot load + WAL replay + truncation of torn tails. | Store, WAL, Snapshot |
| **Snapshot** | Point-in-time dump of the store, atomic file swap. | Store |
| **Cluster** (phase 2) | Replication stream, heartbeats, failure detection, election. | WAL, Store |

The critical design rule: **Protocol, Store, and WAL have no dependency on each other.** They are three libraries. The Server is the only thing that composes them. This is what makes the repo readable to an interviewer, and it's what makes the test suite possible.

---

## 2. Component diagram

```
                         ┌──────────────────────────────┐
   TCP clients ─────────▶│  Acceptor (listen/accept)    │
                         └──────────────┬───────────────┘
                                        │ one conn
                            ┌───────────▼────────────┐
                            │  Connection Handler    │
                            │  ┌──────────────────┐  │
                            │  │ read buffer      │  │
                            │  │ frame decoder    │◀─┼── Protocol pkg
                            │  │ bounded out buf  │  │
                            │  └────────┬─────────┘  │
                            └───────────┼────────────┘
                                        │ Command
                            ┌───────────▼────────────┐
                            │      Dispatcher        │
                            └──┬──────────────────┬──┘
                 mutation      │                  │  read
              ┌────────────────▼──┐          ┌────▼──────────────┐
              │   WAL Writer      │          │                   │
              │  MPSC queue       │          │                   │
              │  group commit     │          │                   │
              │  single fsync thr │          │                   │
              └────────┬──────────┘          │                   │
                       │ durable(lsn)        │                   │
              ┌────────▼─────────────────────▼───────────────┐
              │                 Store                        │
              │   shard 0    shard 1   ...   shard N-1       │
              │  ┌────────┐ ┌────────┐      ┌────────┐       │
              │  │ RWLock │ │ RWLock │      │ RWLock │       │
              │  │ HashMap│ │ HashMap│      │ HashMap│       │
              │  │ memctr │ │ memctr │      │ memctr │       │
              │  └────────┘ └────────┘      └────────┘       │
              └────┬─────────────────────────────┬───────────┘
                   │                             │
        ┌──────────▼──────────┐      ┌───────────▼──────────┐
        │ Expiry Sweeper      │      │ Eviction Controller  │
        │ (periodic, sampled) │      │ (memory-pressure)    │
        └─────────────────────┘      └──────────────────────┘

     ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐
     │ Snapshotter  │   │  Recovery    │   │ Replication (P2) │
     └──────────────┘   └──────────────┘   └──────────────────┘
```

---

## 3. Data flow

### SET (the important one)

```
1.  bytes arrive on socket
2.  read into per-conn buffer; loop until a full 16-byte header is buffered
3.  parse header; validate magic, version, body_len <= MAX_FRAME
4.  loop until body_len bytes buffered
5.  decode body -> Command{op:SET, key, value, ttl}
6.  reject early: key_len==0, value too large, unknown opcode
7.  build WAL record, submit to WAL queue -> receive assigned LSN + a
    completion handle
8.  acquire shard write lock (shard = hash(key) & mask)
9.  memory accounting delta; if over limit, trigger eviction
10. insert/overwrite entry, set expire_at
11. release shard lock
12. wait on completion handle until fsync covering this LSN returns
    (policy = always) or return immediately (policy = everysec/no)
13. encode OK response into bounded output buffer
14. flush output buffer with a short-write loop
```

Steps 7–8 order matters and is a deliberate decision. See §7.

### GET

```
1-6. same framing path
7.  shard read lock
8.  lookup; if found and expire_at != 0 and expire_at <= now:
      treat as MISS, mark for lazy deletion, return NOT_FOUND
9.  copy value out (or bump refcount) while holding read lock
10. release lock; update last_access via atomic store (no lock upgrade)
11. encode response, flush
```

### Recovery

```
startup -> find newest valid snapshot -> load into shards
        -> open WAL segments in LSN order
        -> skip records with lsn <= snapshot.last_included_lsn
        -> apply each valid record
        -> on torn tail: truncate file, continue
        -> drop already-expired keys
        -> next_lsn = last_applied + 1
        -> open newest segment for append
        -> start listener
```

Note that the listener starts **last**. Never accept a connection before recovery completes.

---

## 4. TCP wire protocol

Binary, length-prefixed, fixed 16-byte header. Little-endian throughout (pick one and write it down; mixing is a classic bug).

### Request header

```
offset  size  field         notes
0       2     magic         0x564B ("KV" LE). Cheap desync detector.
2       1     version       0x01
3       1     opcode        see table
4       2     flags         bit0 = no_reply (fire and forget)
6       2     reserved      must be 0, must be validated as 0
8       4     request_id    echoed in the response; enables pipelining
12      4     body_len      bytes following this header
```

### Response header

Identical, except byte 3 is **status** instead of opcode.

### Opcodes

| Code | Op | Body |
|---|---|---|
| 0x01 | PING | empty |
| 0x02 | GET | `key_len:u16, key` |
| 0x03 | SET | `key_len:u16, key, val_len:u32, value, ttl_ms:u64` (0 = no TTL) |
| 0x04 | DELETE | `key_len:u16, key` |
| 0x05 | EXISTS | `key_len:u16, key` |
| 0x06 | EXPIRE | `key_len:u16, key, ttl_ms:u64` |
| 0x07 | STATS | empty |
| 0x10 | REPLCONF | phase 2 |
| 0x11 | SYNC | phase 2 |

### Status codes

| Code | Meaning | Connection survives? |
|---|---|---|
| 0x00 | OK | yes |
| 0x01 | NOT_FOUND | yes |
| 0x02 | BAD_REQUEST (semantic) | yes |
| 0x03 | TOO_LARGE | yes |
| 0x04 | OOM (eviction couldn't free enough) | yes |
| 0x05 | INTERNAL | yes |
| 0x06 | READ_ONLY (replica got a write) | yes |
| 0x07 | NOT_LEADER | yes |
| 0x80 | PROTOCOL_ERROR | **no — close after sending** |

That last distinction is the one people miss. If the framing is wrong you have lost stream synchronisation and there is no safe way to continue. Send the error, then close. If the framing is fine but the request is nonsense, reply and keep going.

### Limits (all configurable, all enforced *before* allocation)

```
MAX_KEY_LEN    = 64 KiB   (u16 caps this naturally)
MAX_VALUE_LEN  = 16 MiB
MAX_FRAME_LEN  = 16 MiB + 64 KiB + 64
```

### Framing rules you must implement by hand

- `read_full(buf, n)`: loop on `read()` until `n` bytes or EOF. A single `read()` returning fewer bytes than requested is normal, not an error.
- `write_full(buf)`: loop on `write()` until all bytes are out. Short writes happen on real sockets under load.
- Never `make([]byte, body_len)` before validating `body_len <= MAX_FRAME_LEN`. A four-byte forged length field is otherwise a one-packet OOM kill.
- Set read and write deadlines on every connection. Half-open connections that never send anything will otherwise leak file descriptors until you hit the ulimit.
- Keys are arbitrary bytes. They may contain `\0`, `\n`, and invalid UTF-8. Never route them through a string type that assumes otherwise.

### Debug text mode (optional, worth it)

Add a `--text-port` that speaks a line protocol (`GET foo\r\n`) so you can telnet into the server while debugging. Keep it strictly separate from the binary path; it is a debugging aid, not a second product.

---

## 5. WAL record format

```
offset  size  field       notes
0       4     crc32c      CRC over bytes [8, 8+length)
4       4     length      payload byte count
8       ...   payload
```

Payload (fixed 32-byte header + variable):

```
0     8   lsn:u64
8     8   created_at_ms:u64        wall clock, for the inspector
16    1   rec_type:u8              1=SET 2=DEL 3=EXPIRE 4=SEGMENT_HDR
17    1   flags:u8
18    2   key_len:u16
20    4   val_len:u32
24    8   expire_at_ms:u64         absolute; 0 = no expiry
32    ..  key bytes
..    ..  value bytes
```

Total record = `8 + 32 + key_len + val_len`.

### Why the CRC doesn't cover the length field

Chicken-and-egg: you must trust `length` before you can know how many bytes to checksum. LevelDB has the same problem and solves it the same way. The defence is bounds validation instead of a checksum:

```
if length > MAX_RECORD_LEN            -> corruption
if offset + 8 + length > file_size    -> torn tail
```

A corrupted length field that survives both checks will fail the CRC on the payload. Write this reasoning into a comment; it is exactly the kind of thing an interviewer will poke at.

### Segments

`wal/000001.log`, `wal/000002.log`, … Rotate at 64 MiB. Each segment starts with a `SEGMENT_HDR` record containing the magic, format version, and the first LSN in the segment. When you create a new segment file you must **fsync the containing directory**, not just the file. Otherwise the file's existence isn't durable even though its contents are.

### Durability policy

| Mode | Behaviour | Loses on crash |
|---|---|---|
| `always` | fsync before ack | nothing acked |
| `everysec` | background fsync each second | up to 1s of acked writes |
| `no` | rely on OS page cache | up to 30s (dirty writeback) |

Default `everysec`. Make it a flag and benchmark all three. The throughput delta between `always` and `everysec` on a consumer SSD is typically 20–100x, and being able to state that number from your own measurements is worth more than any amount of theory.

### Group commit

Concurrent writers must not each do their own fsync. Architecture:

```
writers ──▶ MPSC channel ──▶ WAL thread
                               loop:
                                 drain up to 1024 records or 200µs
                                 serialise into one contiguous buffer
                                 single write()  (or writev)
                                 single fsync()
                                 signal every waiter in the batch
```

One fsync amortised across N writes. This is the difference between ~200 writes/sec and ~100k writes/sec in `always` mode.

### Inspector CLI

```
kvctl wal dump    --dir data/     # human-readable record listing
kvctl wal verify  --dir data/     # CRC-check every record, report first bad offset
kvctl wal replay  --dir data/ --to-lsn 5000   # rebuild state, print resulting keyspace
kvctl wal stats   --dir data/     # record counts by type, LSN range, size
```

Build this at Milestone 7, not later. You will use it constantly to debug the recovery tests.

---

## 6. Storage engine design

### Sharding

```
shards = 2^k, default = next_pow2(2 * num_cpus), configurable
shard_index = hash(key) & (shards - 1)
```

Use a **seeded** xxhash or SipHash. An unseeded FNV lets an attacker (or an unlucky workload) drive every key into one shard, collapsing your concurrency to a single lock. Generate the seed at startup.

Each shard owns:

```
struct Shard {
    lock:      RWMutex
    entries:   HashMap<Vec<u8>, Entry>
    mem_bytes: atomic u64        // logical accounting
    hits, misses, evictions: atomic u64
}

struct Entry {
    value:       Vec<u8>
    expire_at:   u64        // absolute ms, 0 = never
    last_access: atomic u64 // monotonic ms, for eviction
    size:        u32        // cached total cost
}
```

### Why sharding and not the alternatives

- **Single global lock**: simplest, and you should build it first as the baseline. It serialises everything; p99 latency degrades linearly with client count. Keep it in the repo behind a flag so you can benchmark against it.
- **Sharded RWMutex** (recommended): near-linear scaling to core count for uniform keys, trivial to reason about, easy to prove correct with `-race`. Degrades under skewed (Zipfian) key distributions, which is worth measuring and reporting.
- **Lock-free hash map**: better in theory, but you inherit memory reclamation (epoch-based reclamation or hazard pointers) and you cannot easily do multi-field atomic updates. Not worth it for this project. Being able to *explain why you rejected it* is worth more than building it.
- **Actor per shard** (single-threaded shard owning a channel): no locks at all, perfect ordering, easy to reason about, and it's the second architecture you should build for the comparison in Milestone 12. Costs you a channel round-trip per operation.

### Lock discipline (non-negotiable)

**Never hold a shard lock across any I/O.** Not a socket write, not a WAL write, not an fsync. Not a log statement that might block. Copy what you need out, drop the lock, then do the I/O. Violating this creates a lock convoy where one slow disk write stalls every client hashing to that shard.

---

## 7. Concurrency model

### Connection handling — build both, benchmark both

**A. Goroutine/task per connection** (build first)
- Go: one goroutine reading, one writing, connected by a bounded channel. Scales to ~100k connections fine.
- Rust: `tokio::spawn` per connection. Do *not* use OS threads per connection.

**B. Fixed worker pool** (build second)
- Bounded worker count, connections distributed across event loops with `SO_REUSEPORT`, or an accept-and-hand-off queue.
- Demonstrates you understand that unbounded task creation is a resource leak, and lets you show head-of-line blocking effects in the benchmark.

### Slow client protection

Each connection gets a bounded output buffer (default 1 MiB). If a response won't fit because the client isn't reading, you have exactly three choices and you must pick one explicitly:

1. Block the handler — **wrong**, it lets one slow client stall shared resources.
2. Drop the response — wrong, it breaks the protocol contract.
3. **Close the connection with a logged reason** — correct. This is what Redis does with `client-output-buffer-limit`.

Writes must be non-blocking or on a dedicated per-connection writer with a bounded queue. If the queue is full, close.

### The WAL-vs-memory ordering decision

You must choose one and document it:

**Option 1 — WAL durable, then apply to memory.** Strictly correct. A crash can never leave memory ahead of the log. Costs you an fsync of latency inside the critical path and serialises writes to the same key behind disk.

**Option 2 (recommended) — assign LSN, apply to memory, ack after durable.** Under the shard lock you reserve an LSN and update memory. The response is withheld until fsync covers that LSN. Concurrent readers can observe a value that is not yet durable, so a crash between apply and fsync means a reader saw something that never happened. Nobody was ever *told* it succeeded, so no client-visible durability promise is broken.

Option 2 is what Redis and most production stores do. Say so, and be ready to explain the exact anomaly window.

### Race detection

Run the full integration suite under `go test -race` in CI, every time. In Rust, use `loom` for the shard and WAL queue logic. Concurrency bugs that only appear at 500 connections under load are not debuggable after the fact.

---

## 8. Crash recovery algorithm

```
1.  Acquire an exclusive lock file in the data dir. Two servers on one
    data directory silently corrupt each other.
2.  Scan for snapshots. Pick the newest whose footer CRC validates.
    Load it. Record snapshot.last_included_lsn (0 if none).
3.  List WAL segments, sort by first_lsn from their headers.
4.  For each segment, for each record at offset O:
      a. if remaining_bytes < 8            -> torn tail, goto 6
      b. read crc, length
      c. if length > MAX_RECORD_LEN        -> corrupt, goto 6
      d. if O + 8 + length > file_size     -> torn tail, goto 6
      e. read payload; if crc32c(payload) != crc -> corrupt, goto 6
      f. if lsn <= snapshot.last_included_lsn -> skip
      g. apply record to store (no WAL write, no locks needed —
         recovery is single-threaded)
      h. last_good_offset = O + 8 + length
5.  Segment fully read, continue to next.
6.  Corruption or torn tail encountered:
      - if this is the LAST segment: truncate to last_good_offset,
        log a warning with byte offset and reason, continue startup.
      - if this is NOT the last segment: this is real corruption in
        the middle of the log. Refuse to start. Exit non-zero with the
        offset. Require an explicit --unsafe-truncate to proceed.
7.  Sweep: drop every entry with expire_at != 0 && expire_at <= now.
8.  next_lsn = max_applied_lsn + 1
9.  Open the last segment for append at last_good_offset.
10. Start expiry sweeper, eviction controller, snapshotter.
11. Bind the listener. Only now are you accepting traffic.
```

Step 6's distinction is the whole point of the milestone. A torn tail is *expected* — it means the process died mid-write, which is exactly the scenario you're designing for. Corruption in the middle means the disk lied or the file was edited, and silently dropping data at that point would be the worst possible behaviour.

### Automated crash testing

```
loop 200 times:
  start server with a fresh data dir
  start K writer clients; each records every key it received OK for,
    to a local append-only "acked.log", fsynced
  after a random 200–2000ms, kill -9 the server
  restart the server, wait for recovery
  assert: every key in acked.log is present with the right value
  assert: no key is present that was never sent
  assert: recovery emitted at most one torn-tail warning
```

This is a miniature Jepsen harness. It will find bugs. Add a second variant that corrupts the WAL deliberately (truncate at a random offset, flip a random bit) and asserts the server either recovers cleanly or refuses to start, but never returns wrong data.

---

## 9. TTL design

Three mechanisms, layered. Build them in this order.

### 1. Lazy expiration (Milestone 9a)

On every read of a key, compare `expire_at` to now. If expired, return NOT_FOUND and schedule deletion. Free, correct from the client's point of view, but expired keys occupy memory forever if nobody reads them.

### 2. Active sampling sweeper (Milestone 9b)

A background task, every 100 ms, per shard:

```
budget = 25% of one core, enforced by measuring elapsed time
loop:
  take shard write lock
  sample 20 random keys that have a TTL set
  delete the expired ones
  release lock
  if expired_count / 20 > 0.25: repeat
  else: move to next shard
  if elapsed > budget: stop this cycle
```

Two properties matter: the lock is taken and released repeatedly in small bites rather than held for a whole scan, and the total work per cycle is time-bounded. This is what "expiration should not block normal requests" means concretely. Sampling 20 with a 25% continue threshold converges to under ~25% expired keys remaining, which Redis established empirically.

Sampling random keys from a hash map is not free in every language. In Go you can exploit map iteration's randomised start, or maintain a per-shard slice of TTL-bearing keys.

### 3. Timer wheel (Milestone 9c, the interesting one)

A hierarchical timing wheel gives O(1) insert and O(1) per-tick expiration, at the cost of memory per timer and cancellation complexity on overwrite. Build it, benchmark it against sampling with 1M keys at varying TTL densities, and write up which wins where. The honest answer is usually "sampling wins for sparse TTLs, the wheel wins when most keys have TTLs and precision matters," and having measured it yourself is the point.

### Clock discipline

- `expire_at` is stored as **absolute wall-clock ms**, because it must survive a restart and mean the same thing.
- All interval measurement (sweeper cadence, benchmarks, timeouts) uses the **monotonic** clock.
- Document what happens if the wall clock jumps backwards: keys expire late. Forwards: keys expire early. Neither corrupts anything; both are worth mentioning as a known limitation.

### The replication trap

Expiry is *not* deterministic across nodes, because nodes see different `now`. If replicas expire independently, a read on a replica can return NOT_FOUND while the primary still returns the value. The fix: replicas never expire on their own. The primary sends an explicit DEL through the replication stream when a key expires, and replicas apply it. Note this in the design now, even though you'll only need it in phase 2.

---

## 10. Memory management and eviction

### Accounting

Track a logical byte count per shard, updated under the shard lock, exposed as an atomic:

```
entry_cost = key_len
           + value_len
           + sizeof(Entry)        // struct overhead
           + MAP_OVERHEAD         // measured, ~48-64 bytes for Go maps
```

Then calibrate: insert 1M known-size entries and compare your counter against actual RSS from `/proc/self/statm` (you already know this territory from Project #3). Report the ratio in your docs. Being off by 15% and knowing it is fine; being off and not knowing is not.

Also expose real RSS in STATS so the gap between logical and actual is visible at runtime. Allocator fragmentation, Go's GC, and jemalloc arenas all live in that gap.

### Eviction

Policy: **sampled LRU** (approximated), not exact LRU.

Exact LRU needs an intrusive doubly-linked list, which means every GET mutates the list, which means every read needs a *write* lock. That destroys read concurrency. Measure this yourself: implement exact LRU behind a flag and show the read-throughput collapse in your benchmark. It's a great chart.

Sampled LRU instead:

```
on read:  entry.last_access.store(now_ms)   // atomic, no lock upgrade
on memory pressure, per shard:
  sample K=5 random entries
  evict the one with the oldest last_access
  repeat until under the low-water mark
```

Raising K improves approximation quality; K=5 is within a few percent of exact LRU in practice and K=10 is very close. Benchmark eviction accuracy against a true LRU reference on a Zipfian workload and plot hit rate vs K. That plot is a strong portfolio artifact.

### Limit enforcement

```
--max-memory 512MB
--max-memory-policy allkeys-lru | noeviction | volatile-lru
```

High-water mark at 100%, evict down to a low-water mark at 95% to avoid thrashing at the boundary. Under `noeviction`, writes that would exceed the limit return status `OOM` rather than being silently dropped or OOM-killing the process. Reads always succeed.

Eviction runs in the write path when over the limit, plus opportunistically in the background sweeper. It must be bounded per call so a single SET doesn't trigger a 2-second eviction storm.

---

## 11. Testing strategy

Every subsystem gets tests. Organised by kind, not by package.

### Unit
- Protocol codec: round-trip every opcode, every boundary length (0, 1, MAX-1, MAX, MAX+1).
- WAL record: round-trip, CRC detection of single-bit flips at every byte position.
- Shard hashing: distribution uniformity over 1M random keys, chi-squared.
- TTL arithmetic, eviction cost calculation, LRU sampling.

### Fuzz (do not skip this)
```
FuzzDecodeFrame(data []byte):
  decode(data)
  // assertions: never panics, never allocates more than MAX_FRAME,
  // never returns a Command with fields pointing outside the input
```
Run it for an hour. `go-fuzz` / `cargo-fuzz` / Go 1.18+ native fuzzing. This is the single highest-value-per-line test in the project and it directly demonstrates the "handle malformed requests safely" requirement.

### Model-based / differential
Keep a dead-simple reference implementation: `map[string]entry` behind one mutex, no WAL, no shards. Generate random operation sequences and run them against both the reference and the real store, asserting identical observable results. Shrink failing sequences to a minimal repro. This catches sharding bugs, TTL edge cases, and eviction accounting drift that unit tests miss entirely.

### Integration
- Multi-client concurrent read/write, always under `-race`.
- Pipelining: 10k requests in flight on one connection, verify all `request_id`s come back and match.
- Connection churn: 1000 connect/disconnect cycles, assert fd count is stable.

### Chaos / adversarial
- Client disconnects mid-frame (after header, before body).
- Declared `body_len` of 4 GB.
- Zero-length key. Key of exactly MAX_KEY_LEN. Value of MAX+1.
- Client that connects and never sends (deadline must fire).
- Client that sends and never reads (output buffer limit must fire).
- 10k simultaneous connections.

### Crash / durability
As described in §8. Automated, in CI, at least 50 iterations per run.

### Phase 2 (replication)
- Replica catches up from cold, from a snapshot, and from a partial log.
- Network partition: assert no split-brain writes.
- Leader kill: assert exactly one leader emerges, measure time-to-elect.
- Replication lag under sustained write load.

---

## 12. Benchmarking strategy

Build your own tool, `kvbench`. Do not use `redis-benchmark` shaped thinking.

### Two load modes

**Closed loop**: N connections, each sends the next request after receiving a response. Measures max throughput. This is what most people build.

**Open loop**: requests issued at a fixed rate regardless of whether previous ones returned. This is the one that matters, because closed-loop benchmarks suffer from **coordinated omission** — when the server stalls, the client stops sending, so the stall never shows up in the latency histogram. Your p99 comes out beautiful and completely fake. Implement open-loop, mention coordinated omission by name in your README, and you will be ahead of most working engineers.

### Metrics

Record into an HDR histogram: p50, p90, p99, p99.9, max. **Never report only the mean.** Report throughput alongside the latency distribution, because throughput without latency is meaningless.

### Dimensions to sweep

| Dimension | Values |
|---|---|
| Workload | 100% GET, 100% SET, 95/5, 50/50 |
| Key distribution | uniform, Zipfian(0.99) |
| Value size | 64 B, 1 KiB, 64 KiB |
| Connections | 1, 8, 64, 256, 1024 |
| Pipeline depth | 1, 8, 64 |
| fsync policy | always, everysec, no |
| Shard count | 1, 4, 16, 64, 256 |
| Concurrency model | global lock, sharded, actor-per-shard |

You don't need the full cross product. Sweep one dimension at a time from a fixed baseline.

### Recovery benchmark

Plot recovery time against WAL size (1 MB → 10 GB), with and without snapshots. Show that snapshots turn an O(total writes) recovery into O(writes since last snapshot). This is the clearest possible demonstration of why snapshots exist.

### Methodology hygiene

- 30s warmup, 60s measurement, 3 runs, report median and spread.
- Pin server and client to disjoint CPU sets (`taskset`).
- Measure your loopback ceiling first, so you know when you're benchmarking the kernel instead of your server.
- Record hardware, kernel version, filesystem, and mount options in the results file.
- Commit the raw results as CSV plus the plotting script, not just the pretty PNG.

### Deliverable

`docs/BENCHMARKS.md` with charts and a written analysis of *why* each curve looks the way it does. The analysis is the valuable part. Anyone can produce numbers.

---

## 13. Repository structure

```
kvstore/
├── cmd/
│   ├── kvserver/main.go        thin: parse config, wire subsystems, run
│   ├── kvctl/main.go           CLI client + WAL inspector
│   └── kvbench/main.go         load generator
├── internal/
│   ├── protocol/
│   │   ├── frame.go            header encode/decode
│   │   ├── command.go          body encode/decode per opcode
│   │   ├── opcode.go
│   │   ├── status.go
│   │   ├── limits.go
│   │   └── fuzz_test.go
│   ├── server/
│   │   ├── server.go           accept loop, lifecycle, shutdown
│   │   ├── conn.go             per-conn read/write buffers, deadlines
│   │   ├── dispatch.go         Command -> Store/WAL
│   │   └── pool.go             worker-pool variant
│   ├── store/
│   │   ├── store.go            public API, shard routing
│   │   ├── shard.go
│   │   ├── entry.go
│   │   ├── expiry.go           lazy + sampled sweeper
│   │   ├── wheel.go            timing wheel variant
│   │   ├── evict.go            sampled LRU
│   │   ├── memory.go           accounting, RSS probe
│   │   └── reference.go        simple map+mutex, for differential tests
│   ├── wal/
│   │   ├── record.go           format, CRC
│   │   ├── writer.go           MPSC queue, group commit, fsync policy
│   │   ├── reader.go           sequential scan with corruption detection
│   │   ├── segment.go          rotation, naming, dir fsync
│   │   └── recover.go
│   ├── snapshot/
│   │   ├── write.go            tmp + fsync + rename + dir fsync
│   │   └── read.go
│   ├── cluster/                phase 2
│   │   ├── replica.go
│   │   ├── primary.go
│   │   ├── heartbeat.go
│   │   └── election.go
│   ├── config/config.go
│   └── metrics/metrics.go
├── test/
│   ├── crash/                  kill -9 harness
│   ├── chaos/                  adversarial clients
│   ├── differential/           model-based
│   └── cluster/                phase 2
├── docs/
│   ├── DESIGN.md               this document
│   ├── PROTOCOL.md             wire format spec, standalone
│   ├── WAL.md                  format spec + recovery algorithm
│   ├── BENCHMARKS.md           results + analysis
│   └── DECISIONS.md            ADRs: what you chose and what you rejected
├── Makefile
└── README.md
```

`DECISIONS.md` is the file interviewers actually read. One short entry per decision: context, options considered, choice, consequences. Ten entries of five lines each. Write each one *when you make the decision*, not at the end.

Rust equivalent: `src/bin/` for the three binaries, and the `internal/` packages become modules or workspace crates. Making `protocol`, `store`, and `wal` separate workspace crates enforces the dependency rule at compile time, which is a genuine advantage.

---

## 14. Milestones, easiest to hardest

### M0 — Skeleton (half a day)
Repo layout, Makefile (`build`, `test`, `race`, `fuzz`, `bench`, `lint`), config struct with flags and env vars, structured logging, graceful shutdown on SIGTERM.
**Test:** `make test` passes with zero tests. CI green.
**Concept:** project hygiene. Cheap, and everything after depends on it.

### M1 — Protocol codec, no network (1 day)
Encode and decode every frame type as pure functions over byte slices. Bounds validation. Fuzz target.
**Test:** table-driven round-trips, boundary lengths, one hour of fuzzing with zero crashes.
**Concept:** binary serialisation, defensive parsing, treating input length fields as hostile.
*Build this before any socket code. Being able to test the protocol without a network is the whole reason for the layering.*

### M2 — TCP server, PING only (1 day)
Accept loop, per-connection read buffer, `read_full` / `write_full` loops, deadlines, clean shutdown. PING and nothing else.
**Test:** a client that deliberately sends one byte at a time with sleeps between. Another that sends two full frames in one packet. Both must work. This proves your framing.
**Concept:** TCP is a byte stream with no message boundaries. Message framing.

### M3 — Store v0 + full command set (1 day)
`map[string]Entry` behind one global mutex. GET/SET/DELETE/EXISTS wired through the dispatcher. Goroutine per connection.
**Test:** integration test with real sockets, 8 concurrent clients, under `-race`.
**Concept:** end-to-end request path. You now have a working (non-durable) server.

### M4 — kvctl (half a day)
Interactive and one-shot CLI. `kvctl set foo bar --ttl 30s`, `kvctl get foo`.
**Test:** shell-based smoke test in CI.
**Concept:** none, but you now have a debugging tool you'll use for the rest of the project.

### M5 — Sharded store (2 days)
Replace the global lock with N shards. Keep the global-lock version behind a flag. Seeded hash.
**Test:** hash distribution test; concurrency stress test with 256 clients hammering overlapping keys, under `-race`; first benchmark comparing sharded vs global lock.
**Concept:** lock granularity, contention, the relationship between shard count and core count. **First real result to put in BENCHMARKS.md.**

### M6 — WAL writer (3 days)
Record format, CRC32C, segment rotation, dir fsync, the three fsync policies, MPSC queue, group commit.
**Test:** write 100k records, verify with a reader; measure fsync amortisation across batch sizes; benchmark all three policies.
**Concept:** durability, write-ahead logging, the gap between `write()` and `fsync()`, group commit as a throughput technique.
*Hardest milestone so far. Take your time on the group-commit batching.*

### M7 — Recovery + WAL inspector (2 days)
Full replay algorithm. Torn-tail truncation. Mid-file corruption refusal. `kvctl wal dump/verify/replay/stats`.
**Test:** hand-craft WAL files with a truncated final record, a bit-flipped middle record, a garbage length field. Assert the exact expected behaviour for each.
**Concept:** crash consistency, distinguishing expected partial writes from real corruption.

### M8 — Crash test harness (2 days)
The `kill -9` loop from §8, automated, 200 iterations.
**Test:** it *is* the test. Run it in CI at reduced iteration count, full count nightly.
**Concept:** you cannot claim durability without testing durability. This is the milestone that separates a real project from a demo.
*It will fail the first time. That's the point.*

### M9 — TTL (2–3 days)
9a lazy, 9b sampled sweeper, 9c timing wheel.
**Test:** 1M keys with staggered TTLs; assert memory is reclaimed within a bounded time; assert p99 GET latency doesn't spike during sweeps (this is the actual requirement, so measure it directly).
**Concept:** background work that must not create latency spikes. Time-bounded incremental algorithms.

### M10 — Memory limit + eviction (2–3 days)
Accounting, RSS calibration, sampled LRU, high/low water marks, `OOM` status, exact-LRU comparison.
**Test:** fill past the limit, assert memory stabilises and RSS tracks the counter; hit-rate comparison vs exact LRU on Zipfian at K=1,2,5,10.
**Concept:** approximation as an engineering choice. Why exact LRU is the wrong answer at scale.

### M11 — Snapshots (2 days)
Background snapshot, atomic swap (tmp → fsync → rename → fsync dir), WAL truncation below `last_included_lsn`.
**Test:** crash during snapshot writing; assert the old snapshot is still loadable and no data is lost. Recovery-time-vs-WAL-size plot.
**Concept:** bounding recovery time, atomic file replacement, checkpointing.

### M12 — kvbench + full report (3 days)
Open and closed loop, HDR histograms, Zipfian generator, the full sweep, plots, written analysis. Includes the actor-per-shard concurrency variant so the three-way comparison is real.
**Test:** validate the load generator against a known-latency mock server.
**Concept:** measurement methodology, coordinated omission, throughput/latency tradeoff curves.
*This is where the single-node project is genuinely finished. Stop here and it's already a strong project.*

---

### Phase 2 — Replication

### M13 — Async primary/replica (4 days)
Replica connects, sends `SYNC`, primary responds with a snapshot plus a stream of WAL records from that LSN onward. Replica applies them in order and serves reads. Writes to a replica return `READ_ONLY`. Replica reconnect resumes from its last applied LSN, falling back to a full resync if the primary has truncated past it.
**Test:** replica catches up from cold, from partial, and after a disconnect; sustained write load with lag measured as `primary_lsn - replica_applied_lsn` and as wall-clock delay.
**Concept:** log shipping, why you replicate the *log* and not the *state*, eventual consistency, replication lag as a first-class metric.

### M14 — Failure detection (2 days)
Heartbeats both directions, configurable timeout, or phi-accrual for the more interesting version. Node states: alive, suspect, dead. Expose in STATS.
**Test:** SIGSTOP the primary (a *pause*, not a death — this is the case that breaks naive detectors); partition with iptables; assert correct state transitions and no false positives under load.
**Concept:** you cannot distinguish a slow node from a dead node. This is the fundamental limitation, and articulating it clearly is worth a lot in an interview.

### M15 — Leader election (5+ days, hardest)
Terms/epochs, quorum-based voting, randomised election timeouts, fencing so a returning old leader cannot accept writes.
**Test:** kill the leader 100 times, assert exactly one leader emerges each time and no committed write is lost; partition into 2+1 and assert the minority cannot elect.
**Concept:** split-brain, quorums, why "the node with the highest LSN wins" is not an algorithm, fencing tokens.

**Be honest in the README about the scope here.** Implementing Raft's leader election without Raft's log-matching guarantees gives you availability, not linearizability. Say exactly which guarantees you provide and which you don't. An interviewer will respect "I implemented leader election with epoch fencing; I have not implemented the log-matching property, so a leader change under specific interleavings can lose an uncommitted suffix" enormously more than a README claiming "strongly consistent."

---

## 15. Common implementation mistakes

**Networking**
1. Assuming `read()` returns a whole message. It won't, and the bug appears only under load or across a real network.
2. Ignoring short `write()` returns. Same class of bug.
3. Allocating a buffer sized by an unvalidated length field. A four-byte forged value is a one-packet OOM.
4. No read/write deadlines. Half-open connections accumulate until you exhaust file descriptors.
5. Not validating reserved fields as zero, which blocks you from ever using them for versioning.
6. Treating keys as UTF-8 strings. They're bytes.
7. **Go-specific and vicious:** storing a `[]byte` that aliases your reusable read buffer directly into the map. The next read silently rewrites your stored value. Copy on insert.

**Durability**
8. Thinking `write()` means durable. It means "in the page cache."
9. Creating a new segment file and not fsyncing the *directory*. The file can vanish on crash even though its contents were synced.
10. Writing a snapshot in place instead of tmp → fsync → rename → fsync dir. A crash mid-write destroys the only good snapshot you had.
11. Treating mid-file corruption the same as a torn tail. Silently truncating real corruption is data loss disguised as resilience.
12. Not fsyncing after truncating a torn tail.
13. One fsync per write, then wondering why throughput is 200/sec.

**Concurrency**
14. Holding a shard lock across a WAL write or a socket write. Instant lock convoy.
15. An LRU linked list that forces reads to take write locks, silently destroying read concurrency.
16. Unseeded hash function → hash flooding → all keys in one shard.
17. Unbounded per-connection output buffers → one slow client OOMs the server.
18. Unbounded goroutine/task spawning with no accept backpressure.
19. Never running the race detector, so the bug ships and appears once a week in production.

**Time and expiry**
20. A sweeper that scans the whole keyspace, producing a periodic latency cliff.
21. Mixing monotonic and wall clocks.
22. Letting replicas expire keys independently, causing divergence.

**Measurement**
23. Coordinated omission. Your p99 is fictional.
24. Reporting means. Averages hide exactly the behaviour you care about.
25. Benchmarking on a laptop on battery with thermal throttling and not saying so.
26. No warmup, single run, no variance reported.

---

## 16. Interview questions you should be able to answer

Answer each of these out loud, from your own code, without notes. If you can't, that part isn't finished.

**Protocol**
1. Why length-prefixed framing instead of a delimiter? What breaks with delimiters?
2. Walk me through what happens when a client sends half a header, waits 3 seconds, then sends the rest.
3. A client sends a `body_len` of 4294967295. What does your server do, in order?
4. Why does a protocol error close the connection but a missing key doesn't?
5. What's your `request_id` for, and what breaks if two in-flight requests share one?

**Storage and concurrency**
6. Why sharded locks and not a lock-free hash map? What would lock-free cost you?
7. How did you choose the shard count? What happens at 4 shards on 16 cores, and at 4096?
8. What happens to your throughput under a Zipfian key distribution, and why?
9. Why is your hash function seeded?
10. Show me a place where you were tempted to hold a lock across I/O and didn't.
11. How does an actor-per-shard model compare to RWMutex-per-shard in your measurements?

**Durability**
12. Draw the WAL record layout. Why doesn't the CRC cover the length field, and what protects it instead?
13. What's the difference between `write`, `fsync`, and `fdatasync` here? When do you fsync a directory?
14. Explain group commit. What's your batch trigger, and how did you pick it?
15. Your server is killed at the exact instant between updating memory and fsyncing the WAL. What does a client see? What does a client that already got an OK see?
16. How do you tell a torn tail from real corruption, and why do you treat them differently?
17. What's your recovery time for a 10 GB WAL? How did snapshots change that number?

**TTL and memory**
18. How do you expire a million keys without a latency spike? What bounds the work per cycle?
19. Sampled LRU vs exact LRU: what did you measure, and what did you give up?
20. Your memory counter says 400 MB and RSS says 520 MB. Account for the difference.
21. What happens on a SET when you're at the memory limit under `noeviction`? Under `allkeys-lru`?

**Distributed (if you did phase 2)**
22. Why ship the log rather than the state?
23. How does a replica that's been offline for an hour catch up? What if the primary truncated the WAL past its position?
24. Can you distinguish a crashed primary from a slow one? What does that imply for your failure detector's timeout?
25. Walk me through a network partition that would split-brain a naive election. How does your fencing prevent it?
26. What consistency guarantee do you actually provide? What don't you provide?

**Judgement**
27. What's the worst design decision in this codebase and what would you do differently?
28. What would break first if I gave you 100x the traffic?
29. What did you deliberately not implement, and why?

---

## Where to start

M0 through M2, this weekend. Do not skip M1 and go straight to sockets — testing the codec without a network is what makes every later milestone tractable.

And open `docs/DECISIONS.md` on day one. Write the first entry when you pick the shard count.
