# Architecture Decision Records

Each record states the decision, the alternatives, and what it costs. The
last section lists every place this implementation deviates from
`DESIGN.md`, with the reason.

---

## ADR-001: Binary framed protocol, not a text protocol

**Decision.** A 16-byte fixed header with an explicit `body_len`, plus an
optional line protocol on a separate port for debugging.

**Alternatives.** A RESP-style text protocol is human-readable and
`telnet`-able. HTTP/JSON would be trivially interoperable.

**Cost of the choice.** You cannot read the wire with your eyes, which is
why `--text-addr` exists at all.

**Why anyway.** A length-prefixed binary header makes the parser a fixed
16-byte read followed by exactly one sized read. Text protocols make key
encoding a live problem: a key containing `\r\n` or a space either needs
escaping or is silently unrepresentable. Binary keys are arbitrary bytes
here, including NUL and invalid UTF-8, and the tests assert it.

---

## ADR-002: Validate the reserved header field instead of ignoring it

**Decision.** A non-zero `reserved` field is a protocol error.

**Why.** Accepting garbage in a reserved field means you can never use it.
An old server would silently accept frames from a new client whose semantics
it cannot honour. Rejecting now keeps the field genuinely available later.

The same reasoning applies to unknown flag bits.

---

## ADR-003: Protocol errors close the connection; semantic errors do not

**Decision.** `StatusProtocolError` is fatal to the connection. `NOT_FOUND`,
`OOM`, `TOO_LARGE`, `BAD_REQUEST` are not.

**Why.** The distinction is about whether the byte stream is still parseable.
A missing key leaves framing perfectly intact, so the connection is still
usable. A bad magic number means we no longer know where the next message
begins, and guessing is worse than disconnecting. `Status.Fatal()` encodes
this in one place rather than scattering the judgement across every command.

---

## ADR-004: Sharded RWMutex as the default, with two rival engines kept in-tree

**Decision.** `--engine` selects `sharded` (default), `global`, or `actor`.
All three share one shard implementation.

**Why keep the losers.** The Milestone 12 comparison is supposed to be a
measurement. Deleting the alternatives turns "sharding is faster" into an
assertion nobody can check. Keeping them costs about 60 lines of dispatch
and makes `kvbench --engine` a real experiment.

**Measured result, stated honestly.** On the single-CPU VM this was built on,
sharding shows *no* advantage over a global mutex — see `BENCHMARKS.md`. That
is the expected result when there is no parallelism to exploit, and it is
reported rather than hidden.

---

## ADR-005: Seeded hash for shard routing

**Decision.** xxHash64 with a random per-process seed.

**Why.** An unseeded, publicly known hash lets anyone choose keys that all
land in one shard, collapsing an N-lock design back to one lock. That is a
denial-of-service against your own concurrency. `--hash-seed` pins the seed
when reproducibility matters.

**Cost.** Shard assignment differs between runs, so a key's shard is not
stable across restarts. Nothing depends on that.

---

## ADR-006: Sampled LRU (K=5) rather than an exact LRU list

**Decision.** Eviction samples K random entries and evicts the least recently
used of them. `--exact-lru` switches to a true intrusive list.

**Why.** An exact LRU list must be reordered on every *read*, which means
every GET needs a write lock. That converts a read-mostly workload into a
fully serialised one. The approximation error is small (K=5 lands within a
few percent of exact; K=10 is very close) and the concurrency win is large.

**Cost.** Occasionally evicts a slightly-warmer key than optimal.
`TestSampledLRUEvictsColdKeys` asserts 18+/20 hot keys survive a flood of
cold ones.

**Enabler.** Uniform O(1) random sampling needs a flat slice of entries, not
a Go map — reaching the k'th element of a map is O(k). Each shard keeps a
`slots` slice with swap-with-last removal, and entries cache their own index.

---

## ADR-007: Per-shard memory budgets rather than one global counter

**Decision.** `--max-memory` is divided evenly across shards.

**Why.** A single global atomic counter is simpler to reason about but puts
a contended cache line in the write path of every shard — exactly the
contention sharding exists to remove.

**Cost, stated plainly.** A badly skewed keyspace can evict from a hot shard
while cold shards sit under budget. With uniform hashing and a reasonable
key count this is negligible; with a few enormous keys it is not.

---

## ADR-008: Sampled expiry sweeper by default, timing wheel available

**Decision.** `--expiry` selects `lazy`, `sampled` (default), or `wheel`.

**Sampled.** Per shard: take the write lock, sample N keys *that have a TTL*,
delete the expired ones, release. Repeat that shard while the expired
fraction exceeds a threshold. The whole cycle is bounded by a wall-clock
budget.

Two properties matter. The lock is taken and released in small bites, so a
concurrent GET queues behind one 20-key sample rather than a full scan. And
work per cycle is bounded by time, not keyspace size — a 100× larger
keyspace produces a longer tail of cycles, not a 100× latency spike.

Sampling only from TTL-bearing entries (a separate `ttlSlots` slice) means a
keyspace where 1% of keys have TTLs costs the sweeper 1% of the work.

**Wheel.** O(1) insert and O(1) per tick, at the cost of memory per timer.
Cancellation is handled *lazily*: a timer carries the deadline it was
scheduled for, and on firing the expirer checks the live entry. If the key
was deleted or re-set with a different deadline, the timer is dropped. This
removes cancellation bookkeeping entirely — stale timers cost memory until
they fire but can never delete the wrong data.

---

## ADR-009: WAL ordering — memory apply and LSN reservation under one shard lock

**Decision.** The write path is: take a backpressure slot (outside all
locks), take the shard lock, apply to memory, reserve an LSN and queue the
record, release the lock, then wait for durability before acknowledging.

**Why atomic.** If the LSN were assigned outside the lock, two concurrent
SETs to one key could be written to the log in one order and applied to
memory in the other. Replay would then produce a different value than the
live store had. `TestEngineConcurrentSameKeyOrdering` asserts they agree.

**Why the backpressure slot comes first.** The queue send inside the lock
must never block. The gate bounds in-flight mutations below the queue depth,
which guarantees it. The rule "never hold a shard lock across I/O" is
preserved: the fsync happens after the lock is released.

**The anomaly this accepts.** Between the memory apply and the fsync
completing, a concurrent reader can observe a value that will not survive a
crash. No client was *told* the write succeeded, so no durability promise is
broken. The window is documented on `wal.Commit`.

---

## ADR-010: A separate `internal/engine` package (deviation)

**`DESIGN.md` §13** sketches the store/WAL/snapshot composition inside
`internal/server`. It lives in `internal/engine` instead.

**Why.** Three reasons. The crash and differential suites can drive a full
durable engine without opening a socket. `internal/cluster` needs the same
composition and would otherwise have to import the network layer. And
`internal/server` stays purely about connections and framing.

The dependency rule the design actually cares about is preserved: `protocol`,
`store` and `wal` still know nothing about each other, and exactly one
package composes them.

---

## ADR-011: Directory fsync is a no-op on Windows (deviation)

**`DESIGN.md` §5** requires fsyncing the parent directory after creating a
segment or renaming a snapshot, so the *name* is durable and not just the
contents.

**Windows has no equivalent.** `CreateFile` with `FILE_FLAG_BACKUP_SEMANTICS`
can open a directory handle, but `FlushFileBuffers` on it returns
`ERROR_ACCESS_DENIED`.

**Consequence.** On Windows, `wal.SyncDir` returns nil. Record *contents* are
still fsynced normally, so `--fsync=always` still means "this record is on
stable storage". What is weakened is the durability of the directory entry
for a brand-new file. NTFS journals its own metadata, so this is generally
recoverable, but it is a filesystem property being relied on rather than a
guarantee being enforced.

`STATS` reports `wal.dir_sync_supported` so the weakening is visible at
runtime rather than buried here.

---

## ADR-012: PID lock file rather than flock

**Decision.** An `O_EXCL` lock file containing the PID, with staleness
detection via a liveness probe.

**Why not flock.** `flock(2)` and `LockFileEx` have no common portable
interface, and this project must run on Windows.

**Why staleness detection is mandatory.** A plain `O_EXCL` file cannot
distinguish "another server is running" from "the last one was SIGKILLed".
The crash test harness does exactly that, so without staleness detection the
recovery path would be untestable.

**Known limitation.** PID reuse. If the recorded PID has been recycled by an
unrelated process we conservatively refuse to start. Refusing is the safe
direction.

---

## ADR-013: Failure detection is timeout-based and cannot be otherwise

**Decision.** Three states — alive, suspect, dead — driven by heartbeat
silence.

**The fundamental limit.** No failure detector can distinguish a crashed peer
from a slow or partitioned one. This is not an implementation gap; it is why
asynchronous consensus has the results it does. The timeout is therefore a
tradeoff, not a correct value: too short and a GC pause triggers a spurious
disconnect, too long and real failures go unnoticed.

Three states rather than two because "I have not heard from you recently" and
"I am confident you are gone" are different claims, and conflating them
produces detectors that flap.

---

## ADR-014: No automatic leader election (deliberate scope boundary)

**`DESIGN.md` M15** lists basic leader election as the final, hardest
milestone. **It is not implemented.**

**What is implemented:** manual promotion (`kvctl promote`) plus **epoch
fencing**. Promotion bumps an epoch; a replica refuses a stream from a
primary whose epoch is lower than the highest it has seen, so a returning old
leader cannot quietly resume feeding stale data.

**Why stop here.** Correct leader election needs quorum, persistent vote
state, and a log-completeness comparison before a candidate may win. A
half-implemented election is materially worse than none: it produces
split-brain under exactly the conditions it claims to handle, and it would be
the kind of feature that looks finished in a demo and fails in the one
scenario it exists for. Fencing is the part that can be built correctly at
this scale, so that is the part that was built.

**Consequence, stated in `STATS` output as well as here.** Two nodes both
promoted by an operator will both accept writes. There is no split-brain
*prevention* beyond fencing.

---

## ADR-015: Asynchronous replication only

**Decision.** A primary acknowledges a write once it is durable locally,
without waiting for any replica.

**Consequence.** A primary that dies can lose writes no replica received.
This is the same guarantee Redis async replication gives, and it is stated
in the `consistency_model` field of every `STATS` response so nobody has to
infer it.

Synchronous replication would need a configurable ack quorum and a policy for
what to do when it cannot be met — both of which are only meaningful
alongside real election, which is out of scope per ADR-014.

---

## ADR-016: Eviction and expiry deletes go through a best-effort queue

**Decision.** The store fires an eviction/expiry callback while holding a
shard lock, so it cannot write to the WAL directly. Events go to a buffered
channel drained by a background goroutine. If the channel is full, the event
is dropped and counted in `server.evict_log_drops`.

**Why dropping is safe for a single node.** For expiry, the original SET
record carries the absolute deadline, so replay recreates the key and
recovery's drop-expired pass removes it again — identical recovered state.
For eviction, replay reinserts the key and the memory limit evicts again; the
recovered keyspace may differ in *which* keys were evicted, but it is a valid
state under the same policy.

**Where it is not safe.** Replicas would miss the delete. A primary with
replicas attached should treat a non-zero `evict_log_drops` as a real
problem, which is why the counter is exported rather than hidden.

---

## ADR-017: Zero external dependencies

**Decision.** Standard library only.

CRC32C comes from `hash/crc32`'s Castagnoli table, which is hardware
accelerated on amd64 and arm64. xxHash64, the splitmix64 PRNG and the
log-linear latency histogram are implemented here — roughly 200 lines
combined, versus three dependencies for functionality this project is
supposed to be demonstrating that it understands.

**Cost.** The histogram is not HdrHistogram; it is the same idea with
constant relative error, and the bucket layout is documented in
`cmd/kvbench/histogram.go`.

---

## Full list of deviations from `DESIGN.md`

| # | Deviation | Reason |
|---|---|---|
| ADR-010 | Composition lives in `internal/engine`, not `internal/server` | testability; `cluster` needs it without importing the network layer |
| ADR-011 | Directory fsync is a no-op on Windows | the OS provides no equivalent; reported in `STATS` |
| ADR-014 | M15 automatic leader election not implemented | a half-correct election is worse than none; fencing built instead |
| — | `internal/client` added (not in §13) | `kvctl`, `kvbench`, the replica and every integration test need one client |
| — | `test/harness` added (not in §13) | shared subprocess/in-process scaffolding for four test packages |
| — | `KEYS` opcode added | the inspector and tests need keyspace enumeration; bounded and documented as a scan |

Everything else follows `DESIGN.md` as written: the 16-byte little-endian
header and its field layout, the WAL record format, the snapshot format, the
recovery algorithm and its torn-tail/corruption distinction, the three
concurrency engines, the three expiry mechanisms, the eviction policies, and
the repository layout.
