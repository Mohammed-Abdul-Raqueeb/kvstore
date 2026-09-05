# Write-Ahead Log and Recovery

## Record format

```
offset  size  field
0       4     crc32c    CRC over bytes [8, 8+length)
4       4     length    payload byte count
8       ...   payload
```

Payload — a fixed 32-byte header, then key and value bytes:

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

CRC32C (Castagnoli), the same polynomial LevelDB and RocksDB use. Hardware
accelerated on amd64 and arm64 via the Go runtime's intrinsics, so
checksumming is not the bottleneck in group commit.

### Why the CRC does not cover the length field

It cannot: you must trust `length` before you know how many bytes to
checksum. This is a genuine chicken-and-egg problem and LevelDB solves it the
same way — with bounds validation instead of a checksum:

```
length > MaxRecordLen            -> corruption; no legal record is this big
offset + 8 + length > file_size  -> torn tail; the write did not complete
```

A corrupted length surviving both checks makes us read the wrong bytes, whose
CRC then fails. So the field is protected transitively. The residual risk is a
corrupted length that happens to select a byte range whose CRC32C matches the
stored value: 2⁻³² per corrupted record, the same risk every log-structured
store accepts.

`TestCRCDetectsSingleBitFlips` flips every bit at every byte position of a
record and asserts all of them are caught.

## Segments

Named by the LSN of their first record, zero-padded to 16 digits so lexical
order equals numeric order:

```
data/wal/0000000000000001.log
data/wal/0000000000065537.log
```

Listing and sorting gives replay order without opening a single file. Every
segment opens with a `SEGMENT_HDR` record carrying a magic and a format
version, so a stray file is recognisably not a segment.

Rotation at `--segment-size` (default 64 MiB). Creation is: create, write
header, fsync file, **fsync directory**. Skipping the last step means a crash
can leave a file whose contents are durable but whose directory entry is
missing.

## Group commit

```
writers ──▶ MPSC channel ──▶ single WAL goroutine
                               drain up to N records or T microseconds
                               serialise into one contiguous buffer
                               ONE write()
                               ONE fsync()
                               signal every waiter in the batch
```

One fsync amortised across N writes is the difference between a few hundred
durable writes a second and a hundred thousand. `TestGroupCommitAmortisesFsync`
asserts fsyncs ≪ records under concurrent load.

The batch closes on whichever comes first: `--group-commit-max` records
(bounds memory and latency) or `--group-commit-wait` (default 200 µs, bounds
how long a lone writer waits for company). 200 µs sits well under any
realistic fsync — consumer NVMe is 100 µs–1 ms, spinning disk 5–10 ms — so
the timer costs nothing when the disk is the bottleneck.

LSN assignment and the queue send happen under one small mutex, so queue
order equals LSN order.

## Durability policies

| `--fsync` | Acknowledges after | Loses on power failure |
|---|---|---|
| `always` | fsync completes | nothing acknowledged |
| `everysec` | `write()` returns | up to ~1 second of acked writes |
| `no` | `write()` returns | whatever the OS has not flushed (~30 s) |

A clean `Close()` always syncs, so a graceful shutdown loses nothing under
any policy.

## Recovery algorithm

1. Lock the data directory (`data/LOCK`).
2. Load the newest snapshot whose footer CRC validates; fall back to older
   ones if it does not.
3. Replay WAL records with `lsn > snapshot_lsn`.
4. On a fault, apply the policy below.
5. Drop keys that expired while the process was down.
6. Reopen the last segment for append at the last good offset.
7. Start background controllers, then bind the listener — **last**, so a
   client can never reach a half-recovered store.

### Torn tail versus corruption

This distinction is the heart of the design.

| Fault | Where | Behaviour |
|---|---|---|
| Torn tail (partial record at EOF) | final segment | truncate, warn, continue |
| Torn tail | earlier segment | refuse |
| Corruption (bad CRC on a *complete* record) | anywhere | refuse |

A torn tail is the *expected* consequence of dying between `write()` and the
record landing. A bad CRC on a complete record means the bytes changed after
they were written — the disk lied, or somebody edited the file. Silently
truncating that is data loss dressed up as resilience.

`--unsafe-truncate` overrides the refusal. It discards everything from the
fault onward, including later segments, and says so loudly.

Truncation is itself fsynced. Skipping that means a second crash re-exposes
the garbage you just removed.

Recovery is idempotent: replaying twice produces the same state.

## Snapshots

```
header (32 bytes)   magic "KVSNAP01", version, created_at_ms, last_included_lsn
entries (repeated)  key_len:u16, val_len:u32, expire_at_ms:u64, key, value
footer (20 bytes)   magic "KVSNAPFT", entry_count:u64, crc32c over [0, footer_start)
```

The CRC lives in the footer and covers everything before it, so a snapshot is
valid only if written to completion. A half-written file has no footer and is
rejected without needing an "is this finished?" flag.

Written to `.tmp`, fsynced, renamed, then the directory is fsynced. Writing
in place would mean a crash mid-write destroys the only good snapshot you had.

The LSN is captured **before** iteration begins. Writes landing during the
scan may or may not appear in the file, but all have higher LSNs, so replay
applies them afterwards. Taking the LSN after iteration would be the bug.

Only after the snapshot is durable is the log below it truncated. Reversing
those two steps loses data on a crash in between.

## Inspecting a log

```
kvctl wal verify --dir data/     # per-segment health, with a verdict
kvctl wal stats  --dir data/     # record counts by type, LSN range, sizes
kvctl wal dump   --dir data/ --limit 50
kvctl wal replay --dir data/     # rebuild the keyspace offline and print it
kvctl snapshots  --dir data/     # list snapshots, validating each CRC
```

`wal replay` is the tool for when recovery and reality disagree: it rebuilds
state in memory without touching the server or the data directory.
