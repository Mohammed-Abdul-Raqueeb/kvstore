# Wire Protocol

Version 1. Little-endian throughout — the WAL and snapshot formats match, so
there is never a question of which byte order applies where.

## Frame

Every message, request or response, is a 16-byte header followed by a body.

```
offset  size  field
0       2     magic        0x564B  ("KV": 'K'=0x4B at byte 0, 'V'=0x56 at byte 1)
2       1     version      0x01
3       1     opcode (request) / status (response)
4       2     flags        bit0 = no_reply
6       2     reserved     MUST be zero; a non-zero value is a protocol error
8       4     request_id   echoed in the response; enables pipelining
12      4     body_len     bytes following this header
```

`request_id` is what makes pipelining possible: a client may have many
requests in flight and match responses back by id. Responses may legitimately
arrive in an order different from the requests.

### Limits

| Limit | Value |
|---|---|
| `MaxKeyLen` | 65535 |
| `MaxValueLen` | 16 MiB |
| `MaxFrameLen` | `MaxValueLen + MaxKeyLen + 64` |

`body_len` is validated against `MaxFrameLen` **before any allocation**. A
forged four-byte length field is otherwise a one-packet OOM kill; the
decoder returns `ErrFrameTooLarge` without ever sizing a buffer from it, and
`TestReadFrameForgedLengthDoesNotAllocate` asserts this holds.

A server may impose a stricter cap via `--max-value-len`. It may never impose
a looser one.

## Opcodes

| Code | Name | Body |
|---|---|---|
| 0x01 | PING | empty |
| 0x02 | GET | `key_len:u16, key` |
| 0x03 | SET | `key_len:u16, key, val_len:u32, value, ttl_ms:u64` |
| 0x04 | DELETE | `key_len:u16, key` |
| 0x05 | EXISTS | `key_len:u16, key` |
| 0x06 | EXPIRE | `key_len:u16, key, ttl_ms:u64` |
| 0x07 | STATS | empty |
| 0x08 | TTL | `key_len:u16, key` |
| 0x09 | FLUSH | empty |
| 0x0A | SNAPSHOT | empty |
| 0x0B | KEYS | `prefix_len:u16, prefix, limit:u32` |
| 0x10 | REPLCONF | `id_len:u16, node_id, port:u16` |
| 0x11 | SYNC | `from_lsn:u64` |
| 0x12 | REPLACK | `applied_lsn:u64` |
| 0x13 | PROMOTE | empty |

`ttl_ms` is a **relative** duration on the wire and 0 means "no expiry". It is
relative so the client and server need not agree on the wall clock; the server
converts it to an absolute deadline, which is what survives a restart.

Keys are arbitrary bytes. NUL, newlines and invalid UTF-8 are all legal, and
the test suite covers them.

## Statuses

| Code | Name | Connection survives? |
|---|---|---|
| 0x00 | OK | yes |
| 0x01 | NOT_FOUND | yes |
| 0x02 | BAD_REQUEST | yes |
| 0x03 | TOO_LARGE | yes |
| 0x04 | OOM | yes |
| 0x05 | INTERNAL | yes |
| 0x06 | READ_ONLY | yes |
| 0x07 | NOT_LEADER | yes |
| 0x80 | PROTOCOL_ERROR | **no — server closes** |

The split is about whether the byte stream is still parseable. A missing key
leaves framing intact, so the connection is reusable. A bad magic number
means we no longer know where the next message starts, and guessing is worse
than disconnecting.

An **unknown opcode is BAD_REQUEST, not PROTOCOL_ERROR** — the frame was well
formed, so the connection stays open.

## Response bodies

| Command | OK body |
|---|---|
| GET | `val_len:u32, value` |
| EXISTS | `1 byte`, 0 or 1 |
| TTL | `i64` milliseconds remaining; `-1` means no expiry |
| KEYS | `count:u32`, then `count ×` (`key_len:u16, key`) |
| STATS | `len:u32, json` |
| SNAPSHOT | `len:u32, path` |
| SET/DELETE/EXPIRE/FLUSH/PING | empty |

Error responses carry an optional UTF-8 message. It is advisory; clients key
off the status code.

## Partial reads and writes

A single `read()` on a TCP socket returning fewer bytes than requested is
normal, not an error. The decoder loops (`io.ReadFull`) for both the header
and the body. `TestReadFrameSurvivesByteAtATimeDelivery` drives the whole
path one byte per `Read` call.

Writes loop symmetrically via `protocol.WriteFull`.

## Buffer aliasing

`ReadFrame` reuses a caller-supplied scratch buffer, so the steady-state
request path allocates nothing. **The returned `Frame.Body` therefore aliases
that buffer**, and every decoded `Command.Key`/`Value` is a sub-slice of it.
Anything retained past the next `ReadFrame` must be copied — the store copies
on insert, in exactly one place.

Decoded slices are capacity-clamped so an `append` cannot scribble over the
neighbouring field.

## Debug text protocol

`--text-addr` enables a line protocol for `nc`/`telnet` debugging. It is
deliberately separate: it does not share the frame decoder and is disabled by
default. Its tokens are whitespace-delimited, so it **cannot** express keys
containing spaces, newlines or NUL bytes. Use `kvctl` for those.
