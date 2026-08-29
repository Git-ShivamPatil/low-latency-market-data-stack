# Wire format

The byte layout of the market-data feed. `schema/market-data.xml` is the machine
-readable source of truth; this document is the human-readable one, and the two
are checked against each other by the golden vectors described at the bottom.

Everything is **little-endian**. There is no floating point anywhere on the wire.

---

## Framing

```
datagram := packetHeader , message*
message  := messageHeader , rootBlock , group?
```

Two layers, because they answer different questions. The packet header says
*where in the stream this datagram sits*; the message header says *what this
message is and how long it is*.

### A message carries no sequence number

The sequence of message `i` in a datagram is `packetHeader.firstSequence + i`.
Nothing on the wire repeats it.

This is the decision the throughput target rests on, so it is worth stating
plainly. At ~40 bytes per incremental update, 1M msg/s is only ~40MB/s — trivial
bandwidth. The binding constraint is **packets per second**: a kernel UDP receive
path tops out somewhere around 300–600K pps per core even with `recvmmsg`, so one
message per datagram makes 1M msg/s unreachable without kernel bypass. Packing
16–32 messages per datagram turns 1M msg/s into ~31–62K pps, which an ordinary
bridge handles without effort.

So batching is not an optimisation to add later. It is the framing, and the
per-message cost has to stay small for it to pay: 8 bytes of message header, no
sequence number, and one clock read per datagram rather than one per message.

Real exchange feeds are framed this way for the same reason, which is what makes
it legitimate — but **the batch factor has to be published alongside any
throughput figure.** A reader who assumes one message per packet is reading a
much stronger claim than the one this project makes.

---

## packetHeader — 24 bytes

Written once per datagram.

| Offset | Size | Type   | Field             | Notes |
|-------:|-----:|--------|-------------------|-------|
| 0      | 2    | uint16 | `schemaId`        | Must equal 1. A mismatch is a hard reject at the header. |
| 2      | 2    | uint16 | `version`         | Schema version the publisher encoded with. |
| 4      | 2    | uint16 | `messageCount`    | Messages packed into this datagram. |
| 6      | 1    | uint8  | `channel`         | 0 = feed A, 1 = feed B. Diagnostic only — arbitration keys on sequence. |
| 7      | 1    | uint8  | `flags`           | bit0 = this datagram is part of a snapshot cycle. |
| 8      | 8    | uint64 | `firstSequence`   | Sequence of message 0. Message `i` is `firstSequence + i`. |
| 16     | 8    | uint64 | `sendTimestampNs` | Publisher clock at send, nanoseconds since the Unix epoch. |

`channel` is deliberately not load-bearing. A handler that arbitrated on channel
identity rather than on sequence would break the moment the two arms interleave,
which is the normal case.

---

## messageHeader — 8 bytes

Written once per message. This is the standard SBE message-header shape.

| Offset | Size | Type   | Field         | Notes |
|-------:|-----:|--------|---------------|-------|
| 0      | 2    | uint16 | `blockLength` | Bytes of root block after this header, **excluding** any group. |
| 2      | 2    | uint16 | `templateId`  | Which message. See the table below. |
| 4      | 2    | uint16 | `schemaId`    | |
| 6      | 2    | uint16 | `version`     | |

### Forward compatibility

A decoder advances by `blockLength` **as read from the wire**, not by the
constant it was compiled with. A publisher on a later schema version that
appended fields to a root block is therefore skipped correctly rather than
misparsed — the extra bytes are ignored and the next message is found.

The reverse is an error: a `blockLength` *smaller* than this build's constant
means fields the decoder needs are simply not present, and `wrap` rejects it.

---

## groupSizeEncoding — 4 bytes

Written once per repeating group, immediately after the root block.

| Offset | Size | Type   | Field         | Notes |
|-------:|-----:|--------|---------------|-------|
| 0      | 2    | uint16 | `blockLength` | Bytes per entry. |
| 2      | 2    | uint16 | `numInGroup`  | Number of entries. |

**A group with zero entries still carries this header.** Getting that wrong walks
a reader four bytes into the next message, and every message after it is garbage.
`snapshot_empty_group.bin` exists to pin it.

---

## Types

| Name | Encoding | Notes |
|------|----------|-------|
| price | int64 | Fixed point, scaled by 10⁻⁴. `101.2500` is `1012500`. **Signed** — spreads and settlement marks go below zero. |
| quantity | uint32 | |
| orderId | uint64 | |
| symbolId | uint16 | An index into the symbol table in `configs/local.toml`, not a ticker string. A dense integer is what lets the MBP book be a tick-indexed array rather than a map. |
| Side | uint8 | 0 = Bid, 1 = Ask |
| ModifyReason | uint8 | 0 = Reduce (keeps queue priority), 1 = Replace (loses it) |

`ModifyReason` is on the wire rather than inferred from whether the quantity went
down, because the book code depends on the distinction and inferring it is wrong
in the case where a replace happens to reduce.

---

## Messages

| templateId | Message | blockLength | Group |
|-----------:|---------|------------:|-------|
| 1 | `AddOrder` | 24 | — |
| 2 | `ModifyOrder` | 24 | — |
| 3 | `DeleteOrder` | 12 | — |
| 4 | `Trade` | 40 | — |
| 5 | `Snapshot` | 12 | `levels` |
| 6 | `Heartbeat` | 8 | — |
| 7 | `SequenceReset` | 8 | — |

Offsets below are relative to the start of the root block, i.e. 8 bytes past the
start of the message.

### AddOrder — templateId 1, blockLength 24

| Offset | Size | Type | Field |
|-------:|-----:|------|-------|
| 0  | 8 | uint64 | `orderId` |
| 8  | 8 | int64  | `price` |
| 16 | 4 | uint32 | `quantity` |
| 20 | 2 | uint16 | `symbolId` |
| 22 | 1 | Side   | `side` |
| 23 | 1 | uint8  | `reserved` (always 0) |

### ModifyOrder — templateId 2, blockLength 24

| Offset | Size | Type | Field |
|-------:|-----:|------|-------|
| 0  | 8 | uint64 | `orderId` |
| 8  | 8 | int64  | `newPrice` |
| 16 | 4 | uint32 | `newQuantity` |
| 20 | 2 | uint16 | `symbolId` |
| 22 | 1 | Side   | `side` |
| 23 | 1 | ModifyReason | `reason` |

### DeleteOrder — templateId 3, blockLength 12

| Offset | Size | Type | Field |
|-------:|-----:|------|-------|
| 0  | 8 | uint64 | `orderId` |
| 8  | 2 | uint16 | `symbolId` |
| 10 | 1 | Side   | `side` |
| 11 | 1 | uint8  | `reserved` (always 0) |

12 is not a multiple of 8, so the message after a `DeleteOrder` starts on a
4-byte boundary. That is fine and deliberate: every read and write in both codecs
goes through a fixed-size copy, so nothing depends on alignment. Padding
`DeleteOrder` out to 16 would cost 4 bytes on the most frequent message on a real
feed and buy nothing.

### Trade — templateId 4, blockLength 40

| Offset | Size | Type | Field |
|-------:|-----:|------|-------|
| 0  | 8 | uint64 | `tradeId` |
| 8  | 8 | uint64 | `aggressorOrderId` |
| 16 | 8 | uint64 | `restingOrderId` |
| 24 | 8 | int64  | `price` |
| 32 | 4 | uint32 | `quantity` |
| 36 | 2 | uint16 | `symbolId` |
| 38 | 1 | Side   | `aggressorSide` |
| 39 | 1 | uint8  | `reserved` (always 0) |

### Snapshot — templateId 5, blockLength 12, group `levels`

Root block:

| Offset | Size | Type | Field |
|-------:|-----:|------|-------|
| 0  | 8 | uint64 | `lastSequence` — the snapshot reflects every message up to and including this |
| 8  | 2 | uint16 | `symbolId` |
| 10 | 1 | uint8  | `flags` — bit0 = last fragment for this symbol in this cycle |
| 11 | 1 | uint8  | `reserved` (always 0) |

Then a `groupSizeEncoding` with `blockLength` 16, then `numInGroup` entries of:

| Offset | Size | Type | Field |
|-------:|-----:|------|-------|
| 0  | 8 | int64  | `price` |
| 8  | 4 | uint32 | `quantity` |
| 12 | 2 | uint16 | `orderCount` |
| 14 | 1 | Side   | `side` |
| 15 | 1 | uint8  | `reserved` (always 0) |

Total size is `8 + 12 + 4 + 16 × numInGroup`.

### Heartbeat — templateId 6, blockLength 8

| Offset | Size | Type | Field |
|-------:|-----:|------|-------|
| 0 | 8 | uint64 | `lastSequence` — highest sequence published so far on this channel |

Published on an idle channel so a handler can tell quiet from dead. Without it,
a silent arm and a healthy-but-empty arm look identical.

### SequenceReset — templateId 7, blockLength 8

| Offset | Size | Type | Field |
|-------:|-----:|------|-------|
| 0 | 8 | uint64 | `newSequence` — the sequence the next message will carry |

---

## A consequence of aggregating the snapshot

`Snapshot` carries **aggregated price levels** — price, total quantity, order
count — not individual orders. That is enough to rebuild a market-by-price view
and not enough to rebuild a market-by-order one: the aggregate says three orders
total 250 at this price, but not which orders, in what queue order, or with what
ids. Queue position is unrecoverable from it, and queue position is the whole
point of price-time priority.

This is deliberate and it is what real feeds do — but it means **the snapshot
cycle alone cannot recover an MBO book.** So milestone 4 needs both mechanisms
it is scheduled to build, for different reasons:

| Mechanism | Recovers | Cost |
|---|---|---|
| 2-second snapshot cycle | MBP, immediately, from the next cycle | Bounded wait, no request |
| TCP replay service | MBO exactly, by replaying the missed range | A round trip and the publisher's history |

A handler that has lost messages and needs its order book back must replay. One
that only trades off aggregated depth can wait for the next snapshot. The
recovery state machine in milestone 4 has to choose between them rather than
assume one is always available.

`book::apply_message` refuses a `Snapshot` outright today rather than half-applying
one, because a book that is silently missing its queue order is worse than one
that is honestly absent.

---

## Reserved bytes

Every block is a whole number of bytes with no implicit padding: the schema
requires every byte of a block to be named, and `schema/codegen.py` refuses to
generate from a schema where the fields do not exactly fill the declared
`blockLength`.

Reserved bytes are **always written as zero**. Encoders zero the whole block
before writing fields, which is what makes a re-encode byte-identical to a golden
vector — and that in turn is what makes a corruption in a reserved byte
detectable, even though no accessor ever reads one.

---

## How this document is kept honest

There are three independent transcriptions of the tables above:

1. `schema/codegen.py` → `crates/wire/src/generated.rs`
2. `schema/codegen.py` → `cpp/wire/include/wire/generated.hpp`
3. `schema/goldens.py` → explicit `struct.pack` format strings

`goldens.py` deliberately does **not** import `codegen.py`. If the golden vectors
were produced by the same code that decodes them, a wrong offset would produce a
wrong vector that the decoder would happily agree with, and the suite would pass
while the format was broken. Cross-language wire drift fails exactly that way —
silently, as a corrupted field under load rather than as a compile error.

To anchor all three against this document, every message type has one fully
hand-typed hex literal in `schema/goldens.py`, checked against its packer at
generation time. Their values were chosen to be readable in hex, so the annotated
dumps in `schema/golden/*.txt` can be checked against the tables above by eye.

Both test suites then read the same `schema/golden/*.bin` files, assert the same
field values, and re-encode expecting the bytes back:

```bash
make test
```

`scripts/verify-golden-corruption.sh` (run by `make test`) proves the suites
would actually catch a problem: it copies the vectors, flips one bit, and
requires both suites to fail — then requires both to pass again on the intact
vectors, so a suite that failed unconditionally could not sneak through.

The Rust suite goes further: `every_single_byte_flip_is_detected` flips bit 0 of
every byte of every vector in turn and asserts each one is caught.

---

## Not implemented

This encoding is SBE-*flavoured*, not SBE-conformant. Deliberately absent:

- **Variable-length data** (`varData`, `varString`). Nothing on this feed needs it.
- **Nested groups.** One group, at the end of one message.
- **Optional fields and null values.** Every field is always present.
- **The SBE XML type system** beyond fixed primitives and uint8 enums — no
  `set`, no `ref`, no `composite` inside a message body.
- **Big-endian hosts.** The C++ header `static_assert`s little-endian, because a
  byteswap path that is never compiled is a byteswap path that does not work.

A real SBE toolchain would give all of this. It would also mean a code generator
nobody in this repository can debug when the C++ and Rust sides disagree about a
byte — which is the specific failure this milestone exists to prevent.
