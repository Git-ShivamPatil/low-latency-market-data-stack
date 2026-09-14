# Architecture

Five processes, four shared-memory rings and one multicast feed. This document is
the map: what each process owns, what crosses each boundary, and why the
boundaries are where they are. The detail behind each one lives in its own
document, linked from the table at the bottom.

There are two halves here, and the thing worth understanding first is why they
are one system rather than two projects in one repository.

**The market-data half** publishes a binary feed and consumes it: a matching
engine sends book changes on two redundant multicast channels, and a handler
arbitrates the two, detects what it lost and rebuilds order books without
touching the heap. That half stands alone — milestones 1 through 6 built it, and
it was demoable at every step.

**The order-entry half** takes an order in over FIX, checks it against pre-trade
limits, matches it, and reports back. That half also stands alone.

They are the same system because the fill that the order half produces shows up
on the feed that the market-data half is reading. `scripts/order-path-test.sh`
asserts exactly that and nothing weaker: a `feed-handler` process that has never
heard of the order path, that reads only multicast, rebuilds a book identical to
the engine's at every shared checkpoint **after** a client order has crossed the
whole stack and filled. Any two halves can be made to run at the same time; that
assertion is what makes them one thing.

---

## The picture

<img src="architecture.svg" alt="Five processes: a FIX client connects to fix-gateway, which forwards orders to risk-service over the orders ring and receives reports back over the reports ring. Risk forwards accepted orders to matching-engine over the accepted ring and receives execution reports back over the execs ring. The engine publishes a batched binary feed on two redundant multicast channels, A and B. The feed-handler arbitrates the two and rebuilds the books; the replay-service stores the same stream and serves exact sequence ranges back to the handler over TCP." width="100%">

---

## The five nodes

The case study page draws five boxes. Each one is a real operating-system
process with its own binary, and each is startable on its own.

| Node | Binary | Language | What it owns | Detail |
|---|---|---|---|---|
| FIX 4.4 gateway | `fix-gateway` | C++20 | The session layer and the durable sequence numbers; the order write-ahead log | [PROTOCOL.md](PROTOCOL.md) |
| Risk service | `risk-service` | C++20 | Pre-trade limits and per-symbol position; creates all four rings | [ORDER-PATH.md](ORDER-PATH.md) |
| Matching engine | `matching-engine` | Rust | Price-time priority matching; publishes the feed and the snapshot cycle | [WIRE.md](WIRE.md) |
| Snapshot + replay | `replay-service` | Rust | The stored stream, and answering for an exact sequence range | [RECOVERY.md](RECOVERY.md) |
| Rust feed handler | `feed-handler` | Rust | A/B arbitration, gap detection, and both book views | [BOOKS.md](BOOKS.md) |

Two of the five are C++ and three are Rust, and that split is not decoration.
The allocation claim has to hold in both languages, which means it has to be
proved twice by two different mechanisms — a counting `#[global_allocator]` on
the Rust side, a replaceable `operator new`/`operator delete` on the C++ side.
Proving it once and asserting the other would have been the easy version.

---

## Two wires, one schema

Everything that crosses a process boundary in this system is a fixed-layout
little-endian block whose offsets were generated, not written. `schema/market-data.xml`
is the single source of truth, and `schema/codegen.py` emits the Rust codec, the
C++ header and the golden byte vectors from it. Nothing in `cpp/risk` or
`crates/matching-engine` computes a field offset by hand.

But there are two different wires here, and they are framed differently:

| | The feed | The order path |
|---|---|---|
| `scope` in the schema | `feed` (the default) | `orderPath` |
| Carried by | A/B UDP multicast | four SPSC shared-memory rings |
| Framing | `packetHeader`, then `messageHeader` per message | `messageHeader` then the block, one message per slot |
| Batching | 32 messages to a datagram | one per slot; a ring slot has no packet header |

**The generator enforces the split rather than documenting it.** A `feed` message
gets a `PacketWriter::append_*` method and an `orderPath` message does not, so the
feed publisher has no way to put an order request on multicast even by mistake;
and each wire's decoder rejects the other's template ids as unknown rather than
decoding them into the wrong shape.

It is one schema rather than two because `Side` has to mean the same byte on both
wires, and the cheapest way to guarantee that is to define it once.

The order-path messages are few and deliberately so:

| Id | Message | Direction | Block |
|---:|---|---|---:|
| 8 | `NewOrder` | gateway → risk → engine | 24 B |
| 9 | `CancelOrder` | gateway → risk → engine | 24 B |
| 10 | `ExecReport` | engine → risk → gateway, and risk → gateway | 48 B |
| 11 | `ReconcileRequest` | gateway → risk → engine | 16 B |

One `ExecReport` covers acknowledgement, partial fill, fill, cancel, reject and
both reconciliation forms, discriminated by an `ExecType`. That is the same
choice FIX itself makes with tag 150, for the same reason: every one of them is
"here is what happened to your order", and they all need the same identifiers.

---

## The order path, end to end

An order's whole life, in the order it happens:

1. A FIX client sends `NewOrderSingle` (`35=D`) to `fix-gateway` over TCP.
2. The gateway validates the **session** layer — sequence number, checksum, body
   length — and persists its sequence state before the reply goes out. It writes
   the order to its **write-ahead log**, then puts a `NewOrder` on the `orders`
   ring.
3. `risk-service` reads it and applies the pre-trade limits: max order quantity,
   max notional, per-symbol position, open-order count, price collar. **On a
   breach it never forwards the order** — it puts a rejecting `ExecReport` on the
   `reports` ring and the gateway turns that into a FIX reject with the reason.
4. On a pass it puts the order on the `accepted` ring.
5. `matching-engine` reads it and matches it on price-time priority against the
   resting book, exactly as it matches any other order.
6. Fills come back as `ExecReport`s on the `execs` ring — for the aggressor *and*
   for the resting side, which is a thing that had to be fixed rather than
   assumed, because the passive side of a fill is not in the aggressor's stack
   frame.
7. `risk-service` reads each report, **updates the position it enforces against**,
   and forwards it on the `reports` ring.
8. The gateway emits a FIX `ExecutionReport` (`35=8`) to the client.
9. Independently and at the same time, the book change produced by step 5 goes
   out on the A and B multicast channels and lands in `feed-handler`'s books.

**Why risk sits in both directions.** It would be shorter to let the engine report
straight back to the gateway. But a position limit is a statement about fills, so
risk has to see every fill anyway — and routing the reports through it means
position state is updated by the same thread that enforces the limit, in order,
with no synchronisation between the two. A design where risk learns about fills
on a side channel has a window in which it is enforcing a limit against a
position it has not finished updating.

### The rings

Four of them, every one strictly single-producer single-consumer. That is not a
stylistic preference: an SPSC ring is the only lock-free queue whose correctness
argument fits in a paragraph, and the moment a second producer appears the
argument is gone.

Each ring is a file, `mmap`'d by exactly two processes, laid out by the schema so
the Rust and C++ views cannot drift:

```
  header    magic, version, slot size, capacity     64 B, read-only after creation
  write     the producer's index                    64 B, its own cache line
  read      the consumer's index                    64 B, its own cache line
  slots     capacity × slot size
```

The two indices get their own cache lines because they are written by different
cores; sharing a line makes every producer write invalidate the consumer's copy
and the queue gets several times slower for no visible reason. Capacity is a
power of two so index-to-slot is a mask rather than a division, and the indices
are free-running 64-bit counters that are never wrapped — `write - read` is the
depth, with no ambiguity between full and empty.

**One process creates; the others only open.** `risk-service` creates all four,
which is why it starts first and why `docker-compose.yml` waits on its
healthcheck rather than on "the process is up". A second creator would zero the
indices under whoever was already attached, silently, because a freshly created
ring is indistinguishable from an empty one.

Two consequences that only became visible when the stack was first run in
containers:

- **The creator unlinks before it creates.** A ring is plumbing, not durable
  state — nothing in it means anything to a process that was not attached when it
  was written. On a named volume the four files outlive `docker compose down`, so
  without the unlink a healthcheck asking "does `reports.ring` exist?" answers yes
  the moment the volume is mounted, and the gateway and engine attach to the
  previous run's rings.
- **The creator must not restart alone.** `risk` is the one service in
  `docker-compose.yml` with `restart: "no"`. Bringing it back while the other two
  hold the old rings leaves two halves of a stack that both look healthy and
  cannot talk to each other. The lifetime of the rings is the lifetime of the
  stack, and compose cannot express "restart these three together".

---

## The market-data path

The engine publishes every book change as a message on **both** channels, batched
32 to a datagram. The handler reads both, takes whichever datagram arrives first,
discards the duplicate, and keeps per-arm counters so "late" and "lost" stay
distinguishable.

When a sequence goes missing on both arms it has two ways back, and they are kept
deliberately separate:

- **The snapshot cycle.** The engine republishes the whole book every two
  seconds. `Snapshot` carries **orders in queue order** rather than aggregated
  levels, because an aggregate cannot restore price-time priority and a book that
  recovered the right quantities at the wrong queue positions is a book that will
  match wrongly.
- **The replay service.** A TCP request for an exact range, answered with exactly
  that range.

These are never raced against each other in a test. Under load the snapshot
legitimately wins, and a test that asserts replay won is a test that passes by
accident — which is what happened here for two milestones. Each scenario now
exercises one path only.

### The books

`MboBook` maintains **both** the order-by-order and the aggregated-price views in
one structure: a slab for orders, an open-addressed order-id map with
backward-shift deletion, and intrusive per-level FIFO lists over a dense
tick-indexed array with a rebasing anchor.

They are one structure and not two because `DeleteOrder` on this feed carries no
price. An MBP book cannot be driven from this feed alone — it has to find the
order first, which means it needs the MBO map regardless.

A `BTreeMap`-based reference book stays in the tree as an oracle and is
differentially tested against the fast book over five million random operations,
checking every return value, every aggregated level and the exact queue order
within each level. It also serves as the **control** for the allocation claim:
the same run with `--books reference` reports a large non-zero allocation count,
which is what proves the counter can fail.

---

## State that outlives a process

Three things on disk, and each one exists because a specific failure had to be
survivable:

| File | Owner | Why |
|---|---|---|
| `gateway.seq` | `fix-gateway` | FIX sequence numbers, `fsync`'d **before** the message they describe reaches the socket. Two slots on separate sectors with a generation and a checksum, so a torn write damages one and leaves the other intact one generation behind. |
| `orders.log` | `fix-gateway` | The order write-ahead log the gateway rebuilds open order state from after a hard kill. |
| `*.ring` | `risk-service` | The four rings. They are files so both processes can `mmap` the same bytes. |

Coming back one message behind is recoverable through an ordinary FIX resend.
Coming back with a sequence number that was never durable is not — it reads as a
reversal to the counterparty and the session dies. That asymmetry is why
`claim_outbound` returns nothing when the sync failed, and the caller must not
send.

**After a restart the gateway reconciles and does not repair.** It rebuilds from
its log, asks the engine what it is still holding, and compares. Any divergence
is reported explicitly. The report is the deliverable — a system that silently
made the two sides agree would have destroyed the evidence that they disagreed.

In `docker-compose.yml` all of this lives on a **named** volume, because a durable
store on an anonymous volume is not durable.

---

## What this architecture does not do

Stated here so a reader does not have to infer it from what is missing:

- **No kernel bypass.** No DPDK, no `io_uring` on the feed path, no user-space
  networking. Every number is an ordinary-sockets number.
- **It has never run across a NIC.** Single host, loopback or a Docker bridge.
  Multicast crossing a bridge is exercised deliberately, but a bridge is not a
  network.
- **The throughput figure depends on batching.** 32 messages to a datagram. At
  one message per datagram the kernel UDP path is the ceiling and the figure is
  unreachable. [bench/REPORT.md](../bench/REPORT.md) says so in its first
  paragraph.
- **The FIX cross-check judges the session layer only.** QuickFIX runs without a
  data dictionary, because Debian's package strips the FIX XML. It is not a
  conformance claim about application message content.
- **No tail-latency claim.** The in-path p99.9 is reported and explicitly not
  claimed; on a shared CI runner it measures someone else's scheduler.

---

## Where to read next

| Question | Document |
|---|---|
| Byte layout, framing, why batching | [WIRE.md](WIRE.md) |
| A/B arbitration, the loss models, how recovery decides | [RECOVERY.md](RECOVERY.md) |
| The two books, the price window, the allocation claim | [BOOKS.md](BOOKS.md) |
| What FIX does, what it refuses, what QuickFIX judged | [PROTOCOL.md](PROTOCOL.md) |
| The rings, the risk limits, restart reconciliation | [ORDER-PATH.md](ORDER-PATH.md) |
| How to run any of it, and the gotchas | [RUNNING.md](RUNNING.md) |
| What has actually been measured, with caveats | [CLAIMS.md](../CLAIMS.md) |
| Every advertised claim mapped to its evidence | [CLAIMS-MAP.md](CLAIMS-MAP.md) |

---

<div align="center">

[shivamsfolio.com](https://www.shivamsfolio.com) · [Case study](https://www.shivamsfolio.com/projects/low-latency-market-data-order-entry) · [All 7 projects](https://www.shivamsfolio.com/projects)

</div>
