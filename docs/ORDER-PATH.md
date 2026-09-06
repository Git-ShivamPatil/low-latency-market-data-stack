# The order path — gateway to risk to engine, and back

> **Status: milestone 8, complete.** Written *before* the code, for the same
> reason [docs/PROTOCOL.md](PROTOCOL.md) was: the decisions below are the
> milestone. Discovering them while implementing produces a system whose shape
> is an accident of the order things got written in.
>
> Two things this document did not anticipate are recorded at the bottom, under
> [What building it changed](#what-building-it-changed). They are there rather
> than edited into the text above, because a design document that quietly
> rewrites itself to match the code stops being a check on it.

## What has to be true at the end

From the milestone's own verification step, unedited:

> Limit breaches produce the right FIX reject with the right reason and never
> reach the engine. The C++ allocation counter shows zero allocations across 1M
> risk decisions. A hard-kill test with orders in flight is followed by a restart
> that reconciles the gateway's order state against the engine's and reports any
> divergence explicitly — no silent reconciliation. A single end-to-end test
> observes one order become a fill, an execution report, and a feed update on the
> handler's book.

Four claims. Each one decides something below.

---

## The shape

```
   FIX client
       │  NewOrderSingle (35=D) / OrderCancelRequest (35=F)
       ▼
  fix-gateway ──────── orders ────────▶ risk-service ─────── accepted ──────▶ matching-engine
   (C++)      ◀─────── reports ─────── (C++)         ◀─────── execs ──────── (Rust)
       │                                                            │
       │  ExecutionReport (35=8)                                    │  AddOrder / Trade / …
       ▼                                                            ▼
   FIX client                                              A and B multicast ──▶ feed-handler
```

Four rings, every one of them strictly single-producer single-consumer. That is
not a stylistic preference — an SPSC ring is the only lock-free queue whose
correctness argument fits in a paragraph, and the moment a second producer
appears the argument is gone.

**Risk sits in the middle of both directions, and that is the point.** It would
be shorter to let the engine report straight back to the gateway. But a position
limit is a statement about fills, so risk has to see every fill anyway; routing
the reports through it means position state is updated by the same thread that
enforces the limit, in order, with no synchronisation between them. A design
where risk learns about fills on a side channel has a window in which it is
enforcing a limit against a position it has not finished updating.

---

## Where the layouts come from

**The schema.** `schema/market-data.xml` gains four messages and two enums, and
`schema/codegen.py` emits them into both languages exactly as it does the feed
messages. Nothing in `cpp/risk` or `crates/matching-engine` writes a field offset.

This is one schema describing **two wires**, so each message carries a `scope`:

| `scope` | Which wire | Framing |
|---|---|---|
| `feed` (the default) | A/B multicast | `packetHeader` then `messageHeader` per message, batched |
| `orderPath` | the shared-memory rings | `messageHeader` then the block, one message per ring slot |

The generator enforces the split rather than documenting it: `PacketWriter` gets
an `append_*` method **only** for `feed` messages, so the feed publisher has no
way to put an order request on multicast, and the packet reader rejects an
`orderPath` template id as unknown rather than decoding it.

One schema rather than two, because `Side` has to mean the same byte on both
wires and the cheapest way to guarantee that is for it to be defined once. Both
wires then get the same `check_block` treatment — every byte named, padding
included, natural alignment inside the block — and the same three independent
transcriptions before a test can pass.

### The messages

| Id | Message | Direction | Block |
|---:|---|---|---:|
| 8 | `NewOrder` | gateway → risk → engine | 24 |
| 9 | `CancelOrder` | gateway → risk → engine | 24 |
| 10 | `ExecReport` | engine → risk → gateway, and risk → gateway | 48 |
| 11 | `ReconcileRequest` | gateway → risk → engine | 16 |

`ExecReport` carries an `ExecType` that covers acknowledgement, partial fill,
fill, cancel, reject, and the two reconciliation forms — a status line and the
end-of-status marker. Reusing one message for all of them is deliberate: every
one of them is "here is what happened to your order", they all need the same
identifiers, and FIX itself makes the same choice with `ExecType` (150).

---

## The ring

A file, `mmap`'d by both processes, with the header laid out by the schema so
the Rust and C++ views cannot drift.

```
  header    magic, version, slot size, capacity            64 B, read-only after creation
  write     the producer's index                           64 B, its own cache line
  read      the consumer's index                           64 B, its own cache line
  slots     capacity × slot size
```

Separate cache lines for the two indices because they are written by different
cores. Sharing a line makes every producer write invalidate the consumer's copy
and vice versa; the queue still works and gets several times slower for no
visible reason.

**Capacity is a power of two** so the index-to-slot map is a mask rather than a
division, and indices are free-running 64-bit counters that are never wrapped —
`write - read` is the depth, with no ambiguity between full and empty and no
need for a spare slot.

**Publication order.** The producer fills the slot, then stores `write` with
release ordering. The consumer loads `write` with acquire ordering, then reads
the slot. That pairing is what makes the slot's bytes visible; it is also the
only synchronisation in the queue.

**A full ring is not an error to be swallowed.** If the gateway cannot enqueue an
order, the order is rejected back to the client with a reason that says so. The
alternative — block, or drop and count — either stalls the FIX session or loses
an order silently, and losing an order silently is the worst outcome available.

---

## Risk: what it checks and what it costs

Five limits, in this order, cheapest first:

| Limit | Rejects when |
|---|---|
| Unknown symbol | the symbol id is not configured |
| Max order quantity | `quantity > max_order_quantity` |
| Max notional | `price × quantity > max_notional` |
| Price collar | the price is more than a configured fraction away from the last traded price |
| Open order count | the client already has `max_open_orders` live |
| Position limit | the fill, if it fully filled, would take the position outside `±max_position` |

**Worst case, not likely case, for the position limit.** The check is against
the position that would result if this order **and every other live order on the
same side** filled completely. Checking only this order lets a caller walk past
the limit with ten small orders, none of them individually over it; checking only
the current position lets them walk past it with one order that has not filled
yet. The two sides are counted separately rather than netted, because netting
lets a caller sit at the limit on both sides and end up over it when one side
fills and the other does not.

### The zero-allocation claim, in C++ this time

The path from "a `NewOrder` came off the ring" to "a verdict was written" must
not touch the heap. Concretely: no `std::string`, no `std::map`, no
`std::function`, no `std::vector` growth. Symbols are a fixed array indexed by
symbol id; open orders live in a preallocated slab with an open-addressed index,
the same shape `crates/book` uses and for the same reason.

Proved the same way M5 proved it in Rust: a counting `operator new` /
`operator delete` compiled into the binary **unconditionally**, and a test that
runs 1,000,000 decisions and asserts the count is exactly zero. A build flag that
swapped the allocator would be measuring a different program.

There will be a control, too. A test that deliberately allocates and sees the
counter move, because a counter that reports zero because it is not wired up
reports zero very convincingly.

---

## Order state, the WAL, and what survives a kill

The gateway owns order state; risk and the engine hold their own views of it,
and after a crash all three can disagree.

**The log.** Append-only, one record per state transition, each record
length-prefixed and checksummed. A record is written and `fsync`'d **before** the
message it describes leaves for the ring — the same ordering rule and the same
reason as `SeqStore` in milestone 7: coming back believing you sent less than you
did is unrecoverable, and coming back believing you sent more is a resend away
from correct.

**The fsync policy is stated, not implied.** Every state transition, synchronously.
That is the expensive choice and it is the correct one for order state; a
throughput number measured with a laxer policy would be measuring a different
guarantee. It is written down here so no future benchmark quietly relaxes it.

**Recovery.** Replay the log, rebuild the open orders, discard any trailing
partial record — a torn tail is the normal outcome of `SIGKILL` during a write,
not corruption.

### Reconciliation reports, it does not repair

After a restart the gateway sends a `ReconcileRequest`. The engine answers with
one `ExecReport` per open order it holds for that gateway, then a
`StatusComplete` carrying the count. The gateway compares that set against what
its log rebuilt and classifies every difference:

| | Meaning |
|---|---|
| **In both, agreeing** | nothing to do |
| **In both, different leaves quantity** | a fill happened that the gateway's log did not record |
| **Gateway has it, engine does not** | the order never arrived, or was filled or cancelled while the gateway was dead |
| **Engine has it, gateway does not** | the gateway lost a record, which is the serious one |

**Every divergence is reported and none is silently repaired.** A gateway that
quietly adopts the engine's view is a gateway that turns a bug into a shrug —
and the divergence report is the artifact that says whether the durability
design works. This is a deliverable, not a debug print.

---

## Deliberately out of scope

| Not implemented | Why |
|---|---|
| Order modification (`OrderCancelReplaceRequest`, 35=G) | New and cancel are enough to demonstrate the path; amend adds FIX bookkeeping and no new concurrency or durability problem. |
| Multiple gateways against one risk service | Every ring is SPSC by design. More gateways means more ring pairs and a fan-in, which is an architecture question rather than a protocol one. |
| Risk limits that change while running | Configuration is read at startup. Live limit updates need a control plane and a story about what happens to orders in flight. |
| Market orders, stop orders, time-in-force other than day | The engine is a limit-order book. An order type it cannot represent would be rejected somewhere arbitrary. |
| Netting positions across symbols | Position limits are per symbol. Cross-symbol risk is a different product. |

---

## The end-to-end test

One order, all the way through, asserted at every place it should appear:

1. A FIX client sends `NewOrderSingle`; the gateway logs it and enqueues it.
2. Risk passes it and forwards it.
3. The engine matches it against resting liquidity and publishes the resulting
   `Trade` and book change to the A/B feed.
4. The `ExecReport` comes back through risk to the gateway, which sends
   `ExecutionReport` (35=8) over FIX.
5. The `feed-handler`, a separate process reading multicast, shows the book change.

The point of asserting on step 5 is that it is the only step that proves the two
halves of this system are the same system. Everything before it could be
satisfied by an order path that talks to a matching engine nobody is watching.

---

## What building it changed

Two things the design above did not anticipate. They are recorded here rather
than edited into the text, because a design document that quietly rewrites itself
to match the code stops being a check on it.

### Fills against a *resting* order had no path back

The design says an `ExecReport` covers "acknowledgement, partial fill, fill,
cancel, reject" and stops there, as though every fill belongs to the order that
caused it. It does not. The aggressor learns about its fills from the call it
made; the passive side made its call minutes earlier and is not in that stack
frame at all. So a client whose order rested and was then hit heard **nothing**,
and would have discovered it from a reconciliation days later.

The engine now takes registrations for orders somebody wants to be told about.
The list is sorted, so it is a binary search per fill and costs nothing at all
when no order path is attached — which is the ordinary case for this engine.

Found by the reconciliation scenario, which asked the engine for an order it had
already filled and got an honest "I am not holding that" back. The report was
working; what it was reporting was a real hole.

### The flow generator was cancelling client orders

A resting client order joined `symbols[].live`, which is the pool the generator
draws its cancels and amends from. The generator stands in for other market
participants, and other participants do not get to cancel your order. From the
outside it looked exactly like the exchange losing an order.

### And one thing the design got right but described badly

Reconciliation answers with the orders that arrived over the order path, each
carrying the client's own id back — not with the whole book. Answering with the
whole book is what the first version did, and it was correct and useless: three
hundred lines about orders the gateway never placed would bury the one that
diverged. A report nobody can read is not a report.

---

<div align="center">

[shivamsfolio.com](https://www.shivamsfolio.com) · [Case study](https://www.shivamsfolio.com/projects/low-latency-market-data-order-entry) · [All 7 projects](https://www.shivamsfolio.com/projects)

</div>
