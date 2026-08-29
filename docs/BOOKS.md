# The books

Two order-book implementations ship, and both stay.

| | `ReferenceBook` | `MboBook` |
|---|---|---|
| Orders | `HashMap<OrderId, RestingOrder>` | slab of nodes + open-addressed id map |
| Levels | `BTreeMap<Price, VecDeque<OrderId>>` | dense array indexed by tick offset |
| Queue | `VecDeque` per level | intrusive doubly-linked list per level |
| Grows | yes | never — sized once |
| Allocates per message | yes | **no** |
| Role | the oracle | the one that runs |

`--books reference|fast` on the handler picks between them. The default is
`fast`.

Keeping the slow one is not indecision. It is the evidence: a fast book that
agrees with an obviously-correct book over millions of random operations is a far
stronger claim than a fast book that passes its own unit tests, and that
comparison only exists if the slow one is still there to compare against.

---

## Why not a `BTreeMap`

The advertised ~200ns book update is a data-structure decision, not a tuning
pass. A `BTreeMap<Price>` costs a pointer-chasing descent and a cache miss per
level touched and lands somewhere between 400ns and 1µs. No amount of tuning
rescues that; it is a rewrite. So the shape had to be chosen before the code was
written rather than discovered at benchmark time — which is the whole reason this
milestone comes before the benchmark milestone rather than after it.

An array indexed by `(price - anchor) / tick` is one multiplication and one load.

### The window, and why it moves

Prices are unbounded; a book is not. Everything that matters sits within a few
hundred ticks of the touch, so the array covers a **window**: `anchor` is the
price at index 0, and `book_levels` slots of `tick_size` extend from it. Each
symbol gets its own window, anchored on its configured `reference_price` — a
single shared window would be wrong for every symbol but one.

A price outside the window forces a **rebase**: the window slides and the
occupied entries move with it. That is O(capacity) and would be ruinous per
message. It is not per message — a window wide enough for a day's range moves
when the market genuinely walks out of it, a handful of times a session. In the
differential test's 5,000,000 operations, with a mid that random-walks *and*
gaps by ±1,000 ticks every 20,000 operations, it happened 80 times.

**A rebase must never lose a level.** The obvious implementation re-centres the
window on the new price, and it is close to useless: with anything resting near
the middle, re-centring on a price one tick outside throws the far half of the
book out of range, the move gets refused, and the price is rejected — even though
a one-slot slide would have fitted everything. So `TickIndex::rebase_for` is
given the occupied price range and places the window over *the span it has to
cover*, centred in whatever slack is left. It succeeds whenever success is
arithmetically possible.

When it genuinely is not possible — the span is wider than the window — the order
is **refused** with `PriceOutOfRange`. Widening is a resize, not a rebase, and it
allocates. A book that silently forgets its far side is wrong in a way nothing
downstream would notice, which is worse than a message that is visibly rejected.

A price that is not a multiple of the tick is refused too, rather than rounded.
Rounding would rest the order at a price nobody quoted, and the book would then
disagree with the publisher about where it sits.

---

## Why MBO and MBP are one structure

The milestone asks for a market-by-price view and a market-by-order view. They
are built as one store with two accessors, for a reason that is not about saving
code: **MBP cannot be maintained on its own from this feed.**

`DeleteOrder` carries an order id, a symbol and a side — no price. A
price-aggregated book has no way to know which level to decrement. It needs the
per-order detail, which means it needs the MBO store anyway.

So the per-level aggregates live alongside the order lists and are updated in the
same operation. They cannot drift apart, because there is no separate thing to
drift.

---

## The order-id map, and why not tombstones

Order ids are looked up on every modify and every delete. A `HashMap` allocates
on growth and chases a pointer per bucket; this is a flat array of `(id, node)`
with linear probing, sized once at four times the slab so chains stay short.

Deletion uses **backward-shift**, not tombstones. Tombstones are fewer lines and
wrong here: a book processing millions of adds and cancels would fill the table
with them and degrade to a linear scan, on the exact lookup the latency target
depends on. Backward-shift keeps the table as full as it has live entries, and
`the_order_map_survives_heavy_churn_without_degrading` asserts the probe count
stays flat across 4,000 add/delete cycles.

Order ids from this engine are dense and increasing, so their low bits alone
would cluster badly under linear probing. They go through SplitMix64's finaliser
first.

---

## What the differential test compares

`crates/book/tests/differential.rs` runs one operation stream through both books
and requires them to agree on **everything**:

- every return value, including which operations were refused and why
- the aggregated levels, best-first, on both sides
- **the exact queue order within each level**

The last one matters more than it looks. The digest covers aggregated levels, so
a book that ordered orders wrongly within a price would still digest identically —
and would be wrong in precisely the way milestone 4 established that snapshots
exist to prevent. Queue order is compared separately.

The stream includes adds, cancels, in-place reduces, price-changing replaces,
operations naming orders that are not resting, window rebases, and a full
snapshot rebuild in the middle. The default is 300,000 operations so
`cargo test` stays quick; CI runs 5,000,000 in release.

It has already earned its keep. It found a bug in `occupied_extent` that no unit
test would have: the scan restarted its index at 0 for the second side, so the
running maximum came back as the asks' highest slot rather than the highest of
both. A rebase computed from that shifted an occupied bid level off the end of
the window — and the guard meant to refuse such a shift was checking against the
same wrong number, so it agreed. It surfaced as one level count out by one, after
118,000 operations.

---

## The allocation claim

**Consuming a datagram — decoding it, arbitrating it, applying every message to
both views, and writing a digest checkpoint — performs zero heap operations.**

Not "few". Zero allocations, zero deallocations, zero reallocations, counted on
the thread doing the work.

### What is outside it, deliberately

**Startup.** Books, windows, slabs, maps and buffers are sized once, from the
config, before a byte is received. Counting those would either make the claim
unachievable or push the code toward pre-allocating for the worst case at every
scale, which is worse engineering than allocating once at boot.

**Producing the feed.** The engine is a separate process.

**Failure paths.** A panic message or an error being formatted allocates. Those
are not steady state.

Everything else is in — including the **recovery cycle**. Clearing the books,
rebuilding them from a snapshot, and replaying held datagrams is the part most
likely to allocate quietly, because it is the part that looks like it needs to
rebuild something. It is why `MboBook::clear` empties in place and keeps its
memory rather than being a `Default` that hands it back: recovery happens while
the feed is still arriving, and that is not the moment to return several
megabytes and immediately ask for them again.

### How it is checked

Three ways, deliberately overlapping:

1. **`crates/feed-handler/tests/allocation.rs`** — the assertion. One million
   messages through the real arbitrator, recovery buffer, books and digest log,
   including a forced blackout on both arms and the snapshot recovery that
   follows. 125,021 measured scopes, each one required to be clean. This runs in
   CI, so an accidental `Vec` growth breaks the build rather than the demo.
2. **`--verify-allocations`** on the handler — the demonstration. Counts heap
   operations in the receive loop after a 50,000-message warm-up and reports the
   total. It reports; it does not assert. A claim that depends on somebody
   remembering to pass a flag is not a claim.
3. **`scripts/smoke.sh`** — the cross-process check. Runs the same scenario with
   each book and asserts `allocations=0` for the fast one **and a non-zero count
   for the reference one**. That second assertion is not decoration: if the
   counter reported zero for a `BTreeMap` of `VecDeque` it would be broken, and
   the fast book's zero would prove nothing.

The counting allocator is installed in the handler binary unconditionally, not
under the flag. A flag that swapped the allocator would measure a different
binary from the one that ships, which is the same mistake as benchmarking a debug
build. The cost is one thread-local increment per allocation, and in steady state
there are none.

### What it found

The book was allocation-free on its first run of the in-process test. The
two-process smoke test then reported 249 allocations and 507 reallocations over
1,483 passes — `BookDigest::to_fields` built a `String` per checkpoint, on the
receive path. Invisible to a test that had no digest log. The test has one now.

---

## Reading a book without allocating

`OrderBook::for_each_level` takes a callback rather than returning a `Vec` or an
iterator. The `Vec` is the obvious version and allocates. An iterator would be
nicer to use and would need either a named type per implementation or a
`Box<dyn Iterator>` — and the box allocates too, on the same path.

`level_count` exists separately from `for_each_level` for the same reason: the
digest's encoding writes a count before the levels, and producing that count by
walking the array twice would double the cost of every checkpoint.

One consequence worth stating: **the digest ignores a symbol that is present but
empty.** The two implementations differ on when a book springs into existence —
the reference set creates one on first touch, the fast set pre-allocates every
configured symbol at startup — and that is a memory-management decision, not book
state. A digest that could tell them apart would report the engine and the
handler as diverged when both hold nothing.

---

<div align="center">

[shivamsfolio.com](https://www.shivamsfolio.com) · [Case study](https://www.shivamsfolio.com/projects/low-latency-market-data-order-entry) · [All 7 projects](https://www.shivamsfolio.com/projects)

</div>
