# Claims ledger — Low-Latency Market Data & Order Entry Stack

Every number published about this project — in [its README](README.md), on the
[case study page](https://www.shivamsfolio.com/projects/low-latency-market-data-order-entry), or in a CV — must have a row here first.
**A number with no row does not get published.**

| | |
|---|---|
| **Advertised on the portfolio** | `1M+ msg/s · ~100ns decode` |
| **Substantiated so far** | nothing yet |
| **Feasibility assessment** | `yes-with-caveats` |

## Measurements

| Metric | Value | Report | Commit | Date | Host | Caveat |
|--------|-------|--------|--------|------|------|--------|
| Heap operations per message, steady state | **0** allocations, 0 deallocations, 0 reallocations | `crates/feed-handler/tests/allocation.rs`; `scripts/smoke.sh` books scenario | _this commit_ | 2026-08-30 | any — it is a count, not a rate | A **count**, not a performance figure, and the only kind of number this project can honestly publish before it has a benchmark host. Scope is per-message steady state after a 50,000-message warm-up: startup sizing and the publisher are outside it, deliberately. See [docs/BOOKS.md](docs/BOOKS.md#the-allocation-claim). |

## Caveats already locked in

Not measurements — decisions made in the code that any future measurement will
have to be published alongside. Recording them as they are made, rather than
reconstructing them at benchmark time, is the point of this file.

| Since | Decision | Why it constrains the claim |
|-------|----------|------------------------------|
| M1 · 2026-08-29 | **Messages are batched behind one packet header, and a message carries no sequence number of its own** — sequence is `firstSequence + i`. See [docs/WIRE.md](docs/WIRE.md#framing). | `1M+ msg/s` is only reachable with many messages per datagram: a kernel UDP receive path tops out around 300–600K pps per core, so one message per packet would need kernel bypass. Batching is standard on real exchange feeds, but **the batch factor has to be published with any throughput figure** — a reader who assumes one message per packet is reading a much stronger claim than the one this project makes. |
| M1 · 2026-08-29 | `.cargo/config.toml` sets `-C target-cpu=native`. | Every number this project produces will come from a binary compiled for the exact host it ran on. That must be stated, not implied. |
| M5 · 2026-08-30 | **The fast book refuses an order rather than growing**: a price outside its window that cannot be rebased onto, or more resting orders than its slab holds, is an error, not a resize. Sizing comes from `handler.book_levels` and `handler.book_orders`. | Any throughput or latency figure is only valid for a run that stayed inside those bounds. A benchmark that quietly widened the window to finish would be measuring a different program, so the sizing has to be published with the number. |
| M5 · 2026-08-30 | **The counting allocator is compiled into the handler unconditionally**, not switched on by `--verify-allocations`. | Every future measurement includes one thread-local increment per allocation. In steady state there are none, so the cost is zero where it matters — but it is in the binary being measured, and that is the point: a flag that swapped the allocator would benchmark a different program. |
| M1 · 2026-08-29 | CI runs correctness suites only; benchmarks are excluded by design. | Shared GitHub runners are noisy, oversubscribed and virtualised. Numbers from there would be wrong rather than imprecise, and would discredit the honest ones. |

**Where the numbers will have to be measured.** The development host is a 2-core
i3-7020U. This project's own prerequisites call for at least 4 physical cores,
6–8 preferred — the publisher, engine and handler each need one. On 2 cores the
throughput figure is not reachable and the p99s are noise, so no figure will be
published from this machine.

## Rules

1. **Report** points at a file in this repo (`RESULTS.md` or `results/*.json`) holding the raw output and the methodology.
2. **Commit** is the SHA the measurement was taken at. If the code changed, the row is stale — re-measure or delete it.
3. **Host** names the machine: CPU model, physical cores, RAM, OS/kernel, and whether the load generator was co-located.
4. **Caveat** is the one sentence a sceptical reader most needs. Single-host? Loopback? Synthetic load? Say so.
5. Re-verify every row quarterly. A ledger nobody re-checks is decoration.

## Why this file exists

The case study for this project went live before the code did, and it names a specific
figure. That is a promise. This file is where the promise gets kept or corrected —
it is the difference between a portfolio that asserts numbers and one that evidences them.

---

<div align="center">

[shivamsfolio.com](https://www.shivamsfolio.com) · [Case study](https://www.shivamsfolio.com/projects/low-latency-market-data-order-entry) · [All 7 projects](https://www.shivamsfolio.com/projects)

</div>
