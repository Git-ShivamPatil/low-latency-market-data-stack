# Benchmark report — 2026-08-30

## Read this first

**Single host.** The publisher and the consumer ran on the same machine over
loopback. There is no NIC, no switch, and no second host. A trading-systems
reader seeing "consume an exchange feed" and "2.78M msg/s" together will
reasonably hear a NIC-to-NIC claim, and that is **not** what this measures.

**Batched, 32 messages to a datagram.** Not an implementation detail. The kernel
UDP receive path tops out somewhere around 300–600K packets per second per core,
so a million messages a second is only reachable by putting many messages in each
packet. Batching is standard on real exchange feeds — and a reader who assumes
one message per packet is reading a far stronger claim than the one being made.
**The throughput figure below is meaningless without this number beside it.**

**A shared, ephemeral, virtualised CI runner.** Four physical ARM cores on a
Microsoft hypervisor. The scheduler belongs to a host this process cannot see, so
the tail latencies are partly a measurement of that host. What makes the figures
usable anyway is that three independent runs agreed within 0.7%.

**Compiled for this host** — `.cargo/config.toml` sets `-C target-cpu=native`.

---

## Host

From `cargo run --release -p bench --bin hostcheck -- --fields`.

| | |
|---|---|
| CPU | `aarch64 implementer 0x41 part 0xd49` — ARM Neoverse N2 |
| Cores | **4 physical / 4 logical** (server ARM has no SMT, so these are real cores) |
| Kernel | `6.17.0-1022-azure`, Ubuntu 24.04 |
| Bare metal or VM | **VM** — a Microsoft hypervisor (GitHub Actions `ubuntu-24.04-arm`) |
| Governor | unknown (not readable on this runner) |
| Turbo | unknown |
| Invariant counter | **yes** — `cntvct_el0` at 1000.0 MHz, 1ns granularity |
| Timer overhead | 8 ticks, calibration spread 0.00007 |
| RUSTFLAGS | `-C target-cpu=native` |
| Cores pinned | engine → core 2, handler → core 3 |
| Commit | `3c1bc88` |
| Run | GitHub Actions 33281058291 |

**Host gate:** `PUBLISHABLE`, with three caveats it named itself:

1. 4 physical cores, below the 6 this project prefers.
2. Running under a Microsoft hypervisor.
3. The CPU governor could not be read, so frequency scaling during the run is
   unknown.

---

## Workload

| | |
|---|---|
| Batch factor | **32** messages per datagram (measured 31.62 in the in-path run) |
| Message mix | ~55% `AddOrder`, ~25% `DeleteOrder`, ~12% `ModifyOrder`, ~8% `Trade` |
| Symbols | 1 |
| Price levels in the corpus | 64 |
| Book depth during the microbenchmarks | ~1,255 resting orders |
| Book depth at the end of the in-path run | 305 resting orders |
| Corpus seed | `0xBE7C0DE5` — the stream replays exactly from it |
| Throughput run duration | 60s × 3 |
| Book | `--books fast` |

**The book is shallow, and that limits what these numbers mean.** 64 price levels
and roughly a thousand orders is a small book: a `BTreeMap` over 64 keys is one
or two levels deep and entirely cache-resident. See the book results below, where
this matters a great deal.

---

## What "decode" means here

**`decode_fields` — included:** parsing the packet header, walking each message
header, extracting and validating every field of every message including enum
values that can fail.

**`decode_fields` — excluded:** the `recvmmsg` that delivered the datagram; A/B
arbitration; any book update; any digest.

**`decode_and_apply` — included:** all of the above plus applying each message to
the book. This is the per-message consumer cost.

The syscall is in neither. It is amortised across the batch and belongs in the
throughput figure — which is exactly why the batch factor has to be published
next to both.

Nothing here was optimised away: every workload returns a checksum folded from
every field it touched, and `bench::tests::decoding_actually_reads_every_field`
flips one bit of a payload and requires the answer to change.

---

## Results

### Throughput — the headline

Measured **receiver-side**, as `(final sequence − first sequence) / elapsed`.

| Run | msg/s | Messages | Gaps | Apply errors | State at exit |
|---|---|---|---|---|---|
| 1 | **2,766,932** | 207,508,426 | 0 | 0 | `LIVE` |
| 2 | **2,785,966** | 208,933,709 | 0 | 0 | `LIVE` |
| 3 | **2,782,874** | 208,700,386 | 0 | 0 | `LIVE` |

**Spread 0.7%**, against the 10% this project requires. Zero arbitrated gaps and
zero apply errors in all three.

Against the advertised `1M+ msg/s`: **met, at 2.78×**.

### Decode

Criterion, 32 messages per datagram.

| Measurement | Per datagram | **Per message** |
|---|---|---|
| `decode_fields`, streaming over 256 datagrams | 262.45 ns | **8.20 ns** |
| `decode_fields`, one datagram hot in L1 | 157.77 ns | **4.93 ns** |

Against the advertised `~100ns decode`: **met, with a wide margin** — but read the
definition above before quoting it. This is field extraction, not a pipeline. The
streaming figure is the honest one; the hot-L1 figure is the optimistic one and is
labelled as such because a consumer taking datagrams off a socket never sees the
same bytes twice.

### Book update

Criterion, 16 datagrams × 32 messages = 512 messages per iteration, on a book
warmed to ~1,255 resting orders. These include decode.

| Book | Per iteration | **Per message** |
|---|---|---|
| Fast — tick-indexed array, slab, open-addressed map | 19.94 µs | **38.9 ns** |
| Reference — `BTreeMap` of `VecDeque` | 34.23 µs | **66.9 ns** |

Against the advertised `~200ns book update`: **met**.

**But the interesting result here is the one that contradicts this project's own
prediction.** The README says a `HashMap<OrderId>` over a `BTreeMap<Price>` lands
at 400ns–1µs. It measured at 66.9 ns/message — six to fifteen times better than
predicted, and comfortably inside the 200ns target on its own.

The reason is the workload, not the prediction being silly: **64 price levels is
not a book that stresses a `BTreeMap`.** The tree is one or two levels deep and
fits in L1, so the pointer-chasing descent the prediction is about barely
happens. The fast book is **1.72× faster** here, not the order of magnitude the
design argument anticipated.

That does not make the fast book unjustified — it makes this workload the wrong
one to justify it with. A book with hundreds of levels and tens of thousands of
orders is where the two diverge, and this corpus does not build one. **Anyone
quoting the 1.72× should quote the book depth with it.**

Single operations, for attributing a regression rather than for quoting:

| Operation | Time |
|---|---|
| `add` on the fast book | 83.6 ns |
| `delete` on the fast book | 77.7 ns |

These are higher than the 38.9 ns/message above because each Criterion iteration
carries its own setup and a symbol lookup that the batched path amortises.

### In-path latency — the upper bound

2,000,000 messages through the real receive loop, timed with `cntvct_el0`.

| | Per message | Per datagram |
|---|---|---|
| min | 24 ns | 40 ns |
| **median** | **93 ns** | 2,977 ns |
| p90 | 116 ns | 3,697 ns |
| **p99** | **1,532 ns** | 48,767 ns |
| **p99.9** | **1,827 ns** | 55,071 ns |
| max | 14,893 ns | 65,730 ns |
| mean | 138.9 ns | 4,348.6 ns |

63,245 datagrams, 0 overflow, 0 gaps, ended `LIVE`.

**The two methods bracket the answer, as designed.** Criterion says 39.9 ns per
message for decode-and-apply; the in-path histogram says 93 ns. Criterion
amortises the clock and does not serialise, so it is the **lower** bound. The
in-path measurement puts an `isb; mrs` pair around every datagram, which
serialises an out-of-order window the untimed path exploits, so it is the
**upper** bound. The timer's own 8-tick overhead is subtracted; the serialisation
cannot be. Neither number alone is the answer; the answer is between them.

**The p99 and p99.9 are not hidden and are not good.** 1.5–1.8 µs per message at
the tail, against a 93 ns median, is sixteen times the median. That is the shared
hypervisor showing through — it is the caveat the host gate named, and it is why
this report cannot claim a tail-latency result. A p99.9 from a runner whose
scheduler belongs to someone else measures that scheduler.

### Allocation

| | |
|---|---|
| Heap operations per message, steady state | **0** allocations, 0 deallocations, 0 reallocations |

Over the 2,000,000-message in-path run, on aarch64. The claim already
substantiated on x86 in milestone 5 holds on ARM.

---

## What this does not show

- **No NIC.** Loopback only. See the first paragraph.
- **A shallow book.** 64 price levels. The reference-book comparison above is
  much narrower than it would be on a realistic book, and this report says so
  rather than quoting the flattering ratio.
- **No tail-latency claim.** The p99.9 is dominated by hypervisor scheduling.
- **Synthetic order flow.** A seeded generator, not a market data replay.
- **One consumer.** Nothing here measures several handlers on one group.
- **One architecture.** These are ARM Neoverse N2 numbers. The x86 figures from
  the development laptop are not comparable and are not published.
- **No independent cross-check.** The numbers describe this code measured by its
  own harness.

---

## Reproducing it

```bash
gh workflow run Benchmark --ref main
```

`workflow_dispatch` only, on `ubuntu-24.04-arm`. The host gate runs first; on a
host it refuses, the benchmarks still run but the output lands in
`results/bench/NOT-PUBLISHABLE.md` and this file is not touched.

Locally:

```bash
cargo run --release -p bench --bin hostcheck
scripts/bench.sh all
```

---

<div align="center">

[shivamsfolio.com](https://www.shivamsfolio.com) · [Case study](https://www.shivamsfolio.com/projects/low-latency-market-data-order-entry) · [All 7 projects](https://www.shivamsfolio.com/projects)

</div>
