# Benchmark report — TEMPLATE, NOT A RESULT

> **This file contains no measurements.** It is the shape a report has to take
> before a number from this project may be quoted anywhere. Every `<...>` below
> is a blank that a real run fills in. Until one does, the honest answer to "how
> fast is it?" is [CLAIMS.md](../CLAIMS.md), which says nothing has been measured.

---

## Read this first

**Single host.** The publisher and the consumer run on the same machine, over
loopback or a container bridge. There is no NIC, no switch, and no second host.
A trading-systems reader seeing "consume an exchange feed" and "1M+ msg/s"
together will reasonably hear a NIC-to-NIC claim, and that is not what this
measures. Every figure below is a single-host figure.

**Batched.** Messages are carried `<BATCH>` to a datagram. This is not an
implementation detail: the kernel UDP receive path tops out somewhere around
300–600K packets per second per core, so a million messages a second is only
reachable by putting many messages in each packet. Batching is standard on real
exchange feeds — and a reader who assumes one message per packet is reading a
much stronger claim than the one being made. **No throughput figure from this
project may be quoted without the batch factor beside it.**

**Compiled for this host.** `.cargo/config.toml` sets `-C target-cpu=native`.
Every number here comes from a binary built for the exact machine it ran on.

---

## Host

Filled from `cargo run --release -p bench --bin hostcheck -- --fields`.

| | |
|---|---|
| CPU | `<model, physical cores / logical cores>` |
| Kernel | `<uname -a>` |
| Bare metal or VM | `<bare metal / WSL2 / cloud VM and which>` |
| Governor | `<performance / ondemand / ...>` |
| Turbo | `<on / off>` |
| `constant_tsc` / `nonstop_tsc` | `<true/false> / <true/false>` |
| Cores pinned | `<engine core, handler core, or "not pinned">` |
| RUSTFLAGS | `<as built; the default is -C target-cpu=native>` |
| Toolchain | `<rustc version>` |
| Commit | `<SHA>` |
| Date | `<YYYY-MM-DD>` |

**The host gate said:** `<paste the verdict, including every caveat it listed>`

A report whose gate said REFUSED is not a report. It is a record of an
experiment on unsuitable hardware, and it belongs in `results/`, not here.

---

## Workload

| | |
|---|---|
| Batch factor | `<BATCH>` messages per datagram |
| Message mix | `<% AddOrder / % DeleteOrder / % ModifyOrder / % Trade>` |
| Symbols | `<n>` |
| Book depth during measurement | `<resting orders>` |
| Corpus seed | `<seed>` — the stream replays exactly from it |
| Run duration | `<seconds>` |
| Repetitions | `<n>`, agreeing within `<x>%` |

The mix matters. A decode benchmark over one message type measures one branch of
a match statement and predicts perfectly, which is not what a consumer does.

The depth matters more. An empty book has one price level and no queue to walk:
it is the fastest either structure will ever be and nothing like service.

---

## What "decode" means here

The advertised `~100ns decode` is meaningless without this definition, because as
pure field extraction it is 10–30ns and as a full pipeline it is genuinely tight.
Publishing the number without the definition would be publishing nothing.

**`decode_fields` — included:** parsing the packet header; walking each message
header; extracting and validating every field of every message, including enum
values that can fail.

**`decode_fields` — excluded:** the `recvmmsg` that delivered the datagram; A/B
arbitration; any book update; any digest.

**`decode_and_apply` — included:** all of the above, plus applying each message
to the book. This is the per-message consumer cost, and it is the number the
throughput claim rests on.

The syscall is in neither. It is amortised across the batch and belongs in the
throughput figure — which is exactly why the batch factor has to be published
next to both.

---

## Results

### Decode

| Measurement | Median | p99 | p99.9 | Max |
|---|---|---|---|---|
| `decode_fields`, per message, Criterion | `<ns>` | `<ns>` | `<ns>` | `<ns>` |
| `decode_and_apply`, per message, Criterion | `<ns>` | `<ns>` | `<ns>` | `<ns>` |
| `decode_and_apply`, per message, in-path rdtsc | `<ns>` | `<ns>` | `<ns>` | `<ns>` |

**Two methods, and they bound the answer from opposite sides.** Criterion
amortises the clock across many iterations and does not serialise the work, so it
gives the **lower** bound. The in-path histogram puts an `lfence; rdtsc` pair
around each datagram, which costs tens of cycles and — more importantly —
serialises an out-of-order window the untimed path exploits, so it gives the
**upper** bound. The timer's own overhead is subtracted; the serialisation cannot
be. Quote both, say which is which, and never quote one alone.

### Book update

| Book | Median | p99 | p99.9 |
|---|---|---|---|
| Fast (tick-indexed array + slab + open-addressed map) | `<ns>` | `<ns>` | `<ns>` |
| Reference (`BTreeMap` of `VecDeque`) | `<ns>` | `<ns>` | `<ns>` |

The baseline is the point. "200ns" is a number; "200ns, where the obvious
implementation of the same operations on the same corpus takes N" is a result.
The reference book is kept for exactly this — and for being the oracle the fast
book is differentially tested against.

### Throughput

Measured **receiver-side**, as `(final sequence − first sequence) / elapsed`.
Not messages-delivered over elapsed: a gap the handler failed to recover then
makes the number *worse* rather than invisible, which is the honest direction.

| Run | msg/s | Gaps | State at exit |
|---|---|---|---|
| 1 | `<n>` | `<n>` | `<LIVE/GAPPED>` |
| 2 | `<n>` | `<n>` | `<LIVE/GAPPED>` |
| 3 | `<n>` | `<n>` | `<LIVE/GAPPED>` |

Spread: `<x>%`. The milestone requires three runs within 10%, and a run ending
`GAPPED` does not count regardless of its rate — a fast handler with a wrong
book has not done the job.

### Allocation

| | |
|---|---|
| Heap operations per message, steady state | `<n>` — must be 0 |

Already substantiated and already in [CLAIMS.md](../CLAIMS.md); repeated here
because a performance report that omits it invites the reader to wonder.

---

## What this does not show

- **No NIC.** See the first paragraph.
- **Synthetic order flow.** A seeded generator, not a market data replay. The
  mix is chosen to resemble one; it is not one.
- **One consumer.** Nothing here measures what happens with several handlers on
  the same group.
- **No cross-checking against another implementation.** The numbers describe this
  code measured by its own harness.

---

## Reproducing it

```bash
cargo run --release -p bench --bin hostcheck   # do this first, on the real host
scripts/bench.sh all
```

The gate runs first. On a host it refuses, the benchmarks still run — exercising
them is how the harness gets debugged — but the output lands in
`results/bench/NOT-PUBLISHABLE.md` and this file is not touched.

---

<div align="center">

[shivamsfolio.com](https://www.shivamsfolio.com) · [Case study](https://www.shivamsfolio.com/projects/low-latency-market-data-order-entry) · [All 7 projects](https://www.shivamsfolio.com/projects)

</div>
