# Running the stack

Everything here runs on Linux. Under Windows that means WSL2: multicast socket
options, `SO_REUSEADDR`, `recvmmsg` and the core pinning later milestones need
only behave correctly there, and the C++ tree is not wired for Windows at all.

There are two ways to run it. The host-local path is the fast one and needs no
containers. The containerised path exists to exercise multicast across a Docker
bridge, which is the piece of infrastructure most likely to misbehave.

---

## Host-local

Two shells. **Start the handler first** — see [below](#why-the-handler-starts-first).

```bash
cargo run --release --bin feed-handler -- --feed-a 239.1.1.1:30001 --feed-b 239.1.1.2:30001
```

```bash
cargo run --release --bin matching-engine -- --config configs/local.toml
```

The handler prints a stats line per interval:

```
     100000 msgs  73949 msg/s  A 57204/3478  B 42796/3477  seq 1..100000  0 gaps
```

`A 57204/3478` is *first arrivals / datagrams received* on arm A. Both arms carry
the same messages, so a healthy pair splits the first arrivals roughly evenly and
each reports the other's messages as duplicates. An arm sitting at zero datagrams
is dead and the feed is running unprotected; the handler says so explicitly.

Add `--show-book` to print the top of each book on exit.

### Choosing a book

`--books fast` (the default) rebuilds into the tick-indexed, slab-allocated book.
`--books reference` uses the obviously-correct `BTreeMap` one instead. Both are
tested and both reconcile with the engine; the reference book is kept as the
oracle the fast one is differentially tested against, and it is the thing to
switch to first if a book ever looks wrong. If a failure reproduces with
`--books reference`, it is not the fast book.

`scripts/smoke.sh --books reference` does the same for the whole scenario set.

### Counting allocations

```
--verify-allocations
```

Counts heap operations in the receive loop after a 50,000-message warm-up and
prints the total on exit; with `--summary-path` it also writes
`allocations=`, `deallocations=`, `reallocations=` and `alloc_passes=`.

It **reports**; it does not fail the run. The assertion is
`crates/feed-handler/tests/allocation.rs`, which runs in CI — a claim that
depends on somebody remembering to pass a flag is not a claim. The flag is for
looking at a specific run, and for the smoke test to assert against.

Expect `0` with `--books fast` and a large number with `--books reference`. Both
are correct: the reference book is a `BTreeMap` of `VecDeque` and allocates per
level. If it ever reports zero, the counter is broken. See
[BOOKS.md](BOOKS.md#the-allocation-claim) for exactly what the count covers.

The warm-up is not a fudge. The first datagrams through a fresh process
legitimately allocate — a book for a symbol it has not seen, the digest log's
buffer — and that is startup, which the claim explicitly excludes.

### If the handler joins but receives nothing

That is the known multicast failure, and it is an infrastructure problem rather
than a code one: the group join went out an interface the engine is not sending
on. In order of what to try:

1. Set `transport.interface` in `configs/local.toml` to the address of the
   interface the engine sends from, rather than leaving it at `0.0.0.0`.
2. Fall back to `--transport unicast-fanout` on **both** binaries. Same framing,
   same batching, same everything else — it is a supported mode, not a
   workaround, and `scripts/smoke.sh` tests it exactly as hard as multicast.

---

## Containerised

```bash
docker compose up -d
docker compose logs -f handler
docker compose down
```

This puts the engine, the replay service and the handler on a user-defined
bridge with a fixed subnet, and sends the feed across it as real multicast. If it
does not work:

```bash
MDSTACK_TRANSPORT=unicast-fanout docker compose up -d
```

> **A note on the case study's step ordering.** The published steps read
> `docker compose up -d`, then run the engine, then run the handler. Taken
> literally that starts *two* engines — one in the container and one on the host
> — both publishing to the same groups with independent sequence numbers, and the
> handler would report a torrent of gaps.
>
> The replay service is genuinely infrastructure a host binary attaches to, so
> the intent of step 1 now has something real behind it. To follow the published
> steps as written:
>
> ```bash
> docker compose up -d replay
> ```
>
> then run the engine and handler on the host with `--replay-uplink` and
> `--replay`. Bringing up the *whole* compose stack is the alternative, all-in-one
> path. The published copy still needs a word changed before v1.0 to say which.

---

## The smoke test

```bash
scripts/smoke.sh                        # both transports, 100k messages
scripts/smoke.sh --transport multicast  # one
scripts/smoke.sh --messages 500000      # longer
scripts/smoke.sh --keep                 # keep the artifacts on success
```

It runs the engine and the handler as separate processes and then requires that
**the handler's book matches the engine's own book at the same sequence number**.

That is the whole point of the milestone. The two processes reach a book by
completely different routes — one by matching orders, the other by replaying the
feed those matches produced — so agreement at a shared sequence is evidence that
the feed faithfully describes what the engine did. Disagreement localises the
bug to a sequence number.

It also asserts the things that quietly break a batched feed:

| Check | What it catches |
|---|---|
| Sequence `1..N`, `N` messages, 0 gaps | A message lost at a datagram seam |
| `last - first + 1 == messages` | Something counted twice, or skipped |
| Both arms received datagrams | A dead channel, so redundancy is real |
| Duplicates seen on both arms | Only one arm actually being read |
| Every shared checkpoint identical | The book diverging anywhere in the run |
| Engine self-check | The feed not rebuilding the engine's own book |

Both transports are tested. The unicast fallback is not a second-class path.

Two further scenarios run after them: **recovery**, which forces loss on both
arms and requires the handler to rebuild from a snapshot and end `LIVE` with
matching books; and **replay**, which does the same with a replay service running
and requires the gaps to be filled by replay rather than fallen back on.

### With the replay service

Three processes. The service holds a bounded history of the stream and serves
ranges of it, so a consumer that loses messages can fill the hole exactly instead
of waiting for the next snapshot.

```bash
cargo run --release --bin replay-service -- --config configs/local.toml
```

Then add `--replay-uplink 127.0.0.1:32001` to the engine and
`--replay 127.0.0.1:32002` to the handler. Both are optional: without the uplink
the engine publishes normally, and without the request address the handler falls
back to the snapshot cycle. [RECOVERY.md](RECOVERY.md) explains why both
mechanisms exist.

### Injecting loss

The smoke test runs with 2% loss injected — one arm per dropped datagram — and
requires zero gaps. See [RECOVERY.md](RECOVERY.md) for why the loss *model*
decides what a result can prove, and why "zero gaps under independent loss" is
not a claim this project makes.

```bash
scripts/smoke.sh --drop-rate 0.05 --drop-mode correlated
```

### Why the handler starts first

A handler that joins mid-stream has no way to rebuild the orders that rested
before it arrived, so its book diverges from the engine's for the rest of the
run. It detects this and says so:

```
joined mid-stream at sequence 40312; waiting for a snapshot to build a book that
can be trusted.
```

A mid-stream join is treated as a gap by another name — everything before it is
missing — so it goes through the same recovery path, and the next snapshot cycle
gives it a book it can trust. The smoke test still starts the handler first
because that makes the *whole* run comparable against the engine's checkpoints
rather than only the part after recovery.

---

## Reproducibility

The order-flow generator is seeded (`engine.seed` in the config, or `--seed`),
and the PRNG is a hand-rolled SplitMix64 rather than a dependency, so the same
seed produces the same run on every machine and every toolchain.

That is deliberate. A reconciliation failure that cannot be reproduced is not a
bug report, and the whole value of comparing two processes is lost if a
disagreement might just be luck. Two runs of `scripts/smoke.sh` with the same
seed produce byte-identical digest files — including across transports, since
the transport does not touch the message stream.

---

## What the numbers in the output are not

The `msg/s` figures the binaries print are **not** benchmark results and must
not be quoted anywhere.

They come from a publisher-side counter on a 2-core laptop with the engine, the
handler and the OS competing for the same cores, over loopback rather than a
network. The advertised figure has to be measured receiver-side as
`(final sequence − first sequence) / elapsed`, on a quiet host with pinned cores,
reported with p99 and p99.9 alongside the median, and reproduced three times
within 10%. That is milestone 6, on rented hardware.

The one number this project does publish is a **count**, not a rate: zero heap
operations per message in steady state. A count does not depend on the host, so
it can be asserted honestly from this laptop. See
[BOOKS.md](BOOKS.md#the-allocation-claim).

### Measuring it properly

```bash
cargo run --release -p bench --bin hostcheck
```

Says whether this machine may publish a performance number, and if not, exactly
which preconditions failed. Run it on any host **before** provisioning anything
expensive — it takes a fraction of a second and it is the difference between a
rented burn day that produces a number and one that produces a lesson about
`constant_tsc`.

```bash
scripts/bench.sh check        # the gate alone
scripts/bench.sh micro        # Criterion: decode, and the book against its baseline
scripts/bench.sh inpath       # engine + handler, rdtsc histogram over the real path
scripts/bench.sh throughput   # 60s sustained, receiver-side, three runs
scripts/bench.sh all
```

The gate runs first. On a host it refuses, the benchmarks **still run** —
exercising them is how the harness gets debugged — but the output lands in
`results/bench/NOT-PUBLISHABLE.md` and `bench/REPORT.md` is not touched.

The handler's own `--latency-histogram` does the in-path measurement directly:

```
--latency-histogram
```

It reports median, p99 and p99.9 per datagram and per message, and writes them
to `--summary-path`. Read it next to the Criterion numbers and never alone: the
`lfence; rdtsc` pair serialises work the untimed path overlaps, so it is an
**upper** bound while Criterion gives the lower one. Both belong in a report,
labelled.

[bench/REPORT.md](../bench/REPORT.md) is the methodology — what "decode" includes
and excludes, why the batch factor has to appear next to every throughput figure,
and what the numbers do not show.

See [CLAIMS.md](../CLAIMS.md) for the ledger. As of milestone 6 the throughput
and decode figures **have** been measured — on a free arm64 CI runner, single
host, batched 32 messages to a datagram. Read [bench/REPORT.md](../bench/REPORT.md)
before quoting any of them; the caveats are not optional.
