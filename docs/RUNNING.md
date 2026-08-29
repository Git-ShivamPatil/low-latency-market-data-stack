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

This puts the engine and the handler on a user-defined bridge with a fixed
subnet and sends the feed across it as real multicast. If it does not work:

```bash
MDSTACK_TRANSPORT=unicast-fanout docker compose up -d
```

> **A note on the case study's step ordering.** The published steps read
> `docker compose up -d`, then run the engine, then run the handler. As of
> milestone 2 that would start *two* engines — one in the container and one on
> the host — both publishing to the same groups with independent sequence
> numbers, and the handler would report a torrent of gaps. Compose here is the
> whole stack, not infrastructure the host binaries attach to. That becomes
> coherent in milestone 4, when the replay service is the thing compose brings
> up and the host binaries genuinely do connect to it. Until then, pick one path
> or the other. This is tracked as copy that needs correcting before v1.0.

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

### Why the handler starts first

A handler that joins mid-stream has no way to rebuild the orders that rested
before it arrived, so its book diverges from the engine's for the rest of the
run. It detects this and says so:

```
joined mid-stream at sequence 40312. The book will be incomplete until the
snapshot cycle lands in milestone 4 — start this handler before the engine for
a clean run.
```

Starting it first is not papering over the gap; it is the honest scope of
milestone 2. Recovering from a late join is exactly what the 2-second snapshot
cycle and the TCP replay service in milestone 4 are for.

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

See [CLAIMS.md](../CLAIMS.md) for what has actually been measured. As of
milestone 2: nothing.
