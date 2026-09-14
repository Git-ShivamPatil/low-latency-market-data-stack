# Every advertised claim, and what backs it

The [case study page](https://www.shivamsfolio.com/projects/low-latency-market-data-order-entry)
makes a specific set of claims. This file maps each one to a named test or a
named section, so a reader can check the page against the repository without
taking either on trust.

It exists because this is the last point in the build where a claim the code does
not support can still be caught. Two were caught here, and **both were then fixed
on the page** rather than narrowed in a footnote. What they said, what they say
now, and how the change was made are in
[What did not hold up](#what-did-not-hold-up) at the bottom.

**How to read the Evidence column.** A file path plus a test name means you can
run it. A document section means the claim is a design decision rather than a
measurement, and the section says what was decided and why. A `CLAIMS.md` row
means it is a number, and that row carries the caveats that make it honest.

---

## Bullet 1 — "Matching engine and binary feed"

> Price-time-priority matching publishing a binary market-data feed over
> redundant A/B UDP multicast channels, with configurable packet-loss injection,
> a 2-second snapshot cycle, and a TCP replay service for recovery.

| Claim | Evidence | Holds |
|---|---|:--:|
| Price-time-priority matching | `crates/matching-engine/src/engine.rs` — the tests at the bottom assert the exact message sequence a given crossing produces, including that a trade prints at the **resting** order's price | ✅ |
| Publishing a binary feed | `crates/wire` + `schema/market-data.xml`; 19 golden vectors, 13 of them hand-typed hex, checked by both the Rust and C++ suites against the same files | ✅ |
| Over redundant A/B channels | `crates/transport`; `scripts/smoke.sh` runs engine and handler as separate processes over real sockets on both arms | ✅ |
| UDP **multicast** | `scripts/smoke.sh` on the multicast transport, and `docker-compose.yml` crossing a user-defined bridge. A `unicast-fanout` fallback exists and is tested equally hard — see [RUNNING.md](RUNNING.md) | ✅ |
| Configurable packet-loss injection | `--drop-rate` / `--drop-mode` / `--drop-seed` on the engine and `--drop-rate` on the handler; [RECOVERY.md](RECOVERY.md) defines the three loss models | ✅ |
| A 2-second snapshot cycle | `configs/local.toml` → `feed.snapshot_interval_millis = 2000`; the cycle, its fragmentation and its `LAST_FRAGMENT` flag are in [WIRE.md](WIRE.md) | ✅ |
| A TCP replay service for recovery | `crates/replay-service`, 28 tests including a real TCP round trip; `scripts/smoke.sh` replay scenario runs all three processes and requires replay to close the gaps | ✅ |

---

## Bullet 2 — "Allocation-free feed handler"

> A/B feed arbitration, sequence-gap detection and snapshot-based recovery into
> MBP/MBO order books — sustaining 1M+ messages/sec at ~100ns decode and ~200ns
> book update, with zero heap allocations per message verified by a counting
> allocator.

| Claim | Evidence | Holds |
|---|---|:--:|
| A/B feed arbitration | `crates/feed-handler/src/arbitration.rs`; the redundancy tests push **10,000,000 messages** through it under three loss models | ✅ |
| Sequence-gap detection | Same suite. Under *exclusive* loss: zero gaps across 10M messages. Under *independent* loss the test predicts exactly which datagrams die and requires the reported gaps to cover exactly that set | ✅ |
| Snapshot-based recovery | `crates/feed-handler/src/recovery.rs` is the state machine. **Queue position is proved by `crates/feed-handler/tests/recovery.rs::recovery_restores_queue_position_not_just_quantity`**, which walks the recovered book order by order, plus `crates/book/src/apply.rs::a_snapshot_restores_queue_order_not_just_quantity`. `scripts/smoke.sh` proves recovery across a real process boundary but **cannot see queue order** — its digest hashes price levels, and a book rebuilt in a different order digests the same. Cross-process and queue-order-exact are two claims and they are carried by two different tests | ✅ |
| MBP **and** MBO books | `crates/book` — one structure maintaining both, differentially tested against a reference implementation over **5,000,000 random operations**, including exact queue order within each level. [BOOKS.md](BOOKS.md) says why they are one structure and not two | ✅ |
| Sustaining 1M+ messages/sec | [CLAIMS.md](../CLAIMS.md) — **2,782,874 msg/s** measured receiver-side over 60s, three runs within 0.7%. **Read the caveats in that row**: single host, loopback, 32 messages per datagram | ✅ |
| ~100ns decode | [CLAIMS.md](../CLAIMS.md) — **8.20 ns/message** streaming. Beats the advertised figure by an order of magnitude, which is why the report defines precisely what "decode" includes and excludes | ✅ |
| ~200ns book update | [CLAIMS.md](../CLAIMS.md) — **38.9 ns/message**. The row also records that the fast book is only **1.72×** the `BTreeMap` reference on this corpus, not the order of magnitude the design argument predicts | ✅ |
| Zero heap allocations per message | `crates/feed-handler/tests/allocation.rs` — exactly zero across **1,000,000 messages including a forced blackout and the snapshot recovery that follows**. `scripts/smoke.sh` asserts the zero *and* requires the same run with `--books reference` to report a **non-zero** count — it reported 10,814 on the run recorded here — with the failure message spelling out why: a counter that reads zero for a `BTreeMap` of `VecDeque` is not working, and the fast book's zero would prove nothing. The assertion is on the control being non-zero, not on that particular number | ✅ |
| "verified by a counting allocator" | `crates/alloc-guard` — compiled into the handler **unconditionally**, not switched on by a flag. A build that swapped the allocator in for a measurement would be measuring a different program | ✅ |

---

## Bullet 3 — "FIX 4.4 order gateway"

> A full session layer — logon, heartbeats, resend/gap-fill, durable sequence
> persistence — reconciling order state across a hard process restart, plus a
> risk service enforcing pre-trade limits on an allocation-free path.

| Claim | Evidence | Holds |
|---|---|:--:|
| ~~"A **full** session layer"~~ — the page no longer says this | See [What did not hold up](#what-did-not-hold-up), which records what it says now and why | ✅ |
| "cross-checked against QuickFIX as an independent counterparty" | `scripts/quickfix-interop-test.sh`, in `make test` and in CI with `QUICKFIX_REQUIRED=1` so a missing QuickFIX fails rather than skips | ✅ |
| Logon (including `ResetSeqNumFlag`) | `cpp/gateway/tests/session_test.cpp`; QuickFIX accepts a reset and follows it back to 1 in `scripts/quickfix-interop-test.sh` | ✅ |
| Heartbeats | Same suite — the negotiated interval, the test request after it, and the disconnect when a test request goes unanswered | ✅ |
| Resend / gap-fill | Same suite. One test walks a whole requested range and requires **every** sequence to be covered by either a resend or a gap fill, which is the invariant [PROTOCOL.md](PROTOCOL.md) states | ✅ |
| Durable sequence persistence | `cpp/gateway/tests/seqstore_test.cpp`; `scripts/kill-restart-test.sh` `SIGKILL`s both ends mid-session and requires both to resume from the durable numbers with no reversal | ✅ |
| Verified against an independent implementation | `scripts/quickfix-interop-test.sh` — QuickFIX 1.15.1 as an acceptor, **zero rejects** across a clean session, a manufactured gap and a reset. It found a real bug on its first run; the trace is in [PROTOCOL.md](PROTOCOL.md) | ✅ |
| Reconciling order state across a hard process restart | `scripts/order-path-test.sh` scenario three — the gateway is `SIGKILL`ed with an order working, rebuilds from its write-ahead log, asks the engine what it holds, and the two agree. **It reports and does not repair**; see [ORDER-PATH.md](ORDER-PATH.md) | ✅ |
| A risk service enforcing pre-trade limits | `cpp/risk/tests/limits_test.cpp` — one case per reject reason, plus the cases where two limits could both fire, because which reason comes back is what an operator reads | ✅ |
| On an allocation-free path | `cpp/risk/tests/allocation_test.cpp` — **0 allocations across 1,000,000 decisions**, under both g++ and clang++, with a control that allocates on purpose and requires the counter to notice | ✅ |

---

## The five architecture nodes

The case study page draws five. Each is a real process with its own binary.

| Node on the page | Binary | Where |
|---|---|---|
| FIX 4.4 gateway — *session · resend · gap-fill* | `fix-gateway` | `cpp/gateway` |
| Risk service — *pre-trade limits* | `risk-service` | `cpp/risk` |
| Matching engine — *price-time priority* | `matching-engine` | `crates/matching-engine` |
| Snapshot + replay — *2s cycle · TCP recovery* | `replay-service` (+ the engine's snapshot cycle) | `crates/replay-service` |
| Rust feed handler — *A/B arbitration · MBP/MBO* | `feed-handler` | `crates/feed-handler` |

[ARCHITECTURE.md](ARCHITECTURE.md) is the long form.

**One correction, found by checking this table rather than writing it.**
`scripts/order-path-test.sh` reports that an order "crossed five processes", and
that is true — but they are not these five. It runs the gateway, the risk
service, the engine, the handler, and a **second `fix-gateway` instance acting as
the FIX client**. It never starts `replay-service`.

So no single script runs all five *nodes* at once. The replay service is covered
by `scripts/smoke.sh`'s replay scenario, which runs it as a third process against
the engine and the handler. Between the two scripts every node is exercised; in
one script, four of them are.

---

## The four published commands

They are published verbatim on the page, so they are load-bearing rather than
illustrative. `scripts/case-study-commands-test.sh` runs them and fails if any of
them stops working, which is what stops the page drifting away from the
repository.

**"Exactly as published", with one honest qualification.** Every flag the page
prints is passed exactly as printed. The script adds a stop condition to each of
the last three — `--duration` on the engine, `--messages` and `--idle-timeout` on
the handler — because those commands run until interrupted and a test cannot wait
forever. Nothing else is changed and nothing is removed. Step 1 is opt-in behind
`--with-docker`, and the script says which mode it ran in, so a green result can
never quietly mean "three of four".

| Step | Command | Checked by |
|---:|---|---|
| 1 | `docker compose up -d` | the script, and `docker-compose.yml` is in the repo root where the command expects it |
| 2 | `cargo run --release --bin matching-engine -- --config configs/local.toml` | the script; `configs/local.toml` is the file the command names, and it is throttled for exactly that reason — see below |
| 3 | `cargo run --release --bin feed-handler -- --feed-a 239.1.1.1:30001 --feed-b 239.1.1.2:30001` | the script; the addresses match `configs/local.toml` |
| 4 | `cargo run --release --bin feed-handler -- --drop-rate 0.02 --verify-allocations` | the script; it requires the allocation report to exist AND to read zero, and requires the injected loss to actually show up |

### What running step 1 found

Nobody had executed `docker compose up -d` since the benchmark crate joined the
workspace at milestone 6. Three defects were waiting in it, each fatal on its
own, and each invisible to every other suite in this repository:

| Defect | Why nothing caught it |
|---|---|
| `bench` was never copied into the image, so cargo refused to load the workspace before reaching any binary | `make test` builds on the host, where `bench/` is simply there |
| Both C++ parsers rejected `--flag=value`, which is every argument in `docker-compose.yml`, so risk and gateway exited 2 | Every other suite invokes them with space-separated flags — the arguments the *tests* need, not the ones the *deployment* uses |
| The FIX acceptor bound `127.0.0.1`, so the published port 5001 was unreachable from outside the container | Nothing had ever connected to the gateway across a container boundary |

All three are fixed, and each has a test rather than only a fix:
`scripts/cli-args-test.sh` pins the argument spelling in about a second, and the
`case-study` CI job runs `docker compose up -d` for real on every push.

**Verified by hand on 2026-09-14**, since Docker Desktop here runs Windows-side
with WSL integration off and `make` cannot reach it: all five containers up, risk
healthy, four rings created on the named volume, the engine publishing at 50,000
msg/s, the handler `LIVE` across the bridge on **both arms with zero gaps**, and
port 5001 accepting a TCP connection from the host.

### One thing step 1 is not

It brings up a **self-contained copy of the whole system**, not infrastructure
that steps 2, 3 and 4 attach to. Measured: with the compose stack publishing at
50,000 msg/s, a host handler pointed at the same two groups for twelve seconds
received **zero messages** and stayed `SYNCING`. The bridge has its own network
namespace and its multicast does not reach the host.

So the four commands do run in order and nothing conflicts — but step 1 is
harmless rather than load-bearing for the three that follow.
[RUNNING.md](RUNNING.md#what-the-compose-stack-is-and-is-not) says so, and says
which service *does* work as infrastructure a host binary attaches to.

---

## What did not hold up

Two claims where the page said more than the repository supports. Both were
found here, at the last gate before the release, and **both have since been
corrected on the page itself.**

They are still written out in full below, with the original wording, because
deleting them once they were fixed would destroy the only record that the check
did anything. A claims map that lists nothing but claims that passed is not
evidence of an audit — it is evidence of an audit that was not run.

The correction is `scripts/correct-market-data-claims.mts` in the site's own
repository: a dry-run-by-default script that refuses to write if the live text is
not the exact string it expects, and prints the previous values as it goes so its
own output is the rollback record.

### "A **full** session layer" — corrected

`docs/PROTOCOL.md` opens by refusing exactly this word:

> So: **"full" is not claimed.** What is claimed is a specific list of
> behaviours, each of which is tested against an independent implementation, and
> an explicit list of what is left out.

The four things the bullet then enumerates — logon, heartbeats, resend/gap-fill,
durable sequence persistence — are all implemented and all tested, and against
QuickFIX rather than only against this project's own reading of the spec. But
"full session layer" as a phrase covers more than that, and PROTOCOL.md names
eight behaviours deliberately left out, including encryption, `OnBehalfOfCompID`
routing, scheduled session times and business-level rejects.

**The page now reads:** *"A FIX 4.4 session layer — logon, heartbeats,
resend/gap-fill, durable sequence persistence — **cross-checked against QuickFIX
as an independent counterparty**, reconciling order state across a hard process
restart, plus a risk service enforcing pre-trade limits on an allocation-free
path."*

It is shorter, it is stronger — it names the independent check instead of
reaching for an adjective — and it is true.

### "1M+ msg/s" with no room for the batch factor — corrected

The outcome chip reads `1M+ msg/s · ~100ns decode`. The measurement is real and
exceeds it — 2.78M msg/s — but only with **32 messages per datagram**. At one
message per datagram the kernel UDP path is the ceiling, somewhere around
300–600K packets per second per core, and the figure is unreachable without
kernel bypass.

Batching is standard on real exchange feeds, so the number is legitimate. The
problem is that a chip has no room for the qualifier, and a reader who assumes
one message per packet is reading a much stronger claim than the one being made.

**This is not fixable by wording on the chip.** This file originally proposed
fixing it by having the case study link to [bench/REPORT.md](../bench/REPORT.md),
which states the batch factor, the single-host caveat and the loopback caveat in
its first paragraph.

**That turned out not to be available.** The page renders each of these bullets
as a bare paragraph of plain text and carries no outbound link anywhere on it, so
there was nowhere for the link to go without changing the site's components.

So the qualifier went into the bullet itself, which is the better answer anyway —
the reader sees it without having to click. **The page now ends that bullet:**
*"…with zero heap allocations per message verified by a counting allocator.
**Measured single-host over loopback, batched 32 messages to a datagram.**"*

The chip still reads `1M+ msg/s · ~100ns decode`, and still has no room for a
qualifier. That is now fine: the bullet immediately below it carries the one that
matters.

---

**The one bug this repository had not closed is now closed, and said so
honestly.** A replay answer applied past the range it asked for, so the live feed
delivered those sequences again and the book double-applied them — found at
milestone 9, fixed, and guarded by two controls that are now part of `is_clean`:
the applied stream must be contiguous, and no sequence may be applied twice.
Twelve of twelve clean runs against a prior rate of roughly one in three.
[RECOVERY.md](RECOVERY.md) has the full account, including the honest limit:
there is a deterministic test for the precondition and a named mechanism, but not
a deterministic reproduction of the race.

## What this file does not claim

It does not claim the numbers were measured on production-grade hardware, on a
physical network, or against a real exchange feed. Every figure here is a
single-host figure from a free shared CI runner, and `CLAIMS.md` says so in
every row. The tail latencies in particular are **not** claimed: a p99.9 of
1.8 µs against a 93 ns median is a measurement of somebody else's scheduler.

---

<div align="center">

[shivamsfolio.com](https://www.shivamsfolio.com) · [Case study](https://www.shivamsfolio.com/projects/low-latency-market-data-order-entry) · [All 7 projects](https://www.shivamsfolio.com/projects)

</div>
