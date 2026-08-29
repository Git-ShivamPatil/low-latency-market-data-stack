# Redundancy, arbitration and gaps

What publishing the same stream twice actually buys, what it cannot buy, and how
the handler tells the difference.

---

## The two arms

The engine publishes identical datagrams — same messages, same sequence numbers —
on two channels. Only `packetHeader.channel` differs between the copies. The
consumer takes whichever arrives first and discards the other.

That is the whole mechanism, and it is worth being precise about what it
protects against: **loss on one arm at a time**. Nothing else. It does not help
with a publisher bug, a correlated network event that takes out both paths, or a
consumer too slow to keep up.

---

## Why arbitration is not just "is this the sequence I expected"

When one arm drops a datagram, the other still has it — but that copy arrives
*after* datagrams the surviving arm has already delivered:

```
  arm A:  100..131   [lost]     164..195   196..227
  arm B:  100..131   132..163   164..195   196..227
  order:  100..131   164..195   132..163   196..227
                                ^^^^^^^^ late, not lost
```

A handler that keys on "is this the sequence I expected" declares a gap at 164,
then treats 132..163 as a duplicate when it turns up. It loses 32 messages while
reporting a recovery it never made. That is exactly what the naive logic in
milestone 2 did, and it said so in its own doc comment.

The fix is a **bounded reorder window**. Out-of-order datagrams are held by
sequence and released when the hole ahead of them fills.

### Why bounded

An unbounded buffer turns one lost datagram into unbounded memory growth and
unbounded latency: the handler holds messages it could have delivered, forever,
waiting for one that is never coming. The bound is what makes "wait for the other
arm" a strategy rather than a hang.

When the window fills and the hole is still open, the missing range is not late.
It is lost, and the handler says so.

### Why datagrams rather than messages

Loss happens to datagrams. A datagram of 30 messages arrives or does not; there
is no such thing as losing message 17 of it. Buffering at the datagram level
means one slot per loss event instead of thirty, so the window covers thirty
times as much stream for the same memory.

---

## States

| State | Meaning |
|---|---|
| `SYNCING` | Nothing received yet. There is no sequence to be contiguous with. |
| `LIVE` | Every sequence up to the frontier has been delivered, in order, exactly once. |
| `GAPPED` | A range is confirmed lost and named. The book downstream of it is not trustworthy. |

`GAPPED` is currently terminal, and it makes the handler exit non-zero. Recovering
from it — rebuilding via the snapshot cycle or the replay service — is milestone
4. Until then, the honest thing is to stop claiming the book is right.

---

## Two ways a hole is declared lost

**The window fills.** Another datagram arrives, the reorder window is full, and
the hole is still open. Waiting longer would mean dropping live traffic to hold
onto a datagram that is evidently not coming.

**The feed goes quiet.** Nothing arrives for `gap_timeout_millis` while a hole is
outstanding. Without this a publisher that stopped mid-stream would leave the
messages behind the hole buffered indefinitely.

Either way the range is named — `sequence 4993..=5024 (32 messages)` — rather
than silently skipped. A gap you can see is a bug report; a gap you cannot is
data loss.

---

## Injecting loss

Loss is injected **at the publisher**: for a dropped datagram, `send` is simply
not called. That is cruder than a network emulator and deliberately at the right
layer — what the handler must survive is a datagram that never arrives, and
reproducing that needs no `tc qdisc`, no privileges, and no second machine. It
also replays exactly from a seed, which a real network never does.

```bash
cargo run --release --bin matching-engine -- \
    --config configs/local.toml --drop-rate 0.02 --drop-mode exclusive
```

The drop RNG is seeded separately from the order-flow RNG, so turning loss on
does not change which orders are generated. A run with loss and a run without
stay comparable.

### The three modes, and why the distinction matters

| Mode | A dropped datagram is lost on | Redundancy can recover it |
|---|---|---|
| `exclusive` | exactly one arm | always |
| `independent` | each arm, decided separately | usually — but not when both drop it |
| `correlated` | both arms | never |

This is not a knob for taste. It decides what a test can prove.

---

## The arithmetic the milestone spec got wrong

The build plan for this milestone says:

> With 2% independent loss on each arm, the arbitrated output stream has ZERO
> gaps across 10M messages.

**That is not achievable**, and not because of any defect in the code. With
genuinely independent loss at rate `p` per arm, a datagram is lost on both arms
with probability `p²`. At `p = 0.02` that is 0.04%; 10M messages at 32 per
datagram is ~312K datagrams, so roughly 125 datagrams vanish entirely. No amount
of redundancy recovers a message when both copies are gone.

Measured, at exactly that configuration:

```
10M messages, 2% independent loss: 124 datagrams lost on both arms (0.040%,
predicted ~0.04%), 124 gaps reported covering exactly those 3968 sequences
```

So the claim is split into the two separate things it was conflating, and both
are tested:

**Single-arm loss costs nothing.** Under `exclusive` loss, zero gaps is a
property the code either has or does not:

```
10M messages, 2% exclusive loss: 0 gaps, A first 9903168 / B first 96832,
window peak 3/64
```

**Double loss is detected, not silently skipped.** Under `independent` and
`correlated` loss, the test predicts exactly which datagrams die and requires the
reported gaps to cover exactly that set — no more (over-reporting would hide
real recoveries) and no less (under-reporting is silent data loss).

Both statements are stronger than the original, and both are true.

---

## Why the integration tests do not use sockets

`crates/feed-handler/tests/redundancy.rs` drives the arbitrator directly. That is
not a shortcut. Two processes over real UDP cannot tell you *which* datagrams the
kernel dropped, so a cross-process test can only observe that something went
wrong — never that the right thing went right. Asserting "the reported gaps are
exactly the datagrams that died" requires knowing the answer independently, and
only a simulated network provides that.

`scripts/smoke.sh` covers the socket path, with loss injected, on both
transports. The two tests answer different questions and neither replaces the
other.

---

## A note on read fairness

The handler reads at most 16 datagrams from one arm before switching. This
matters more than it sounds.

Without the cap, a handler that has fallen behind drains everything queued on one
arm before touching the other, so the arms drift apart by the whole socket
backlog. Every datagram that arm lost must then be held until the other arm is
finally read, and the reorder window fills with traffic that was never really out
of order.

Measured on a 2-core host at 2% loss:

| | Window peak |
|---|---|
| Uncapped reads | 56 of 64 |
| 16 reads per arm per pass | 15 of 256 |

The uncapped run passed — but it was one bad moment away from inventing gaps that
never happened, which is the worst possible failure for a component whose entire
job is telling real loss from apparent loss.
