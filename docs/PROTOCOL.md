# FIX 4.4 session layer — what is implemented, and what is not

> **Status: milestone 7, in progress.** This document is written *before* the
> code, because its job is to fix the boundary rather than describe wherever the
> code happens to stop.

## Why this file exists

This project's own risk list says it plainly:

> **'FULL SESSION LAYER' IS A STRONG WORD FOR FIX 4.4.** Resend requests, gap
> fill, sequence reset in both modes, and PossDup/PossResend semantics are
> precisely where FIX implementations quietly disagree with the spec and with
> each other. Tested only against your own simulator, 'full' means 'consistent
> with my own reading'.

So: **"full" is not claimed.** What is claimed is a specific list of behaviours,
each of which is tested against an independent implementation, and an explicit
list of what is left out. A reader who wants to know whether this gateway would
survive a session with their counterparty should be able to answer that from
this page without reading the code.

---

## In scope

### Session establishment

| Behaviour | Notes |
|---|---|
| `Logon` (35=A) initiator and acceptor | |
| `ResetSeqNumFlag` (141=Y) | Both sending and honouring it. Resets both directions to 1. |
| `HeartBtInt` (108) negotiation | The acceptor echoes the initiator's value, per spec. |
| `Logout` (35=5) | Including logout as a response to an unrecoverable sequence error. |

### Liveness

| Behaviour | Notes |
|---|---|
| `Heartbeat` (35=0) on the negotiated interval | |
| `TestRequest` (35=1) after `HeartBtInt` + a tolerance | |
| `Heartbeat` with `TestReqID` (112) echoed | Answering a test request. |
| Disconnect when a test request goes unanswered | |

### The part that is actually hard

| Behaviour | Notes |
|---|---|
| `ResendRequest` (35=2) handling | Including `EndSeqNo=0` meaning "everything from BeginSeqNo". |
| Gap fill — `SequenceReset` (35=4) with `GapFillFlag=Y` | For administrative messages, which are never resent. |
| Sequence reset — `SequenceReset` with `GapFillFlag=N` | The destructive form. Accepted only when it moves the sequence forward. |
| `PossDupFlag` (43=Y) on every resent message | With `OrigSendingTime` (122) preserved. |
| Detecting a sequence **gap** on inbound | Higher than expected → send a `ResendRequest`, queue what arrived. |
| Detecting a sequence **reversal** on inbound | Lower than expected without `PossDupFlag` → logout and disconnect. That is a spec requirement, not a choice. |

**Anything the gateway declines to resend is gap-filled, never silently
dropped.** An application message it no longer holds becomes a `SequenceReset`
with `GapFillFlag=Y` covering exactly that range. A counterparty must never see
a sequence number simply vanish.

### Durability

Inbound and outbound sequence numbers are persisted and **`fsync`'d on every
outbound message**, before the message reaches the socket.

That ordering is the whole point. If the process dies between sending and
persisting, it comes back believing it sent less than it did, reuses a sequence
number, and the counterparty — correctly — treats that as a fatal reversal. The
`fsync` is expensive and it is not optional; a session layer that loses its
sequence state on a hard kill has not implemented the hard part.

---

## Deliberately out of scope

Not oversights. Each of these is a real part of FIX that this gateway does not
do, and saying so is what makes the list above meaningful.

| Not implemented | Why |
|---|---|
| **Encryption (98/EncryptMethod other than 0)** | Nobody uses FIX-level encryption; TLS at the transport is the real answer and is not this milestone. |
| **`XmlData`, raw data fields, repeating-group-heavy application messages** | This is a session layer. The application layer it carries is the order flow of milestone 8, which is small and fixed. |
| **`OnBehalfOfCompID` / `DeliverToCompID` routing** | Third-party routing is a hub concern. One session, two parties. |
| **Message-level `SecureData`, `Signature`** | Same reason as encryption. |
| **FIXT.1.1 / FIX 5.0 transport separation** | 4.4 has the session layer in the protocol; that is what the case study names. |
| **Multiple concurrent sessions per process** | One session per process. Scaling that is an architecture question, not a protocol one. |
| **Scheduled session start/end times, weekly reset** | A real deployment needs them. They add no protocol subtlety, only a calendar. |
| **`Reject` (35=3) for every business-level rule** | Session-level rejects are implemented; business-level validation belongs to the application layer. |

---

## Where implementations disagree, and what this one does

These are the places the spec is ambiguous or widely read differently. Each is a
decision, not an accident.

### A `ResendRequest` with `EndSeqNo=0`

Read as "everything from `BeginSeqNo` to the current outbound sequence". Some
implementations use `999999` for the same meaning; both are accepted.

### A resend request arriving *during* a resend

Queued, not interleaved. Interleaving two resends produces a stream whose
sequence numbers are correct individually and incoherent together.

### `SequenceReset` with `GapFillFlag=N` that moves the sequence *backwards*

**Rejected, and the session is logged out.** The spec permits a reset to a lower
number in principle; accepting one silently is how two sides end up disagreeing
about what has been sent, with no way to notice. This is the single most
opinionated decision here and it is recorded as such.

### `PossDupFlag=Y` on a message with a sequence number *higher* than expected

Treated as a gap, not as a duplicate. The flag says "this may be a repeat", not
"trust this number".

### A message with a sequence number lower than expected and **no** `PossDupFlag`

Logout and disconnect, per spec. There is no safe recovery: the counterparty's
notion of what it has sent disagrees with ours, and continuing would mean
choosing one arbitrarily.

---

## How it is tested

**Two counterparties, and that is the point.** Tested only against its own
simulator, "correct" would mean "consistent with my own reading of the spec" —
which is exactly the failure mode this document opens with.

1. **`fix-sim`** — a scripted counterparty in this repo. It can be told to send
   an exact sequence of malformed, out-of-order and duplicate messages, which a
   real implementation will not do on demand. This is how the awkward cases get
   covered.
2. **QuickFIX** — an independent implementation, used as an acceptor. The same
   conformance suite runs against both. A behaviour that passes against `fix-sim`
   and fails against QuickFIX is a bug in this gateway or a misreading of the
   spec, and either way it is worth knowing.

QuickFIX is a free, apt-installable C++ library. No paid dependency.

### The kill-restart test

`scripts/kill-restart-test.sh` `SIGKILL`s the gateway mid-session — not a clean
shutdown, no destructors, no flush. On restart it must:

1. Log on with the correct expected sequence numbers in both directions.
2. Correctly request the range **it** missed while dead.
3. Correctly answer the counterparty's resend request for the range the
   counterparty missed.
4. Gap-fill anything it declines to resend, rather than dropping it.

A session layer that only works when it is shut down politely has not
implemented the part that matters.

---

<div align="center">

[shivamsfolio.com](https://www.shivamsfolio.com) · [Case study](https://www.shivamsfolio.com/projects/low-latency-market-data-order-entry) · [All 7 projects](https://www.shivamsfolio.com/projects)

</div>
