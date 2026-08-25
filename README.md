# Low-Latency Market Data & Order Entry Stack

> **Status: in active development.** The figures below are **targets, not measurements.**
> See [CLAIMS.md](CLAIMS.md) for what has actually been measured, on what hardware, at which commit.

Price-time-priority matching engine, A/B UDP multicast binary feed, allocation-free Rust handler, C++ FIX 4.4 gateway.

**Target outcome:** `1M+ msg/s · ~100ns decode`

## What this is

One schema file is the single source of truth for the binary message layout, and the Rust codec and the C++ header agree on it byte for byte.

Full project spec, milestone plan and progress tracking live in the programme document:
`Desktop/Projects/PROGRAM.md` → section `01 · Low-Latency Market Data & Order Entry Stack`.

## Stack

- Rust (stable, 2021 edition) — engine, feed handler, replay service, books, benches
- C++20 (clang 16+ / gcc 12+) — FIX 4.4 gateway and risk service
- CMake 3.25+ / Ninja
- FIX 4.4 session + application layer (hand-rolled; QuickFIX used only as a test counterparty)
- UDP multicast (IGMPv2/v3), SO_REUSEPORT, recvmmsg/sendmmsg batching
- SBE-style fixed-layout little-endian binary encoding with schema-driven codegen
- Docker / docker compose on a user-defined bridge network
- Criterion + rdtsc histograms for latency, custom counting #[global_allocator] for allocation proofs
- WSL2 Ubuntu 24.04 as the actual dev/run target (Windows host)
- GitHub Actions (Linux runners, both toolchains)

## Roadmap

- [ ] **M1. Workspace, wire schema, and cross-language codegen** — One schema file is the single source of truth for the binary message layout, and the Rust codec and the C++ header agree on it byte for byte.
- [ ] **M2. Matching engine publishing a live binary feed, end to end** — The two advertised commands run and a handler prints a book built from real UDP datagrams — the whole pipeline exists, badly, before anything is made fast.
- [ ] **M3. A/B redundancy, loss injection, arbitration and gap detection** — Two independent channels carry the same stream, the handler takes whichever datagram lands first, and it knows the difference between 'late' and 'lost'.
- [ ] **M4. Snapshot cycle and TCP replay recovery** — A handler that has fallen behind rejoins the live stream with a correct book instead of being restarted.
- [ ] **M5. MBP and MBO books on an allocation-free path** — Both book views are maintained with zero heap allocations per message, and that is proved by a test rather than asserted in a README.
- [ ] **M6. Benchmark harness, tuning, and an honest report** — The advertised 1M+ msg/s and ~100ns decode are measured, reproducible, and published with the methodology that makes them mean something.
- [ ] **M7. C++ FIX 4.4 gateway — the session layer** — A FIX session that survives a hard kill: correct sequence numbers, correct resend, correct gap fill, verified against an independent implementation.
- [ ] **M8. Risk service, order path into the engine, and restart reconciliation** — An order crosses the whole stack — gateway to risk to engine to fill to execution report — and open order state is reconstructed correctly after a hard crash.
- [ ] **M9. Hardening, documentation, and a tagged release** — A stranger clones the repo on a clean machine and the four commands on the portfolio page work in order.

## Running it

Not yet runnable — see the roadmap above. Build instructions land with M2.

## Licence

MIT — see [LICENSE](LICENSE).
