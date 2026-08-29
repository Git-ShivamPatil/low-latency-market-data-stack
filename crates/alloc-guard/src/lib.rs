//! Allocation accounting. **Empty until milestone 5** — the crate exists now so
//! the workspace shape is fixed.
//!
//! What lands here: a `#[global_allocator]` wrapping the system allocator with
//! atomic counters, plus the assertion helpers behind `--verify-allocations`.
//!
//! The claim this has to defend is "zero heap allocations per message", and it
//! is easy to achieve and easy to silently lose — one `format!` on an error
//! path, one `Vec` that grows during a resend, one `String` in a log line.
//! So the proof has to be a CI assertion over a million messages *including a
//! recovery cycle*, not a flag somebody ran once by hand. Allocation during
//! startup and during snapshot rebuild is expected and is not part of the
//! claim; the counters are sampled around the steady-state loop only.

#![doc(test(attr(deny(warnings))))]
