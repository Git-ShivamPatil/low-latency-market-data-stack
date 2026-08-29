//! The measurement apparatus, and the gate that decides whether its output may
//! be published.
//!
//! # The problem this crate exists to solve
//!
//! Milestone 6 is the one this project is pointed at: the advertised
//! `1M+ msg/s` and `~100ns decode` measured, reproducible, and published with
//! the methodology that makes them mean something. It is also the one milestone
//! the development machine cannot complete — a 2-core i3 under WSL2 cannot
//! produce those numbers, and a number it produced anyway would be worse than no
//! number at all.
//!
//! So the apparatus is built and verified here, and the measurement happens
//! elsewhere. That split is the whole design:
//!
//! - [`histogram`] — bounded relative error, allocation-free recording, exact
//!   quantile arithmetic. Testable anywhere, and tested here.
//! - [`tsc`] — the cycle counter, its calibration, and its refusal to pretend a
//!   masked TSC is a clock.
//! - [`hostcheck`] — what the host actually is, and whether a number from it may
//!   be published.
//!
//! # Why the refusal is code
//!
//! The portfolio page already advertises the figure. The way that becomes a
//! false claim is not dishonesty; it is a benchmark that ran once on a laptop,
//! printed something plausible, and got quoted later by someone who no longer
//! remembers where it came from. [`hostcheck::Verdict`] makes that a mechanical
//! refusal rather than a matter of remembering.
//!
//! Nothing here refuses to *run*. Exercising the paths is useful on any machine
//! — it is how the harness itself is debugged. What it refuses to do is write
//! something that looks like a result.

pub mod histogram;
pub mod hostcheck;
pub mod tsc;

pub use histogram::Histogram;
pub use hostcheck::{report_header, HostFacts, Verdict};
pub use tsc::{Tsc, TscQuality};
