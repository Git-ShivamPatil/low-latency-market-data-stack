//! The consumer side of the feed.
//!
//! Split into a library and a binary so the arbitration logic can be driven
//! directly by integration tests. The milestone's headline claim — that a
//! datagram lost on one arm costs nothing across ten million messages — is not
//! something to establish by watching two processes and hoping; it needs a
//! deterministic harness that can inject exactly the loss pattern it means to,
//! and that harness needs to import [`arbitration`].

pub mod arbitration;
pub mod recovery;
pub mod stats;
