//! Order-book views.
//!
//! What lives here today is the **reference book**: a deliberately obvious
//! `BTreeMap`-of-`VecDeque` implementation whose only goal is to be correct and
//! easy to read. It is not fast, and it is not meant to be — a `HashMap<OrderId>`
//! over a `BTreeMap<Price>` lands at 400ns–1µs per update, which is two to five
//! times over the advertised ~200ns budget.
//!
//! That is not a mistake being deferred. The fast books arrive in milestone 5 as
//! separate types — MBP as a dense array indexed by tick offset from a rebasing
//! anchor, MBO as slab-allocated nodes with an open-addressed order-id map and
//! intrusive per-level FIFO lists — and this one **stays**, as the oracle they
//! are differentially tested against. A fast book that agrees with an obviously
//! correct book over millions of random operations is a much stronger claim than
//! a fast book that merely passes its own unit tests.
//!
//! Both the matching engine and the feed handler use this type in milestone 2:
//! the engine matches against it, the handler rebuilds it from the feed, and
//! `scripts/smoke.sh` requires their [`BookDigest`]s to agree at the same
//! sequence number. That reconciliation is the whole point of the milestone —
//! it is what proves the feed faithfully describes what the engine did.

pub mod apply;
pub mod digest;
pub mod digestlog;
pub mod reference;

pub use apply::{apply_message, ApplyError};
pub use digest::{BookDigest, DIGEST_DEPTH};
pub use digestlog::DigestLog;
pub use reference::{BookError, Books, Level, ReferenceBook, RestingOrder};

/// Re-exported so downstream crates get the codec and the books from one place.
pub use wire;
