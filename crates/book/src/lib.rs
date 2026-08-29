//! Order-book views.
//!
//! # Two books, on purpose
//!
//! **[`ReferenceBook`]** is a deliberately obvious `BTreeMap`-of-`VecDeque`
//! whose only goal is to be correct and easy to read. It is not fast and is not
//! meant to be — a `HashMap<OrderId>` over a `BTreeMap<Price>` lands at
//! 400ns–1µs per update, two to five times over the advertised ~200ns budget.
//!
//! **[`MboBook`]** is the fast one, added in milestone 5: a dense price array
//! indexed by tick offset from a rebasing anchor, slab-allocated order nodes, an
//! open-addressed order-id map, and intrusive per-level FIFO lists. It maintains
//! both views at once — market-by-order is the store, market-by-price is the
//! per-level aggregate maintained alongside it — because `DeleteOrder` carries
//! no price, so a price-aggregated book cannot be driven from this feed on its
//! own.
//!
//! The reference book **stays**. It is the oracle: `tests/differential.rs` runs
//! the same operation stream through both and requires identical digests over
//! millions of random operations, which is a far stronger claim than a fast book
//! passing its own unit tests. Keeping the slow one is not technical debt — it
//! is the evidence.
//!
//! # One interface
//!
//! [`view::BookSet`] and [`view::OrderBook`] are what make that comparison
//! possible, and what let the feed handler switch between the two with
//! `--books reference|fast` while every other line of it stays the same. They
//! are also why [`BookDigest`] is allocation-free: the handler digests during
//! the same window the zero-allocation claim covers.
//!
//! # What the digest is for
//!
//! The engine and the handler are separate processes that reach a book by
//! different routes — one by matching, the other by replaying the feed those
//! matches produced. `scripts/smoke.sh` requires their [`BookDigest`]s to agree
//! at the same sequence number. That reconciliation is what proves the feed
//! faithfully describes what the engine did.

pub mod apply;
pub mod digest;
pub mod digestlog;
pub mod fast;
pub mod mbo;
pub mod reference;
pub mod tick;
pub mod view;

pub use apply::{apply_message, apply_snapshot, ApplyError};
pub use digest::{BookDigest, DIGEST_DEPTH};
pub use digestlog::DigestLog;
pub use fast::{default_capacity, FastBooks};
pub use mbo::{MboBook, MboCapacity, MboStats};
pub use reference::{BookError, Books, Level, ReferenceBook, RestingOrder};
pub use tick::{Slot, TickIndex};
pub use view::{BookSet, OrderBook};

/// Re-exported so downstream crates get the codec and the books from one place.
pub use wire;
