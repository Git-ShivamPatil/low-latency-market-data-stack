//! Order-book views. **Empty until milestone 5** — this crate exists now so the
//! workspace shape is fixed and `wire` has a consumer to keep it honest.
//!
//! What lands here, and why the shape is decided before the code is written:
//!
//! - **MBP** (market by price) as a dense array indexed by tick offset from a
//!   rebasing anchor. Not a `BTreeMap`. The advertised ~200ns book update is a
//!   data-structure decision rather than a tuning pass: a `HashMap<OrderId>`
//!   over a `BTreeMap<Price>` lands at 400ns–1µs and no amount of tuning
//!   rescues it, so building it the obvious way first would mean rewriting it
//!   in milestone 6.
//! - **MBO** (market by order) as slab-allocated nodes, an open-addressed
//!   order-id map, and intrusive per-level FIFO lists to preserve price-time
//!   priority without chasing a `Vec` per level.
//!
//! Both are fed from [`wire`] decoders, so nothing is copied out of the
//! datagram on the way in.

#![doc(test(attr(deny(warnings))))]

/// Re-exported so downstream crates get the codec and the books from one place.
pub use wire;
