//! The replay service: the bit of recovery that gives back the messages
//! themselves rather than the state they produced.
//!
//! # Why this exists when the snapshot cycle already works
//!
//! A snapshot recovers the *book*, which is what most consumers want, and it
//! needs no request-response path at all. But it discards the messages between
//! the gap and the snapshot: the trades that printed in that window, the orders
//! that came and went. A consumer keeping its own audit trail, or driving
//! something downstream from the message stream rather than from the book,
//! cannot get those back from a snapshot.
//!
//! Replay serves exactly that range. It is also faster — a round trip rather
//! than a wait for the next cycle — and it does not require throwing the book
//! away and rebuilding it, because filling the hole is all that was needed.
//!
//! So a consumer tries replay first and falls back to the snapshot cycle when
//! the range is older than the service still holds, or the service is down.
//! Neither mechanism makes the other redundant.
//!
//! # Shape
//!
//! - [`store`] — a bounded, deliberately contiguous ring of published datagrams.
//! - [`protocol`] — the two small binary protocols: the engine's uplink, and a
//!   consumer's range request.
//! - [`client`] — the consumer side, which runs off the receive loop's thread.
//!
//! The server binary is `src/main.rs`.

pub mod client;
pub mod protocol;
pub mod store;

pub use client::{request, request_in_background, ReplayResult};
pub use protocol::{RangeRequest, ResponseHeader, Status};
pub use store::{DatagramStore, StoreStats};
