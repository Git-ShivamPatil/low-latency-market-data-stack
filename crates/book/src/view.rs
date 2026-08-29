//! What every book implementation has to offer, so the rest of the system does
//! not care which one it has.
//!
//! There are two books: the [reference](crate::reference) one, which is
//! obviously correct and slow, and the [fast](crate::mbo) one, which is neither
//! obvious nor slow. Everything above them — applying a message, hashing a
//! digest, rebuilding from a snapshot, the feed handler itself — is written once
//! against these traits.
//!
//! That is not tidiness. It is what makes the two claims of this milestone
//! checkable:
//!
//! * **The differential test** runs the same operation stream through both and
//!   requires identical digests. That test can only exist if "the same
//!   operation stream" means something, which requires one interface.
//! * **`--books reference|fast`** lets the smoke test reconcile a fast-book
//!   handler against a reference-book engine across a process boundary. If the
//!   handler had its own bespoke apply path, a disagreement would not tell you
//!   whether the book or the path was wrong.
//!
//! # Why the callbacks
//!
//! [`OrderBook::for_each_level`] takes a callback rather than returning a `Vec`
//! or an iterator. A `Vec` allocates, which is the whole thing this milestone is
//! trying not to do. An iterator would be nicer to use and would need either a
//! named type per implementation or a `Box<dyn Iterator>` — and the box
//! allocates too, on the same path.

use wire::Side;

use crate::reference::{BookError, Level, RestingOrder};

/// One symbol's book.
pub trait OrderBook {
    fn add(
        &mut self,
        order_id: u64,
        side: Side,
        price: i64,
        quantity: u32,
    ) -> Result<(), BookError>;

    fn delete(&mut self, order_id: u64) -> Result<RestingOrder, BookError>;

    fn reduce(&mut self, order_id: u64, new_quantity: u32) -> Result<(), BookError>;

    fn replace(
        &mut self,
        order_id: u64,
        new_price: i64,
        new_quantity: u32,
    ) -> Result<(), BookError>;

    fn get(&self, order_id: u64) -> Option<RestingOrder>;

    /// Resting orders.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Occupied levels on a side, capped at `depth` when `depth` is non-zero.
    ///
    /// Separate from [`for_each_level`](Self::for_each_level) because the digest
    /// writes a count before the levels, and a second walk to produce it would
    /// double the cost of every checkpoint.
    fn level_count(&self, side: Side, depth: usize) -> usize;

    /// Aggregated levels, best first. `depth` of 0 means every level.
    fn for_each_level(&self, side: Side, depth: usize, f: &mut dyn FnMut(Level));

    /// Every resting order on a side, in the order an aggressor would match
    /// them: best price first, oldest first within a price.
    ///
    /// This is the order a snapshot has to be written in — it is what lets an
    /// order-level snapshot restore price-time priority exactly.
    fn for_each_order(&self, side: Side, f: &mut dyn FnMut(RestingOrder) -> bool);

    /// Empties the book, keeping whatever it has allocated.
    fn clear(&mut self);

    /// Cross-checks the redundant indexes against each other. Tests only.
    fn check_invariants(&self) -> Result<(), String>;
}

/// Every symbol's book, keyed by the `symbolId` on the wire.
pub trait BookSet {
    type Book: OrderBook;

    fn get_or_create(&mut self, symbol_id: u16) -> &mut Self::Book;

    fn get(&self, symbol_id: u16) -> Option<&Self::Book>;

    /// Visits every symbol **in symbol-id order**.
    ///
    /// The order is part of the contract, not an implementation detail: the
    /// digest hashes symbols in the order it is handed them, and two processes
    /// that visit them differently would disagree about an identical book.
    fn for_each_symbol(&self, f: &mut dyn FnMut(u16, &Self::Book));

    /// Empties every book, keeping allocated capacity.
    ///
    /// A snapshot *cycle* replaces the whole set, not one symbol: a symbol that
    /// has gone away since the last cycle simply stops appearing, and clearing
    /// only the symbols the cycle mentions would leave it resting forever.
    fn clear_all(&mut self);

    fn clear_symbol(&mut self, symbol_id: u16);

    fn total_orders(&self) -> usize;

    fn check_invariants(&self) -> Result<(), String>;
}
