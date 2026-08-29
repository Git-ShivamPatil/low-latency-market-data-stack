//! A reproducible fingerprint of book state.
//!
//! The engine and the feed handler are separate processes that arrive at a book
//! by completely different routes — one by matching orders, the other by
//! replaying the feed those matches produced. If the feed is a faithful
//! description of what the engine did, the two books are identical at the same
//! sequence number. Comparing them field by field across a process boundary is
//! awkward; comparing one number is not.
//!
//! Two digests are taken rather than one. `top` covers the best
//! [`DIGEST_DEPTH`] levels of each side, which is what the milestone asks for
//! and what a consumer actually trades on. `full` covers every level, because
//! divergence deep in the book is still divergence — it just has not reached the
//! touch yet, and a top-of-book-only check would report agreement right up until
//! the moment it became expensive.
//!
//! FNV-1a rather than a cryptographic hash: this is a corruption check between
//! two processes that are trying to agree, not a defence against an adversary
//! constructing a collision. It is also stable forever, which a `DefaultHasher`
//! explicitly is not — `std`'s hasher is allowed to change between releases and
//! is randomly seeded per process, so two processes would disagree by design.

use std::fmt;

use wire::Side;

use crate::view::{BookSet, OrderBook};

/// Levels per side covered by the `top` digest.
pub const DIGEST_DEPTH: usize = 10;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Debug, Clone, Copy, Default)]
struct Fnv1a(u64);

impl Fnv1a {
    fn new() -> Self {
        Self(FNV_OFFSET)
    }

    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(FNV_PRIME);
        }
    }

    fn finish(self) -> u64 {
        self.0
    }
}

/// A book fingerprint at one point in the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookDigest {
    /// Over the best [`DIGEST_DEPTH`] levels of each side.
    pub top: u64,
    /// Over every level.
    pub full: u64,
    /// Resting orders across all symbols. Not a hash — a plain count, because
    /// when the hashes disagree this is the first thing worth knowing.
    pub orders: u64,
}

impl BookDigest {
    /// Generic over the book implementation, and allocation-free for both.
    ///
    /// It has to be allocation-free: the handler takes a digest at checkpoints
    /// *during* the steady-state window that the zero-allocation claim covers,
    /// so a digest that built a `Vec` of levels would break the claim at every
    /// hundredth message. That is why [`OrderBook::for_each_level`] takes a
    /// callback and why [`OrderBook::level_count`] exists separately.
    pub fn of<B: BookSet + ?Sized>(books: &B) -> Self {
        Self {
            top: hash_books(books, DIGEST_DEPTH),
            full: hash_books(books, 0),
            orders: books.total_orders() as u64,
        }
    }

    /// `top full orders`, hex for the hashes. Parsed back by [`from_fields`].
    ///
    /// [`from_fields`]: BookDigest::from_fields
    pub fn to_fields(self) -> String {
        format!("{:016x} {:016x} {}", self.top, self.full, self.orders)
    }

    pub fn from_fields(s: &str) -> Option<Self> {
        let mut it = s.split_whitespace();
        let top = u64::from_str_radix(it.next()?, 16).ok()?;
        let full = u64::from_str_radix(it.next()?, 16).ok()?;
        let orders = it.next()?.parse().ok()?;
        if it.next().is_some() {
            return None;
        }
        Some(Self { top, full, orders })
    }
}

impl fmt::Display for BookDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "top={:016x} full={:016x} orders={}",
            self.top, self.full, self.orders
        )
    }
}

fn hash_books<B: BookSet + ?Sized>(books: &B, depth: usize) -> u64 {
    let mut h = Fnv1a::new();
    // `BookSet` promises symbol-id order, so no sorting is needed to make this
    // deterministic across processes — or across the two implementations.
    books.for_each_symbol(&mut |symbol_id, book| {
        // A symbol that is present but empty hashes as if it were absent.
        // The two implementations differ on when a book springs into existence
        // — the reference set creates one on first touch, the fast set can
        // pre-allocate every symbol up front — and that is a memory-management
        // decision, not book state. A digest that could tell them apart would
        // report the engine and the handler as diverged when both hold nothing.
        if book.is_empty() {
            return;
        }
        h.write(&symbol_id.to_le_bytes());
        for side in [Side::Bid, Side::Ask] {
            h.write(&[side as u8]);
            h.write(&(book.level_count(side, depth) as u32).to_le_bytes());
            book.for_each_level(side, depth, &mut |level| {
                h.write(&level.price.to_le_bytes());
                h.write(&level.quantity.to_le_bytes());
                h.write(&level.order_count.to_le_bytes());
            });
        }
    });
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reference::Books;

    fn books_with(entries: &[(u16, u64, Side, i64, u32)]) -> Books {
        let mut books = Books::new();
        for (symbol, id, side, price, qty) in entries {
            books
                .get_or_create(*symbol)
                .add(*id, *side, *price, *qty)
                .unwrap();
        }
        books
    }

    #[test]
    fn the_same_book_built_in_a_different_order_digests_the_same() {
        // This is the property the whole reconciliation rests on: the engine and
        // the handler reach the same state by different routes.
        let a = books_with(&[
            (1, 10, Side::Bid, 100, 5),
            (1, 11, Side::Ask, 105, 7),
            (2, 12, Side::Bid, 200, 1),
        ]);
        let b = books_with(&[
            (2, 12, Side::Bid, 200, 1),
            (1, 11, Side::Ask, 105, 7),
            (1, 10, Side::Bid, 100, 5),
        ]);
        assert_eq!(BookDigest::of(&a), BookDigest::of(&b));
    }

    #[test]
    fn a_quantity_change_anywhere_changes_the_digest() {
        let a = books_with(&[(1, 10, Side::Bid, 100, 5)]);
        let b = books_with(&[(1, 10, Side::Bid, 100, 6)]);
        assert_ne!(BookDigest::of(&a), BookDigest::of(&b));
    }

    #[test]
    fn a_side_swap_changes_the_digest() {
        let a = books_with(&[(1, 10, Side::Bid, 100, 5)]);
        let b = books_with(&[(1, 10, Side::Ask, 100, 5)]);
        assert_ne!(BookDigest::of(&a), BookDigest::of(&b));
    }

    #[test]
    fn two_orders_at_one_level_differ_from_one_order_of_the_same_size() {
        // Aggregating only quantity would make these look identical, which
        // would hide a whole class of feed bug.
        let a = books_with(&[(1, 10, Side::Bid, 100, 10)]);
        let b = books_with(&[(1, 10, Side::Bid, 100, 5), (1, 11, Side::Bid, 100, 5)]);
        assert_ne!(BookDigest::of(&a), BookDigest::of(&b));
    }

    #[test]
    fn divergence_below_the_top_depth_shows_up_in_full_but_not_in_top() {
        let mut a = Books::new();
        let mut b = Books::new();
        for i in 0..DIGEST_DEPTH as i64 + 4 {
            let id = i as u64;
            a.get_or_create(1).add(id, Side::Bid, 100 - i, 5).unwrap();
            b.get_or_create(1).add(id, Side::Bid, 100 - i, 5).unwrap();
        }
        // Change a level well below the touch.
        b.get_or_create(1)
            .reduce(DIGEST_DEPTH as u64 + 2, 1)
            .unwrap();

        let da = BookDigest::of(&a);
        let db = BookDigest::of(&b);
        assert_eq!(
            da.top, db.top,
            "the change is deeper than the top digest sees"
        );
        assert_ne!(da.full, db.full, "the full digest still catches it");
    }

    #[test]
    fn an_empty_book_has_a_stable_digest() {
        // Pinned so an accidental change to the encoding is caught here rather
        // than as an unexplained smoke-test failure across two processes.
        let d = BookDigest::of(&Books::new());
        assert_eq!(d.top, FNV_OFFSET);
        assert_eq!(d.full, FNV_OFFSET);
        assert_eq!(d.orders, 0);
    }

    #[test]
    fn fields_round_trip() {
        let d = BookDigest {
            top: 0x0123_4567_89ab_cdef,
            full: 0xfedc_ba98_7654_3210,
            orders: 42,
        };
        assert_eq!(BookDigest::from_fields(&d.to_fields()), Some(d));
        assert_eq!(BookDigest::from_fields("nonsense"), None);
        assert_eq!(BookDigest::from_fields("00 00 1 extra"), None);
    }
}
