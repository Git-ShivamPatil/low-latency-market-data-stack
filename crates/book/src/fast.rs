//! Every symbol's fast book.
//!
//! The counterpart to [`Books`](crate::reference::Books), and deliberately not a
//! `BTreeMap`: a map of symbol to book costs a tree descent on every single
//! message, before any book work happens at all. There are a handful of symbols,
//! so this is a sorted `Vec` — a binary search over three or four cache-resident
//! entries, and for the common case of one symbol, a single comparison.
//!
//! # First touch allocates; steady state does not
//!
//! [`get_or_create`](FastBooks::get_or_create) builds a book the first time it
//! sees a symbol, which allocates several megabytes of levels, slab and map.
//! That is startup, and startup is explicitly outside the claim this milestone
//! makes — see the module docs of `alloc-guard` for where that boundary sits and
//! why it sits there.
//!
//! What matters is that it happens **once per symbol, ever**. It is not a
//! high-water-mark that can be exceeded later: after the first message for each
//! symbol, no path through this type touches the heap. Callers that want the
//! cost paid up front — the allocation test does — use
//! [`with_symbols`](FastBooks::with_symbols).

use crate::mbo::{MboBook, MboCapacity, MboStats};
use crate::view::BookSet;

#[derive(Debug)]
pub struct FastBooks {
    /// Kept sorted by symbol id, so iteration is symbol order for free — which
    /// the digest requires and which a `HashMap` would not give.
    books: Vec<(u16, MboBook)>,
    /// Used for a symbol that arrives without having been configured. Real
    /// feeds do introduce symbols mid-session, and refusing one outright would
    /// be worse than sizing it from a default and saying so.
    fallback: MboCapacity,
}

impl FastBooks {
    pub fn new(fallback: MboCapacity) -> Self {
        Self {
            books: Vec::with_capacity(16),
            fallback,
        }
    }

    /// Builds every configured symbol's book up front, each at its own size.
    ///
    /// Per-symbol rather than one shared capacity, because the window is
    /// anchored on a reference price in tick units: a single window sized for
    /// one instrument is wrong for every other one, and "wrong" here means a
    /// rebase on the first message.
    ///
    /// Use this when the allocation has to happen before a measured window
    /// rather than on the first message inside it.
    pub fn with_symbols(symbols: &[(u16, MboCapacity)], fallback: MboCapacity) -> Self {
        let mut set = Self::new(fallback);
        for (symbol, capacity) in symbols {
            match set.books.binary_search_by_key(symbol, |(s, _)| *s) {
                Ok(_) => {}
                Err(i) => set.books.insert(i, (*symbol, MboBook::new(*capacity))),
            }
        }
        set
    }

    /// Every symbol at the same size. For tests and for a single-instrument run.
    pub fn uniform(symbols: &[u16], capacity: MboCapacity) -> Self {
        let mut set = Self::new(capacity);
        for symbol in symbols {
            set.get_or_create(*symbol);
        }
        set
    }

    /// The size a symbol gets when it was not configured.
    pub fn fallback_capacity(&self) -> MboCapacity {
        self.fallback
    }

    pub fn symbols(&self) -> usize {
        self.books.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (u16, &MboBook)> {
        self.books.iter().map(|(s, b)| (*s, b))
    }

    /// Summed across every symbol, for the handler's end-of-run report.
    pub fn stats(&self) -> MboStats {
        let mut out = MboStats::default();
        for (_, book) in &self.books {
            let s = book.stats();
            out.rebases += s.rebases;
            out.extra_probes += s.extra_probes;
            out.lookups += s.lookups;
            out.peak_orders = out.peak_orders.max(s.peak_orders);
        }
        out
    }
}

impl BookSet for FastBooks {
    type Book = MboBook;

    fn get_or_create(&mut self, symbol_id: u16) -> &mut MboBook {
        match self.books.binary_search_by_key(&symbol_id, |(s, _)| *s) {
            Ok(i) => &mut self.books[i].1,
            Err(i) => {
                // First touch for this symbol. Allocates, once, ever.
                self.books
                    .insert(i, (symbol_id, MboBook::new(self.fallback)));
                &mut self.books[i].1
            }
        }
    }

    fn get(&self, symbol_id: u16) -> Option<&MboBook> {
        self.books
            .binary_search_by_key(&symbol_id, |(s, _)| *s)
            .ok()
            .map(|i| &self.books[i].1)
    }

    fn for_each_symbol(&self, f: &mut dyn FnMut(u16, &MboBook)) {
        for (symbol_id, book) in &self.books {
            f(*symbol_id, book);
        }
    }

    fn clear_all(&mut self) {
        for (_, book) in &mut self.books {
            book.clear();
        }
    }

    fn clear_symbol(&mut self, symbol_id: u16) {
        if let Ok(i) = self.books.binary_search_by_key(&symbol_id, |(s, _)| *s) {
            self.books[i].1.clear();
        }
    }

    fn total_orders(&self) -> usize {
        self.books.iter().map(|(_, b)| b.len()).sum()
    }

    fn check_invariants(&self) -> Result<(), String> {
        for (symbol_id, book) in &self.books {
            book.check_invariants()
                .map_err(|e| format!("symbol {symbol_id}: {e}"))?;
        }
        Ok(())
    }
}

/// A capacity that suits the shipped generator and smoke test.
///
/// Sized against what `matching-engine` actually produces — a mid price that
/// random-walks around 1,000,000 in ticks of 100 — rather than against a round
/// number. 4096 levels is a ±204,800 window either side of the anchor, and the
/// generator's walk does not cover a tenth of that in a smoke run.
pub fn default_capacity() -> MboCapacity {
    MboCapacity::new(1_000_000, 100)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire::Side;

    #[test]
    fn symbols_are_visited_in_id_order_whatever_order_they_arrived_in() {
        // Not cosmetic: the digest hashes symbols in the order it is handed
        // them, so two processes that saw the same symbols in a different order
        // would disagree about an identical book.
        let mut set = FastBooks::new(default_capacity());
        for symbol in [9u16, 1, 5, 3] {
            set.get_or_create(symbol)
                .add(u64::from(symbol), Side::Bid, 1_000_000, 1)
                .unwrap();
        }
        let mut seen = Vec::new();
        set.for_each_symbol(&mut |symbol, _| seen.push(symbol));
        assert_eq!(seen, vec![1, 3, 5, 9]);
        assert_eq!(set.total_orders(), 4);
        set.check_invariants().unwrap();
    }

    #[test]
    fn a_symbol_is_created_once_and_found_thereafter() {
        let mut set = FastBooks::new(default_capacity());
        set.get_or_create(7)
            .add(1, Side::Bid, 1_000_000, 1)
            .unwrap();
        set.get_or_create(7)
            .add(2, Side::Bid, 1_000_000, 1)
            .unwrap();
        assert_eq!(set.symbols(), 1);
        assert_eq!(set.get(7).unwrap().len(), 2);
        assert!(set.get(8).is_none());
    }

    #[test]
    fn clear_all_empties_symbols_the_cycle_never_mentions() {
        // A snapshot cycle replaces the whole set. A symbol that has gone away
        // simply stops appearing, so clearing only what the cycle names would
        // leave it resting forever.
        let mut set = FastBooks::new(default_capacity());
        set.get_or_create(1)
            .add(1, Side::Bid, 1_000_000, 1)
            .unwrap();
        set.get_or_create(2)
            .add(2, Side::Bid, 1_000_000, 1)
            .unwrap();
        set.clear_all();
        assert_eq!(set.total_orders(), 0);
        assert_eq!(set.symbols(), 2, "the books stay, keeping their memory");
        set.check_invariants().unwrap();
    }

    #[test]
    fn preconfigured_symbols_pay_their_allocation_up_front() {
        let set = FastBooks::uniform(&[1, 2, 3], default_capacity());
        assert_eq!(set.symbols(), 3);
        assert_eq!(set.total_orders(), 0);
    }
}
