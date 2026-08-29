//! Mapping a price to a dense array index.
//!
//! # Why not a map
//!
//! The advertised ~200ns book update is a data-structure decision, not a tuning
//! pass. A `BTreeMap<Price>` costs a pointer-chasing descent and a cache miss per
//! level touched, and lands somewhere between 400ns and 1µs; no amount of tuning
//! rescues that. An array indexed by `(price - anchor) / tick` is one
//! multiplication and one load.
//!
//! # The anchor, and why it moves
//!
//! Prices are unbounded but a book is not: everything that matters sits within a
//! few hundred ticks of the touch. So the array covers a *window*, `anchor` is
//! the price at index 0, and a price outside the window forces a **rebase** —
//! the window slides and the occupied entries move with it.
//!
//! Rebasing is O(capacity) and would be ruinous per message. It is not: a window
//! wide enough to hold a day's range is rebased when the market genuinely walks
//! out of it, which on a real instrument is a handful of times a session. The
//! cost that matters is the per-message one, and that is the multiply.
//!
//! What a rebase must never do is *lose* a level. [`TickIndex::rebase_for`]
//! reports whether the new window can still hold everything occupied, and the
//! caller refuses the price rather than silently dropping the far side of the
//! book.

/// A price window: `capacity` slots of `tick` size starting at `anchor`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TickIndex {
    anchor: i64,
    tick: i64,
    capacity: usize,
}

/// What happened when a price was looked up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Slot {
    /// The price maps to this index in the current window.
    At(usize),
    /// The price is outside the window. The window must be rebased first, or
    /// the price refused.
    Outside,
    /// The price is not on the tick grid. A feed that sends one is broken, and
    /// rounding it would put an order at a price nobody quoted.
    OffGrid,
}

impl TickIndex {
    /// `anchor` is centred: the window runs `capacity/2` ticks either side.
    pub fn centred_on(price: i64, tick: i64, capacity: usize) -> Self {
        let tick = tick.max(1);
        let capacity = capacity.max(2);
        let half = (capacity / 2) as i64;
        Self {
            anchor: price - half * tick,
            tick,
            capacity,
        }
    }

    pub fn anchor(&self) -> i64 {
        self.anchor
    }

    pub fn tick(&self) -> i64 {
        self.tick
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The price at `index`.
    pub fn price_at(&self, index: usize) -> i64 {
        self.anchor + (index as i64) * self.tick
    }

    /// Where `price` lives in the current window.
    #[inline]
    pub fn slot(&self, price: i64) -> Slot {
        let offset = price - self.anchor;
        if offset % self.tick != 0 {
            return Slot::OffGrid;
        }
        let index = offset / self.tick;
        if index < 0 || index as usize >= self.capacity {
            return Slot::Outside;
        }
        Slot::At(index as usize)
    }

    /// Whether `price` is inside the window.
    #[inline]
    pub fn contains(&self, price: i64) -> bool {
        matches!(self.slot(price), Slot::At(_))
    }

    /// A window of the same shape, moved so it contains `price` **and**
    /// everything already occupied.
    ///
    /// `keep` is the lowest and highest occupied price, or `None` for an empty
    /// book. Returns the shift in slots: positive means everything moves *down*
    /// in index by that many, negative means up. `None` when no placement works
    /// — either `price` is off the tick grid, which no shift can fix, or the
    /// span that would have to be covered is wider than the window.
    ///
    /// # Why this takes `keep`
    ///
    /// The obvious rebase re-centres on the new price. It is also close to
    /// useless: with anything resting near the middle of the window, re-centring
    /// on a price one tick outside it throws the far half of the book out of
    /// range, so the move gets refused and the price is rejected — even though a
    /// one-slot slide would have fitted everything comfortably.
    ///
    /// So the window is placed over the *span it has to cover* rather than over
    /// the new price, and centred within whatever slack is left. That succeeds
    /// whenever success is arithmetically possible, and it still leaves headroom
    /// on both sides so the next tick outside does not immediately force another
    /// move.
    pub fn rebase_for(&self, price: i64, keep: Option<(i64, i64)>) -> Option<(Self, i64)> {
        if (price - self.anchor) % self.tick != 0 {
            return None;
        }
        let (low, high) = match keep {
            Some((lo, hi)) => (lo.min(price), hi.max(price)),
            None => (price, price),
        };
        let span = (high - low) / self.tick + 1;
        if span > self.capacity as i64 {
            // Genuinely does not fit. Widening the window is a resize, not a
            // rebase, and it allocates — so the caller is told no.
            return None;
        }
        // `(slack + 1) / 2` rather than `slack / 2` so that the single-price
        // case lands where `centred_on` would put it. Two functions that both
        // claim to centre a window should not differ by a slot.
        let slack = self.capacity as i64 - span;
        let anchor = low - ((slack + 1) / 2) * self.tick;
        Some((Self { anchor, ..*self }, (anchor - self.anchor) / self.tick))
    }

    /// Whether a shift of `shift` slots keeps `[lowest, highest]` in range.
    ///
    /// The caller passes the occupied extent it must not lose. A rebase that
    /// would push a live price out of the window is refused rather than
    /// performed, because the alternative is a book that quietly forgets its far
    /// side — and a book that is wrong in a way nothing reports is worse than a
    /// message that is rejected.
    pub fn shift_preserves(
        &self,
        shift: i64,
        lowest: Option<usize>,
        highest: Option<usize>,
    ) -> bool {
        let (Some(lo), Some(hi)) = (lowest, highest) else {
            // Nothing occupied, so nothing to lose.
            return true;
        };
        let new_lo = lo as i64 - shift;
        let new_hi = hi as i64 - shift;
        new_lo >= 0 && (new_hi as usize) < self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn idx() -> TickIndex {
        // 1000 ticks of 100 units, centred on 1_000_000: covers 950_000..1_049_900.
        TickIndex::centred_on(1_000_000, 100, 1000)
    }

    #[test]
    fn the_centre_price_lands_in_the_middle() {
        let t = idx();
        assert_eq!(t.slot(1_000_000), Slot::At(500));
        assert_eq!(t.price_at(500), 1_000_000);
    }

    #[test]
    fn prices_map_to_adjacent_slots_one_tick_apart() {
        let t = idx();
        assert_eq!(t.slot(1_000_100), Slot::At(501));
        assert_eq!(t.slot(999_900), Slot::At(499));
    }

    #[test]
    fn the_window_edges_are_inside_and_one_past_is_not() {
        let t = idx();
        assert_eq!(t.slot(t.anchor()), Slot::At(0));
        assert_eq!(t.slot(t.price_at(999)), Slot::At(999));
        assert_eq!(t.slot(t.anchor() - t.tick()), Slot::Outside);
        assert_eq!(t.slot(t.price_at(1000)), Slot::Outside);
    }

    #[test]
    fn a_price_off_the_tick_grid_is_refused_not_rounded() {
        // Rounding would put an order at a price nobody quoted, and the book
        // would then disagree with the publisher about where it rests.
        let t = idx();
        assert_eq!(t.slot(1_000_050), Slot::OffGrid);
        assert_eq!(t.slot(1_000_001), Slot::OffGrid);
    }

    #[test]
    fn rebasing_an_empty_window_centres_it_on_the_new_price() {
        let t = idx();
        let far = 2_000_000;
        assert_eq!(t.slot(far), Slot::Outside);
        let (moved, shift) = t.rebase_for(far, None).unwrap();
        assert_eq!(moved.slot(far), Slot::At(500));
        assert_eq!(moved.tick(), t.tick());
        assert_eq!(moved.capacity(), t.capacity());
        assert_eq!(shift, (moved.anchor() - t.anchor()) / t.tick());
    }

    #[test]
    fn rebasing_slides_just_far_enough_to_keep_what_is_occupied() {
        // The case that makes rebasing useful at all. Re-centring on the new
        // price would throw the occupied range out of the window and the move
        // would have to be refused; sliding over the span keeps everything.
        let t = idx(); // 1000 ticks of 100 from 950_000
        let occupied = (999_000, 1_001_000);
        let just_outside = t.price_at(t.capacity() - 1) + t.tick();

        let (moved, _shift) = t.rebase_for(just_outside, Some(occupied)).unwrap();
        assert!(moved.contains(just_outside), "the new price has to fit");
        assert!(
            moved.contains(occupied.0),
            "and so does the lowest occupied"
        );
        assert!(moved.contains(occupied.1), "and the highest");
    }

    #[test]
    fn a_span_wider_than_the_window_is_refused_rather_than_truncated() {
        let t = idx(); // covers 100_000 units
                       // 1M away from the occupied range: no placement of a 100_000-unit
                       // window covers both, and pretending otherwise would silently drop one.
        assert_eq!(t.rebase_for(2_000_000, Some((999_000, 1_001_000))), None);
    }

    #[test]
    fn rebasing_cannot_fix_an_off_grid_price() {
        let t = idx();
        assert_eq!(t.rebase_for(1_000_050, None), None);
    }

    #[test]
    fn a_shift_that_would_lose_an_occupied_level_is_refused() {
        // The property that stops a rebase quietly forgetting the far side of
        // the book.
        let t = idx();
        // Occupied from slot 10 to slot 990.
        assert!(t.shift_preserves(0, Some(10), Some(990)));
        assert!(t.shift_preserves(9, Some(10), Some(990)));
        assert!(
            !t.shift_preserves(11, Some(10), Some(990)),
            "shifting past the lowest occupied slot must be refused"
        );
        assert!(
            !t.shift_preserves(-10, Some(10), Some(990)),
            "shifting past the highest occupied slot must be refused"
        );
    }

    #[test]
    fn an_empty_book_can_always_be_rebased() {
        let t = idx();
        assert!(t.shift_preserves(10_000, None, None));
    }

    #[test]
    fn negative_prices_work() {
        // Spreads and settlement marks go below zero, and the index arithmetic
        // must not assume otherwise.
        let t = TickIndex::centred_on(0, 100, 1000);
        assert_eq!(t.slot(0), Slot::At(500));
        assert_eq!(t.slot(-100), Slot::At(499));
        assert_eq!(t.slot(-50_000), Slot::At(0));
        assert_eq!(t.price_at(0), -50_000);
    }

    #[test]
    fn every_slot_round_trips_through_its_price() {
        let t = idx();
        for i in 0..t.capacity() {
            assert_eq!(t.slot(t.price_at(i)), Slot::At(i), "slot {i}");
        }
    }
}
