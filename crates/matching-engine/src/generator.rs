//! Synthetic order flow.
//!
//! This is not a market simulator and does not try to be one. Its job is to
//! exercise every path through the engine and every message on the wire — rests,
//! crossings that fully fill, crossings that partially fill, cancels, size-downs
//! that keep queue priority and re-prices that lose it — while keeping the book
//! at a stable depth so a long run neither empties out nor grows without bound.
//!
//! It is driven entirely by [`Rng`], so the same seed produces the same stream
//! on every run and every machine. That matters more than realism: a smoke test
//! that reconciles two processes is only useful if a disagreement can be
//! reproduced rather than shrugged at.

use wire::Side;

use crate::engine::SymbolState;
use crate::rng::Rng;

/// What the generator wants to happen next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Intent {
    Submit {
        symbol_id: u16,
        side: Side,
        price: i64,
        quantity: u32,
    },
    Cancel {
        symbol_id: u16,
        order_id: u64,
    },
    Amend {
        symbol_id: u16,
        order_id: u64,
        /// `None` means leave the price alone, which is what lets the engine
        /// choose the priority-preserving `Reduce` rather than a `Replace`.
        new_price: Option<i64>,
        new_quantity: u32,
    },
}

/// The knobs the flow is shaped by. A trimmed copy of the engine config section,
/// so the generator does not depend on the whole configuration type.
#[derive(Debug, Clone, Copy)]
pub struct Shape {
    pub target_depth: usize,
    pub price_spread_ticks: i64,
    pub aggressive_chance: f64,
    pub cancel_chance: f64,
    pub modify_chance: f64,
    pub min_quantity: u32,
    pub max_quantity: u32,
}

impl From<&mdconfig::Engine> for Shape {
    fn from(e: &mdconfig::Engine) -> Self {
        Self {
            target_depth: e.target_depth,
            price_spread_ticks: e.price_spread_ticks.max(1),
            aggressive_chance: e.aggressive_chance,
            cancel_chance: e.cancel_chance,
            modify_chance: e.modify_chance,
            min_quantity: e.min_quantity.max(1),
            max_quantity: e.max_quantity.max(1),
        }
    }
}

pub struct Generator {
    rng: Rng,
    shape: Shape,
}

impl Generator {
    pub fn new(seed: u64, shape: Shape) -> Self {
        Self {
            rng: Rng::new(seed),
            shape,
        }
    }

    /// Picks which symbol to act on next.
    pub fn pick_symbol(&mut self, symbols: usize) -> usize {
        self.rng.below(symbols as u64) as usize
    }

    /// Decides the next action for one symbol.
    ///
    /// Takes the symbol's state by shared reference so the decision can depend
    /// on how full the book already is — that feedback is what keeps depth
    /// stable instead of drifting to empty or unbounded.
    pub fn next(&mut self, sym: &SymbolState) -> Intent {
        let resting = sym.live.len();
        let symbol_id = sym.id;

        let mid = sym.mid;

        // Thin book: only add, so a run that has cancelled itself empty recovers.
        let starving = resting < self.shape.target_depth / 2;
        // Overfull book: bias hard towards removing, so it cannot grow forever.
        let crowded = resting > self.shape.target_depth * 2;

        if !starving && resting > 0 {
            let cancel_p = if crowded {
                // Enough to dominate the adds and pull depth back down.
                0.75
            } else {
                self.shape.cancel_chance
            };
            if self.rng.chance(cancel_p) {
                let idx = self.rng.below(resting as u64) as usize;
                return Intent::Cancel {
                    symbol_id,
                    order_id: sym.live[idx],
                };
            }
            if self.rng.chance(self.shape.modify_chance) {
                let idx = self.rng.below(resting as u64) as usize;
                let order_id = sym.live[idx];
                let new_quantity = self.quantity();
                // Half the time keep the price, which lets the engine choose
                // Reduce and exercise the priority-preserving path.
                let new_price = if self.rng.chance(0.5) {
                    None
                } else {
                    Some(self.price_near(mid, sym.tick_size))
                };
                return Intent::Amend {
                    symbol_id,
                    order_id,
                    new_price,
                    new_quantity,
                };
            }
        }

        let side = if self.rng.chance(0.5) {
            Side::Bid
        } else {
            Side::Ask
        };
        let aggressive = !starving && self.rng.chance(self.shape.aggressive_chance);
        let price = if aggressive {
            // Reach across the spread far enough to cross whatever is resting.
            let reach = self.rng.range_inclusive(1, 3) as i64 * sym.tick_size;
            match side {
                Side::Bid => mid + reach,
                Side::Ask => mid - reach,
            }
        } else {
            self.passive_price(mid, sym.tick_size, side)
        };

        Intent::Submit {
            symbol_id,
            side,
            price,
            quantity: self.quantity(),
        }
    }

    /// Lets the mid wander a tick at a time, so the book does not sit at one
    /// price for the whole run.
    ///
    /// Separate from [`next`](Self::next) and taking `&mut` because the drift
    /// has to be *stored*: computing it inside `next` and dropping it on the
    /// floor would leave the mid pinned forever while looking like it moved.
    pub fn drift_mid(&mut self, sym: &mut SymbolState) {
        match self.rng.below(64) {
            0 => sym.mid = sym.mid.saturating_add(sym.tick_size),
            1 => sym.mid = sym.mid.saturating_sub(sym.tick_size),
            _ => {}
        }
    }

    fn quantity(&mut self) -> u32 {
        self.rng.range_inclusive(
            u64::from(self.shape.min_quantity),
            u64::from(self.shape.max_quantity),
        ) as u32
    }

    fn price_near(&mut self, mid: i64, tick: i64) -> i64 {
        let offset = self.rng.below(self.shape.price_spread_ticks as u64 * 2 + 1) as i64
            - self.shape.price_spread_ticks;
        mid + offset * tick
    }

    /// A price on the passive side of the mid, so the order rests rather than
    /// crossing.
    fn passive_price(&mut self, mid: i64, tick: i64, side: Side) -> i64 {
        let ticks = self
            .rng
            .range_inclusive(1, self.shape.price_spread_ticks as u64) as i64;
        match side {
            Side::Bid => mid - ticks * tick,
            Side::Ask => mid + ticks * tick,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape() -> Shape {
        Shape {
            target_depth: 20,
            price_spread_ticks: 5,
            aggressive_chance: 0.2,
            cancel_chance: 0.25,
            modify_chance: 0.15,
            min_quantity: 1,
            max_quantity: 100,
        }
    }

    fn symbol(live: usize) -> SymbolState {
        SymbolState {
            id: 3,
            tick_size: 100,
            mid: 1_000_000,
            live: (1..=live as u64).collect(),
        }
    }

    #[test]
    fn the_same_seed_produces_the_same_intents() {
        let sym = symbol(30);
        let mut a = Generator::new(4242, shape());
        let mut b = Generator::new(4242, shape());
        for _ in 0..2000 {
            assert_eq!(a.next(&sym), b.next(&sym));
        }
    }

    #[test]
    fn an_empty_book_is_only_ever_added_to() {
        // Otherwise a run that cancelled itself flat would emit nothing but
        // cancels for orders that are not there.
        let sym = symbol(0);
        let mut g = Generator::new(1, shape());
        for _ in 0..500 {
            assert!(
                matches!(g.next(&sym), Intent::Submit { .. }),
                "nothing rests, so there is nothing to cancel or amend"
            );
        }
    }

    #[test]
    fn a_crowded_book_is_pushed_back_down() {
        let sym = symbol(shape().target_depth * 3);
        let mut g = Generator::new(2, shape());
        let mut cancels = 0;
        const N: usize = 2000;
        for _ in 0..N {
            if matches!(g.next(&sym), Intent::Cancel { .. }) {
                cancels += 1;
            }
        }
        assert!(
            cancels > N / 2,
            "an overfull book should mostly cancel, saw {cancels} of {N}"
        );
    }

    #[test]
    fn every_intent_names_an_order_that_is_actually_resting() {
        let sym = symbol(12);
        let mut g = Generator::new(3, shape());
        for _ in 0..5000 {
            match g.next(&sym) {
                Intent::Cancel { order_id, .. } | Intent::Amend { order_id, .. } => {
                    assert!(
                        sym.live.contains(&order_id),
                        "generator named order {order_id}, which is not resting"
                    );
                }
                Intent::Submit { .. } => {}
            }
        }
    }

    #[test]
    fn quantities_stay_inside_the_configured_band() {
        let sym = symbol(5);
        let mut g = Generator::new(9, shape());
        for _ in 0..5000 {
            let q = match g.next(&sym) {
                Intent::Submit { quantity, .. } => quantity,
                Intent::Amend { new_quantity, .. } => new_quantity,
                Intent::Cancel { .. } => continue,
            };
            assert!((1..=100).contains(&q), "quantity {q} is outside 1..=100");
        }
    }

    #[test]
    fn the_mid_actually_moves_when_it_is_drifted() {
        let mut sym = symbol(5);
        let start = sym.mid;
        let mut g = Generator::new(17, shape());
        for _ in 0..2000 {
            g.drift_mid(&mut sym);
        }
        assert_ne!(sym.mid, start, "the drift has to be stored, not recomputed");
    }

    #[test]
    fn the_flow_produces_a_mix_rather_than_one_kind_of_intent() {
        let sym = symbol(20);
        let mut g = Generator::new(11, shape());
        let (mut submits, mut cancels, mut amends) = (0, 0, 0);
        for _ in 0..5000 {
            match g.next(&sym) {
                Intent::Submit { .. } => submits += 1,
                Intent::Cancel { .. } => cancels += 1,
                Intent::Amend { .. } => amends += 1,
            }
        }
        assert!(
            submits > 100 && cancels > 100 && amends > 100,
            "expected a mix, saw {submits} submits / {cancels} cancels / {amends} amends"
        );
    }
}
