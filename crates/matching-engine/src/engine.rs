//! Price-time-priority matching, and the feed that describes it.
//!
//! # One path for a book change
//!
//! Every change to the engine's book goes through [`Engine::publish`], which
//! applies the change and encodes the message describing it, in that order and
//! with nothing in between. That makes "the engine changed its book but forgot
//! to say so" structurally impossible rather than merely tested for.
//!
//! It also fixes the alignment the digest reconciliation depends on: immediately
//! after `publish` hands back sequence `S`, the engine's book is exactly the
//! result of messages `1..=S`. The handler reaches the same state by applying
//! `1..=S`, so a checkpoint at any `S` is comparable across the two processes
//! without either side coordinating with the other.
//!
//! What that deliberately does *not* prove is that the matching logic is right —
//! both ends would agree on a wrong-but-consistent book. Matching correctness is
//! covered by the tests at the bottom of this file, which assert the exact
//! message sequence a given crossing produces. The digest comparison covers the
//! other half: framing, batching, sequencing and transport.

use std::io;

use book::{BookDigest, Books};
use wire::{ModifyReason, Side};

use crate::feed::FeedPublisher;

/// One fill against an order that was just submitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fill {
    pub trade_id: u64,
    /// The resting order's price. The passive side set the terms.
    pub price: i64,
    pub quantity: u32,
    /// Still unfilled after this fill. Reported rather than derived, because a
    /// running total kept at the far end is a second source of truth.
    pub leaves: u32,
}

/// A fill against an order that was already resting on the book.
///
/// The aggressor learns about its fills from [`Engine::submit_reported`], which
/// is the call it made. The *passive* side made its call minutes ago and has
/// been waiting ever since -- so without this, a client whose order rests and is
/// then hit is never told, and finds out only when a reconciliation says the
/// engine no longer holds an order the client still believes in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassiveFill {
    pub resting_order_id: u64,
    pub trade_id: u64,
    pub price: i64,
    pub quantity: u32,
    /// Still resting after this fill. Zero means the order is gone.
    pub leaves: u32,
    pub symbol_id: u16,
    pub side: Side,
}

/// What became of a submitted order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubmitOutcome {
    /// The engine's id for the order. This is what a FIX `OrderID` is.
    pub order_id: u64,
    pub filled: u32,
    /// What is resting on the book. Zero when the order filled completely, in
    /// which case no `AddOrder` was published for it either.
    pub resting: u32,
}

/// A book change and the message that announces it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change {
    Add {
        order_id: u64,
        side: Side,
        price: i64,
        quantity: u32,
    },
    /// A partial fill, or a size-down that keeps queue priority.
    Reduce {
        order_id: u64,
        side: Side,
        price: i64,
        new_quantity: u32,
    },
    /// A re-price or size-up. Loses queue priority.
    Replace {
        order_id: u64,
        side: Side,
        new_price: i64,
        new_quantity: u32,
    },
    Delete {
        order_id: u64,
        side: Side,
    },
    /// Informational. Carries no book change of its own — the fill arrives as
    /// the `Reduce` or `Delete` that follows it.
    Trade {
        trade_id: u64,
        aggressor_order_id: u64,
        resting_order_id: u64,
        price: i64,
        quantity: u32,
        aggressor_side: Side,
    },
}

/// Per-symbol state the generator reads and the engine maintains.
#[derive(Debug)]
pub struct SymbolState {
    pub id: u16,
    pub tick_size: i64,
    /// Random-walks; new orders are placed relative to it.
    pub mid: i64,
    /// Ids resting **that the flow generator is allowed to act on again**, in
    /// insertion order.
    ///
    /// A `Vec` rather than iterating the book's `HashMap`, because `HashMap`
    /// iteration order is randomised per process — picking a victim that way
    /// would make the whole run unreproducible, and reproducibility is what lets
    /// the smoke test tell a bug from a coin flip.
    ///
    /// **A client's order never goes in here.** The generator stands in for
    /// other market participants, and other participants do not get to cancel
    /// your order. When the order path landed, client orders joined this list
    /// and the generator started cancelling them seconds later — which looks
    /// exactly like the exchange losing an order, and was found by the
    /// reconciliation test asking the engine for an order it had just cancelled
    /// on its own initiative.
    pub live: Vec<u64>,
}

impl SymbolState {
    fn remove_live(&mut self, order_id: u64) {
        if let Some(pos) = self.live.iter().position(|id| *id == order_id) {
            self.live.swap_remove(pos);
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct EngineStats {
    pub orders_submitted: u64,
    pub trades: u64,
    pub shares_traded: u64,
    pub cancels: u64,
    pub modifies: u64,
    /// Aggressive orders that crossed and matched at least once.
    pub aggressive_fills: u64,
}

pub struct Engine {
    books: Books,
    pub symbols: Vec<SymbolState>,
    next_order_id: u64,
    next_trade_id: u64,
    stats: EngineStats,
    /// Resting orders somebody wants to be told about, sorted so membership is
    /// a binary search rather than a scan on every fill.
    ///
    /// Empty unless an order path is attached, which is why the engine on its
    /// own pays nothing for this.
    watched: Vec<u64>,
    /// Fills against watched orders, waiting to be collected.
    passive_fills: Vec<PassiveFill>,
    /// Fills against a watched order that did not fit. Non-zero means a client
    /// was not told about a fill, which is the one thing on this path that must
    /// never pass quietly.
    passive_fills_dropped: u64,
}

impl Engine {
    pub fn new(symbols: &[mdconfig::Symbol]) -> Self {
        Self {
            books: Books::new(),
            symbols: symbols
                .iter()
                .map(|s| SymbolState {
                    id: s.id,
                    tick_size: s.tick_size,
                    mid: s.reference_price,
                    live: Vec::new(),
                })
                .collect(),
            // Ids start at 1 so 0 is never a valid order, which makes an
            // uninitialised field obvious rather than plausible.
            next_order_id: 1,
            next_trade_id: 1,
            stats: EngineStats::default(),
            watched: Vec::new(),
            passive_fills: Vec::new(),
            passive_fills_dropped: 0,
        }
    }

    /// Asks to be told when `order_id` is filled while resting.
    ///
    /// Registered by the order path when a client's order rests, and dropped
    /// when it reaches a terminal state. The engine does not otherwise care who
    /// an order belongs to, and should not.
    pub fn watch_order(&mut self, order_id: u64) {
        if let Err(pos) = self.watched.binary_search(&order_id) {
            self.watched.insert(pos, order_id);
        }
    }

    pub fn unwatch_order(&mut self, order_id: u64) {
        if let Ok(pos) = self.watched.binary_search(&order_id) {
            self.watched.remove(pos);
        }
    }

    /// Hands over every passive fill recorded since the last call.
    pub fn take_passive_fills(&mut self, mut f: impl FnMut(PassiveFill)) {
        for fill in self.passive_fills.drain(..) {
            f(fill);
        }
    }

    /// Fills against a watched order that could not be recorded.
    pub fn passive_fills_dropped(&self) -> u64 {
        self.passive_fills_dropped
    }

    pub fn books(&self) -> &Books {
        &self.books
    }

    pub fn stats(&self) -> EngineStats {
        self.stats
    }

    pub fn digest(&self) -> BookDigest {
        BookDigest::of(&self.books)
    }

    fn symbol_index(&self, symbol_id: u16) -> Option<usize> {
        self.symbols.iter().position(|s| s.id == symbol_id)
    }

    /// Applies `change` to the book and publishes the message describing it.
    ///
    /// Returns the sequence the message was given. After this returns, the
    /// engine's book equals the result of applying messages `1..=seq`.
    pub fn publish(
        &mut self,
        feed: &mut FeedPublisher,
        symbol_id: u16,
        change: Change,
    ) -> io::Result<u64> {
        let book = self.books.get_or_create(symbol_id);
        match change {
            Change::Add {
                order_id,
                side,
                price,
                quantity,
            } => {
                book.add(order_id, side, price, quantity)
                    .map_err(book_err)?;
                feed.add_order(order_id, price, quantity, symbol_id, side)
            }
            Change::Reduce {
                order_id,
                side,
                price,
                new_quantity,
            } => {
                book.reduce(order_id, new_quantity).map_err(book_err)?;
                feed.modify_order(
                    order_id,
                    price,
                    new_quantity,
                    symbol_id,
                    side,
                    ModifyReason::Reduce,
                )
            }
            Change::Replace {
                order_id,
                side,
                new_price,
                new_quantity,
            } => {
                book.replace(order_id, new_price, new_quantity)
                    .map_err(book_err)?;
                feed.modify_order(
                    order_id,
                    new_price,
                    new_quantity,
                    symbol_id,
                    side,
                    ModifyReason::Replace,
                )
            }
            Change::Delete { order_id, side } => {
                book.delete(order_id).map_err(book_err)?;
                feed.delete_order(order_id, symbol_id, side)
            }
            Change::Trade {
                trade_id,
                aggressor_order_id,
                resting_order_id,
                price,
                quantity,
                aggressor_side,
            } => feed.trade(
                trade_id,
                aggressor_order_id,
                resting_order_id,
                price,
                quantity,
                symbol_id,
                aggressor_side,
            ),
        }
    }

    /// Submits an aggressing or resting limit order.
    ///
    /// Matches against the opposite side while the price crosses, taking the
    /// front of the best level first, then rests whatever is left.
    ///
    /// `on_sequence` is called after every published message with the sequence
    /// it received, which is where the caller takes digest checkpoints.
    /// One fill against a submitted order, in the order it happened.
    ///
    /// `leaves` is what is still unfilled *after* this fill, which is what a FIX
    /// `ExecutionReport` carries and what the risk service needs to take the
    /// right amount out of its pending exposure. Deriving it at the far end from
    /// a running total is how the two sides end up disagreeing.
    pub fn submit(
        &mut self,
        feed: &mut FeedPublisher,
        symbol_id: u16,
        side: Side,
        price: i64,
        quantity: u32,
        on_sequence: impl FnMut(&Self, u64) -> io::Result<()>,
    ) -> io::Result<()> {
        self.submit_reported(
            feed,
            symbol_id,
            side,
            price,
            quantity,
            // The generator's own flow: it may cancel and amend this later.
            true,
            on_sequence,
            |_| {},
        )
        .map(|_| ())
    }

    /// `submit`, plus a running account of what happened to the order.
    ///
    /// The order flow generator does not care and calls `submit`. The order path
    /// does: a gateway has to turn each fill into an `ExecutionReport`, and it
    /// cannot reconstruct them from the feed, because the feed says a trade
    /// happened without saying whose order it was.
    // Nine arguments, and clippy is right that this is a lot. Bundling them into
    // a struct would move the same nine values one line further away from the
    // matching loop that uses them, which is where the reader wants them.
    #[allow(clippy::too_many_arguments)]
    pub fn submit_reported(
        &mut self,
        feed: &mut FeedPublisher,
        symbol_id: u16,
        side: Side,
        price: i64,
        quantity: u32,
        // Whether the flow generator may later cancel or amend this order: true
        // for the generator's own orders, false for a client's.
        generator_may_recycle: bool,
        mut on_sequence: impl FnMut(&Self, u64) -> io::Result<()>,
        mut on_fill: impl FnMut(Fill),
    ) -> io::Result<SubmitOutcome> {
        let order_id = self.next_order_id;
        self.next_order_id += 1;
        self.stats.orders_submitted += 1;

        let opposite = opposite(side);
        let mut remaining = quantity;
        let mut matched_any = false;

        while remaining > 0 {
            let book = self.books.get_or_create(symbol_id);
            let Some((best_price, resting_id)) = book.front(opposite) else {
                break;
            };
            let crosses = match side {
                Side::Bid => price >= best_price,
                Side::Ask => price <= best_price,
            };
            if !crosses {
                break;
            }
            let resting = *book
                .get(resting_id)
                .expect("front() named an order that is on the book");

            let fill = remaining.min(resting.quantity);
            let trade_id = self.next_trade_id;
            self.next_trade_id += 1;

            // The trade is announced first and moves nothing. The resulting
            // change to the resting order is the message that follows.
            let seq = self.publish(
                feed,
                symbol_id,
                Change::Trade {
                    trade_id,
                    aggressor_order_id: order_id,
                    resting_order_id: resting_id,
                    // Trades print at the resting order's price, not the
                    // aggressor's limit. The passive side set the terms.
                    price: best_price,
                    quantity: fill,
                    aggressor_side: side,
                },
            )?;
            on_sequence(self, seq)?;

            let change = if fill == resting.quantity {
                Change::Delete {
                    order_id: resting_id,
                    side: opposite,
                }
            } else {
                Change::Reduce {
                    order_id: resting_id,
                    side: opposite,
                    price: resting.price,
                    new_quantity: resting.quantity - fill,
                }
            };
            let fully_filled = matches!(change, Change::Delete { .. });
            let seq = self.publish(feed, symbol_id, change)?;
            on_sequence(self, seq)?;

            if fully_filled {
                if let Some(idx) = self.symbol_index(symbol_id) {
                    self.symbols[idx].remove_live(resting_id);
                }
            }

            remaining -= fill;
            matched_any = true;
            self.stats.trades += 1;
            self.stats.shares_traded += u64::from(fill);
            on_fill(Fill {
                trade_id,
                price: best_price,
                quantity: fill,
                leaves: remaining,
            });
            // And the other side of the same trade. The passive order's owner
            // made its call long ago and is not in this stack frame; if nobody
            // records this, nobody ever tells them.
            if self.watched.binary_search(&resting_id).is_ok() {
                const MAX_PASSIVE_FILLS: usize = 1024;
                if self.passive_fills.len() < MAX_PASSIVE_FILLS {
                    self.passive_fills.push(PassiveFill {
                        resting_order_id: resting_id,
                        trade_id,
                        price: best_price,
                        quantity: fill,
                        leaves: resting.quantity - fill,
                        symbol_id,
                        side: opposite,
                    });
                } else {
                    self.passive_fills_dropped += 1;
                }
            }
        }

        if matched_any {
            self.stats.aggressive_fills += 1;
        }

        if remaining > 0 {
            let seq = self.publish(
                feed,
                symbol_id,
                Change::Add {
                    order_id,
                    side,
                    price,
                    quantity: remaining,
                },
            )?;
            on_sequence(self, seq)?;
            if generator_may_recycle {
                if let Some(idx) = self.symbol_index(symbol_id) {
                    self.symbols[idx].live.push(order_id);
                }
            }
        }
        Ok(SubmitOutcome {
            order_id,
            filled: quantity - remaining,
            resting: remaining,
        })
    }

    pub fn cancel(
        &mut self,
        feed: &mut FeedPublisher,
        symbol_id: u16,
        order_id: u64,
        on_sequence: impl FnMut(&Self, u64) -> io::Result<()>,
    ) -> io::Result<()> {
        self.cancel_reported(feed, symbol_id, order_id, on_sequence)
            .map(|_| ())
    }

    /// `cancel`, and says whether there was anything to cancel.
    ///
    /// `Ok(None)` means the book does not hold that order. That is a reject at
    /// the far end rather than a silent success: a client that asked to cancel
    /// something and was told nothing would go on believing it is still live.
    pub fn cancel_reported(
        &mut self,
        feed: &mut FeedPublisher,
        symbol_id: u16,
        order_id: u64,
        mut on_sequence: impl FnMut(&Self, u64) -> io::Result<()>,
    ) -> io::Result<Option<u32>> {
        let Some(resting) = self.books.get_or_create(symbol_id).get(order_id).copied() else {
            return Ok(None);
        };
        let seq = self.publish(
            feed,
            symbol_id,
            Change::Delete {
                order_id,
                side: resting.side,
            },
        )?;
        on_sequence(self, seq)?;
        if let Some(idx) = self.symbol_index(symbol_id) {
            self.symbols[idx].remove_live(order_id);
        }
        self.stats.cancels += 1;
        Ok(Some(resting.quantity))
    }

    pub fn amend(
        &mut self,
        feed: &mut FeedPublisher,
        symbol_id: u16,
        order_id: u64,
        // `None` leaves the price where it is.
        new_price: Option<i64>,
        new_quantity: u32,
        mut on_sequence: impl FnMut(&Self, u64) -> io::Result<()>,
    ) -> io::Result<()> {
        let Some(resting) = self.books.get_or_create(symbol_id).get(order_id).copied() else {
            return Ok(());
        };
        let new_price = new_price.unwrap_or(resting.price);
        // Same price and smaller size keeps priority; anything else loses it.
        let change = if new_price == resting.price && new_quantity < resting.quantity {
            Change::Reduce {
                order_id,
                side: resting.side,
                price: resting.price,
                new_quantity,
            }
        } else {
            Change::Replace {
                order_id,
                side: resting.side,
                new_price,
                new_quantity,
            }
        };
        let seq = self.publish(feed, symbol_id, change)?;
        on_sequence(self, seq)?;
        self.stats.modifies += 1;
        Ok(())
    }
}

pub fn opposite(side: Side) -> Side {
    match side {
        Side::Bid => Side::Ask,
        Side::Ask => Side::Bid,
    }
}

fn book_err(e: book::BookError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use transport::{Publisher, Receiver, SocketOptions, TransportMode};
    use wire::{Message, PacketReader};

    fn opts() -> SocketOptions {
        SocketOptions {
            interface: Ipv4Addr::LOCALHOST,
            ttl: 0,
            loopback: true,
            buffer_bytes: 1024 * 1024,
        }
    }

    fn rig() -> (Engine, FeedPublisher, Receiver) {
        let r = Receiver::bind(
            TransportMode::UnicastFanout,
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            opts(),
        )
        .unwrap();
        let target = match r.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            other => panic!("expected IPv4, got {other}"),
        };
        let pa = Publisher::bind(TransportMode::UnicastFanout, &[target], opts()).unwrap();
        // The B arm goes to a socket nobody reads; these tests only inspect A.
        let pb = Publisher::bind(
            TransportMode::UnicastFanout,
            &[SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9)],
            opts(),
        )
        .unwrap();
        let symbols = vec![mdconfig::Symbol {
            id: 7,
            name: "TEST".into(),
            reference_price: 1_000_000,
            tick_size: 100,
        }];
        (
            Engine::new(&symbols),
            FeedPublisher::new(pa, pb, 64, 1400),
            r,
        )
    }

    fn published(feed: &mut FeedPublisher, r: &Receiver) -> Vec<(u64, u16)> {
        feed.flush().unwrap();
        r.set_read_timeout(Some(std::time::Duration::from_millis(200)))
            .unwrap();
        let mut out = Vec::new();
        let mut buf = [0u8; 2048];
        while let Ok(n) = r.recv(&mut buf) {
            let reader = PacketReader::new(&buf[..n]).unwrap();
            for m in reader.messages() {
                let (seq, msg) = m.unwrap();
                out.push((seq, msg.template_id()));
            }
        }
        out
    }

    fn noop(_: &Engine, _: u64) -> io::Result<()> {
        Ok(())
    }

    #[test]
    fn a_resting_order_produces_one_add() {
        let (mut e, mut feed, r) = rig();
        e.submit(&mut feed, 7, Side::Bid, 1_000_000, 10, noop)
            .unwrap();
        assert_eq!(
            published(&mut feed, &r),
            vec![(1, wire::template::ADD_ORDER)]
        );
        assert_eq!(e.books().get(7).unwrap().len(), 1);
    }

    #[test]
    fn a_full_fill_publishes_a_trade_then_a_delete() {
        let (mut e, mut feed, r) = rig();
        e.submit(&mut feed, 7, Side::Ask, 1_000_000, 10, noop)
            .unwrap();
        e.submit(&mut feed, 7, Side::Bid, 1_000_000, 10, noop)
            .unwrap();

        assert_eq!(
            published(&mut feed, &r),
            vec![
                (1, wire::template::ADD_ORDER),
                (2, wire::template::TRADE),
                (3, wire::template::DELETE_ORDER),
            ],
            "the aggressor fully filled, so it never rests and never adds"
        );
        assert!(e.books().get(7).unwrap().is_empty());
    }

    #[test]
    fn a_partial_fill_reduces_the_resting_order_and_keeps_its_priority() {
        let (mut e, mut feed, r) = rig();
        e.submit(&mut feed, 7, Side::Ask, 1_000_000, 10, noop)
            .unwrap();
        e.submit(&mut feed, 7, Side::Bid, 1_000_000, 4, noop)
            .unwrap();

        assert_eq!(
            published(&mut feed, &r),
            vec![
                (1, wire::template::ADD_ORDER),
                (2, wire::template::TRADE),
                (3, wire::template::MODIFY_ORDER),
            ]
        );
        let book = e.books().get(7).unwrap();
        assert_eq!(book.get(1).unwrap().quantity, 6);
        assert_eq!(book.front(Side::Ask), Some((1_000_000, 1)));
    }

    #[test]
    fn an_aggressor_larger_than_the_book_fills_then_rests_the_remainder() {
        let (mut e, mut feed, r) = rig();
        e.submit(&mut feed, 7, Side::Ask, 1_000_000, 5, noop)
            .unwrap();
        e.submit(&mut feed, 7, Side::Bid, 1_000_000, 12, noop)
            .unwrap();

        assert_eq!(
            published(&mut feed, &r),
            vec![
                (1, wire::template::ADD_ORDER),
                (2, wire::template::TRADE),
                (3, wire::template::DELETE_ORDER),
                (4, wire::template::ADD_ORDER),
            ]
        );
        let book = e.books().get(7).unwrap();
        assert_eq!(book.best_bid().unwrap().quantity, 7, "12 in, 5 filled");
    }

    #[test]
    fn matching_walks_price_levels_and_respects_time_at_each() {
        let (mut e, mut feed, r) = rig();
        // Two at the best ask, one behind it; the aggressor should take the
        // first-arrived at 1_000_000 before the second, then move to 1_000_100.
        e.submit(&mut feed, 7, Side::Ask, 1_000_000, 5, noop)
            .unwrap();
        e.submit(&mut feed, 7, Side::Ask, 1_000_000, 5, noop)
            .unwrap();
        e.submit(&mut feed, 7, Side::Ask, 1_000_100, 5, noop)
            .unwrap();
        let _ = published(&mut feed, &r);

        e.submit(&mut feed, 7, Side::Bid, 1_000_100, 12, noop)
            .unwrap();
        let msgs = published(&mut feed, &r);
        let kinds: Vec<u16> = msgs.iter().map(|(_, t)| *t).collect();
        assert_eq!(
            kinds,
            vec![
                wire::template::TRADE,
                wire::template::DELETE_ORDER,
                wire::template::TRADE,
                wire::template::DELETE_ORDER,
                wire::template::TRADE,
                wire::template::MODIFY_ORDER,
            ],
            "two full fills at the touch, then a partial at the next level"
        );
        assert_eq!(e.books().get(7).unwrap().get(3).unwrap().quantity, 3);
    }

    #[test]
    fn an_order_that_does_not_cross_just_rests() {
        let (mut e, mut feed, r) = rig();
        e.submit(&mut feed, 7, Side::Ask, 1_000_100, 5, noop)
            .unwrap();
        e.submit(&mut feed, 7, Side::Bid, 1_000_000, 5, noop)
            .unwrap();
        assert_eq!(
            published(&mut feed, &r),
            vec![
                (1, wire::template::ADD_ORDER),
                (2, wire::template::ADD_ORDER),
            ],
            "a bid below the ask is not a trade"
        );
    }

    #[test]
    fn a_trade_prints_at_the_resting_price_not_the_aggressors_limit() {
        let (mut e, mut feed, r) = rig();
        e.submit(&mut feed, 7, Side::Ask, 1_000_000, 5, noop)
            .unwrap();
        // Willing to pay far more, but the passive side set the terms.
        e.submit(&mut feed, 7, Side::Bid, 1_500_000, 5, noop)
            .unwrap();
        feed.flush().unwrap();

        r.set_read_timeout(Some(std::time::Duration::from_millis(200)))
            .unwrap();
        let mut buf = [0u8; 2048];
        let mut trade_price = None;
        while let Ok(n) = r.recv(&mut buf) {
            let reader = PacketReader::new(&buf[..n]).unwrap();
            for m in reader.messages() {
                if let (_, Message::Trade(t)) = m.unwrap() {
                    trade_price = Some(t.price());
                }
            }
        }
        assert_eq!(trade_price, Some(1_000_000));
    }

    #[test]
    fn cancelling_an_order_that_is_not_there_publishes_nothing() {
        let (mut e, mut feed, r) = rig();
        e.cancel(&mut feed, 7, 999, noop).unwrap();
        assert!(published(&mut feed, &r).is_empty());
    }

    #[test]
    fn amend_picks_reduce_only_when_priority_survives() {
        let (mut e, mut feed, r) = rig();
        e.submit(&mut feed, 7, Side::Bid, 1_000_000, 10, noop)
            .unwrap();
        let _ = published(&mut feed, &r);

        e.amend(&mut feed, 7, 1, None, 4, noop).unwrap();
        assert_eq!(
            published(&mut feed, &r),
            vec![(2, wire::template::MODIFY_ORDER)]
        );

        // Sizing up loses priority, so it must be a Replace and the book must
        // accept it — a Reduce would have been rejected outright.
        e.amend(&mut feed, 7, 1, None, 20, noop).unwrap();
        assert_eq!(e.books().get(7).unwrap().get(1).unwrap().quantity, 20);
    }

    #[test]
    fn the_book_after_publishing_sequence_s_is_the_result_of_one_through_s() {
        // The invariant the whole cross-process digest comparison rests on.
        let (mut e, mut feed, _r) = rig();
        let mut checkpoints: Vec<(u64, BookDigest)> = Vec::new();

        for i in 0..20u32 {
            let side = if i % 2 == 0 { Side::Ask } else { Side::Bid };
            let price = 1_000_000 + i64::from(i % 5) * 100;
            e.submit(&mut feed, 7, side, price, 3 + i, |eng, seq| {
                checkpoints.push((seq, eng.digest()));
                Ok(())
            })
            .unwrap();
        }
        feed.flush().unwrap();

        // Sequences are contiguous and every one has exactly one checkpoint.
        let seqs: Vec<u64> = checkpoints.iter().map(|(s, _)| *s).collect();
        assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
        // And the last checkpoint is the engine's current book.
        assert_eq!(checkpoints.last().unwrap().1, e.digest());
    }
}
