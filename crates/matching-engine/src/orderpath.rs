//! The engine's end of the order path.
//!
//! Two rings: accepted orders come in from the risk service, execution reports
//! go back out to it. Everything the engine already does — matching, publishing
//! to the A/B feed, snapshots — is unchanged. This module is the part that lets
//! an order arrive from somewhere other than the flow generator.
//!
//! # Why the engine reports rather than letting the gateway infer
//!
//! The feed says a trade happened. It does not say whose order it was, and it
//! cannot: the feed is public and order ownership is not. So a fill has to come
//! back down the order path with the client's own id on it, or the gateway is
//! left correlating trades against its own orders by price and time, which is
//! guesswork dressed up as reconciliation.
//!
//! # Reconciliation is answered here
//!
//! The engine is the only process that knows what is actually resting on the
//! book. When a `ReconcileRequest` arrives it answers with one `ExecReport` per
//! live order and then a `StatusComplete` carrying the count — see
//! `docs/ORDER-PATH.md`. It reports; it does not repair.

use std::io;

use book::view::{BookSet, OrderBook};
use ring::{Consumer, Producer, PushError, Ring, RingError};
use wire::{
    decode_order_path_message, encode_exec_report, CancelOrderDecoder, ExecType, NewOrderDecoder,
    OrderPathMessage, ReconcileRequestDecoder, RejectReason, Side,
};

use crate::engine::Engine;
use crate::feed::FeedPublisher;

/// How many order-path messages to take in one pass.
///
/// Bounded rather than "until empty" for the same reason the ring's own drain
/// is: a burst of orders must not hold the engine out of its snapshot and
/// heartbeat timers, and a feed that stops heartbeating looks dead to every
/// consumer.
const ORDERS_PER_PASS: usize = 64;

/// What the engine did with the order path, for the run summary.
#[derive(Debug, Default, Clone, Copy)]
pub struct OrderPathStats {
    pub orders_received: u64,
    pub cancels_received: u64,
    pub fills_reported: u64,
    pub acks_reported: u64,
    pub cancels_reported: u64,
    pub rejects_reported: u64,
    pub reconciles_answered: u64,
    pub orders_named_in_reconciliation: u64,
    /// Reports that could not be enqueued because the outbound ring was full.
    ///
    /// **Not zero by construction, and not ignorable.** The engine cannot block
    /// on a slow consumer without stalling the feed, and it cannot drop a report
    /// silently without leaving the gateway believing an order is still working.
    /// So it counts them and says so loudly at exit; the operator's answer is a
    /// bigger ring, and the gateway's answer is reconciliation.
    pub reports_dropped: u64,
}

/// The engine's two rings.
#[derive(Debug)]
pub struct OrderPath {
    orders: Consumer,
    reports: Producer,
    stats: OrderPathStats,
    scratch: [u8; 256],
}

impl OrderPath {
    /// Opens rings the risk service created.
    ///
    /// The engine never creates them. One process has to own their geometry and
    /// it is the one that touches all four; a second creator would silently
    /// reset the indices under whoever was already attached.
    pub fn open(orders_path: &str, reports_path: &str) -> Result<Self, RingError> {
        let orders = Ring::open(std::path::Path::new(orders_path))?.into_consumer();
        let reports = Ring::open(std::path::Path::new(reports_path))?.into_producer();
        Ok(Self {
            orders,
            reports,
            stats: OrderPathStats::default(),
            scratch: [0u8; 256],
        })
    }

    pub fn stats(&self) -> OrderPathStats {
        self.stats
    }

    /// Takes up to [`ORDERS_PER_PASS`] messages and acts on each.
    ///
    /// Returns how many it handled, so the caller can tell a quiet pass from a
    /// busy one without asking twice.
    pub fn pump(&mut self, engine: &mut Engine, feed: &mut FeedPublisher) -> io::Result<usize> {
        let mut handled = 0;
        while handled < ORDERS_PER_PASS {
            // The message is copied out of the slot before anything is done
            // with it. Holding the borrow across the engine call would mean
            // holding a pointer into the shared page while running arbitrary
            // work, and the borrow checker would refuse it anyway because the
            // engine call needs `&mut self`.
            let mut buf = [0u8; 256];
            let Some(len) = self.orders.pop_with(|bytes| {
                let n = bytes.len().min(buf.len());
                buf[..n].copy_from_slice(&bytes[..n]);
                n
            }) else {
                break;
            };
            handled += 1;
            self.dispatch(&buf[..len], engine, feed)?;
        }
        Ok(handled)
    }

    fn dispatch(
        &mut self,
        bytes: &[u8],
        engine: &mut Engine,
        feed: &mut FeedPublisher,
    ) -> io::Result<()> {
        let Ok(msg) = decode_order_path_message(bytes) else {
            // Not decodable as an order-path message. The ring is not a network
            // and the only writer is the risk service, so this means a version
            // mismatch between two binaries rather than a corrupt datagram. It
            // is counted as a dropped report so the number is visible.
            self.stats.reports_dropped += 1;
            return Ok(());
        };
        match msg {
            OrderPathMessage::NewOrder(d) => self.on_new_order(d, engine, feed),
            OrderPathMessage::CancelOrder(d) => self.on_cancel(d, engine, feed),
            OrderPathMessage::ReconcileRequest(d) => self.on_reconcile(d, engine),
            // The engine never receives execution reports; it writes them.
            OrderPathMessage::ExecReport(_) => Ok(()),
        }
    }

    fn on_new_order(
        &mut self,
        d: NewOrderDecoder<'_>,
        engine: &mut Engine,
        feed: &mut FeedPublisher,
    ) -> io::Result<()> {
        self.stats.orders_received += 1;
        let client_order_id = d.client_order_id();
        let symbol_id = d.symbol_id();
        let Ok(side) = d.side() else {
            self.emit_reject(client_order_id, symbol_id, Side::Bid, RejectReason::UnknownSymbol);
            return Ok(());
        };
        let price = d.price();
        let quantity = d.quantity();

        // Fills are collected as they happen and reported afterwards. Reporting
        // from inside the callback would mean pushing to a ring in the middle of
        // a book mutation, and a full ring there would leave the book changed
        // and the report missing.
        let mut fills: [(u64, i64, u32, u32); 32] = [(0, 0, 0, 0); 32];
        let mut fill_count = 0usize;
        let mut overflowed = 0u32;

        let outcome = engine.submit_reported(
            feed,
            symbol_id,
            side,
            price,
            quantity,
            |_, _| Ok(()),
            |f| {
                if fill_count < fills.len() {
                    fills[fill_count] = (f.trade_id, f.price, f.quantity, f.leaves);
                    fill_count += 1;
                } else {
                    // An order that fills against more than 32 resting orders.
                    // The quantities are still correct because the last report
                    // carries `leaves`; what is lost is the individual prints.
                    // Counted rather than silently truncated.
                    overflowed += 1;
                }
            },
        )?;

        for i in 0..fill_count {
            let (trade_id, px, qty, leaves) = fills[i];
            let last = i + 1 == fill_count && leaves == 0 && overflowed == 0;
            self.emit(
                client_order_id,
                outcome.order_id,
                trade_id,
                px,
                qty,
                leaves,
                symbol_id,
                side,
                if last { ExecType::Fill } else { ExecType::PartialFill },
                RejectReason::NotRejected,
            );
            self.stats.fills_reported += 1;
        }
        if overflowed > 0 {
            self.stats.reports_dropped += u64::from(overflowed);
        }

        if outcome.resting > 0 {
            // Acknowledged and resting. Sent after the fills so the gateway sees
            // the partial fills before the acknowledgement of what is left,
            // which is the order FIX expects.
            self.emit(
                client_order_id,
                outcome.order_id,
                0,
                price,
                quantity,
                outcome.resting,
                symbol_id,
                side,
                ExecType::Acknowledged,
                RejectReason::NotRejected,
            );
            self.stats.acks_reported += 1;
        }
        Ok(())
    }

    fn on_cancel(
        &mut self,
        d: CancelOrderDecoder<'_>,
        engine: &mut Engine,
        feed: &mut FeedPublisher,
    ) -> io::Result<()> {
        self.stats.cancels_received += 1;
        let client_order_id = d.client_order_id();
        let orig = d.orig_client_order_id();
        let symbol_id = d.symbol_id();
        let side = d.side().unwrap_or(Side::Bid);

        // The cancel names the order by the exchange id the gateway was told.
        match engine.cancel_reported(feed, symbol_id, orig, |_, _| Ok(()))? {
            Some(remaining) => {
                self.emit(
                    client_order_id,
                    orig,
                    0,
                    0,
                    remaining,
                    0,
                    symbol_id,
                    side,
                    ExecType::Canceled,
                    RejectReason::NotRejected,
                );
                self.stats.cancels_reported += 1;
            }
            None => {
                // The book does not hold it. Silence here would leave the client
                // believing the order is still working.
                self.emit(
                    client_order_id,
                    orig,
                    0,
                    0,
                    0,
                    0,
                    symbol_id,
                    side,
                    ExecType::Rejected,
                    RejectReason::UnknownOrder,
                );
                self.stats.rejects_reported += 1;
            }
        }
        Ok(())
    }

    fn on_reconcile(
        &mut self,
        d: ReconcileRequestDecoder<'_>,
        engine: &mut Engine,
    ) -> io::Result<()> {
        self.stats.reconciles_answered += 1;
        let request_id = d.request_id();
        let mut named = 0u64;

        // Every order the book is holding, whoever put it there. The engine does
        // not distinguish orders that arrived over the order path from the flow
        // generator's, and neither should the answer: what the gateway needs to
        // know is what is actually resting.
        //
        // Emitted from inside the walk rather than collected first. A collected
        // list would allocate, and this is the one place where the size of the
        // allocation is proportional to the size of the book.
        let reports = &mut self.reports;
        let scratch = &mut self.scratch;
        let mut dropped = 0u64;
        engine.books().for_each_symbol(&mut |symbol_id, book| {
            for side in [Side::Bid, Side::Ask] {
                book.for_each_order(side, &mut |o| {
                    // A resting order has not been filled at all as far as the
                    // book is concerned; quantity and leaves are the same number
                    // and both are sent, because a gateway comparing against its
                    // own log needs to see them agree.
                    let Ok(n) = encode_exec_report(
                        scratch,
                        0,
                        o.order_id,
                        0,
                        o.price,
                        o.quantity,
                        o.quantity,
                        symbol_id,
                        o.side,
                        ExecType::OrderStatus,
                        RejectReason::NotRejected,
                    ) else {
                        dropped += 1;
                        return true;
                    };
                    if reports.push(&scratch[..n]).is_err() {
                        dropped += 1;
                    } else {
                        named += 1;
                    }
                    true
                });
            }
        });
        self.stats.orders_named_in_reconciliation += named;
        self.stats.reports_dropped += dropped;

        // The marker that ends the answer. `quantity` carries the count of lines
        // actually sent and `client_order_id` echoes the request, so a gateway
        // that receives the marker can tell "the engine holds nothing" from
        // "some of the lines did not fit" -- the count is what was sent, and
        // `reports_dropped` on this side says whether that was all of them.
        let count = u32::try_from(named).unwrap_or(u32::MAX);
        self.emit(
            request_id,
            0,
            0,
            0,
            count,
            0,
            0,
            Side::Bid,
            ExecType::StatusComplete,
            RejectReason::NotRejected,
        );
        Ok(())
    }

    fn emit_reject(
        &mut self,
        client_order_id: u64,
        symbol_id: u16,
        side: Side,
        reason: RejectReason,
    ) {
        self.emit(
            client_order_id,
            0,
            0,
            0,
            0,
            0,
            symbol_id,
            side,
            ExecType::Rejected,
            reason,
        );
        self.stats.rejects_reported += 1;
    }

    #[allow(clippy::too_many_arguments)]
    fn emit(
        &mut self,
        client_order_id: u64,
        exchange_order_id: u64,
        trade_id: u64,
        price: i64,
        quantity: u32,
        leaves_quantity: u32,
        symbol_id: u16,
        side: Side,
        exec_type: ExecType,
        reject_reason: RejectReason,
    ) {
        let Ok(n) = encode_exec_report(
            &mut self.scratch,
            client_order_id,
            exchange_order_id,
            trade_id,
            price,
            quantity,
            leaves_quantity,
            symbol_id,
            side,
            exec_type,
            reject_reason,
        ) else {
            self.stats.reports_dropped += 1;
            return;
        };
        match self.reports.push(&self.scratch[..n]) {
            Ok(()) => {}
            Err(PushError::Full) | Err(PushError::TooLarge { .. }) | Err(PushError::Abandoned) => {
                // The engine cannot block here without stalling the feed for
                // every consumer, and it must not pretend this did not happen.
                self.stats.reports_dropped += 1;
            }
        }
    }
}
