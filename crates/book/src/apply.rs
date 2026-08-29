//! Turning a decoded feed message into a book change.
//!
//! This is the consumer's side of the contract: given the messages the engine
//! published, rebuild the book the engine has. The feed handler is the obvious
//! caller. The engine is the less obvious one — it runs a shadow book through
//! this same function under `--self-check` and compares it against the book its
//! matching logic maintains, so a message it forgot to emit is caught in the
//! engine rather than eight seconds later as an unexplained digest mismatch.
//!
//! Note what the engine does *not* do: it never mutates its real book through
//! here. If it did, the digest reconciliation in `scripts/smoke.sh` would be
//! tautological — both sides would be applying the same messages and would
//! agree even if the messages described something the matching engine never did.
//! The engine's book is maintained by its matching logic, independently, and
//! that is what makes the comparison mean something.

use wire::{Message, ModifyReason, WireError};

use crate::reference::{BookError, Books};

/// Why a message could not be applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyError {
    /// The message described a change that does not fit the book we have.
    Book { symbol_id: u16, error: BookError },
    /// A field held a value the schema does not define.
    Wire(WireError),
    /// A `Snapshot` fragment named an order the book already holds.
    ///
    /// Within one snapshot cycle every order appears once, so this means the
    /// cycle was not started cleanly — the caller applied a continuation
    /// fragment without first clearing the symbol via [`apply_snapshot`].
    SnapshotOverlap { symbol_id: u16, order_id: u64 },
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Book { symbol_id, error } => write!(f, "symbol {symbol_id}: {error}"),
            Self::Wire(e) => write!(f, "{e}"),
            Self::SnapshotOverlap {
                symbol_id,
                order_id,
            } => write!(
                f,
                "symbol {symbol_id}: snapshot order {order_id} is already on the book; \
                 the cycle was not started with apply_snapshot"
            ),
        }
    }
}

impl std::error::Error for ApplyError {}

impl From<WireError> for ApplyError {
    fn from(e: WireError) -> Self {
        Self::Wire(e)
    }
}

/// Applies one message to `books`.
///
/// `Trade` is deliberately a no-op. A trade reports that a match happened; the
/// resulting change to the resting order arrives as its own `ModifyOrder`
/// (partial fill, reason `Reduce`) or `DeleteOrder` (full fill). Applying both
/// would double-count the fill, which is the single easiest way to build a book
/// that is subtly wrong and still looks plausible.
pub fn apply_message(books: &mut Books, msg: &Message<'_>) -> Result<(), ApplyError> {
    match msg {
        Message::AddOrder(d) => {
            let symbol_id = d.symbol_id();
            let side = d.side()?;
            books
                .get_or_create(symbol_id)
                .add(d.order_id(), side, d.price(), d.quantity())
                .map_err(|error| ApplyError::Book { symbol_id, error })?;
        }
        Message::ModifyOrder(d) => {
            let symbol_id = d.symbol_id();
            let book = books.get_or_create(symbol_id);
            let result = match d.reason()? {
                ModifyReason::Reduce => book.reduce(d.order_id(), d.new_quantity()),
                ModifyReason::Replace => {
                    book.replace(d.order_id(), d.new_price(), d.new_quantity())
                }
            };
            result.map_err(|error| ApplyError::Book { symbol_id, error })?;
        }
        Message::DeleteOrder(d) => {
            let symbol_id = d.symbol_id();
            books
                .get_or_create(symbol_id)
                .delete(d.order_id())
                .map_err(|error| ApplyError::Book { symbol_id, error })?;
        }
        // Informational: carries no book change of its own.
        Message::Trade(_) => {}
        // Liveness and stream control; neither touches the book.
        Message::Heartbeat(_) | Message::SequenceReset(_) => {}
        // A continuation fragment of a snapshot cycle. The first fragment goes
        // through `apply_snapshot`, which clears the symbol; the rest append.
        Message::Snapshot(d) => apply_snapshot_orders(books, d)?,
    }
    Ok(())
}

/// Applies a `Snapshot` as the **start** of a fresh book for its symbol.
///
/// A snapshot is the whole book as of `lastSequence`, not an increment, so the
/// symbol is cleared first. Continuation fragments of the same cycle go through
/// [`apply_message`], which appends.
///
/// Orders are added in the order they appear on the wire, and the publisher
/// writes them in queue order. That is what lets an order-level snapshot restore
/// price-time priority exactly — the thing an aggregated snapshot fundamentally
/// cannot do, because "three orders totalling 250" does not say which of them is
/// at the front of the queue.
pub fn apply_snapshot(books: &mut Books, d: &wire::SnapshotDecoder<'_>) -> Result<(), ApplyError> {
    books.clear_symbol(d.symbol_id());
    apply_snapshot_orders(books, d)
}

fn apply_snapshot_orders(
    books: &mut Books,
    d: &wire::SnapshotDecoder<'_>,
) -> Result<(), ApplyError> {
    let symbol_id = d.symbol_id();
    let book = books.get_or_create(symbol_id);
    for order in d.orders() {
        let side = order.side()?;
        book.add(order.order_id(), side, order.price(), order.quantity())
            .map_err(|error| match error {
                BookError::DuplicateOrderId(order_id) => ApplyError::SnapshotOverlap {
                    symbol_id,
                    order_id,
                },
                error => ApplyError::Book { symbol_id, error },
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire::{PacketReader, PacketWriter, Side};

    /// Builds a datagram, decodes it, and applies every message — the same path
    /// the feed handler takes.
    fn round_trip(build: impl FnOnce(&mut PacketWriter<'_>)) -> (Books, Vec<ApplyError>) {
        let mut buf = vec![0u8; 4096];
        let mut w = PacketWriter::new(&mut buf, 0, 0, 1, 0).expect("header");
        build(&mut w);
        let n = w.finish();

        let mut books = Books::new();
        let mut errors = Vec::new();
        let reader = PacketReader::new(&buf[..n]).expect("reader");
        for m in reader.messages() {
            let (_seq, msg) = m.expect("decode");
            if let Err(e) = apply_message(&mut books, &msg) {
                errors.push(e);
            }
        }
        (books, errors)
    }

    #[test]
    fn a_feed_rebuilds_the_book_it_describes() {
        let (books, errors) = round_trip(|w| {
            w.add_order(1, 1_000_000, 10, 7, Side::Bid).unwrap();
            w.add_order(2, 1_000_100, 5, 7, Side::Ask).unwrap();
            w.add_order(3, 999_900, 20, 7, Side::Bid).unwrap();
        });
        assert!(errors.is_empty(), "{errors:?}");
        let book = books.get(7).expect("symbol 7");
        assert_eq!(book.len(), 3);
        assert_eq!(book.best_bid().unwrap().price, 1_000_000);
        assert_eq!(book.best_ask().unwrap().price, 1_000_100);
        books.check_invariants().unwrap();
    }

    #[test]
    fn a_trade_alone_does_not_move_the_book() {
        // The fill arrives separately. If Trade also applied one, every partial
        // fill would be counted twice.
        let (books, errors) = round_trip(|w| {
            w.add_order(1, 1_000_000, 10, 7, Side::Bid).unwrap();
            w.trade(500, 900, 1, 1_000_000, 4, 7, Side::Ask).unwrap();
        });
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(
            books.get(7).unwrap().get(1).unwrap().quantity,
            10,
            "the Trade must not have touched the resting quantity"
        );
    }

    #[test]
    fn a_partial_fill_is_the_modify_that_follows_the_trade() {
        let (books, errors) = round_trip(|w| {
            w.add_order(1, 1_000_000, 10, 7, Side::Bid).unwrap();
            w.trade(500, 900, 1, 1_000_000, 4, 7, Side::Ask).unwrap();
            w.modify_order(1, 1_000_000, 6, 7, Side::Bid, ModifyReason::Reduce)
                .unwrap();
        });
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(books.get(7).unwrap().get(1).unwrap().quantity, 6);
    }

    #[test]
    fn a_full_fill_removes_the_resting_order() {
        let (books, errors) = round_trip(|w| {
            w.add_order(1, 1_000_000, 10, 7, Side::Bid).unwrap();
            w.trade(500, 900, 1, 1_000_000, 10, 7, Side::Ask).unwrap();
            w.delete_order(1, 7, Side::Bid).unwrap();
        });
        assert!(errors.is_empty(), "{errors:?}");
        assert!(books.get(7).unwrap().is_empty());
    }

    #[test]
    fn replace_moves_the_order_and_reduce_does_not() {
        let (books, errors) = round_trip(|w| {
            w.add_order(1, 1_000_000, 10, 7, Side::Bid).unwrap();
            w.add_order(2, 1_000_000, 10, 7, Side::Bid).unwrap();
            w.modify_order(1, 1_000_000, 4, 7, Side::Bid, ModifyReason::Reduce)
                .unwrap();
        });
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(
            books.get(7).unwrap().front(Side::Bid),
            Some((1_000_000, 1)),
            "Reduce keeps the front of the queue"
        );

        let (books, errors) = round_trip(|w| {
            w.add_order(1, 1_000_000, 10, 7, Side::Bid).unwrap();
            w.add_order(2, 1_000_000, 10, 7, Side::Bid).unwrap();
            w.modify_order(1, 1_000_000, 12, 7, Side::Bid, ModifyReason::Replace)
                .unwrap();
        });
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(
            books.get(7).unwrap().front(Side::Bid),
            Some((1_000_000, 2)),
            "Replace goes to the back"
        );
    }

    #[test]
    fn heartbeats_and_sequence_resets_leave_the_book_alone() {
        let (books, errors) = round_trip(|w| {
            w.add_order(1, 1_000_000, 10, 7, Side::Bid).unwrap();
            w.heartbeat(99).unwrap();
            w.sequence_reset(1000).unwrap();
        });
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(books.get(7).unwrap().len(), 1);
    }

    #[test]
    fn a_message_that_contradicts_the_book_is_reported() {
        // On a live feed this is what a missed message looks like from the
        // consumer's side, which is why it is surfaced rather than ignored.
        let (_books, errors) = round_trip(|w| {
            w.delete_order(42, 7, Side::Bid).unwrap();
        });
        assert_eq!(
            errors,
            vec![ApplyError::Book {
                symbol_id: 7,
                error: BookError::UnknownOrderId(42)
            }]
        );
    }

    /// Builds a one-message snapshot datagram for symbol 7.
    fn snapshot(orders: &[(u64, i64, u32, Side)]) -> Vec<u8> {
        use wire::SnapshotEncoder;
        let mut buf = vec![0u8; 8192];
        let mut w = PacketWriter::new(&mut buf, 0, wire::PACKET_FLAG_SNAPSHOT, 1, 0).unwrap();
        let n = {
            let mut e = SnapshotEncoder::start(w.tail(), 500, 7, wire::SNAPSHOT_FLAG_LAST_FRAGMENT)
                .unwrap();
            for (id, price, qty, side) in orders {
                e.push_order(*id, *price, *qty, *side).unwrap();
            }
            e.finish()
        };
        w.commit(n).unwrap();
        let len = w.finish();
        buf.truncate(len);
        buf
    }

    fn decode_snapshot(bytes: &[u8]) -> wire::SnapshotDecoder<'_> {
        let reader = PacketReader::new(bytes).unwrap();
        let (_seq, msg) = reader.messages().next().unwrap().unwrap();
        match msg {
            Message::Snapshot(d) => d,
            other => panic!("expected a Snapshot, got template {}", other.template_id()),
        }
    }

    #[test]
    fn a_snapshot_rebuilds_the_book_it_describes() {
        let bytes = snapshot(&[
            (1, 1_000_000, 10, Side::Bid),
            (2, 1_000_000, 5, Side::Bid),
            (3, 1_000_100, 7, Side::Ask),
        ]);
        let mut books = Books::new();
        apply_snapshot(&mut books, &decode_snapshot(&bytes)).unwrap();

        let book = books.get(7).unwrap();
        assert_eq!(book.len(), 3);
        assert_eq!(book.best_bid().unwrap().price, 1_000_000);
        assert_eq!(book.best_ask().unwrap().price, 1_000_100);
        books.check_invariants().unwrap();
    }

    #[test]
    fn a_snapshot_restores_queue_order_not_just_quantity() {
        // This is the property that justifies carrying orders rather than
        // aggregated levels. An aggregate could reproduce "15 resting at
        // 1_000_000" but never which order is at the front of the queue — and
        // queue position is the whole of price-time priority.
        let bytes = snapshot(&[
            (11, 1_000_000, 5, Side::Bid),
            (22, 1_000_000, 5, Side::Bid),
            (33, 1_000_000, 5, Side::Bid),
        ]);
        let mut books = Books::new();
        apply_snapshot(&mut books, &decode_snapshot(&bytes)).unwrap();

        assert_eq!(
            books.get(7).unwrap().front(Side::Bid),
            Some((1_000_000, 11)),
            "the first order on the wire must be the front of the queue"
        );
    }

    #[test]
    fn a_snapshot_replaces_the_book_rather_than_merging_into_it() {
        let mut books = Books::new();
        books.get_or_create(7).add(99, Side::Ask, 5_000, 1).unwrap();

        let bytes = snapshot(&[(1, 1_000_000, 10, Side::Bid)]);
        apply_snapshot(&mut books, &decode_snapshot(&bytes)).unwrap();

        let book = books.get(7).unwrap();
        assert_eq!(book.len(), 1, "the stale order must be gone");
        assert!(book.get(99).is_none());
        assert!(book.get(1).is_some());
    }

    #[test]
    fn continuation_fragments_append_to_the_cycle() {
        // apply_snapshot starts a cycle; apply_message continues it.
        let mut books = Books::new();
        apply_snapshot(
            &mut books,
            &decode_snapshot(&snapshot(&[(1, 1_000_000, 10, Side::Bid)])),
        )
        .unwrap();

        let second = snapshot(&[(2, 999_900, 4, Side::Bid)]);
        let reader = PacketReader::new(&second).unwrap();
        let (_seq, msg) = reader.messages().next().unwrap().unwrap();
        apply_message(&mut books, &msg).unwrap();

        assert_eq!(books.get(7).unwrap().len(), 2);
    }

    #[test]
    fn a_fragment_that_repeats_an_order_is_reported_as_a_broken_cycle() {
        let mut books = Books::new();
        let bytes = snapshot(&[(1, 1_000_000, 10, Side::Bid)]);
        apply_snapshot(&mut books, &decode_snapshot(&bytes)).unwrap();

        let reader = PacketReader::new(&bytes).unwrap();
        let (_seq, msg) = reader.messages().next().unwrap().unwrap();
        assert_eq!(
            apply_message(&mut books, &msg),
            Err(ApplyError::SnapshotOverlap {
                symbol_id: 7,
                order_id: 1
            })
        );
    }
}
