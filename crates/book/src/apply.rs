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
    /// Rebuilding an order-level book from an aggregated snapshot is not
    /// possible and is not attempted. See the note in `docs/WIRE.md`.
    SnapshotNeedsReplay,
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Book { symbol_id, error } => write!(f, "symbol {symbol_id}: {error}"),
            Self::Wire(e) => write!(f, "{e}"),
            Self::SnapshotNeedsReplay => f.write_str(
                "a Snapshot carries aggregated levels, which cannot rebuild an \
                 order-level book; recovery needs the replay service (milestone 4)",
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
        Message::Snapshot(_) => return Err(ApplyError::SnapshotNeedsReplay),
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

    #[test]
    fn a_snapshot_says_plainly_that_it_cannot_rebuild_an_order_book() {
        use wire::SnapshotEncoder;
        let mut buf = vec![0u8; 4096];
        let mut w = PacketWriter::new(&mut buf, 0, 0, 1, 0).unwrap();
        let n = {
            let mut e = SnapshotEncoder::start(w.tail(), 10, 7, 1).unwrap();
            e.push_level(1_000_000, 10, 1, Side::Bid).unwrap();
            e.finish()
        };
        w.commit(n).unwrap();
        let len = w.finish();

        let mut books = Books::new();
        let reader = PacketReader::new(&buf[..len]).unwrap();
        let (_seq, msg) = reader.messages().next().unwrap().unwrap();
        assert_eq!(
            apply_message(&mut books, &msg),
            Err(ApplyError::SnapshotNeedsReplay)
        );
    }
}
