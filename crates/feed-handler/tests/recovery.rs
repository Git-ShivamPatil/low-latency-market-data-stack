//! Milestone 4's verification: does a handler that lost messages get back to a
//! book it can trust?
//!
//! These drive the arbitrator and the recovery buffer directly. As with the
//! redundancy tests, that is not a shortcut: only a simulated feed can black out
//! both arms for a controlled interval and then assert on the exact state that
//! came out the other side. `scripts/smoke.sh` covers the two-process path.

use std::time::{Duration, Instant};

use book::{apply_message, apply_snapshot, BookDigest, Books};
use feed_handler::arbitration::{Accepted, Arbitrator};
use feed_handler::recovery::{RecoveryBuffer, RecoveryFailure};
use wire::{Message, PacketReader, PacketWriter, Side};

const BATCH: u16 = 8;

/// A publisher that also keeps the book it is describing, so a snapshot can be
/// produced from it at any point.
struct Feed {
    books: Books,
    next_seq: u64,
    next_order: u64,
}

impl Feed {
    fn new() -> Self {
        Self {
            books: Books::new(),
            next_seq: 1,
            next_order: 1,
        }
    }

    /// One datagram of `BATCH` AddOrders, applied to the publisher's own book.
    fn next_datagram(&mut self) -> Vec<u8> {
        let mut buf = vec![0u8; 4096];
        let first = self.next_seq;
        let mut w = PacketWriter::new(&mut buf, 0, 0, first, 0).unwrap();
        for i in 0..BATCH {
            let id = self.next_order;
            self.next_order += 1;
            let price = 1_000_000 + i64::from(i % 5) * 100;
            w.add_order(id, price, 10, 7, Side::Bid).unwrap();
            self.books
                .get_or_create(7)
                .add(id, Side::Bid, price, 10)
                .unwrap();
        }
        let n = w.finish();
        buf.truncate(n);
        self.next_seq += u64::from(BATCH);
        buf
    }

    /// A single-fragment snapshot of the whole book, as of everything published.
    fn snapshot(&self) -> Vec<u8> {
        let mut buf = vec![0u8; 262_144];
        let mut w = PacketWriter::new(&mut buf, 0, wire::PACKET_FLAG_SNAPSHOT, 1, 0).unwrap();
        let n = {
            let mut e = wire::SnapshotEncoder::start(
                w.tail(),
                self.next_seq - 1,
                7,
                wire::SNAPSHOT_FLAG_CYCLE_START
                    | wire::SNAPSHOT_FLAG_LAST_FRAGMENT
                    | wire::SNAPSHOT_FLAG_CYCLE_END,
            )
            .unwrap();
            if let Some(b) = self.books.get(7) {
                for o in b.orders_in_queue_order(Side::Bid) {
                    e.push_order(o.order_id, o.price, o.quantity, o.side)
                        .unwrap();
                }
                for o in b.orders_in_queue_order(Side::Ask) {
                    e.push_order(o.order_id, o.price, o.quantity, o.side)
                        .unwrap();
                }
            }
            e.finish()
        };
        w.commit(n).unwrap();
        let len = w.finish();
        buf.truncate(len);
        buf
    }
}

/// A consumer: arbitrator, recovery buffer, and the book being rebuilt.
struct Consumer {
    arb: Arbitrator,
    rec: RecoveryBuffer,
    books: Books,
}

impl Consumer {
    fn new() -> Self {
        Self {
            arb: Arbitrator::new(64, 4096),
            rec: RecoveryBuffer::new(512, 4096, Duration::from_secs(30)),
            books: Books::new(),
        }
    }

    fn apply_datagram(books: &mut Books, bytes: &[u8], skip_below: u64) {
        let reader = PacketReader::new(bytes).unwrap();
        for m in reader.messages() {
            let (seq, msg) = m.unwrap();
            if seq < skip_below {
                continue;
            }
            apply_message(books, &msg).unwrap();
        }
    }

    fn offer(&mut self, arm: u8, bytes: &[u8], now: Instant) -> Result<(), RecoveryFailure> {
        match self.arb.accept(arm, bytes) {
            Accepted::Ready {
                first_sequence,
                count,
            } => {
                if self.rec.is_recovering() {
                    self.rec.hold(first_sequence, count, bytes)?;
                } else {
                    Self::apply_datagram(&mut self.books, bytes, 0);
                }
            }
            Accepted::ForcedGap(gap) => self.rec.begin(gap.from, now),
            _ => {}
        }

        if self.rec.is_recovering() {
            let rec = &mut self.rec;
            let mut overflow = None;
            self.arb.drain_ready(|first, count, bytes| {
                if overflow.is_none() {
                    if let Err(e) = rec.hold(first, count, bytes) {
                        overflow = Some(e);
                    }
                }
            });
            if let Some(e) = overflow {
                return Err(e);
            }
        } else {
            let books = &mut self.books;
            self.arb
                .drain_ready(|_f, _c, bytes| Self::apply_datagram(books, bytes, 0));
        }
        Ok(())
    }

    /// Feeds a snapshot and, if it closes the gap, replays the held traffic.
    fn offer_snapshot(&mut self, bytes: &[u8], now: Instant) -> bool {
        if !self.rec.is_recovering() {
            return false;
        }
        let reader = PacketReader::new(bytes).unwrap();
        let (_seq, msg) = reader.messages().next().unwrap().unwrap();
        let Message::Snapshot(d) = msg else {
            return false;
        };
        if !self.rec.snapshot_is_usable(d.last_sequence()) {
            return false;
        }
        self.books.clear_all();
        apply_snapshot(&mut self.books, &d).unwrap();

        let last = d.last_sequence();
        self.rec.adopt_snapshot(last);
        self.arb.resync_to(last + 1);

        let books = &mut self.books;
        self.rec
            .replay(|skip_below, bytes| Self::apply_datagram(books, bytes, skip_below));
        self.rec.complete(now);
        true
    }

    /// What the handler does when the feed goes quiet with a hole outstanding.
    ///
    /// A gap is normally forced by the reorder window filling, which needs a lot
    /// of subsequent traffic. When a burst simply ends — or the publisher pauses
    /// — the hole is found this way instead, and the real receive loop calls it
    /// after `gap_timeout_millis` of silence.
    fn quiesce(&mut self, now: Instant) {
        if let Some(gap) = self.arb.declare_gap_if_stalled() {
            self.rec.begin(gap.from, now);
        }
        if self.rec.is_recovering() {
            let rec = &mut self.rec;
            self.arb.drain_ready(|first, count, bytes| {
                let _ = rec.hold(first, count, bytes);
            });
        } else {
            let books = &mut self.books;
            self.arb
                .drain_ready(|_f, _c, bytes| Self::apply_datagram(books, bytes, 0));
        }
    }

    fn is_recovering(&self) -> bool {
        self.rec.is_recovering()
    }
}

/// The headline: a range lost on both arms, then a snapshot puts the book back.
#[test]
fn a_gap_is_recovered_and_the_book_matches_the_publisher_exactly() {
    let mut feed = Feed::new();
    let mut c = Consumer::new();
    let t0 = Instant::now();

    for _ in 0..20 {
        let d = feed.next_datagram();
        c.offer(0, &d, t0).unwrap();
        c.offer(1, &d, t0).unwrap();
    }
    assert_eq!(
        BookDigest::of(&c.books),
        BookDigest::of(&feed.books),
        "a clean stream should already agree"
    );

    // One datagram lost on BOTH arms. Redundancy cannot help.
    let _lost = feed.next_datagram();

    // More live traffic arrives, which forces the gap to be noticed and then has
    // to be held rather than applied.
    for _ in 0..10 {
        let d = feed.next_datagram();
        c.offer(0, &d, t0).unwrap();
        c.offer(1, &d, t0).unwrap();
    }
    c.quiesce(t0);
    assert!(c.is_recovering(), "a double loss must start a recovery");
    assert_ne!(
        BookDigest::of(&c.books),
        BookDigest::of(&feed.books),
        "while recovering, the book is knowingly behind"
    );

    let snap = feed.snapshot();
    assert!(
        c.offer_snapshot(&snap, t0 + Duration::from_millis(50)),
        "the snapshot must be usable"
    );

    assert!(!c.is_recovering(), "recovery should be complete");
    assert_eq!(
        BookDigest::of(&c.books),
        BookDigest::of(&feed.books),
        "the recovered book must be the publisher's book, exactly"
    );
    c.books.check_invariants().unwrap();
}

/// The case the milestone names explicitly: both arms dark for a while.
#[test]
fn a_blackout_on_both_arms_is_recovered_from() {
    let mut feed = Feed::new();
    let mut c = Consumer::new();
    let t0 = Instant::now();

    for _ in 0..10 {
        let d = feed.next_datagram();
        c.offer(0, &d, t0).unwrap();
        c.offer(1, &d, t0).unwrap();
    }

    // Nothing reaches the consumer at all for a long stretch: 200 datagrams,
    // 1600 messages, gone on both arms. A total outage, not packet loss — no
    // amount of redundancy addresses it.
    for _ in 0..200 {
        let _ = feed.next_datagram();
    }

    for _ in 0..5 {
        let d = feed.next_datagram();
        c.offer(0, &d, t0).unwrap();
        c.offer(1, &d, t0).unwrap();
    }
    c.quiesce(t0);
    assert!(c.is_recovering(), "a blackout must be noticed");

    let snap = feed.snapshot();
    assert!(c.offer_snapshot(&snap, t0 + Duration::from_millis(120)));

    assert!(!c.is_recovering());
    assert_eq!(
        BookDigest::of(&c.books),
        BookDigest::of(&feed.books),
        "1600 missed messages must not leave a trace once the snapshot lands"
    );
    assert_eq!(
        c.books.total_orders(),
        feed.books.total_orders(),
        "including the orders that rested entirely during the blackout"
    );
}

/// A snapshot older than the gap cannot close it and must be refused.
#[test]
fn a_snapshot_from_before_the_gap_is_refused_rather_than_adopted() {
    let mut feed = Feed::new();
    let mut c = Consumer::new();
    let t0 = Instant::now();

    for _ in 0..5 {
        let d = feed.next_datagram();
        c.offer(0, &d, t0).unwrap();
    }
    // Taken here, so it reflects the book as of sequence 40.
    let stale = feed.snapshot();

    // More traffic arrives and is applied, so the consumer is now AHEAD of the
    // snapshot. Only then does a loss occur. A snapshot from before the messages
    // the consumer already has is the genuinely useless case: adopting it would
    // discard good state and still leave the hole.
    for _ in 0..3 {
        let d = feed.next_datagram();
        c.offer(0, &d, t0).unwrap();
    }
    let _lost = feed.next_datagram();
    for _ in 0..5 {
        let d = feed.next_datagram();
        c.offer(0, &d, t0).unwrap();
    }
    c.quiesce(t0);
    assert!(c.is_recovering());

    assert!(
        !c.offer_snapshot(&stale, t0),
        "a snapshot predating the gap leaves the hole and destroys good state"
    );
    assert!(c.is_recovering(), "and recovery must still be outstanding");

    let fresh = feed.snapshot();
    assert!(c.offer_snapshot(&fresh, t0));
    assert_eq!(BookDigest::of(&c.books), BookDigest::of(&feed.books));
}

/// Recovery has a failure mode, and it is reported rather than hung on.
#[test]
fn a_recovery_that_never_gets_a_snapshot_fails_instead_of_hanging() {
    let mut feed = Feed::new();
    let mut c = Consumer::new();
    // A buffer far too small for the traffic that will arrive.
    c.rec = RecoveryBuffer::new(4, 4096, Duration::from_millis(50));
    let t0 = Instant::now();

    for _ in 0..3 {
        let d = feed.next_datagram();
        c.offer(0, &d, t0).unwrap();
    }
    let _lost = feed.next_datagram();

    // The arbitrator cannot know anything is missing until traffic past the hole
    // arrives, so offer some first, then let the feed go quiet to declare it.
    for _ in 0..2 {
        let d = feed.next_datagram();
        c.offer(0, &d, t0).unwrap();
    }
    c.quiesce(t0);
    assert!(
        c.is_recovering(),
        "the gap must be open before the buffer can fill"
    );

    let mut failed = false;
    for _ in 0..40 {
        let d = feed.next_datagram();
        if c.offer(0, &d, t0).is_err() {
            failed = true;
            break;
        }
    }
    assert!(
        failed,
        "a buffer that cannot hold the traffic must report failure, not grow"
    );

    // The deadline is the other way it ends.
    let mut c2 = Consumer::new();
    c2.rec = RecoveryBuffer::new(512, 4096, Duration::from_millis(10));
    c2.rec.begin(1, t0);
    assert!(c2
        .rec
        .check_deadline(t0 + Duration::from_millis(50))
        .is_err());
}

/// Queue order has to survive the round trip, or price-time priority is a lie.
#[test]
fn recovery_restores_queue_position_not_just_quantity() {
    let mut feed = Feed::new();
    let mut c = Consumer::new();
    let t0 = Instant::now();

    for _ in 0..12 {
        let d = feed.next_datagram();
        c.offer(0, &d, t0).unwrap();
        c.offer(1, &d, t0).unwrap();
    }
    let _lost = feed.next_datagram();
    for _ in 0..5 {
        let d = feed.next_datagram();
        c.offer(0, &d, t0).unwrap();
        c.offer(1, &d, t0).unwrap();
    }

    c.quiesce(t0);
    let snap = feed.snapshot();
    assert!(c.offer_snapshot(&snap, t0));

    // The front of each price level must be the same order on both sides. The
    // digest already covers this; stating it directly is what makes the failure
    // legible when it breaks.
    for side in [Side::Bid, Side::Ask] {
        assert_eq!(
            c.books.get(7).map(|b| b.front(side)),
            feed.books.get(7).map(|b| b.front(side)),
            "the front of the {side:?} queue must be the same order"
        );
    }
}

/// A clean stream must never enter recovery.
#[test]
fn a_lossless_stream_never_recovers_and_never_needs_to() {
    let mut feed = Feed::new();
    let mut c = Consumer::new();
    let t0 = Instant::now();

    for _ in 0..100 {
        let d = feed.next_datagram();
        c.offer(0, &d, t0).unwrap();
        c.offer(1, &d, t0).unwrap();
    }
    c.quiesce(t0);
    assert!(!c.is_recovering(), "a quiet period must not invent a gap");
    assert_eq!(c.rec.stats().attempts, 0);
    assert_eq!(BookDigest::of(&c.books), BookDigest::of(&feed.books));
}
