//! Milestone 5's verification: the steady-state path does not touch the heap.
//!
//! # What is being claimed, precisely
//!
//! *Consuming a datagram — decoding it, arbitrating it, and applying every
//! message in it to both book views — performs zero heap operations.* Not "few".
//! Zero allocations, zero deallocations, zero reallocations, measured on the
//! thread doing the work.
//!
//! # What is deliberately outside it
//!
//! **Startup.** Books, windows, slabs and maps are sized once. Every one of
//! those allocations happens before the measurement starts, and the test forces
//! that by pre-creating the symbol rather than letting the first message do it.
//!
//! **Producing the feed.** The publisher in this file allocates freely. It is
//! standing in for a separate process; measuring it would be measuring the test
//! harness.
//!
//! **Failure paths.** A panic message or an error being formatted allocates.
//! Those are not steady state.
//!
//! Everything else is in. In particular the **recovery cycle** is in: clearing
//! the books, applying a snapshot over them, and replaying the held datagrams.
//! That is the part most likely to allocate quietly, because it is the part that
//! looks like it needs to rebuild something — and it is why the fast book has a
//! `clear` that keeps its memory instead of a `Default` that hands it back.
//!
//! # Why a test and not a flag
//!
//! `--verify-allocations` reports the same number, and a human has to run it and
//! read it. One `format!` on an error path, one `Vec` that grows during a
//! resend, and the claim is gone without a single test going red. This is the
//! assertion that makes that impossible.

use std::time::{Duration, Instant};

use alloc_guard::{AllocCounts, AllocGuard, CountingAllocator};
use book::{
    apply_message, apply_snapshot, BookDigest, BookSet, Books, DigestLog, FastBooks, MboCapacity,
};
use feed_handler::arbitration::{Accepted, Arbitrator};
use feed_handler::recovery::RecoveryBuffer;
use wire::{Message, PacketReader, PacketWriter, Side};

/// Installed for this test binary only — the same opt-in a real binary makes.
#[global_allocator]
static ALLOC: CountingAllocator<std::alloc::System> = CountingAllocator::new(std::alloc::System);

const SYMBOL: u16 = 7;
const BATCH: u16 = 8;
const TICK: i64 = 100;
const MID: i64 = 1_000_000;
const MAX_DATAGRAM: usize = 4096;
/// Where the publisher stops growing the book and starts cancelling to make
/// room. Comfortably inside the slab, so a `SlabFull` never masks the thing
/// being measured.
const RESTING_CAP: usize = 20_000;

fn capacity() -> MboCapacity {
    MboCapacity {
        levels: 4096,
        orders: 1 << 16,
        reference_price: MID,
        tick: TICK,
    }
}

/// The publisher. Allocates as much as it likes: it stands in for the process on
/// the other side of the wire, and nothing here is measured.
struct Feed {
    books: Books,
    next_seq: u64,
    next_order: u64,
    /// Orders resting long enough to be cancelled, oldest first.
    resting: std::collections::VecDeque<u64>,
}

impl Feed {
    fn new() -> Self {
        Self {
            books: Books::new(),
            next_seq: 1,
            next_order: 1,
            resting: std::collections::VecDeque::new(),
        }
    }

    /// Writes one datagram into `buf` and returns its length.
    ///
    /// A mix of adds and cancels rather than adds alone, so the book churns
    /// through the slab and the order-id map instead of only ever growing —
    /// which is where a `HashMap` would have reallocated and a tombstoned table
    /// would have degraded.
    fn next_datagram(&mut self, buf: &mut [u8]) -> usize {
        let first = self.next_seq;
        let mut w = PacketWriter::new(buf, 0, 0, first, 0).unwrap();
        for i in 0..BATCH {
            let book = self.books.get_or_create(SYMBOL);
            if self.resting.len() > RESTING_CAP && i % 2 == 1 {
                let id = self.resting.pop_front().unwrap();
                w.delete_order(id, SYMBOL, Side::Bid).unwrap();
                book.delete(id).unwrap();
            } else {
                let id = self.next_order;
                self.next_order += 1;
                let price = MID + i64::from(i % 32) * TICK;
                w.add_order(id, price, 10, SYMBOL, Side::Bid).unwrap();
                book.add(id, Side::Bid, price, 10).unwrap();
                self.resting.push_back(id);
            }
        }
        self.next_seq += u64::from(BATCH);
        w.finish()
    }

    /// A single-fragment snapshot of the whole book, as of everything published.
    fn snapshot(&self) -> Vec<u8> {
        let mut buf = vec![0u8; 4 * 1024 * 1024];
        let mut w = PacketWriter::new(&mut buf, 0, wire::PACKET_FLAG_SNAPSHOT, 1, 0).unwrap();
        let n = {
            let mut e = wire::SnapshotEncoder::start(
                w.tail(),
                self.next_seq - 1,
                SYMBOL,
                wire::SNAPSHOT_FLAG_CYCLE_START
                    | wire::SNAPSHOT_FLAG_LAST_FRAGMENT
                    | wire::SNAPSHOT_FLAG_CYCLE_END,
            )
            .unwrap();
            if let Some(b) = self.books.get(SYMBOL) {
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

/// The consumer: exactly the pieces the real receive loop runs per datagram.
struct Consumer {
    arb: Arbitrator,
    rec: RecoveryBuffer,
    books: FastBooks,
    /// Checkpointing is part of the measured path, and it is where the last
    /// allocation in the handler turned out to be hiding: `BookDigest::to_fields`
    /// built a `String` per checkpoint. The two-process smoke test found it
    /// because it ran the whole binary; this test did not, because it had no
    /// digest log. Now it has one.
    digest_log: DigestLog,
}

impl Consumer {
    fn new() -> Self {
        Self {
            arb: Arbitrator::new(64, MAX_DATAGRAM),
            rec: RecoveryBuffer::new(512, MAX_DATAGRAM, Duration::from_secs(30)),
            // Pre-created, so the several megabytes this symbol costs are spent
            // before anything is measured. That is the startup boundary, made
            // explicit rather than assumed.
            books: FastBooks::uniform(&[SYMBOL], capacity()),
            digest_log: DigestLog::open(Some(&digest_path())).expect("digest log"),
        }
    }

    fn apply_datagram(books: &mut FastBooks, bytes: &[u8], skip_below: u64) {
        let reader = PacketReader::new(bytes).unwrap();
        for m in reader.messages() {
            let (seq, msg) = m.unwrap();
            if seq < skip_below {
                continue;
            }
            apply_message(books, &msg).unwrap();
        }
    }

    /// One datagram, start to finish. This is the measured unit.
    fn offer(&mut self, arm: u8, bytes: &[u8], now: Instant) {
        match self.arb.accept(arm, bytes) {
            Accepted::Ready {
                first_sequence,
                count,
            } => {
                if self.rec.is_recovering() {
                    let _ = self.rec.hold(first_sequence, count, bytes);
                } else {
                    Self::apply_datagram(&mut self.books, bytes, 0);
                }
            }
            Accepted::ForcedGap(gap) => self.rec.begin(gap.from, now),
            _ => {}
        }
        let recovering = self.rec.is_recovering();
        let rec = &mut self.rec;
        let books = &mut self.books;
        self.arb.drain_ready(|first, count, bytes| {
            if recovering {
                let _ = rec.hold(first, count, bytes);
            } else {
                Self::apply_datagram(books, bytes, 0);
            }
        });
    }

    /// The recovery cycle: clear, rebuild from the snapshot, replay what was
    /// held. Also measured — this is the part most likely to allocate quietly.
    fn adopt_snapshot(&mut self, bytes: &[u8], now: Instant) -> bool {
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
}

fn digest_path() -> std::path::PathBuf {
    std::env::temp_dir().join("mdstack-allocation-test-digests.txt")
}

/// Accumulates the heap traffic of a scope and remembers where it first
/// happened, so a failure names the datagram rather than just the total.
#[derive(Default)]
struct Ledger {
    total: AllocCounts,
    first_dirty: Option<(usize, AllocCounts)>,
    scopes: usize,
}

impl Ledger {
    fn record(&mut self, at: usize, d: AllocCounts) {
        self.scopes += 1;
        self.total.allocations += d.allocations;
        self.total.deallocations += d.deallocations;
        self.total.reallocations += d.reallocations;
        self.total.bytes += d.bytes;
        if !d.is_clean() && self.first_dirty.is_none() {
            self.first_dirty = Some((at, d));
        }
    }

    fn assert_clean(&self, what: &str) {
        if let Some((at, d)) = self.first_dirty {
            panic!(
                "{what} allocated. First at scope {at}: {d}. \
                 Across {} scopes: {}",
                self.scopes, self.total
            );
        }
        assert!(
            self.total.is_clean(),
            "{what} allocated across {} scopes: {}",
            self.scopes,
            self.total
        );
    }
}

/// The headline assertion: a million messages, including a recovery cycle,
/// without one heap operation.
#[test]
fn a_million_messages_and_a_recovery_cycle_never_touch_the_heap() {
    const MESSAGES: u64 = 1_000_000;
    const DATAGRAMS: usize = (MESSAGES / BATCH as u64) as usize;
    // Where both arms black out. Chosen well past the warm-up so the books are
    // deep by the time they are thrown away and rebuilt.
    const BLACKOUT_AT: usize = DATAGRAMS / 2;
    const BLACKOUT_LEN: usize = 40;

    let mut feed = Feed::new();
    let mut consumer = Consumer::new();
    let mut buf = vec![0u8; MAX_DATAGRAM];
    let now = Instant::now();

    // Warm-up, unmeasured. Anything lazily initialised — thread-locals, the
    // first touch of a symbol, any `std` machinery with a one-time cost — gets
    // its allocation here rather than inside the window.
    for _ in 0..64 {
        let n = feed.next_datagram(&mut buf);
        consumer.offer(0, &buf[..n], now);
        consumer.offer(1, &buf[..n], now);
    }
    // Including a checkpoint: the digest log's buffer is allocated on its first
    // write, and that is startup, not steady state.
    consumer
        .digest_log
        .write(0, BookDigest::of(&consumer.books))
        .expect("warm-up checkpoint");

    let mut steady = Ledger::default();
    let mut recovered = false;
    let mut checkpoints = 0u64;

    for i in 64..DATAGRAMS {
        // Publishing is outside the measurement: it stands in for another
        // process.
        let n = feed.next_datagram(&mut buf);

        // Lost on both arms. The publisher still advances, so a hole opens.
        if (BLACKOUT_AT..BLACKOUT_AT + BLACKOUT_LEN).contains(&i) {
            continue;
        }

        let guard = AllocGuard::start();
        consumer.offer(0, &buf[..n], now);
        consumer.offer(1, &buf[..n], now);
        steady.record(i, guard.finish());

        // A digest at checkpoints, exactly as the handler takes them — inside
        // the window, because that is where the handler takes them.
        if i % 1_000 == 0 {
            let seq = consumer.arb.next_expected();
            let guard = AllocGuard::start();
            let digest = BookDigest::of(&consumer.books);
            consumer.digest_log.write(seq, digest).expect("checkpoint");
            steady.record(i, guard.finish());
            checkpoints += 1;
        }

        // Once the reorder window has filled and the gap has been forced, the
        // snapshot service publishes the book as it stands. Building it is the
        // publisher's work and is not measured; adopting it is the consumer's
        // and is.
        if consumer.rec.is_recovering() && !recovered {
            let snap = feed.snapshot();
            let guard = AllocGuard::start();
            let adopted = consumer.adopt_snapshot(&snap, now);
            steady.record(i, guard.finish());
            assert!(adopted, "the snapshot should have been usable at {i}");
            recovered = true;
        }
    }

    consumer.digest_log.flush().expect("flush");
    steady.assert_clean("the steady-state consume path");
    assert!(
        checkpoints > 100,
        "only {checkpoints} checkpoints were written, so the digest path was barely measured"
    );

    // The measurement is only worth anything if the run actually did the work.
    assert!(
        consumer.arb.gap_count() > 0,
        "no gap was ever declared, so the recovery cycle went unmeasured"
    );
    assert!(recovered, "the snapshot was never adopted");
    assert!(
        !consumer.rec.is_recovering(),
        "the run ended still recovering, so the recovery path did not complete"
    );
    assert!(
        consumer.books.total_orders() > 1_000,
        "the book ended nearly empty, so this measured very little"
    );
    assert!(
        consumer.arb.messages_delivered() > MESSAGES / 2,
        "only {} messages were delivered",
        consumer.arb.messages_delivered()
    );
    println!(
        "{} messages delivered, {} gaps, {} orders resting, {} measured scopes, all clean",
        consumer.arb.messages_delivered(),
        consumer.arb.gap_count(),
        consumer.books.total_orders(),
        steady.scopes
    );
}

/// The same claim, isolated to the operation most likely to break it.
#[test]
fn clearing_and_rebuilding_a_deep_book_returns_no_memory_to_the_allocator() {
    // A book that is cleared and rebuilt is the natural place to write
    // `*book = Book::new()`, which frees several megabytes and asks for them
    // straight back — during a recovery, which is exactly when the process is
    // already behind. This is the test that keeps that from being written.
    let mut feed = Feed::new();
    let mut books = FastBooks::uniform(&[SYMBOL], capacity());
    let mut buf = vec![0u8; MAX_DATAGRAM];

    for _ in 0..2_000 {
        let n = feed.next_datagram(&mut buf);
        Consumer::apply_datagram(&mut books, &buf[..n], 0);
    }
    let before = books.total_orders();
    assert!(before > 5_000, "expected a deep book, got {before}");

    let snapshot = feed.snapshot();
    let reader = PacketReader::new(&snapshot).unwrap();
    let (_seq, msg) = reader.messages().next().unwrap().unwrap();
    let Message::Snapshot(d) = msg else {
        panic!("expected a snapshot")
    };

    let guard = AllocGuard::start();
    books.clear_all();
    apply_snapshot(&mut books, &d).unwrap();
    let delta = guard.finish();

    assert!(
        delta.is_clean(),
        "rebuilding a {before}-order book from a snapshot touched the heap: {delta}"
    );
    assert_eq!(
        books.total_orders(),
        before,
        "the rebuild did not restore the book"
    );
    books.check_invariants().unwrap();
}

/// The counting allocator has to be able to fail, or the tests above prove
/// nothing.
#[test]
fn the_measurement_itself_catches_an_allocation() {
    // Without this, a broken `CountingAllocator` — or an `AllocGuard` that
    // silently measured nothing — would make every assertion above pass.
    let guard = AllocGuard::start();
    let mut v: Vec<u64> = Vec::new();
    for i in 0..1_000 {
        v.push(i);
    }
    let delta = guard.finish();
    assert!(
        !delta.is_clean(),
        "a growing Vec was not detected, so the zero-allocation assertions are vacuous"
    );
    assert!(v.len() == 1_000);
}
