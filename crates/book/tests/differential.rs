//! The fast book against the obviously-correct one, operation for operation.
//!
//! # Why this and not more unit tests
//!
//! The fast book has three redundant structures — a slab, an open-addressed
//! map, and intrusive lists over a rebasing dense array — that have to stay in
//! agreement through adds, cancels, in-place reduces, price-changing replaces,
//! window slides, and wholesale snapshot rebuilds. The interesting bugs are not
//! in any one operation; they are in sequences. A unit test asserts what its
//! author thought to check, and the author is the same person who wrote the bug.
//!
//! So the real claim is this: **run the same stream through both books and they
//! agree on everything, every time.** Not just the final state — every return
//! value, including which operations were refused, the aggregated levels, and
//! the exact queue order within each level.
//!
//! That last one matters more than it looks. The digest covers aggregated
//! levels, so a book that put orders in the wrong order within a price would
//! still digest identically — and would be wrong in exactly the way milestone 4
//! established that snapshots exist to prevent. Queue order is compared
//! separately.
//!
//! # Size
//!
//! The default is a few hundred thousand operations so `cargo test` stays quick.
//! `BOOK_DIFF_OPS` raises it; CI runs a larger number in release. The stream is
//! seeded and reproducible, so a failure can be replayed exactly by rerunning
//! with the seed it prints.

use book::{
    apply_snapshot, BookDigest, BookError, BookSet, Books, FastBooks, Level, MboCapacity,
    OrderBook, RestingOrder,
};
use std::collections::VecDeque;

use wire::Side;

/// SplitMix64. Hand-rolled rather than pulled in, for the same reason the rest
/// of this project hand-rolls its randomness: a test whose stream depends on a
/// crate version is not reproducible from a seed alone.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

const SYMBOLS: &[u16] = &[1, 2, 3];
const TICK: i64 = 100;
const MID: i64 = 1_000_000;
/// Kept well inside the fast book's window so a capacity refusal never fires.
/// If one does, the test says so rather than passing on a technicality.
const SPREAD_TICKS: i64 = 300;
/// How often the mid gaps, and by how much. See `Stream::price`.
const JUMP_EVERY: usize = 20_000;
const JUMP_TICKS: i64 = 1_000;

fn capacity() -> MboCapacity {
    MboCapacity {
        levels: 4096,
        orders: 1 << 14,
        reference_price: MID,
        tick: TICK,
    }
}

fn ops() -> usize {
    std::env::var("BOOK_DIFF_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300_000)
}

fn levels_of<B: OrderBook>(book: &B, side: Side) -> Vec<Level> {
    let mut out = Vec::new();
    book.for_each_level(side, 0, &mut |l| out.push(l));
    out
}

fn queue_of<B: OrderBook>(book: &B, side: Side) -> Vec<RestingOrder> {
    let mut out = Vec::new();
    book.for_each_order(side, &mut |o| {
        out.push(o);
        true
    });
    out
}

/// Everything the two books must agree about, checked for one symbol.
fn compare_symbol(slow: &impl OrderBook, fast: &impl OrderBook, symbol: u16, at: usize) {
    assert_eq!(
        slow.len(),
        fast.len(),
        "symbol {symbol} after {at} ops: resting order counts differ"
    );
    for side in [Side::Bid, Side::Ask] {
        assert_eq!(
            levels_of(slow, side),
            levels_of(fast, side),
            "symbol {symbol} after {at} ops: {side:?} levels differ"
        );
        // The check the digest cannot make. Two books can aggregate to the same
        // levels and still disagree about who is at the front of the queue,
        // which is precisely the property an order-level snapshot exists to
        // restore.
        assert_eq!(
            queue_of(slow, side),
            queue_of(fast, side),
            "symbol {symbol} after {at} ops: {side:?} queue order differs"
        );
    }
}

fn compare_all(slow: &Books, fast: &FastBooks, at: usize) {
    slow.check_invariants()
        .unwrap_or_else(|e| panic!("reference book broke its own invariants after {at} ops: {e}"));
    fast.check_invariants()
        .unwrap_or_else(|e| panic!("fast book broke its own invariants after {at} ops: {e}"));
    assert_eq!(
        BookDigest::of(slow),
        BookDigest::of(fast),
        "digests diverged after {at} ops"
    );
    for symbol in SYMBOLS {
        // A symbol missing from one set and empty in the other is agreement:
        // the fast set pre-allocates its books, the reference set creates them
        // on first touch. Both hold nothing.
        match (slow.get(*symbol), fast.get(*symbol)) {
            (Some(a), Some(b)) => compare_symbol(a, b, *symbol, at),
            (None, Some(b)) => assert!(
                b.is_empty(),
                "symbol {symbol} after {at} ops: absent from the reference set \
                 but holds {} orders in the fast set",
                b.len()
            ),
            (Some(a), None) => assert!(
                a.is_empty(),
                "symbol {symbol} after {at} ops: absent from the fast set but \
                 holds {} orders in the reference set",
                a.len()
            ),
            (None, None) => {}
        }
    }
}

/// A capacity refusal means the test is mis-sized, not that the book is wrong.
/// Saying so loudly beats silently exercising a smaller book than intended.
fn assert_not_a_capacity_limit(e: &BookError, at: usize) {
    match e {
        BookError::SlabFull { .. } | BookError::PriceOutOfRange { .. } => panic!(
            "the fast book hit a capacity limit after {at} ops ({e}); \
             the test is mis-sized, so it is no longer comparing what it claims to"
        ),
        _ => {}
    }
}

struct Stream {
    rng: Rng,
    /// Order ids believed to be resting, oldest first, with the symbol they
    /// rest under. Only a hint — the stream deliberately also names ids that
    /// are not resting.
    live: VecDeque<(u16, u64)>,
    next_id: u64,
    mid: i64,
    since_jump: usize,
}

impl Stream {
    fn new(seed: u64) -> Self {
        Self {
            rng: Rng(seed),
            live: VecDeque::new(),
            next_id: 1,
            mid: MID,
            since_jump: 0,
        }
    }

    fn symbol(&mut self) -> u16 {
        SYMBOLS[self.rng.below(SYMBOLS.len() as u64) as usize]
    }

    fn price(&mut self) -> i64 {
        self.since_jump += 1;
        if self.since_jump >= JUMP_EVERY {
            // The market gaps: it reopens somewhere else. This is here because
            // a plain random walk does not leave a 4096-tick window over a
            // 300k-operation run, and the first version of this test recorded
            // zero rebases — silently never exercising the one path in the fast
            // book that has no counterpart in the reference book.
            //
            // The jump is sized to stay reboundable: the old orders have not
            // been cancelled yet, so the occupied span briefly covers both
            // neighbourhoods, and it has to still fit the window.
            self.since_jump = 0;
            let sign = if self.rng.below(2) == 0 { 1 } else { -1 };
            self.mid += sign * JUMP_TICKS * TICK;
        }
        // A random walk, so the window slides gradually as well as in jumps.
        self.mid += (self.rng.below(3) as i64 - 1) * TICK;
        let off = self.rng.below(SPREAD_TICKS as u64 * 2) as i64 - SPREAD_TICKS;
        self.mid + off * TICK
    }

    fn quantity(&mut self) -> u32 {
        1 + self.rng.below(1000) as u32
    }

    /// An id that is probably resting; occasionally one that certainly is not,
    /// so the two books are compared on their refusals as well as their work.
    fn some_id(&mut self) -> (u16, u64) {
        if self.live.is_empty() || self.rng.below(100) < 3 {
            (self.symbol(), self.next_id + self.rng.below(50))
        } else {
            let i = self.rng.below(self.live.len() as u64) as usize;
            self.live[i]
        }
    }

    /// Which resting order a cancel should name.
    ///
    /// Weighted heavily towards the oldest. A uniform pick leaves orders
    /// resting hundreds of ticks from the touch for the entire run, so the
    /// occupied span grows without bound and eventually exceeds any window a
    /// real system would size — which is not a fact about the book, it is a
    /// fact about a generator that does not behave like a market.
    ///
    /// `None` means "name an id that is definitely not resting", so the two
    /// books are compared on their refusals too.
    fn cancel_target(&mut self) -> Option<usize> {
        if self.live.is_empty() || self.rng.below(100) < 3 {
            None
        } else if self.rng.below(100) < 80 {
            Some(0)
        } else {
            Some(self.rng.below(self.live.len() as u64) as usize)
        }
    }
}

/// Applies one random operation to both books and requires identical outcomes.
///
/// Returns nothing: every assertion is inside, because the point is that there
/// is no outcome either book can produce that the other cannot.
fn step(s: &mut Stream, slow: &mut Books, fast: &mut FastBooks, at: usize) {
    // Adds outnumber cancels until the book reaches a realistic depth, then the
    // mix inverts. A fixed mix with more adds than deletes grows the book
    // without bound, and a long run stops being a comparison and becomes a
    // slab-capacity test — which is how the first version of this ended.
    const TARGET_RESTING: usize = 6_000;
    let (add_pct, del_pct) = if s.live.len() < TARGET_RESTING {
        (55, 20)
    } else {
        (20, 55)
    };
    let roll = s.rng.below(100);
    if roll < add_pct {
        let symbol = s.symbol();
        let id = s.next_id;
        s.next_id += 1;
        let side = if s.rng.below(2) == 0 {
            Side::Bid
        } else {
            Side::Ask
        };
        let (price, qty) = (s.price(), s.quantity());
        let a = OrderBook::add(slow.get_or_create(symbol), id, side, price, qty);
        let b = OrderBook::add(fast.get_or_create(symbol), id, side, price, qty);
        if let Err(e) = &b {
            assert_not_a_capacity_limit(e, at);
        }
        assert_eq!(a, b, "add({id}, {side:?}, {price}, {qty}) after {at} ops");
        if a.is_ok() {
            s.live.push_back((symbol, id));
        }
    } else if roll < add_pct + del_pct {
        let pick = s.cancel_target();
        let (symbol, id) = match pick {
            Some(i) => s.live[i],
            None => (s.symbol(), s.next_id + s.rng.below(50)),
        };
        let a = OrderBook::delete(slow.get_or_create(symbol), id);
        let b = OrderBook::delete(fast.get_or_create(symbol), id);
        assert_eq!(a, b, "delete({id}) after {at} ops");
        if a.is_ok() {
            if let Some(i) = pick {
                s.live.remove(i);
            }
        }
    } else if roll < add_pct + del_pct + 13 {
        let (symbol, id) = s.some_id();
        // Aim at a real reduce most of the time, but leave room for the two
        // refusals — zero, and an increase — so those agree too.
        let current = slow.get(symbol).and_then(|b| OrderBook::get(b, id));
        let new_qty = match current {
            Some(o) if s.rng.below(10) > 0 => 1 + s.rng.below(u64::from(o.quantity)) as u32,
            _ => s.quantity(),
        };
        let a = OrderBook::reduce(slow.get_or_create(symbol), id, new_qty);
        let b = OrderBook::reduce(fast.get_or_create(symbol), id, new_qty);
        assert_eq!(a, b, "reduce({id}, {new_qty}) after {at} ops");
    } else {
        let (symbol, id) = s.some_id();
        let (price, qty) = (s.price(), s.quantity());
        let a = OrderBook::replace(slow.get_or_create(symbol), id, price, qty);
        let b = OrderBook::replace(fast.get_or_create(symbol), id, price, qty);
        if let Err(e) = &b {
            assert_not_a_capacity_limit(e, at);
        }
        assert_eq!(a, b, "replace({id}, {price}, {qty}) after {at} ops");
    }
}

#[test]
fn the_fast_book_agrees_with_the_reference_book_over_a_long_random_stream() {
    let seed = 0x5EED_0001;
    let total = ops();
    let mut s = Stream::new(seed);
    let mut slow = Books::new();
    let mut fast = FastBooks::uniform(SYMBOLS, capacity());

    for at in 0..total {
        step(&mut s, &mut slow, &mut fast, at);
        // Full comparison is O(book), so it is periodic. Every operation is
        // still compared on its return value inside `step`.
        if at % 2_000 == 0 {
            compare_all(&slow, &fast, at);
        }
    }
    compare_all(&slow, &fast, total);
    assert!(
        slow.total_orders() > 100,
        "seed {seed:#x} produced an almost-empty book, so this proved very little"
    );
    let rebases = fast.stats().rebases;
    assert!(
        rebases > 0,
        "the price window never moved, so the rebase path — the only part of \
            the fast book with nothing to compare against — was not exercised"
    );
    println!(
        "{total} operations, seed {seed:#x}, {} orders resting, {rebases} rebases",
        slow.total_orders()
    );
}

#[test]
fn a_snapshot_rebuild_lands_both_books_in_the_same_state() {
    // The recovery path, which is where a book is most likely to diverge: it is
    // cleared wholesale and refilled from the wire in queue order. A fast book
    // whose `clear` left the free list or the level counts wrong would rebuild
    // into something plausible and subtly different.
    let mut s = Stream::new(0x5EED_0002);
    let mut slow = Books::new();
    let mut fast = FastBooks::uniform(SYMBOLS, capacity());

    for at in 0..20_000 {
        step(&mut s, &mut slow, &mut fast, at);
    }
    compare_all(&slow, &fast, 20_000);

    // Publish what the reference book holds, in the order a snapshot would carry
    // it, then rebuild both from that.
    let mut published: Vec<(u16, Vec<RestingOrder>)> = Vec::new();
    for symbol in SYMBOLS {
        let Some(b) = slow.get(*symbol) else { continue };
        let mut orders = queue_of(b, Side::Bid);
        orders.extend(queue_of(b, Side::Ask));
        published.push((*symbol, orders));
    }

    slow.clear_all();
    fast.clear_all();
    assert_eq!(slow.total_orders(), 0);
    assert_eq!(fast.total_orders(), 0);

    for (symbol, orders) in &published {
        for o in orders {
            OrderBook::add(
                slow.get_or_create(*symbol),
                o.order_id,
                o.side,
                o.price,
                o.quantity,
            )
            .unwrap();
            OrderBook::add(
                fast.get_or_create(*symbol),
                o.order_id,
                o.side,
                o.price,
                o.quantity,
            )
            .unwrap();
        }
    }
    compare_all(&slow, &fast, 20_000);

    // And the rebuilt books keep working, from the same slab and window.
    for at in 20_000..40_000 {
        step(&mut s, &mut slow, &mut fast, at);
    }
    compare_all(&slow, &fast, 40_000);
}

#[test]
fn a_wire_snapshot_rebuilds_both_implementations_identically() {
    // The same thing again, but through the real decoder rather than through
    // structs the test built itself — so the generic `apply_snapshot` is what is
    // under test, not just the books.
    use wire::{PacketReader, PacketWriter, SnapshotEncoder};

    let orders = [
        (1u64, Side::Bid, 1_000_000i64, 10u32),
        (2, Side::Bid, 1_000_000, 20),
        (3, Side::Bid, 999_900, 5),
        (4, Side::Ask, 1_000_100, 7),
        (5, Side::Ask, 1_000_100, 3),
        (6, Side::Ask, 1_000_300, 9),
    ];

    let mut buf = vec![0u8; 8192];
    let mut w = PacketWriter::new(&mut buf, 0, wire::PACKET_FLAG_SNAPSHOT, 1, 0).expect("header");
    let written = {
        let mut e =
            SnapshotEncoder::start(w.tail(), 100, 1, wire::SNAPSHOT_FLAG_LAST_FRAGMENT).unwrap();
        for (id, side, price, qty) in orders {
            e.push_order(id, price, qty, side).unwrap();
        }
        e.finish()
    };
    w.commit(written).unwrap();
    let n = w.finish();

    let mut slow = Books::new();
    let mut fast = FastBooks::uniform(&[1], capacity());
    let reader = PacketReader::new(&buf[..n]).expect("reader");
    for m in reader.messages() {
        let (_seq, msg) = m.expect("decode");
        let wire::Message::Snapshot(d) = msg else {
            panic!("expected a snapshot")
        };
        apply_snapshot(&mut slow, &d).expect("reference");
        apply_snapshot(&mut fast, &d).expect("fast");
    }

    compare_all(&slow, &fast, 0);
    assert_eq!(slow.total_orders(), orders.len());
    // Queue order, not just totals: order 1 must still be ahead of order 2.
    let q = queue_of(fast.get(1).unwrap(), Side::Bid);
    assert_eq!(
        q.iter().map(|o| o.order_id).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}
