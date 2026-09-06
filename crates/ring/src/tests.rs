//! What has to be true of the ring.
//!
//! These run in one process with the producer and consumer on the same thread,
//! which is not how the ring is used — but every property below is a property
//! of the index arithmetic and the slot layout, and those do not care how many
//! processes are involved. The cross-process story is checked by the C++/Rust
//! interop test, which is the one that would catch a layout disagreement.

use super::*;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering as O};

/// A unique path per test, removed when the guard drops.
struct TempRing(PathBuf);

impl TempRing {
    fn new(name: &str) -> Self {
        static N: AtomicU32 = AtomicU32::new(0);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "mdstack-ring-{name}-{}-{}",
            std::process::id(),
            N.fetch_add(1, O::Relaxed)
        ));
        let _ = std::fs::remove_file(&p);
        Self(p)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempRing {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn pair(t: &TempRing, capacity: u32, slot_size: u32) -> (Producer, Consumer) {
    Ring::create(t.path(), capacity, slot_size).expect("create");
    let p = Ring::open(t.path()).expect("open producer").into_producer();
    let c = Ring::open(t.path()).expect("open consumer").into_consumer();
    (p, c)
}

fn popped(c: &mut Consumer) -> Option<Vec<u8>> {
    c.pop_with(|b| b.to_vec())
}

// --- geometry --------------------------------------------------------------

#[test]
fn a_capacity_that_is_not_a_power_of_two_is_refused() {
    let t = TempRing::new("pow2");
    // Not rounded up silently: the mask depends on it, and a ring that quietly
    // holds 8 when it was asked for 6 makes a capacity-planning number wrong.
    assert_eq!(
        Ring::create(t.path(), 6, 128).unwrap_err(),
        RingError::CapacityNotPowerOfTwo(6)
    );
    assert_eq!(
        Ring::create(t.path(), 0, 128).unwrap_err(),
        RingError::CapacityZero
    );
}

#[test]
fn a_slot_that_would_straddle_a_cache_line_is_refused() {
    let t = TempRing::new("align");
    assert_eq!(
        Ring::create(t.path(), 8, 100).unwrap_err(),
        RingError::BadSlotSize(100)
    );
    // A slot with no room for a message is a slot for nothing.
    assert_eq!(
        Ring::create(t.path(), 8, 0).unwrap_err(),
        RingError::BadSlotSize(0)
    );
    assert!(Ring::create(t.path(), 8, 64).is_ok());
}

#[test]
fn the_file_is_exactly_the_header_plus_the_slots() {
    let t = TempRing::new("size");
    Ring::create(t.path(), 16, 128).expect("create");
    let got = std::fs::metadata(t.path()).expect("stat").len() as usize;
    assert_eq!(got, Ring::file_size(16, 128));
    assert_eq!(got, wire::layout::ring_header::LEN + 16 * 128);
}

#[test]
fn the_geometry_survives_a_reopen() {
    let t = TempRing::new("reopen");
    Ring::create(t.path(), 32, 256).expect("create");
    let r = Ring::open(t.path()).expect("open");
    assert_eq!(r.capacity(), 32);
    assert_eq!(r.slot_size(), 256);
    assert_eq!(r.max_message_len(), 256 - wire::layout::ring_slot::LEN);
}

#[test]
fn a_file_that_is_not_a_ring_is_refused_rather_than_read() {
    let t = TempRing::new("magic");
    // Long enough to map, so the refusal has to come from the magic and not
    // from the length. A ring that trusted the header here would hand out
    // indices computed from somebody else's bytes.
    std::fs::write(t.path(), vec![0xAB; 4096]).expect("write");
    match Ring::open(t.path()) {
        Err(RingError::BadMagic(m)) => assert_eq!(m, 0xABAB_ABAB_ABAB_ABAB),
        other => panic!("expected BadMagic, got {other:?}"),
    }
}

#[test]
fn a_ring_from_a_future_build_is_refused() {
    let t = TempRing::new("version");
    Ring::create(t.path(), 8, 128).expect("create");
    let mut bytes = std::fs::read(t.path()).expect("read");
    let off = wire::layout::ring_header::VERSION;
    bytes[off..off + 4].copy_from_slice(&(RING_VERSION + 1).to_le_bytes());
    std::fs::write(t.path(), &bytes).expect("write");
    assert_eq!(
        Ring::open(t.path()).unwrap_err(),
        RingError::BadVersion(RING_VERSION + 1)
    );
}

#[test]
fn a_truncated_ring_is_refused() {
    let t = TempRing::new("truncated");
    Ring::create(t.path(), 8, 128).expect("create");
    let bytes = std::fs::read(t.path()).expect("read");
    // Keep the header, lose half the slots. The header still describes the full
    // file, so mapping it would map past the end.
    std::fs::write(t.path(), &bytes[..bytes.len() / 2]).expect("truncate");
    match Ring::open(t.path()) {
        Err(RingError::Truncated { need, got }) => {
            assert_eq!(need, Ring::file_size(8, 128));
            assert!(got < need);
        }
        other => panic!("expected Truncated, got {other:?}"),
    }
}

// --- the queue -------------------------------------------------------------

#[test]
fn a_message_comes_out_as_it_went_in() {
    let t = TempRing::new("roundtrip");
    let (mut p, mut c) = pair(&t, 8, 128);
    assert!(popped(&mut c).is_none(), "a fresh ring is empty");

    p.push(b"hello").expect("push");
    assert_eq!(popped(&mut c).as_deref(), Some(&b"hello"[..]));
    assert!(popped(&mut c).is_none(), "and empty again afterwards");
}

#[test]
fn messages_come_out_in_the_order_they_went_in() {
    let t = TempRing::new("fifo");
    let (mut p, mut c) = pair(&t, 8, 128);
    for i in 0u8..8 {
        p.push(&[i, i, i]).expect("push");
    }
    for i in 0u8..8 {
        assert_eq!(popped(&mut c).as_deref(), Some(&[i, i, i][..]));
    }
}

#[test]
fn a_full_ring_refuses_rather_than_overwriting() {
    let t = TempRing::new("full");
    let (mut p, mut c) = pair(&t, 4, 128);
    for i in 0u8..4 {
        p.push(&[i]).expect("push");
    }
    // The whole point. Overwriting would lose an order and report success,
    // which is the worst outcome available on this path.
    assert_eq!(p.push(b"x"), Err(PushError::Full));
    assert_eq!(p.depth(), 4);

    // One pop makes exactly one slot available.
    assert_eq!(popped(&mut c).as_deref(), Some(&[0u8][..]));
    p.push(b"x").expect("push after a pop");
    assert_eq!(p.push(b"y"), Err(PushError::Full));
}

#[test]
fn full_and_empty_are_told_apart_without_a_spare_slot() {
    let t = TempRing::new("distinct");
    let (mut p, mut c) = pair(&t, 4, 128);
    // Every slot is usable, which is what free-running indices buy: a ring that
    // wrapped its indices would have to leave one slot empty to tell the two
    // states apart.
    for i in 0u8..4 {
        p.push(&[i]).expect("push");
    }
    assert_eq!(p.depth(), 4);
    assert_eq!(c.depth(), 4);
    for _ in 0..4 {
        assert!(popped(&mut c).is_some());
    }
    assert_eq!(c.depth(), 0);
    assert!(popped(&mut c).is_none());
}

#[test]
fn the_indices_keep_counting_past_the_capacity() {
    let t = TempRing::new("wrap");
    let (mut p, mut c) = pair(&t, 4, 128);
    // Ten times round a four-slot ring. If the slot index were the counter
    // rather than a mask of it, this would have gone wrong at slot four.
    for i in 0u32..40 {
        p.push(&i.to_le_bytes()).expect("push");
        let got = popped(&mut c).expect("pop");
        assert_eq!(u32::from_le_bytes(got.try_into().unwrap()), i);
    }
}

#[test]
fn a_message_too_large_for_a_slot_is_refused() {
    let t = TempRing::new("toolarge");
    let (mut p, mut c) = pair(&t, 4, 128);
    let usable = p.ring().max_message_len();
    assert_eq!(
        p.push(&vec![0u8; usable + 1]),
        Err(PushError::TooLarge {
            len: usable + 1,
            capacity: usable,
        })
    );
    // And a message of exactly the usable size fits, so the boundary is where
    // it claims to be rather than one byte off.
    p.push(&vec![7u8; usable]).expect("exactly full slot");
    assert_eq!(popped(&mut c).map(|v| v.len()), Some(usable));
}

#[test]
fn an_abandoned_fill_consumes_no_slot() {
    let t = TempRing::new("abandon");
    let (mut p, mut c) = pair(&t, 4, 128);
    p.push(b"first").expect("push");
    assert_eq!(p.push_with(|_| None), Err(PushError::Abandoned));
    // The abandoned push must not have moved the index, or the consumer would
    // read a slot the producer never filled.
    assert_eq!(p.depth(), 1);
    assert_eq!(popped(&mut c).as_deref(), Some(&b"first"[..]));
    assert!(popped(&mut c).is_none());
}

#[test]
fn a_fill_that_overruns_its_slot_publishes_nothing() {
    let t = TempRing::new("overrun");
    let (mut p, mut c) = pair(&t, 4, 128);
    let usable = p.ring().max_message_len();
    // A closure that lies about how much it wrote. It cannot corrupt the
    // consumer's view, because the slot is not published until the length has
    // been checked.
    assert_eq!(
        p.push_with(|_| Some(usable + 1)),
        Err(PushError::TooLarge {
            len: usable + 1,
            capacity: usable,
        })
    );
    assert_eq!(p.depth(), 0);
    assert!(popped(&mut c).is_none());
}

#[test]
fn push_with_writes_the_slot_directly() {
    let t = TempRing::new("pushwith");
    let (mut p, mut c) = pair(&t, 4, 128);
    let n = p
        .push_with(|slot| {
            slot[..4].copy_from_slice(b"abcd");
            Some(4)
        })
        .expect("push_with");
    assert_eq!(n, 4);
    assert_eq!(popped(&mut c).as_deref(), Some(&b"abcd"[..]));
}

#[test]
fn an_empty_message_round_trips() {
    let t = TempRing::new("empty");
    let (mut p, mut c) = pair(&t, 4, 128);
    // Zero length is a real length, not a sentinel for "no message". A
    // consumer that treated it as empty would stall behind it forever.
    p.push(b"").expect("push");
    assert_eq!(popped(&mut c).as_deref(), Some(&b""[..]));
    assert!(popped(&mut c).is_none());
}

#[test]
fn drain_stops_at_its_limit() {
    let t = TempRing::new("drain");
    let (mut p, mut c) = pair(&t, 16, 128);
    for i in 0u8..10 {
        p.push(&[i]).expect("push");
    }
    let mut seen = Vec::new();
    // Bounded on purpose: a consumer that drains without a limit can be held
    // in the loop by a fast producer and never reach its timers.
    assert_eq!(c.drain(4, |b| seen.push(b[0])), 4);
    assert_eq!(seen, vec![0, 1, 2, 3]);
    assert_eq!(c.drain(100, |b| seen.push(b[0])), 6);
    assert_eq!(seen.len(), 10);
    assert_eq!(c.drain(100, |b| seen.push(b[0])), 0);
}
