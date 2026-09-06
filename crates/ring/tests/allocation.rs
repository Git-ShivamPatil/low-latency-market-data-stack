//! The ring does not touch the heap once it is open.
//!
//! Milestone 5 made this claim about the Rust receive path and proved it with a
//! counting allocator rather than by reading the code. The order path inherits
//! the claim: a queue that allocates per message has a tail latency set by the
//! allocator rather than by the queue, and the whole reason for a shared-memory
//! ring is to not have one.
//!
//! The control matters as much as the assertion. A counter that reports zero
//! because it was never wired up reports zero very convincingly, so this file
//! also allocates on purpose and requires the counter to notice.

use std::path::{Path, PathBuf};

use alloc_guard::{AllocCounts, AllocGuard, CountingAllocator};
use ring::{Consumer, Producer, Ring};

/// Installed for this test binary only — the same opt-in a real binary makes.
#[global_allocator]
static ALLOC: CountingAllocator<std::alloc::System> = CountingAllocator::new(std::alloc::System);

struct TempRing(PathBuf);

impl TempRing {
    fn new(name: &str) -> Self {
        let mut p = std::env::temp_dir();
        p.push(format!("mdstack-ring-alloc-{name}-{}", std::process::id()));
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

fn pair(t: &TempRing) -> (Producer, Consumer) {
    Ring::create(t.path(), 64, 128).expect("create");
    (
        Ring::open(t.path()).expect("producer").into_producer(),
        Ring::open(t.path()).expect("consumer").into_consumer(),
    )
}

#[test]
fn a_million_messages_through_the_ring_allocate_nothing() {
    let t = TempRing::new("million");
    let (mut p, mut c) = pair(&t);
    let payload = [0xA5u8; 48];

    // Warm up outside the measured window. The first pass through anything can
    // allocate for reasons that have nothing to do with steady state, and a
    // claim about steady state should not be a claim about start-up.
    for _ in 0..1_000 {
        p.push(&payload).expect("warmup push");
        c.pop_with(|_| ()).expect("warmup pop");
    }

    let guard = AllocGuard::start();
    let mut bytes_seen = 0u64;
    for _ in 0..1_000_000 {
        p.push_with(|slot| {
            slot[..payload.len()].copy_from_slice(&payload);
            Some(payload.len())
        })
        .expect("push");
        bytes_seen += c.pop_with(|b| b.len() as u64).expect("pop");
    }
    let delta = guard.finish();

    // Proves the loop did the work rather than short-circuiting into nothing.
    assert_eq!(bytes_seen, 1_000_000 * payload.len() as u64);
    assert!(
        delta.is_clean(),
        "the ring allocated over 1,000,000 messages: {delta}"
    );
}

#[test]
fn a_full_ring_and_an_empty_one_allocate_nothing_either() {
    // The error paths are where a `format!` hides. `PushError` is a plain enum
    // for exactly this reason, and this is what keeps it one.
    let t = TempRing::new("edges");
    let (mut p, mut c) = pair(&t);
    for _ in 0..64 {
        p.push(b"x").expect("fill");
    }

    let guard = AllocGuard::start();
    let mut refusals = 0;
    for _ in 0..10_000 {
        if p.push(b"x").is_err() {
            refusals += 1;
        }
    }
    for _ in 0..64 {
        c.pop_with(|_| ()).expect("drain");
    }
    let mut empties = 0;
    for _ in 0..10_000 {
        if c.pop_with(|_| ()).is_none() {
            empties += 1;
        }
    }
    let delta = guard.finish();

    assert_eq!(refusals, 10_000, "a full ring refused every push");
    assert_eq!(empties, 10_000, "an empty ring returned nothing every time");
    assert!(delta.is_clean(), "the refusal paths allocated: {delta}");
}

#[test]
fn the_counter_notices_when_something_does_allocate() {
    // The control. Without it, all this file proves is that the counter is
    // capable of returning zero.
    let before = AllocCounts::now();
    let v: Vec<u8> = Vec::with_capacity(4096);
    let after = AllocCounts::now();
    // Reading the capacity keeps the allocation from being optimised away.
    assert!(v.capacity() >= 4096);
    let delta = before.delta(after);
    assert!(
        delta.allocations >= 1,
        "the counting allocator is not installed, so the zeros above mean nothing"
    );
}
