//! Counting the heap so a "zero allocations" claim can be checked rather than
//! asserted.
//!
//! # The claim this exists to defend
//!
//! *Zero heap allocations per message, in steady state.* That is easy to achieve
//! and easy to silently lose: one `format!` on an error path, one `Vec` that
//! grows during a resend, one `String` in a log line, and it is gone — without
//! breaking a single test. So it has to be a CI assertion over a large number of
//! messages *including a recovery cycle*, not a flag somebody ran once by hand.
//!
//! # What is deliberately not in the claim
//!
//! Allocation at **startup** and during a **snapshot rebuild** is expected and
//! fine. Books are sized once; a recovery discards and rebuilds them. Counting
//! those would either make the claim unachievable or push the code towards
//! pre-allocating for the worst case at every scale, which is worse engineering
//! than allocating twice a second during an unusual event.
//!
//! The defensible claim is per-message steady state, which is exactly what the
//! case study says, and [`AllocGuard`] measures exactly that window.
//!
//! # Per-thread, not per-process
//!
//! Counters are thread-local. A process-wide counter would be useless under
//! `cargo test`, which runs tests concurrently — one test's `String` would fail
//! another's assertion. It also isolates the handler's receive loop from its
//! background threads, so a replay request allocating on its own thread cannot
//! be mistaken for the book allocating on the hot path.
//!
//! # Installing it
//!
//! The allocator is not installed here. A library that declared
//! `#[global_allocator]` would impose it on every binary in the workspace,
//! including ones that have no interest in being measured. The binary that wants
//! counting opts in:
//!
//! ```ignore
//! #[global_allocator]
//! static ALLOC: alloc_guard::CountingAllocator<std::alloc::System> =
//!     alloc_guard::CountingAllocator::new(std::alloc::System);
//! ```

// A `GlobalAlloc` implementation cannot be written in safe Rust: the trait's
// methods are unsafe by definition because the caller guarantees the layout and
// the pointer provenance. The unsafety is confined to this file, and every
// method does nothing but bump a thread-local counter and forward to the inner
// allocator.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::fmt;

thread_local! {
    // `const` initialisers, so touching these never itself allocates. A lazily
    // initialised thread-local would allocate on first access — inside the
    // allocator, which is a recursion nobody wants to debug.
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
    static DEALLOCS: Cell<u64> = const { Cell::new(0) };
    static REALLOCS: Cell<u64> = const { Cell::new(0) };
    static BYTES: Cell<u64> = const { Cell::new(0) };
}

#[inline]
fn bump(key: &'static std::thread::LocalKey<Cell<u64>>, by: u64) {
    // `try_with` rather than `with`: during thread-local destruction at thread
    // exit, `with` panics. An allocator that panics while a thread is shutting
    // down would turn a clean exit into an abort.
    let _ = key.try_with(|c| c.set(c.get().wrapping_add(by)));
}

#[inline]
fn read(key: &'static std::thread::LocalKey<Cell<u64>>) -> u64 {
    key.try_with(Cell::get).unwrap_or(0)
}

/// Wraps another allocator and counts what goes through it.
#[derive(Debug)]
pub struct CountingAllocator<A> {
    inner: A,
}

impl<A> CountingAllocator<A> {
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

// SAFETY: every method forwards to `inner`, which is a correct allocator, and
// adds only thread-local counter arithmetic. No pointer is created, moved or
// interpreted here.
unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAllocator<A> {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        bump(&ALLOCS, 1);
        bump(&BYTES, layout.size() as u64);
        unsafe { self.inner.alloc(layout) }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        bump(&DEALLOCS, 1);
        unsafe { self.inner.dealloc(ptr, layout) }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        bump(&ALLOCS, 1);
        bump(&BYTES, layout.size() as u64);
        unsafe { self.inner.alloc_zeroed(layout) }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // Counted separately. A realloc is the signature of a `Vec` growing,
        // which is the most common way a hot path quietly starts allocating —
        // and folding it into `alloc` would hide which of the two happened.
        bump(&REALLOCS, 1);
        if new_size > layout.size() {
            bump(&BYTES, (new_size - layout.size()) as u64);
        }
        unsafe { self.inner.realloc(ptr, layout, new_size) }
    }
}

/// A reading of this thread's counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AllocCounts {
    pub allocations: u64,
    pub deallocations: u64,
    pub reallocations: u64,
    pub bytes: u64,
}

impl AllocCounts {
    /// This thread's counters, right now.
    pub fn now() -> Self {
        Self {
            allocations: read(&ALLOCS),
            deallocations: read(&DEALLOCS),
            reallocations: read(&REALLOCS),
            bytes: read(&BYTES),
        }
    }

    /// What happened between `self` and `later`.
    pub fn delta(self, later: Self) -> Self {
        Self {
            allocations: later.allocations.saturating_sub(self.allocations),
            deallocations: later.deallocations.saturating_sub(self.deallocations),
            reallocations: later.reallocations.saturating_sub(self.reallocations),
            bytes: later.bytes.saturating_sub(self.bytes),
        }
    }

    /// True when nothing touched the heap.
    ///
    /// A reallocation counts. A `Vec` that grew once is not "zero allocations
    /// per message" — it is a bound nobody has checked.
    pub fn is_clean(&self) -> bool {
        self.allocations == 0 && self.deallocations == 0 && self.reallocations == 0
    }
}

impl fmt::Display for AllocCounts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} allocations, {} deallocations, {} reallocations, {} bytes",
            self.allocations, self.deallocations, self.reallocations, self.bytes
        )
    }
}

/// Measures a scope.
///
/// ```ignore
/// let guard = AllocGuard::start();
/// run_the_steady_state_loop();
/// let delta = guard.finish();
/// assert!(delta.is_clean(), "the hot path allocated: {delta}");
/// ```
#[derive(Debug)]
pub struct AllocGuard {
    at_start: AllocCounts,
}

impl AllocGuard {
    pub fn start() -> Self {
        Self {
            at_start: AllocCounts::now(),
        }
    }

    /// What this thread allocated since [`start`](Self::start).
    pub fn finish(self) -> AllocCounts {
        self.at_start.delta(AllocCounts::now())
    }

    /// Reads the delta without ending the scope.
    pub fn sample(&self) -> AllocCounts {
        self.at_start.delta(AllocCounts::now())
    }
}

impl Default for AllocGuard {
    fn default() -> Self {
        Self::start()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The tests install the allocator for this test binary only, which is the
    // same opt-in a real binary makes.
    #[global_allocator]
    static ALLOC: CountingAllocator<std::alloc::System> =
        CountingAllocator::new(std::alloc::System);

    #[test]
    fn a_scope_that_allocates_is_caught() {
        let guard = AllocGuard::start();
        let v: Vec<u8> = Vec::with_capacity(1024);
        let delta = guard.finish();
        assert!(!delta.is_clean(), "a Vec allocation must be visible");
        assert!(delta.allocations >= 1);
        assert!(delta.bytes >= 1024);
        drop(v);
    }

    #[test]
    fn a_scope_that_does_not_allocate_is_clean() {
        // Warm anything lazy before measuring, exactly as a real steady-state
        // measurement has to.
        let mut buf = vec![0u8; 4096];
        let guard = AllocGuard::start();
        for (i, b) in buf.iter_mut().enumerate() {
            *b = i as u8;
        }
        let sum: u64 = buf.iter().map(|b| u64::from(*b)).sum();
        let delta = guard.finish();
        assert!(
            delta.is_clean(),
            "arithmetic over a warm buffer allocated: {delta}"
        );
        assert!(sum > 0);
    }

    #[test]
    fn a_vec_growing_is_reported_as_a_reallocation() {
        // The most common way a hot path quietly starts allocating, and the
        // reason realloc is counted separately rather than folded into alloc.
        let mut v: Vec<u64> = Vec::with_capacity(1);
        v.push(1);
        let guard = AllocGuard::start();
        v.push(2); // forces a grow
        let delta = guard.finish();
        assert!(!delta.is_clean());
        assert!(
            delta.reallocations >= 1 || delta.allocations >= 1,
            "a grow must show up somewhere: {delta}"
        );
    }

    #[test]
    fn a_format_on_an_error_path_is_caught() {
        // Named for the thing it is guarding against: an error path that builds
        // a String breaks the claim without breaking any other test.
        let guard = AllocGuard::start();
        let s = format!("order {} is not on the book", 42);
        let delta = guard.finish();
        assert!(!delta.is_clean(), "format! must be visible: {delta}");
        assert!(!s.is_empty());
    }

    #[test]
    fn deltas_compose() {
        let a = AllocCounts {
            allocations: 10,
            deallocations: 4,
            reallocations: 1,
            bytes: 100,
        };
        let b = AllocCounts {
            allocations: 15,
            deallocations: 9,
            reallocations: 3,
            bytes: 250,
        };
        let d = a.delta(b);
        assert_eq!(d.allocations, 5);
        assert_eq!(d.deallocations, 5);
        assert_eq!(d.reallocations, 2);
        assert_eq!(d.bytes, 150);
    }

    #[test]
    fn a_delta_never_goes_negative() {
        // Counters are per thread and read without synchronisation, so an
        // out-of-order pair must saturate rather than wrap to a huge number that
        // would read as a catastrophic leak.
        let later = AllocCounts {
            allocations: 1,
            ..Default::default()
        };
        let earlier = AllocCounts {
            allocations: 5,
            ..Default::default()
        };
        assert_eq!(earlier.delta(later).allocations, 0);
    }

    #[test]
    fn counting_is_per_thread() {
        // The property that makes this usable under a concurrent test harness,
        // and under a handler with background threads.
        //
        // Note what is *not* asserted: that a guard spanning a spawn-and-join is
        // clean. It is not, and cannot be — creating a thread allocates its
        // handle and its boxed closure on the *spawning* thread, which is this
        // one. That is a real allocation by this thread and the counter is right
        // to report it.
        let mut sink = Vec::new();
        for _ in 0..1000 {
            sink.push(vec![0u8; 64]);
        }
        let parent = AllocCounts::now();
        assert!(
            parent.allocations >= 1000,
            "the parent should be busy by now"
        );

        let child = std::thread::spawn(AllocCounts::now).join().unwrap();
        assert!(
            child.allocations < parent.allocations / 10,
            "a fresh thread must not inherit this thread's {} allocations, saw {}; \
             a process-wide counter would show them all",
            parent.allocations,
            child.allocations
        );
        drop(sink);
    }
}
