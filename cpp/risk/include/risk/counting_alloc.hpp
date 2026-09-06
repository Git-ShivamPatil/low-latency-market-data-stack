// Counting the heap, so "no allocation after startup" can be checked rather
// than asserted.
//
// This is milestone 5's counting allocator, in the other language. The claim it
// polices is the same one and it fails the same way: a `std::string` on an error
// path, a `std::vector` that grows once under load, a lambda that outgrows the
// small-buffer optimisation and quietly becomes a heap `std::function`. None of
// those turn a test red on their own. This does.
//
// # It is compiled in unconditionally
//
// Not behind a flag, and not behind a build option. A build that swapped the
// allocator in for a measurement would be measuring a different program from the
// one that ships — the same reason `crates/alloc-guard` is compiled into the
// handler unconditionally rather than switched on by `--verify-allocations`.
//
// The cost in steady state is one increment per allocation, and in steady state
// there are no allocations, so it is zero where it matters.
//
// # Thread-local, on purpose
//
// The counters are per thread. The risk service is one thread doing one job, and
// a global counter would need an atomic on a path whose whole point is not to
// have one. It also means a test can measure its own thread without a background
// thread's noise landing in the number.

#pragma once

#include <cstddef>
#include <cstdint>

namespace risk {

/// A reading of this thread's counters.
struct AllocCounts {
    std::uint64_t allocations{0};
    std::uint64_t deallocations{0};
    std::uint64_t bytes{0};

    friend bool operator==(const AllocCounts&, const AllocCounts&) = default;

    /// What happened between `*this` and `later`.
    [[nodiscard]] AllocCounts delta(const AllocCounts& later) const noexcept {
        return AllocCounts{
            later.allocations - allocations,
            later.deallocations - deallocations,
            later.bytes - bytes,
        };
    }

    /// True when nothing touched the heap.
    ///
    /// A deallocation counts. Freeing something is evidence that something was
    /// allocated, even if the allocation happened before the window opened —
    /// and on this path neither should happen at all.
    [[nodiscard]] bool is_clean() const noexcept {
        return allocations == 0 && deallocations == 0;
    }
};

/// This thread's counters, right now.
[[nodiscard]] AllocCounts alloc_counts() noexcept;

/// A scope that reports what it allocated.
class AllocGuard {
  public:
    AllocGuard() noexcept : at_start_(alloc_counts()) {}

    /// What this thread allocated since construction.
    [[nodiscard]] AllocCounts sample() const noexcept {
        return at_start_.delta(alloc_counts());
    }

  private:
    AllocCounts at_start_;
};

}  // namespace risk
