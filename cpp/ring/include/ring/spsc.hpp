// A single-producer single-consumer ring in shared memory, shared with Rust.
//
// This is the C++ half of `crates/ring`. The two are separate implementations
// of one agreement, and the agreement is `schema/market-data.xml`: every offset
// below comes from `wire::layout::ring_header` and `wire::layout::ring_slot`,
// which the generator emits into both languages from the same source. Neither
// side hand-writes an offset, so neither side can drift.
//
// # Why SPSC, and why four rings
//
// The correctness argument fits in a paragraph: the producer owns `writeIndex`,
// the consumer owns `readIndex`, and the release store that publishes an index
// is what makes the slot's bytes visible to the acquire load that reads it. Add
// a second producer and the argument is gone. The order path therefore uses four
// rings rather than one with a fan-in — see docs/ORDER-PATH.md.
//
// # Free-running indices
//
// The indices count slots and are never wrapped; the slot is `index & (capacity
// - 1)`. So `write - read` is the depth, full and empty are different states,
// and the ring needs no spare slot to tell them apart. At a billion messages a
// second, a 64-bit counter lasts 584 years.
//
// # The honest caveat
//
// Two processes read and write the same pages. Within the letter of the C++
// memory model the slot bytes are a data race — copied non-atomically by one
// process, read non-atomically by another, ordered only by the release/acquire
// pair on the indices. Every shared-memory ring in existence is built this way,
// and on every architecture this project targets the pair compiles to exactly
// the fence the argument needs. `crates/ring/src/lib.rs` says the same thing
// about the same bytes; it is written down in both places rather than left for
// a reader to notice in either.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>

#include "wire/generated.hpp"

namespace mdstack::ring {

/// Ring layout version, separate from the schema version. Bump it when the
/// meaning of the bytes changes, not when a message is added.
inline constexpr std::uint32_t kRingVersion = 1;

/// "MDSTKRNG" in ASCII, so `head -c8` on the file says what it is.
inline constexpr std::uint64_t kRingMagic = 0x474e524b5453444dULL;

/// Slots are cache-line aligned so two adjacent slots never share a line — the
/// producer writing slot n must not invalidate the line the consumer is reading
/// slot n-1 from.
inline constexpr std::size_t kSlotAlign = 64;

enum class RingError {
    CapacityZero,
    /// The mask depends on it. A ring that quietly held 8 when it was asked for
    /// 6 would make a capacity-planning number wrong.
    CapacityNotPowerOfTwo,
    /// Not a multiple of kSlotAlign, or too small to hold the prefix and a byte.
    BadSlotSize,
    /// Not a ring, or a ring this build does not understand. Refused rather
    /// than guessed at: a mis-read header produces a queue that appears to work
    /// and delivers nothing.
    BadMagic,
    BadVersion,
    /// The file is shorter than its own header says it should be.
    Truncated,
    CannotOpen,
    CannotSize,
    CannotMap,
};

std::string_view describe(RingError e);

/// Why a push did not happen.
enum class PushError {
    /// The consumer has not kept up. **Not something to swallow:** the caller
    /// must tell whoever sent the order. Blocking stalls the session and
    /// dropping loses an order silently, which is the worst outcome available.
    Full,
    /// The message is larger than a slot. A sizing mistake, not a runtime
    /// condition; it cannot be retried.
    TooLarge,
};

std::string_view describe(PushError e);

/// The mapping plus its validated geometry.
///
/// A process takes exactly one role. `Ring` is the shared machinery; `Producer`
/// and `Consumer` are the two ends, and holding both in one process is only
/// done by the tests.
class Ring {
  public:
    Ring() = default;
    ~Ring();

    Ring(const Ring&) = delete;
    Ring& operator=(const Ring&) = delete;
    Ring(Ring&& other) noexcept;
    Ring& operator=(Ring&& other) noexcept;

    /// Bytes a ring of this geometry occupies on disk.
    static std::size_t file_size(std::uint32_t capacity, std::uint32_t slot_size) noexcept {
        return wire::layout::ring_header::kLen +
               static_cast<std::size_t>(capacity) * static_cast<std::size_t>(slot_size);
    }

    /// Creates or re-creates the ring at `path`, zeroing the indices.
    ///
    /// The creator is whichever side starts first, and it is the only side that
    /// may write the header. docs/ORDER-PATH.md names the creator of each ring.
    std::optional<RingError> create(const std::string& path, std::uint32_t capacity,
                                    std::uint32_t slot_size);

    /// Opens a ring somebody else created, taking its geometry from the header.
    std::optional<RingError> open(const std::string& path);

    [[nodiscard]] bool is_open() const noexcept { return base_ != nullptr; }
    [[nodiscard]] std::uint64_t capacity() const noexcept { return capacity_; }
    [[nodiscard]] std::size_t slot_size() const noexcept { return slot_size_; }

    /// The largest message a slot can hold.
    [[nodiscard]] std::size_t max_message_len() const noexcept {
        return slot_size_ - wire::layout::ring_slot::kLen;
    }

    [[nodiscard]] std::uint64_t write_index_relaxed() const noexcept {
        return write_index().load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t read_index_relaxed() const noexcept {
        return read_index().load(std::memory_order_relaxed);
    }

  protected:
    // std::atomic_ref over the mapped bytes. The referenced storage is
    // page-aligned and the offsets are multiples of 8, so the alignment
    // atomic_ref requires is satisfied.
    [[nodiscard]] std::atomic_ref<std::uint64_t> write_index() const noexcept {
        return index_at(wire::layout::ring_header::kWriteIndex);
    }
    [[nodiscard]] std::atomic_ref<std::uint64_t> read_index() const noexcept {
        return index_at(wire::layout::ring_header::kReadIndex);
    }

    [[nodiscard]] std::byte* slot_ptr(std::uint64_t index) const noexcept {
        const std::uint64_t n = index & (capacity_ - 1);
        return base_ + wire::layout::ring_header::kLen + static_cast<std::size_t>(n) * slot_size_;
    }

    void close() noexcept;

  private:
    [[nodiscard]] std::atomic_ref<std::uint64_t> index_at(std::size_t offset) const noexcept {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        return std::atomic_ref<std::uint64_t>(
            *reinterpret_cast<std::uint64_t*>(base_ + offset));
    }

    void store_u64(std::size_t offset, std::uint64_t v) noexcept {
        std::memcpy(base_ + offset, &v, sizeof v);
    }
    void store_u32(std::size_t offset, std::uint32_t v) noexcept {
        std::memcpy(base_ + offset, &v, sizeof v);
    }
    [[nodiscard]] std::uint64_t load_u64(std::size_t offset) const noexcept {
        std::uint64_t v{};
        std::memcpy(&v, base_ + offset, sizeof v);
        return v;
    }
    [[nodiscard]] std::uint32_t load_u32(std::size_t offset) const noexcept {
        std::uint32_t v{};
        std::memcpy(&v, base_ + offset, sizeof v);
        return v;
    }

    std::byte* base_{nullptr};
    std::size_t mapped_{0};
    std::uint64_t capacity_{0};
    std::size_t slot_size_{0};
};

/// The writing end. Owns `writeIndex` and reads `readIndex`.
class Producer : public Ring {
  public:
    using Ring::Ring;

    /// Slots currently occupied.
    [[nodiscard]] std::uint64_t depth() const noexcept {
        return write_index().load(std::memory_order_relaxed) -
               read_index().load(std::memory_order_acquire);
    }

    /// Writes a message straight into the next slot.
    ///
    /// `fill` receives the slot's usable bytes and returns how many it wrote.
    /// Nothing is published until it returns, so a message that turns out not
    /// to fit leaves the ring exactly as it was. This is the path the order
    /// flow uses: the encoder writes the shared page once rather than writing a
    /// buffer that is then copied in.
    template <typename Fill>
    std::optional<PushError> push_with(Fill&& fill) noexcept {
        const std::uint64_t w = write_index().load(std::memory_order_relaxed);
        if (w - cached_read_ >= capacity()) {
            // Only now is the consumer's cache line worth touching.
            cached_read_ = read_index().load(std::memory_order_acquire);
            if (w - cached_read_ >= capacity()) {
                return PushError::Full;
            }
        }

        std::byte* base = slot_ptr(w);
        const std::size_t usable = max_message_len();
        const std::size_t n = fill(base + wire::layout::ring_slot::kLen, usable);
        if (n > usable) {
            // `fill` overran. It has scribbled on the slot, but the slot is not
            // published, so the damage is confined to bytes nobody will read.
            return PushError::TooLarge;
        }

        const auto len = static_cast<std::uint32_t>(n);
        std::memcpy(base + wire::layout::ring_slot::kLength, &len, sizeof len);
        // Publication. Everything above happens-before the acquire load in
        // Consumer::pop_with that observes this value.
        write_index().store(w + 1, std::memory_order_release);
        return std::nullopt;
    }

    /// Copies `bytes` into the next slot. Convenience over push_with.
    std::optional<PushError> push(const std::byte* bytes, std::size_t len) noexcept {
        if (len > max_message_len()) {
            return PushError::TooLarge;
        }
        return push_with([bytes, len](std::byte* dst, std::size_t) {
            std::memcpy(dst, bytes, len);
            return len;
        });
    }

  private:
    /// The last readIndex we saw. Re-read only when this says the ring is full,
    /// so the common case never touches the consumer's cache line.
    std::uint64_t cached_read_{0};
};

/// The reading end. Owns `readIndex` and reads `writeIndex`.
class Consumer : public Ring {
  public:
    using Ring::Ring;

    [[nodiscard]] std::uint64_t depth() const noexcept {
        return write_index().load(std::memory_order_acquire) -
               read_index().load(std::memory_order_relaxed);
    }

    /// Hands the next message to `visit`, or returns false if the ring is empty.
    ///
    /// The pointer refers to the shared page rather than a copy. That is sound
    /// because the slot is not reusable until `readIndex` advances, which
    /// happens after `visit` returns.
    template <typename Visit>
    bool pop_with(Visit&& visit) noexcept {
        const std::uint64_t r = read_index().load(std::memory_order_relaxed);
        // `>=` and not `==`. The only thing the cache is allowed to be is
        // stale-low, and a stale-low `==` would sail past this check and read a
        // slot the producer has not written. With `>=`, any cache value that is
        // not strictly ahead of `r` forces a re-read, so the hint can never
        // cause a wrong answer -- only an occasional extra load.
        if (r >= cached_write_) {
            cached_write_ = write_index().load(std::memory_order_acquire);
            if (r >= cached_write_) {
                return false;
            }
        }

        const std::byte* base = slot_ptr(r);
        std::uint32_t raw{};
        std::memcpy(&raw, base + wire::layout::ring_slot::kLength, sizeof raw);
        // The clamp is a bounds guarantee for the read below, not error
        // handling: a correct producer cannot write a length past the slot,
        // because push_with refuses one. It is here so a corrupted page cannot
        // turn into an out-of-bounds read.
        const std::size_t n = std::min(static_cast<std::size_t>(raw), max_message_len());

        visit(base + wire::layout::ring_slot::kLen, n);
        // Release, so a producer that observes this index also observes that we
        // are done reading the slot it is about to overwrite.
        read_index().store(r + 1, std::memory_order_release);
        return true;
    }

    /// Drains up to `limit` messages.
    ///
    /// Bounded rather than "until empty" on purpose: a consumer that drains an
    /// unbounded queue can be held in this loop by a fast producer and never
    /// get back to its timers.
    template <typename Visit>
    std::size_t drain(std::size_t limit, Visit&& visit) noexcept {
        std::size_t n = 0;
        while (n < limit && pop_with(visit)) {
            ++n;
        }
        return n;
    }

  private:
    /// The last writeIndex we saw. Re-read only when this says the ring is
    /// empty, so a burst is drained without re-reading the producer's line per
    /// message.
    std::uint64_t cached_write_{0};
};

}  // namespace mdstack::ring
