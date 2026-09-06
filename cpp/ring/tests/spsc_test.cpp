// What has to be true of the C++ half of the ring.
//
// Deliberately the same list as `crates/ring/src/tests.rs`, in the same order.
// Two implementations of one agreement should fail the same way when the
// agreement is broken, and a property tested on only one side is a property
// only one side has.
//
// The cross-language check is scripts/ring-interop-test.sh: this file proves
// each half is self-consistent, and that one proves the halves agree.

#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "ring/spsc.hpp"

namespace {

int failures = 0;

void check(bool ok, const std::string& what) {
    if (ok) {
        std::cout << "ok   " << what << "\n";
    } else {
        std::cerr << "FAIL " << what << "\n";
        ++failures;
    }
}

using mdstack::ring::Consumer;
using mdstack::ring::Producer;
using mdstack::ring::PushError;
using mdstack::ring::Ring;
using mdstack::ring::RingError;

std::string temp_path(const char* name) {
    return std::string("/tmp/mdstack-ring-cpp-") + name + "-" + std::to_string(::getpid());
}

/// A path that removes itself.
struct TempRing {
    std::string path;
    explicit TempRing(const char* name) : path(temp_path(name)) { ::unlink(path.c_str()); }
    ~TempRing() { ::unlink(path.c_str()); }
    TempRing(const TempRing&) = delete;
    TempRing& operator=(const TempRing&) = delete;
};

bool push_str(Producer& p, const std::string& s) {
    return !p.push(reinterpret_cast<const std::byte*>(s.data()), s.size()).has_value();
}

std::optional<std::string> pop_str(Consumer& c) {
    std::string out;
    const bool got = c.pop_with([&out](const std::byte* p, std::size_t n) {
        out.assign(reinterpret_cast<const char*>(p), n);
    });
    if (!got) return std::nullopt;
    return out;
}

// --- geometry -------------------------------------------------------------

void a_capacity_that_is_not_a_power_of_two_is_refused() {
    TempRing t("pow2");
    Ring r;
    check(r.create(t.path, 6, 128) == RingError::CapacityNotPowerOfTwo,
          "a capacity that is not a power of two is refused rather than rounded");
    check(r.create(t.path, 0, 128) == RingError::CapacityZero, "and zero is refused");
}

void a_slot_that_would_straddle_a_cache_line_is_refused() {
    TempRing t("align");
    Ring r;
    check(r.create(t.path, 8, 100) == RingError::BadSlotSize,
          "a slot size that is not a multiple of the cache line is refused");
    check(r.create(t.path, 8, 0) == RingError::BadSlotSize,
          "a slot with no room for a message is refused");
    check(!r.create(t.path, 8, 64).has_value(), "and 64 is accepted");
}

void the_file_is_exactly_the_header_plus_the_slots() {
    TempRing t("size");
    Ring r;
    check(!r.create(t.path, 16, 128).has_value(), "created");
    struct ::stat st {};
    ::stat(t.path.c_str(), &st);
    check(static_cast<std::size_t>(st.st_size) == Ring::file_size(16, 128),
          "the file is the header plus the slots and nothing else");
    check(Ring::file_size(16, 128) == mdstack::wire::layout::ring_header::kLen + 16 * 128,
          "and file_size agrees with the generated header length");
}

void the_geometry_survives_a_reopen() {
    TempRing t("reopen");
    Ring w;
    check(!w.create(t.path, 32, 256).has_value(), "created");
    Ring r;
    check(!r.open(t.path).has_value(), "reopened");
    check(r.capacity() == 32, "capacity came back from the header");
    check(r.slot_size() == 256, "slot size came back from the header");
    check(r.max_message_len() == 256 - mdstack::wire::layout::ring_slot::kLen,
          "and the usable length is the slot minus its prefix");
}

void a_file_that_is_not_a_ring_is_refused_rather_than_read() {
    TempRing t("magic");
    // Long enough to map, so the refusal has to come from the magic and not the
    // length. A ring that trusted this header would compute indices from
    // somebody else's bytes.
    std::vector<char> junk(4096, '\xAB');
    std::FILE* f = std::fopen(t.path.c_str(), "wb");
    std::fwrite(junk.data(), 1, junk.size(), f);
    std::fclose(f);
    Ring r;
    check(r.open(t.path) == RingError::BadMagic, "a file that is not a ring is refused");
}

void a_truncated_ring_is_refused() {
    TempRing t("truncated");
    Ring w;
    check(!w.create(t.path, 8, 128).has_value(), "created");
    // Keep the header, lose half the slots. The header still describes the
    // whole file, so mapping it would map past the end.
    check(::truncate(t.path.c_str(), static_cast<off_t>(Ring::file_size(8, 128) / 2)) == 0,
          "truncated");
    Ring r;
    check(r.open(t.path) == RingError::Truncated, "a truncated ring is refused");
}

// --- the queue ------------------------------------------------------------

void a_message_comes_out_as_it_went_in() {
    TempRing t("roundtrip");
    Ring seed;
    seed.create(t.path, 8, 128);
    Producer p;
    Consumer c;
    p.open(t.path);
    c.open(t.path);

    check(!pop_str(c).has_value(), "a fresh ring is empty");
    check(push_str(p, "hello"), "pushed");
    auto got = pop_str(c);
    check(got.has_value() && *got == "hello", "a message comes out as it went in");
    check(!pop_str(c).has_value(), "and the ring is empty again");
}

void messages_come_out_in_the_order_they_went_in() {
    TempRing t("fifo");
    Ring seed;
    seed.create(t.path, 8, 128);
    Producer p;
    Consumer c;
    p.open(t.path);
    c.open(t.path);

    for (int i = 0; i < 8; ++i) {
        push_str(p, std::to_string(i));
    }
    bool ordered = true;
    for (int i = 0; i < 8; ++i) {
        auto got = pop_str(c);
        ordered = ordered && got.has_value() && *got == std::to_string(i);
    }
    check(ordered, "messages come out in the order they went in");
}

void a_full_ring_refuses_rather_than_overwriting() {
    TempRing t("full");
    Ring seed;
    seed.create(t.path, 4, 128);
    Producer p;
    Consumer c;
    p.open(t.path);
    c.open(t.path);

    for (int i = 0; i < 4; ++i) {
        push_str(p, std::to_string(i));
    }
    // The whole point. Overwriting would lose an order and report success,
    // which is the worst outcome available on this path.
    check(!push_str(p, "x"), "a full ring refuses rather than overwriting");
    check(p.depth() == 4, "and reports itself full");

    auto first = pop_str(c);
    check(first.has_value() && *first == "0", "the oldest message is still the first out");
    check(push_str(p, "x"), "one pop makes exactly one slot available");
    check(!push_str(p, "y"), "and only one");
}

void full_and_empty_are_told_apart_without_a_spare_slot() {
    TempRing t("distinct");
    Ring seed;
    seed.create(t.path, 4, 128);
    Producer p;
    Consumer c;
    p.open(t.path);
    c.open(t.path);

    // Every slot is usable, which is what free-running indices buy: a ring that
    // wrapped its indices would have to leave one empty to tell the two apart.
    for (int i = 0; i < 4; ++i) {
        push_str(p, "x");
    }
    check(p.depth() == 4 && c.depth() == 4, "all four slots are usable");
    for (int i = 0; i < 4; ++i) {
        pop_str(c);
    }
    check(c.depth() == 0 && !pop_str(c).has_value(), "and empty is distinguishable from full");
}

void the_indices_keep_counting_past_the_capacity() {
    TempRing t("wrap");
    Ring seed;
    seed.create(t.path, 4, 128);
    Producer p;
    Consumer c;
    p.open(t.path);
    c.open(t.path);

    // Ten times round a four-slot ring. If the slot index were the counter
    // rather than a mask of it, this would have gone wrong at slot four.
    bool ok = true;
    for (std::uint32_t i = 0; i < 40; ++i) {
        p.push(reinterpret_cast<const std::byte*>(&i), sizeof i);
        std::uint32_t got = 0;
        const bool have = c.pop_with([&got](const std::byte* q, std::size_t n) {
            if (n == sizeof got) std::memcpy(&got, q, sizeof got);
        });
        ok = ok && have && got == i;
    }
    check(ok, "the indices keep counting past the capacity");
}

void a_message_too_large_for_a_slot_is_refused() {
    TempRing t("toolarge");
    Ring seed;
    seed.create(t.path, 4, 128);
    Producer p;
    Consumer c;
    p.open(t.path);
    c.open(t.path);

    const std::size_t usable = p.max_message_len();
    std::vector<std::byte> big(usable + 1, std::byte{0});
    check(p.push(big.data(), big.size()) == PushError::TooLarge,
          "a message larger than a slot is refused");
    std::vector<std::byte> exact(usable, std::byte{7});
    check(!p.push(exact.data(), exact.size()).has_value(),
          "and one of exactly the usable size fits, so the boundary is where it claims");
    std::size_t seen = 0;
    c.pop_with([&seen](const std::byte*, std::size_t n) { seen = n; });
    check(seen == usable, "and comes back whole");
}

void a_fill_that_overruns_its_slot_publishes_nothing() {
    TempRing t("overrun");
    Ring seed;
    seed.create(t.path, 4, 128);
    Producer p;
    Consumer c;
    p.open(t.path);
    c.open(t.path);

    // A fill that lies about how much it wrote cannot corrupt the consumer's
    // view, because the slot is not published until the length is checked.
    const std::size_t usable = p.max_message_len();
    check(p.push_with([usable](std::byte*, std::size_t) { return usable + 1; }) ==
              PushError::TooLarge,
          "a fill that overruns its slot is refused");
    check(p.depth() == 0, "and publishes nothing");
    check(!pop_str(c).has_value(), "so the consumer sees an empty ring");
}

void an_empty_message_round_trips() {
    TempRing t("empty");
    Ring seed;
    seed.create(t.path, 4, 128);
    Producer p;
    Consumer c;
    p.open(t.path);
    c.open(t.path);

    // Zero length is a real length, not a sentinel for "no message". A consumer
    // that treated it as empty would stall behind it forever.
    check(push_str(p, ""), "pushed a zero-length message");
    auto got = pop_str(c);
    check(got.has_value() && got->empty(), "an empty message round trips");
    check(!pop_str(c).has_value(), "and does not stall the ring");
}

void drain_stops_at_its_limit() {
    TempRing t("drain");
    Ring seed;
    seed.create(t.path, 16, 128);
    Producer p;
    Consumer c;
    p.open(t.path);
    c.open(t.path);

    for (int i = 0; i < 10; ++i) {
        push_str(p, "x");
    }
    std::size_t seen = 0;
    // Bounded on purpose: a consumer that drains without a limit can be held in
    // the loop by a fast producer and never reach its timers.
    check(c.drain(4, [&seen](const std::byte*, std::size_t) { ++seen; }) == 4, "drain stops at 4");
    check(c.drain(100, [&seen](const std::byte*, std::size_t) { ++seen; }) == 6,
          "and takes the remaining 6");
    check(seen == 10, "which is all of them");
    check(c.drain(100, [](const std::byte*, std::size_t) {}) == 0, "and then nothing");
}

void a_consumer_joining_a_ring_in_progress_does_not_read_ahead() {
    TempRing t("join");
    Ring seed;
    seed.create(t.path, 8, 128);
    Producer p;
    p.open(t.path);
    for (int i = 0; i < 3; ++i) {
        push_str(p, std::to_string(i));
    }
    for (int i = 0; i < 3; ++i) {
        Consumer warm;
        warm.open(t.path);
        // Advance the read index without a consumer that has cached anything.
        warm.pop_with([](const std::byte*, std::size_t) {});
    }
    // Now readIndex is 3 and writeIndex is 3. A consumer whose cached
    // writeIndex starts at 0 must still work out that the ring is empty rather
    // than reading slot 3, which nobody has written.
    Consumer late;
    late.open(t.path);
    check(!pop_str(late).has_value(),
          "a consumer joining a ring in progress does not read a slot nobody wrote");
    check(push_str(p, "next"), "pushed after the join");
    auto got = pop_str(late);
    check(got.has_value() && *got == "next", "and then sees the next real message");
}

}  // namespace

int main() {
    a_capacity_that_is_not_a_power_of_two_is_refused();
    a_slot_that_would_straddle_a_cache_line_is_refused();
    the_file_is_exactly_the_header_plus_the_slots();
    the_geometry_survives_a_reopen();
    a_file_that_is_not_a_ring_is_refused_rather_than_read();
    a_truncated_ring_is_refused();

    a_message_comes_out_as_it_went_in();
    messages_come_out_in_the_order_they_went_in();
    a_full_ring_refuses_rather_than_overwriting();
    full_and_empty_are_told_apart_without_a_spare_slot();
    the_indices_keep_counting_past_the_capacity();
    a_message_too_large_for_a_slot_is_refused();
    a_fill_that_overruns_its_slot_publishes_nothing();
    an_empty_message_round_trips();
    drain_stops_at_its_limit();
    a_consumer_joining_a_ring_in_progress_does_not_read_ahead();

    if (failures != 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all ring checks passed\n";
    return 0;
}
