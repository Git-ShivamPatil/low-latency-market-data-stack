// Drives one end of a ring, so a process in the other language can drive the
// other end.
//
//   ring-poke --path P --create --capacity 1024 --slot-size 128
//   ring-poke --path P --produce 10000 --symbol 7
//   ring-poke --path P --consume 10000 --symbol 7
//
// The messages are real schema messages -- NewOrder on the way in, ExecReport
// on the way back -- so this checks two agreements at once: that both languages
// place the ring's indices at the same offsets, and that both place a message's
// fields at the same offsets. A test that pushed opaque bytes would prove only
// the first, and the second is the one that silently produces a plausible wrong
// number rather than an error.
//
// Field values are a deterministic function of the message index, so the
// consumer can say not just "I received 10,000 messages" but "message 6,231 was
// the one I was supposed to receive".

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

#include "ring/spsc.hpp"
#include "wire/generated.hpp"

namespace {

using mdstack::ring::Consumer;
using mdstack::ring::Producer;
using mdstack::ring::Ring;

/// The same function the Rust side computes. Anything derived from `i` will do;
/// what matters is that both sides derive it identically and that the fields
/// span the whole block rather than clustering at the front, where an offset
/// mistake would be invisible.
struct Expected {
    std::uint64_t client_order_id;
    std::int64_t price;
    std::uint32_t quantity;
    mdstack::wire::Side side;
};

Expected expected(std::uint64_t i, std::uint16_t symbol) {
    Expected e{};
    e.client_order_id = 0x1122334400000000ULL + i;
    // Alternating sign, so a decoder that reads the price unsigned fails
    // rather than agreeing for the first half of the run.
    e.price = (i % 2 == 0) ? static_cast<std::int64_t>(1'000'000 + i)
                           : -static_cast<std::int64_t>(1'000'000 + i);
    e.quantity = static_cast<std::uint32_t>(100 + (i % 900));
    e.side = (i % 3 == 0) ? mdstack::wire::Side::kAsk : mdstack::wire::Side::kBid;
    (void)symbol;
    return e;
}

int usage() {
    std::cerr << "usage: ring-poke --path P [--create --capacity N --slot-size N]\n"
              << "                 [--produce N] [--consume N] [--symbol N] [--spin-ms N]\n";
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    std::string path;
    std::uint32_t capacity = 1024;
    std::uint32_t slot_size = 128;
    long long produce = -1;
    long long consume = -1;
    std::uint16_t symbol = 7;
    long long spin_ms = 10'000;
    bool create = false;

    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&]() -> std::string { return (i + 1 < argc) ? argv[++i] : std::string{}; };
        if (a == "--path") {
            path = next();
        } else if (a == "--create") {
            create = true;
        } else if (a == "--capacity") {
            capacity = static_cast<std::uint32_t>(std::atoll(next().c_str()));
        } else if (a == "--slot-size") {
            slot_size = static_cast<std::uint32_t>(std::atoll(next().c_str()));
        } else if (a == "--produce") {
            produce = std::atoll(next().c_str());
        } else if (a == "--consume") {
            consume = std::atoll(next().c_str());
        } else if (a == "--symbol") {
            symbol = static_cast<std::uint16_t>(std::atoll(next().c_str()));
        } else if (a == "--spin-ms") {
            spin_ms = std::atoll(next().c_str());
        } else {
            return usage();
        }
    }
    if (path.empty()) {
        return usage();
    }

    if (create) {
        Ring r;
        if (auto e = r.create(path, capacity, slot_size)) {
            std::cerr << "ring-poke: " << mdstack::ring::describe(*e) << "\n";
            return 1;
        }
        std::cout << "created=1\ncapacity=" << capacity << "\nslot_size=" << slot_size << "\n";
        if (produce < 0 && consume < 0) {
            return 0;
        }
    }

    if (produce >= 0) {
        Producer p;
        if (auto e = p.open(path)) {
            std::cerr << "ring-poke: " << mdstack::ring::describe(*e) << "\n";
            return 1;
        }
        long long sent = 0;
        long long blocked = 0;
        // A bounded spin rather than an unbounded one: a test that hangs when
        // the other side never starts is a test that fails a CI run by timeout
        // and says nothing about why.
        const long long budget = spin_ms * 20'000;
        long long spins = 0;
        while (sent < produce && spins < budget) {
            const Expected want = expected(static_cast<std::uint64_t>(sent), symbol);
            auto err = p.push_with([&want, symbol](std::byte* dst, std::size_t cap) -> std::size_t {
                auto n = mdstack::wire::encode_new_order(dst, cap, want.client_order_id,
                                                         want.price, want.quantity, symbol,
                                                         want.side);
                return n ? *n : cap + 1;
            });
            if (err) {
                ++blocked;
                ++spins;
                continue;
            }
            ++sent;
        }
        std::cout << "produced=" << sent << "\nblocked=" << blocked << "\n";
        if (sent != produce) {
            std::cerr << "ring-poke: only produced " << sent << " of " << produce << "\n";
            return 1;
        }
    }

    if (consume >= 0) {
        Consumer c;
        if (auto e = c.open(path)) {
            std::cerr << "ring-poke: " << mdstack::ring::describe(*e) << "\n";
            return 1;
        }
        long long got = 0;
        long long mismatches = 0;
        long long first_bad = -1;
        long long spins = 0;
        const long long budget = spin_ms * 20'000;
        while (got < consume && spins < budget) {
            const bool have = c.pop_with([&](const std::byte* p, std::size_t n) {
                const Expected want = expected(static_cast<std::uint64_t>(got), symbol);
                auto d = mdstack::wire::NewOrderDecoder::wrap(p, n);
                const bool ok =
                    d && d->client_order_id() == want.client_order_id &&
                    d->price() == want.price && d->quantity() == want.quantity &&
                    d->symbol_id() == symbol && d->side() &&
                    *d->side() == want.side;
                if (!ok) {
                    if (first_bad < 0) {
                        first_bad = got;
                    }
                    ++mismatches;
                }
            });
            if (!have) {
                ++spins;
                continue;
            }
            ++got;
        }
        std::cout << "consumed=" << got << "\nmismatches=" << mismatches
                  << "\nfirst_mismatch=" << first_bad << "\n";
        if (got != consume || mismatches != 0) {
            std::cerr << "ring-poke: consumed " << got << " of " << consume << " with "
                      << mismatches << " mismatch(es)\n";
            return 1;
        }
    }

    return 0;
}
