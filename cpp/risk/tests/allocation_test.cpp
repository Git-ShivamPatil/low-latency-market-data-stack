// The milestone's headline number: zero allocations across 1,000,000 risk
// decisions.
//
// The number is easy to produce and easy to produce dishonestly, so this file
// is arranged so that a dishonest zero fails:
//
//   1. The control runs first. If the replaced `operator new` is not linked in,
//      the control fails and nothing below is worth reading. A counter that is
//      not installed reports zero very convincingly.
//   2. The warm-up is outside the window and the window is a million
//      iterations, so a per-call allocation cannot hide in start-up.
//   3. The rejected orders are counted, so the loop cannot pass by rejecting
//      everything at the first check and never reaching the rest.
//   4. The error paths are exercised on purpose. That is where a `std::string`
//      hides: nobody writes `format!` on the happy path.

#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "risk/counting_alloc.hpp"
#include "risk/limits.hpp"

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

using mdstack::wire::ExecType;
using mdstack::wire::RejectReason;
using mdstack::wire::Side;
using risk::AllocGuard;
using risk::RiskConfig;
using risk::RiskEngine;
using risk::SymbolLimits;

constexpr std::uint16_t kSymbol = 7;
constexpr std::int64_t kRef = 1'000'000;

RiskConfig config() {
    RiskConfig cfg;
    cfg.symbols.assign(64, SymbolLimits{});
    cfg.symbols[kSymbol] = SymbolLimits{true, 1'000, 2'000'000'000, 1'000'000'000, 500, kRef};
    cfg.max_open_orders = 1'024;
    return cfg;
}

/// Deliberately hard to optimise away.
///
/// C++14 permits an implementation to *elide* a call to a replaceable global
/// allocation function whose result is unobserved, and clang does. So the first
/// version of this control -- allocate a vector, read its size, free it --
/// quietly stopped controlling anything under one of the two compilers this
/// project builds with, and every zero below it was vacuous there. It was g++
/// and clang++ disagreeing that surfaced it.
///
/// The size comes from a volatile, the pointer escapes through a volatile, and
/// bytes are written and read back through it. There is nothing left for the
/// optimiser to prove unobserved.
volatile std::size_t g_alloc_size = 4096;
void* volatile g_escaped = nullptr;

/// Runs first, and everything after it depends on it.
void the_counter_notices_when_something_does_allocate() {
    const AllocGuard guard;
    const std::size_t n = g_alloc_size;
    auto* raw = new unsigned char[n];
    g_escaped = raw;
    raw[0] = 0x5A;
    raw[n - 1] = 0xA5;
    const unsigned char first = raw[0];
    const unsigned char last = raw[n - 1];
    delete[] static_cast<unsigned char*>(g_escaped);
    g_escaped = nullptr;
    const auto d = guard.sample();
    check(first == 0x5A && last == 0xA5 && d.allocations >= 1 && d.deallocations >= 1,
          "the replaced operator new is linked in and reached, so the zeros below "
          "mean something");
}

void a_million_decisions_allocate_nothing() {
    RiskEngine e(config());

    // Warm up outside the window. Anything's first pass can allocate for
    // reasons that have nothing to do with steady state, and this is a claim
    // about steady state.
    for (std::uint64_t i = 1; i <= 1'000; ++i) {
        e.on_new_order(i, kSymbol, Side::kBid, kRef, 1);
        e.on_exec_report(i, i, ExecType::kFill, kRef, 1, 0);
    }

    const AllocGuard guard;
    std::uint64_t accepted = 0;
    std::uint64_t rejected = 0;
    for (std::uint64_t i = 1; i <= 1'000'000; ++i) {
        // A mixture, so the run reaches every branch rather than the first one.
        //   - most orders pass every check and rest
        //   - one in seven is over the quantity limit
        //   - one in eleven names a symbol that is not configured
        //   - one in thirteen is priced outside the collar
        const std::uint32_t qty = (i % 7 == 0) ? 5'000 : 1;
        const std::uint16_t sym = (i % 11 == 0) ? static_cast<std::uint16_t>(3) : kSymbol;
        const std::int64_t px = (i % 13 == 0) ? kRef * 4 : kRef;
        const Side side = (i % 2 == 0) ? Side::kBid : Side::kAsk;

        const auto d = e.on_new_order(i, sym, side, px, qty);
        if (d.accepted) {
            ++accepted;
            // Close it again, so the table churns and the backward shift runs a
            // million times rather than never.
            if (i % 3 == 0) {
                e.on_exec_report(i, i, ExecType::kFill, px, qty, 0);
            } else {
                e.on_exec_report(i, i, ExecType::kCanceled, 0, 0, 0);
            }
        } else {
            ++rejected;
        }
        // And a cancel for an order that is definitely gone, which is the
        // cheapest way to reach the unknown-order path.
        if (i % 17 == 0) {
            e.on_cancel(i + 1'000'000'000, i);
        }
    }
    const auto d = guard.sample();

    // The loop has to have done the work. A version of this test that rejected
    // everything at the first check would allocate nothing too.
    check(accepted > 500'000, "most of the million orders passed every check: " +
                                  std::to_string(accepted));
    check(rejected > 100'000, "and a substantial minority were rejected: " +
                                  std::to_string(rejected));
    check(accepted + rejected == 1'000'000, "every order got a decision");
    check(d.is_clean(), "1,000,000 risk decisions allocated nothing: " +
                            std::to_string(d.allocations) + " allocations, " +
                            std::to_string(d.deallocations) + " deallocations");
}

void every_reject_reason_is_reachable_without_allocating() {
    RiskConfig cfg = config();
    cfg.max_open_orders = 4;
    cfg.symbols[kSymbol].max_position = 10;
    RiskEngine e(std::move(cfg));

    // Fill the table so the open-order limit is reachable.
    for (std::uint64_t i = 1; i <= 4; ++i) {
        e.on_new_order(i, kSymbol, Side::kBid, kRef, 1);
    }

    // The error paths are where a std::string hides: nobody writes a formatted
    // message on the happy path. Each of these takes a different early exit.
    const AllocGuard guard;
    const RejectReason reasons[] = {
        e.on_new_order(100, 3, Side::kBid, kRef, 1).reason,             // unknown symbol
        e.on_new_order(101, kSymbol, Side::kBid, kRef, 0).reason,       // invalid quantity
        e.on_new_order(102, kSymbol, Side::kBid, kRef, 99'999).reason,  // max quantity
        e.on_new_order(103, kSymbol, Side::kBid, kRef * 8, 1).reason,   // collar
        e.on_new_order(1, kSymbol, Side::kBid, kRef, 1).reason,         // duplicate id
        e.on_new_order(104, kSymbol, Side::kBid, kRef, 1).reason,       // open order limit
        e.on_cancel(105, 999).reason,                                   // unknown order
    };
    const auto d = guard.sample();
    check(d.is_clean(), "seven different rejections allocated nothing");

    const RejectReason want[] = {
        RejectReason::kUnknownSymbol,
        RejectReason::kInvalidQuantity,
        RejectReason::kMaxOrderQuantity,
        RejectReason::kPriceCollar,
        RejectReason::kDuplicateClientOrderId,
        RejectReason::kOpenOrderLimit,
        RejectReason::kUnknownOrder,
    };
    bool all = true;
    for (std::size_t i = 0; i < std::size(want); ++i) {
        all = all && reasons[i] == want[i];
    }
    // Not incidental. Which reason comes back is what an operator reads, and a
    // rejection that reports the wrong one sends them to the wrong limit.
    check(all, "and each came back with the reason it should have");
}

}  // namespace

int main() {
    the_counter_notices_when_something_does_allocate();
    a_million_decisions_allocate_nothing();
    every_reject_reason_is_reachable_without_allocating();

    if (failures != 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    const auto total = risk::alloc_counts();
    std::cout << "all allocation checks passed (" << total.allocations
              << " allocations in the whole process, all outside the measured windows)\n";
    return 0;
}
