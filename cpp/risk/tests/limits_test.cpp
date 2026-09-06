// What the pre-trade limits have to do.
//
// The milestone's verification step says breaches "produce the right FIX reject
// with the right reason and never reach the engine". The second half is the
// end-to-end test's job. This file is the first half, and it is written as one
// case per reason plus the cases where two limits could both fire — because
// which reason comes back is what an operator reads at three in the morning,
// and a stable answer is worth pinning down.

#include <cstdint>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

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
using risk::Decision;
using risk::OpenOrder;
using risk::RiskConfig;
using risk::RiskEngine;
using risk::SymbolLimits;

constexpr std::uint16_t kSymbol = 7;
constexpr std::int64_t kRef = 1'000'000;

RiskConfig config() {
    RiskConfig cfg;
    cfg.symbols.assign(16, SymbolLimits{});
    cfg.symbols[kSymbol] = SymbolLimits{
        /*configured=*/true,
        /*max_order_quantity=*/1'000,
        /*max_notional=*/2'000'000'000,
        /*max_position=*/5'000,
        /*collar_bps=*/500,  // 5%
        /*reference_price=*/kRef,
    };
    cfg.max_open_orders = 8;
    return cfg;
}

bool rejected_with(const Decision& d, RejectReason r) {
    return !d.accepted && d.reason == r;
}

// --- one case per reason ---------------------------------------------------

void an_unconfigured_symbol_is_refused() {
    RiskEngine e(config());
    check(rejected_with(e.on_new_order(1, 3, Side::kBid, kRef, 10), RejectReason::kUnknownSymbol),
          "a symbol that is in range but not configured is refused");
    // Past the end of the table. Must be a reject and not an out-of-bounds read,
    // which is why the range check comes before every other check.
    check(rejected_with(e.on_new_order(2, 9999, Side::kBid, kRef, 10),
                        RejectReason::kUnknownSymbol),
          "and a symbol id past the end of the table is refused, not read");
}

void a_zero_quantity_order_is_refused_as_invalid_not_as_oversized() {
    RiskEngine e(config());
    // Reporting zero as "over the maximum" would put a sentence in the log that
    // is not true of the order.
    check(rejected_with(e.on_new_order(1, kSymbol, Side::kBid, kRef, 0),
                        RejectReason::kInvalidQuantity),
          "a zero-quantity order is refused as invalid rather than as oversized");
}

void an_order_over_the_quantity_limit_is_refused() {
    RiskEngine e(config());
    check(e.on_new_order(1, kSymbol, Side::kBid, kRef, 1'000).accepted,
          "exactly the limit is allowed, so the boundary is where it claims to be");
    check(rejected_with(e.on_new_order(2, kSymbol, Side::kBid, kRef, 1'001),
                        RejectReason::kMaxOrderQuantity),
          "one over the limit is refused");
}

void an_order_over_the_notional_limit_is_refused() {
    RiskConfig cfg = config();
    cfg.symbols[kSymbol].max_notional = 5'000'000;  // 5 lots at the reference
    RiskEngine e(std::move(cfg));
    check(e.on_new_order(1, kSymbol, Side::kBid, kRef, 5).accepted, "five lots is inside");
    check(rejected_with(e.on_new_order(2, kSymbol, Side::kBid, kRef, 6),
                        RejectReason::kMaxNotional),
          "six is over the notional limit");
}

void a_negative_price_counts_its_magnitude_against_the_notional() {
    RiskConfig cfg = config();
    cfg.symbols[kSymbol].max_notional = 5'000'000;
    cfg.symbols[kSymbol].collar_bps = 0;  // isolate the notional check
    RiskEngine e(std::move(cfg));
    // A signed price is on the wire, so a notional check that forgot the sign
    // would let an arbitrarily large short-side notional through as "negative,
    // therefore under the limit".
    check(rejected_with(e.on_new_order(1, kSymbol, Side::kBid, -kRef, 6),
                        RejectReason::kMaxNotional),
          "a negative price counts its magnitude against the notional limit");
}

void an_absurd_price_is_refused_rather_than_overflowing() {
    // `price` arrives off a ring with no bound on it. The obvious
    // `price * quantity` is signed overflow on a value somebody else chose,
    // which is undefined behaviour rather than a wrong answer -- and the wrong
    // answer it usually produces is a *small* notional, so the order is
    // accepted. Found by review, not by a failing test, which is why these
    // exist now.
    RiskEngine e(config());
    const std::int64_t huge = std::numeric_limits<std::int64_t>::max() / 2;
    check(rejected_with(e.on_new_order(1, kSymbol, Side::kBid, huge, 1'000),
                        RejectReason::kMaxNotional),
          "a price near the top of the range is refused, not multiplied");
    check(rejected_with(e.on_new_order(2, kSymbol, Side::kBid, -huge, 1'000),
                        RejectReason::kMaxNotional),
          "and the same on the negative side");
    // The one value with no positive counterpart; negating it is undefined.
    check(rejected_with(e.on_new_order(3, kSymbol, Side::kBid,
                                       std::numeric_limits<std::int64_t>::min(), 1),
                        RejectReason::kMaxNotional),
          "and INT64_MIN, which cannot even be negated");
    check(e.open_orders() == 0, "none of them became a live order");
}

void the_collar_arithmetic_does_not_overflow_either() {
    RiskConfig cfg = config();
    // A reference price and a collar wide enough that `ref * bps` would not fit
    // in 64 bits. The comparison has to reach an answer rather than wrap.
    cfg.symbols[kSymbol].reference_price = std::numeric_limits<std::int64_t>::max() / 4;
    cfg.symbols[kSymbol].collar_bps = 1'000'000;
    cfg.symbols[kSymbol].max_notional = std::numeric_limits<std::int64_t>::max();
    RiskEngine e(std::move(cfg));
    // The collar is wider than any price can be, so nothing is outside it.
    check(e.on_new_order(1, kSymbol, Side::kBid, 1'000, 1).accepted,
          "a collar too wide to overflow into a rejection rejects nothing");

    RiskConfig tight = config();
    tight.symbols[kSymbol].reference_price = 1'000;
    tight.symbols[kSymbol].collar_bps = 1;
    tight.symbols[kSymbol].max_notional = std::numeric_limits<std::int64_t>::max();
    RiskEngine t(std::move(tight));
    check(rejected_with(t.on_new_order(1, kSymbol, Side::kBid,
                                       std::numeric_limits<std::int64_t>::max() / 4, 1),
                        RejectReason::kPriceCollar),
          "and a price too far away to scale is refused rather than wrapping into range");
}

void an_order_outside_the_collar_is_refused() {
    RiskEngine e(config());
    // 5% of 1,000,000 is 50,000.
    check(e.on_new_order(1, kSymbol, Side::kBid, kRef + 50'000, 1).accepted,
          "exactly at the collar is allowed");
    check(rejected_with(e.on_new_order(2, kSymbol, Side::kBid, kRef + 50'001, 1),
                        RejectReason::kPriceCollar),
          "one tick outside the collar is refused, above the reference");
    check(rejected_with(e.on_new_order(3, kSymbol, Side::kBid, kRef - 50'001, 1),
                        RejectReason::kPriceCollar),
          "and below it, so the collar is not one-sided");
}

void the_collar_follows_the_market() {
    RiskEngine e(config());
    const std::int64_t far = kRef + 90'000;  // 9% away: outside a 5% collar
    check(rejected_with(e.on_new_order(1, kSymbol, Side::kBid, far, 1),
                        RejectReason::kPriceCollar),
          "a price 9% away is outside the collar to begin with");

    // A fill moves the reference. A collar frozen at start-up rejects every
    // order an hour into a trending session, which is worse than no collar
    // because it looks like the system is broken rather than protecting anyone.
    check(e.on_new_order(2, kSymbol, Side::kBid, kRef + 40'000, 10).accepted, "inside, accepted");
    e.on_exec_report(2, 99, ExecType::kFill, kRef + 40'000, 10, 0);
    check(e.on_new_order(3, kSymbol, Side::kBid, far, 1).accepted,
          "and after a fill at a higher price the same order is inside the collar");
}

void too_many_live_orders_is_refused() {
    RiskEngine e(config());  // max_open_orders = 8
    for (std::uint64_t i = 1; i <= 8; ++i) {
        check(e.on_new_order(i, kSymbol, Side::kBid, kRef, 1).accepted,
              "order " + std::to_string(i) + " of 8 accepted");
    }
    check(rejected_with(e.on_new_order(9, kSymbol, Side::kBid, kRef, 1),
                        RejectReason::kOpenOrderLimit),
          "the ninth live order is refused");

    // A terminal report frees exactly one slot.
    e.on_exec_report(1, 0, ExecType::kCanceled, 0, 0, 0);
    check(e.open_orders() == 7, "a cancel frees exactly one slot");
    check(e.on_new_order(9, kSymbol, Side::kBid, kRef, 1).accepted, "and one more fits");
}

void a_duplicate_client_order_id_is_refused() {
    RiskEngine e(config());
    check(e.on_new_order(42, kSymbol, Side::kBid, kRef, 1).accepted, "first 42 accepted");
    // Two live orders sharing an id make every later cancel and every later fill
    // ambiguous, and the ambiguity is unresolvable rather than merely annoying.
    check(rejected_with(e.on_new_order(42, kSymbol, Side::kBid, kRef, 1),
                        RejectReason::kDuplicateClientOrderId),
          "a second live order with the same id is refused");
    e.on_exec_report(42, 0, ExecType::kCanceled, 0, 0, 0);
    check(e.on_new_order(42, kSymbol, Side::kBid, kRef, 1).accepted,
          "and the id is reusable once the first order is done");
}

// --- the position limit, which is the one with teeth -----------------------

void the_position_limit_counts_the_worst_case() {
    RiskConfig cfg = config();
    cfg.symbols[kSymbol].max_position = 100;
    cfg.max_open_orders = 64;
    RiskEngine e(std::move(cfg));

    check(e.on_new_order(1, kSymbol, Side::kBid, kRef, 100).accepted,
          "an order that would take the position exactly to the limit is allowed");
    // Nothing has filled. A limit checked against the *current* position would
    // see zero here and wave this through, and the two orders together would
    // take the position to 200.
    check(rejected_with(e.on_new_order(2, kSymbol, Side::kBid, kRef, 1),
                        RejectReason::kPositionLimit),
          "a second order is refused even though nothing has filled yet");
}

void the_position_limit_cannot_be_walked_past_in_pieces() {
    RiskConfig cfg = config();
    cfg.symbols[kSymbol].max_position = 100;
    cfg.max_open_orders = 64;
    RiskEngine e(std::move(cfg));

    // Ten orders of ten. Each is trivially inside the limit on its own; the
    // eleventh is what the limit is for.
    for (std::uint64_t i = 1; i <= 10; ++i) {
        check(e.on_new_order(i, kSymbol, Side::kBid, kRef, 10).accepted,
              "small order " + std::to_string(i) + " accepted");
    }
    check(rejected_with(e.on_new_order(11, kSymbol, Side::kBid, kRef, 1),
                        RejectReason::kPositionLimit),
          "and the eleventh is refused: the limit counts pending exposure");
}

void the_two_sides_are_counted_separately() {
    RiskConfig cfg = config();
    cfg.symbols[kSymbol].max_position = 100;
    cfg.max_open_orders = 64;
    RiskEngine e(std::move(cfg));

    check(e.on_new_order(1, kSymbol, Side::kBid, kRef, 100).accepted, "long to the limit");
    // A sell does not net against a pending buy. Netting them would let a
    // caller sit at the limit on both sides at once and end up 200 long if the
    // buys filled and the sells did not.
    check(e.on_new_order(2, kSymbol, Side::kAsk, kRef, 100).accepted,
          "and short to the limit, because the worst case on each side is its own");
    check(rejected_with(e.on_new_order(3, kSymbol, Side::kAsk, kRef, 1),
                        RejectReason::kPositionLimit),
          "but one more on the short side is refused");
}

void fills_move_the_position_and_free_the_pending() {
    RiskConfig cfg = config();
    cfg.symbols[kSymbol].max_position = 100;
    RiskEngine e(std::move(cfg));

    check(e.on_new_order(1, kSymbol, Side::kBid, kRef, 100).accepted, "accepted");
    check(e.pending_buy(kSymbol) == 100 && e.position(kSymbol) == 0,
          "before any fill it is all pending and no position");

    e.on_exec_report(1, 500, ExecType::kPartialFill, kRef, 40, 60);
    check(e.position(kSymbol) == 40, "a partial fill moves the position");
    check(e.pending_buy(kSymbol) == 60, "and takes the same amount out of pending");
    check(e.open_orders() == 1, "the order is still live");

    e.on_exec_report(1, 500, ExecType::kFill, kRef, 60, 0);
    check(e.position(kSymbol) == 100, "the rest fills");
    check(e.pending_buy(kSymbol) == 0, "pending is clear");
    check(e.open_orders() == 0, "and the order is no longer live");
}

void a_cancel_after_a_partial_fill_releases_only_the_remainder() {
    RiskConfig cfg = config();
    cfg.symbols[kSymbol].max_position = 100;
    RiskEngine e(std::move(cfg));

    e.on_new_order(1, kSymbol, Side::kBid, kRef, 100);
    e.on_exec_report(1, 500, ExecType::kPartialFill, kRef, 40, 60);
    e.on_exec_report(1, 500, ExecType::kCanceled, 0, 0, 0);
    // 40 filled and 60 cancelled. Releasing the original 100 would leave
    // pending at -40, and a negative pending quietly widens the limit.
    check(e.position(kSymbol) == 40, "the filled 40 stays on the position");
    check(e.pending_buy(kSymbol) == 0, "and the cancelled 60 leaves pending at zero, not below");
    check(e.open_orders() == 0, "the order is done");
}

void a_reject_after_acceptance_releases_the_whole_order() {
    RiskConfig cfg = config();
    cfg.symbols[kSymbol].max_position = 100;
    RiskEngine e(std::move(cfg));
    e.on_new_order(1, kSymbol, Side::kBid, kRef, 100);
    // The engine can refuse an order risk accepted -- an unknown symbol on its
    // side, a book that cannot hold it. The exposure has to come back.
    e.on_exec_report(1, 0, ExecType::kRejected, 0, 0, 0);
    check(e.pending_buy(kSymbol) == 0 && e.open_orders() == 0,
          "an engine reject releases the whole order's exposure");
}

// --- cancels and unknown orders -------------------------------------------

void a_cancel_for_an_order_nobody_knows_is_refused() {
    RiskEngine e(config());
    check(rejected_with(e.on_cancel(1, 999), RejectReason::kUnknownOrder),
          "a cancel naming an order that is not live is refused here");
    e.on_new_order(10, kSymbol, Side::kBid, kRef, 1);
    check(e.on_cancel(11, 10).accepted, "and one naming a live order is accepted");
}

void a_report_for_an_unknown_order_is_counted_not_ignored() {
    RiskEngine e(config());
    e.on_exec_report(777, 1, ExecType::kFill, kRef, 10, 0);
    // Legitimate right after a restart, and a symptom in steady state. Either
    // way it is a number somebody can look at rather than a silent return.
    check(e.stats().unknown_order_reports == 1,
          "a report for an order this service never accepted is counted");
    check(e.position(kSymbol) == 0, "and moves no position");
}

// --- the table -------------------------------------------------------------

void the_order_table_survives_heavy_churn() {
    RiskConfig cfg = config();
    cfg.max_open_orders = 64;
    cfg.symbols[kSymbol].max_position = 1'000'000'000;
    RiskEngine e(std::move(cfg));

    // Open and close 100,000 orders through a 64-slot table. Backward-shift
    // deletion has to leave every remaining order findable; a tombstone scheme
    // would still pass this and would be slower every round, and a broken shift
    // fails it within a few hundred.
    bool ok = true;
    for (std::uint64_t i = 1; i <= 100'000; ++i) {
        ok = ok && e.on_new_order(i, kSymbol, Side::kBid, kRef, 1).accepted;
        // Close the one from 32 rounds ago, so up to 32 are live at a time and
        // the table always holds a mix of runs.
        if (i > 32) {
            e.on_exec_report(i - 32, 0, ExecType::kCanceled, 0, 0, 0);
        }
        if (!ok) {
            break;
        }
    }
    check(ok, "100,000 orders through a 64-slot table, all found and all released");
    check(e.open_orders() == 32, "and the live count is exactly what was left open");
}

void adopt_restores_an_order_without_re_running_the_limits() {
    RiskConfig cfg = config();
    cfg.symbols[kSymbol].max_position = 10;  // deliberately tighter than the order
    RiskEngine e(std::move(cfg));

    // The order was accepted once, under limits that may since have changed. It
    // is live on the exchange either way, so re-checking it would reject
    // something that exists -- and the position it represents is real whether
    // this service approves of it or not.
    const OpenOrder o{42, 500, kRef, 100, kSymbol, Side::kBid};
    check(e.adopt(o), "an order from the log is adopted despite being over today's limit");
    check(e.open_orders() == 1, "and counts as live");
    check(e.pending_buy(kSymbol) == 100, "and its exposure is counted");
    check(!e.adopt(o), "adopting the same id twice is refused");

    bool seen = false;
    e.for_each_open_order([&seen](const OpenOrder& x) {
        seen = seen || (x.client_order_id == 42 && x.exchange_order_id == 500 &&
                        x.leaves_quantity == 100);
    });
    check(seen, "and it is visible to reconciliation");
}

}  // namespace

int main() {
    an_unconfigured_symbol_is_refused();
    a_zero_quantity_order_is_refused_as_invalid_not_as_oversized();
    an_order_over_the_quantity_limit_is_refused();
    an_order_over_the_notional_limit_is_refused();
    an_absurd_price_is_refused_rather_than_overflowing();
    the_collar_arithmetic_does_not_overflow_either();
    a_negative_price_counts_its_magnitude_against_the_notional();
    an_order_outside_the_collar_is_refused();
    the_collar_follows_the_market();
    too_many_live_orders_is_refused();
    a_duplicate_client_order_id_is_refused();

    the_position_limit_counts_the_worst_case();
    the_position_limit_cannot_be_walked_past_in_pieces();
    the_two_sides_are_counted_separately();
    fills_move_the_position_and_free_the_pending();
    a_cancel_after_a_partial_fill_releases_only_the_remainder();
    a_reject_after_acceptance_releases_the_whole_order();

    a_cancel_for_an_order_nobody_knows_is_refused();
    a_report_for_an_unknown_order_is_counted_not_ignored();

    the_order_table_survives_heavy_churn();
    adopt_restores_an_order_without_re_running_the_limits();

    if (failures != 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all risk checks passed\n";
    return 0;
}
