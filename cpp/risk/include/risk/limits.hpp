// Pre-trade risk limits, on a path that does not touch the heap.
//
// # What this is for
//
// Everything an order can do that the firm cannot afford: too big, too much
// money, at a price nobody is quoting, too many live at once, or taking a
// position past what is allowed. Each of those is one comparison. The reason
// this file is interesting is not the comparisons — it is that they happen
// between an order arriving off a ring and the same order reaching the matching
// engine, so the cost of the check is a cost every order pays.
//
// # The rule: nothing on this path allocates
//
// Symbols are a flat array indexed by symbol id. Open orders live in a
// preallocated slab with an open-addressed index — the same shape
// `crates/book` uses, for the same reason. There is no `std::string`, no
// `std::map`, no `std::function`, and no container that can grow.
//
// `risk/counting_alloc.hpp` is what turns that from a claim into a test.
//
// # Order of the checks
//
// Cheapest first, and the order is deliberate rather than incidental: an
// unknown symbol is one array lookup, and doing it before the arithmetic means
// a fat-fingered symbol id never reaches code that would index a position with
// it. Beyond that the ordering only decides *which* reason a doubly-bad order
// is rejected for, and the answer should be stable, so it is fixed here and
// asserted in the tests.
//
// See docs/ORDER-PATH.md.

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "wire/generated.hpp"

namespace risk {

using mdstack::wire::RejectReason;
using mdstack::wire::Side;

/// Per-symbol limits. Read at startup; there is no live update, and
/// docs/ORDER-PATH.md says why.
struct SymbolLimits {
    /// Zero means the symbol is not configured, and an order naming it is
    /// rejected. There is no default symbol.
    bool configured{false};
    /// Largest quantity a single order may carry.
    std::uint32_t max_order_quantity{0};
    /// Largest `price * quantity` a single order may carry, in price units
    /// times quantity. Integer throughout: there is no floating point on this
    /// path any more than there is on the wire.
    std::int64_t max_notional{0};
    /// Largest absolute position, long or short.
    std::int64_t max_position{0};
    /// How far from `reference_price` an order may be priced, in basis points.
    /// Zero disables the collar.
    std::int64_t collar_bps{0};
    /// What the collar measures against. Updated by fills.
    std::int64_t reference_price{0};
};

/// Everything the engine needs at construction. The only allocation it will
/// ever do happens here.
struct RiskConfig {
    /// One entry per symbol id the service will accept, indexed by id. Sized
    /// once; an order naming an id past the end is an unknown symbol, not an
    /// out-of-bounds read.
    std::vector<SymbolLimits> symbols;
    /// Live orders the service can track at once. A slab, not a bound that
    /// grows.
    std::size_t max_open_orders{4096};
};

/// The answer to "may this order proceed".
struct Decision {
    bool accepted{false};
    RejectReason reason{RejectReason::kNotRejected};

    friend bool operator==(const Decision&, const Decision&) = default;

    static Decision accept() noexcept { return Decision{true, RejectReason::kNotRejected}; }
    static Decision reject(RejectReason r) noexcept { return Decision{false, r}; }
};

/// What the service knows about one live order.
struct OpenOrder {
    std::uint64_t client_order_id{0};
    std::uint64_t exchange_order_id{0};
    std::int64_t price{0};
    std::uint32_t leaves_quantity{0};
    std::uint16_t symbol_id{0};
    Side side{Side::kBid};
};

/// Running totals, for the operator and for the tests.
struct RiskStats {
    std::uint64_t accepted{0};
    std::uint64_t rejected{0};
    std::uint64_t fills_applied{0};
    std::uint64_t cancels_applied{0};
    /// Reports naming an order the service does not know about. Not an error on
    /// its own -- after a restart the engine legitimately knows about orders
    /// this process has not rebuilt yet -- but it is counted rather than
    /// ignored, because a number that climbs in steady state means something is
    /// wrong upstream.
    std::uint64_t unknown_order_reports{0};
};

/// Pre-trade limits over preallocated state.
class RiskEngine {
  public:
    explicit RiskEngine(RiskConfig cfg);

    /// May this order proceed?
    ///
    /// On acceptance the order is recorded as live, which is what makes the
    /// open-order and position limits mean anything: a limit checked against
    /// state nobody updates is a limit that passes forever.
    Decision on_new_order(std::uint64_t client_order_id, std::uint16_t symbol_id, Side side,
                          std::int64_t price, std::uint32_t quantity) noexcept;

    /// May this cancel proceed? Rejected when the named order is not live,
    /// because a cancel for an order the engine has already finished with would
    /// come back as a reject anyway, one round trip later.
    Decision on_cancel(std::uint64_t client_order_id,
                       std::uint64_t orig_client_order_id) noexcept;

    /// Applies what the engine reported: fills move the position, terminal
    /// states free the slot.
    void on_exec_report(std::uint64_t client_order_id, std::uint64_t exchange_order_id,
                        mdstack::wire::ExecType type, std::int64_t price,
                        std::uint32_t quantity, std::uint32_t leaves_quantity) noexcept;

    /// Signed position in `symbol_id`; positive is long.
    [[nodiscard]] std::int64_t position(std::uint16_t symbol_id) const noexcept;

    /// Quantity of live orders on each side of `symbol_id`. This is what makes
    /// the position limit hold against an order that fills in pieces.
    [[nodiscard]] std::int64_t pending_buy(std::uint16_t symbol_id) const noexcept;
    [[nodiscard]] std::int64_t pending_sell(std::uint16_t symbol_id) const noexcept;

    [[nodiscard]] std::size_t open_orders() const noexcept { return live_; }
    [[nodiscard]] const RiskStats& stats() const noexcept { return stats_; }

    /// Walks every live order. Used by reconciliation, which has to name them
    /// all; a callback rather than a returned container, because returning one
    /// would allocate.
    template <typename Visit>
    void for_each_open_order(Visit&& visit) const {
        for (const auto& s : slots_) {
            if (s.occupied) {
                visit(s.order);
            }
        }
    }

    /// Re-establishes an order the service already believed in, without running
    /// the limits over it. Used only when rebuilding from a log: the order was
    /// accepted once, and re-checking it against today's limits could reject
    /// something that is already live on the exchange.
    bool adopt(const OpenOrder& order) noexcept;

  private:
    struct Slot {
        OpenOrder order{};
        bool occupied{false};
    };

    struct SymbolState {
        std::int64_t position{0};
        std::int64_t pending_buy{0};
        std::int64_t pending_sell{0};
    };

    [[nodiscard]] std::size_t index_of(std::uint64_t client_order_id) const noexcept;
    [[nodiscard]] std::size_t find(std::uint64_t client_order_id) const noexcept;
    [[nodiscard]] std::size_t free_slot(std::uint64_t client_order_id) const noexcept;
    void release(std::size_t index) noexcept;

    RiskConfig cfg_;
    /// Parallel to `cfg_.symbols`, so one bounds check covers both.
    std::vector<SymbolState> state_;
    /// Open-addressed, power-of-two capacity, linear probing. Deletion is a
    /// backward shift rather than a tombstone -- the same choice
    /// `crates/book` makes, and for the same reason: tombstones turn a table
    /// that churns into one that degrades.
    std::vector<Slot> slots_;
    std::size_t mask_{0};
    std::size_t live_{0};
    RiskStats stats_{};
};

}  // namespace risk
