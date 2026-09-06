#include "risk/limits.hpp"

#include <bit>
#include <utility>

namespace risk {

using mdstack::wire::ExecType;

namespace {

/// The table is sized above the slab so probes stay short. A load factor near 1
/// turns linear probing into a linear scan, which is a latency cliff rather
/// than a slowdown.
std::size_t table_size_for(std::size_t max_open_orders) {
    const std::size_t want = (max_open_orders < 1 ? 1 : max_open_orders) * 2;
    return std::bit_ceil(want);
}

/// FNV-1a over the eight bytes of the id.
///
/// Client order ids are frequently sequential, and sequential keys through a
/// weak hash cluster into one run of the table. Mixing them costs a handful of
/// cycles and keeps the probe length flat.
std::size_t mix(std::uint64_t id) noexcept {
    std::uint64_t h = 0xcbf29ce484222325ULL;
    for (int i = 0; i < 8; ++i) {
        h ^= (id >> (i * 8)) & 0xFF;
        h *= 0x100000001b3ULL;
    }
    return static_cast<std::size_t>(h);
}

}  // namespace

RiskEngine::RiskEngine(RiskConfig cfg) : cfg_(std::move(cfg)) {
    // The only allocation this object will ever do. Everything after this point
    // indexes into what these three lines reserved.
    state_.assign(cfg_.symbols.size(), SymbolState{});
    const std::size_t n = table_size_for(cfg_.max_open_orders);
    slots_.assign(n, Slot{});
    mask_ = n - 1;
}

std::size_t RiskEngine::index_of(std::uint64_t client_order_id) const noexcept {
    return mix(client_order_id) & mask_;
}

std::size_t RiskEngine::find(std::uint64_t client_order_id) const noexcept {
    std::size_t i = index_of(client_order_id);
    // Terminated by the first empty slot, which backward-shift deletion
    // guarantees is a real end of run rather than a hole left by a removal.
    while (slots_[i].occupied) {
        if (slots_[i].order.client_order_id == client_order_id) {
            return i;
        }
        i = (i + 1) & mask_;
    }
    return slots_.size();
}

std::size_t RiskEngine::free_slot(std::uint64_t client_order_id) const noexcept {
    std::size_t i = index_of(client_order_id);
    while (slots_[i].occupied) {
        i = (i + 1) & mask_;
    }
    return i;
}

void RiskEngine::release(std::size_t index) noexcept {
    // Backward-shift deletion. A tombstone would be shorter and would make the
    // table degrade under exactly the workload this service has: a long run of
    // orders that are opened and closed.
    slots_[index].occupied = false;
    std::size_t hole = index;
    std::size_t i = (index + 1) & mask_;
    while (slots_[i].occupied) {
        const std::size_t home = index_of(slots_[i].order.client_order_id);
        // Is `i` reachable from `home` without passing the hole? If not, moving
        // it into the hole keeps it findable.
        const bool wraps = i < home;
        const bool hole_between =
            wraps ? (hole >= home || hole < i) : (hole >= home && hole < i);
        if (hole_between) {
            slots_[hole] = slots_[i];
            slots_[i].occupied = false;
            hole = i;
        }
        i = (i + 1) & mask_;
    }
    --live_;
}

Decision RiskEngine::on_new_order(std::uint64_t client_order_id, std::uint16_t symbol_id,
                                  Side side, std::int64_t price,
                                  std::uint32_t quantity) noexcept {
    auto reject = [this](RejectReason r) {
        ++stats_.rejected;
        return Decision::reject(r);
    };

    // 1. Unknown symbol. First because it is one lookup, and because every
    //    check below indexes state by this id.
    if (symbol_id >= cfg_.symbols.size() || !cfg_.symbols[symbol_id].configured) {
        return reject(RejectReason::kUnknownSymbol);
    }
    const SymbolLimits& lim = cfg_.symbols[symbol_id];

    // A duplicate id is refused here rather than at the engine. Two live orders
    // sharing an id make every later cancel and every later fill ambiguous.
    if (find(client_order_id) != slots_.size()) {
        return reject(RejectReason::kDuplicateClientOrderId);
    }

    // 2. Max order quantity.
    if (quantity == 0) {
        return reject(RejectReason::kInvalidQuantity);
    }
    if (quantity > lim.max_order_quantity) {
        return reject(RejectReason::kMaxOrderQuantity);
    }

    // 3. Max notional. In 64-bit integers throughout; a price of 10^12 times a
    //    quantity of 10^6 is 10^18, still inside the range, and the quantity is
    //    already bounded by the check above.
    const std::int64_t notional = price * static_cast<std::int64_t>(quantity);
    const std::int64_t magnitude = notional < 0 ? -notional : notional;
    if (magnitude > lim.max_notional) {
        return reject(RejectReason::kMaxNotional);
    }

    // 4. Price collar. Basis points against the reference, in integers: the
    //    comparison is `|price - ref| * 10000 > ref * bps`, which needs no
    //    division and therefore no rounding decision.
    if (lim.collar_bps > 0 && lim.reference_price != 0) {
        const std::int64_t ref = lim.reference_price < 0 ? -lim.reference_price
                                                         : lim.reference_price;
        const std::int64_t away = price > lim.reference_price ? price - lim.reference_price
                                                              : lim.reference_price - price;
        if (away * 10'000 > ref * lim.collar_bps) {
            return reject(RejectReason::kPriceCollar);
        }
    }

    // 5. Open order count.
    if (live_ >= cfg_.max_open_orders) {
        return reject(RejectReason::kOpenOrderLimit);
    }

    // 6. Position limit, against the worst case rather than the likely one.
    //
    //    The worst case is the position that would result if this order and
    //    every other live order on the same side filled completely. Checking
    //    only against the current position lets a caller walk past the limit
    //    with a queue of small orders, none of which is individually over it.
    const SymbolState& st = state_[symbol_id];
    const auto qty = static_cast<std::int64_t>(quantity);
    const std::int64_t worst = (side == Side::kBid) ? st.position + st.pending_buy + qty
                                                    : st.position - st.pending_sell - qty;
    const std::int64_t worst_magnitude = worst < 0 ? -worst : worst;
    if (worst_magnitude > lim.max_position) {
        return reject(RejectReason::kPositionLimit);
    }

    const std::size_t i = free_slot(client_order_id);
    slots_[i].order = OpenOrder{client_order_id, 0, price, quantity, symbol_id, side};
    slots_[i].occupied = true;
    ++live_;
    if (side == Side::kBid) {
        state_[symbol_id].pending_buy += qty;
    } else {
        state_[symbol_id].pending_sell += qty;
    }
    ++stats_.accepted;
    return Decision::accept();
}

Decision RiskEngine::on_cancel(std::uint64_t client_order_id,
                               std::uint64_t orig_client_order_id) noexcept {
    // The cancel's own id identifies the *request*, and this service tracks
    // orders rather than requests. It travels on the wire so the gateway can
    // match the reply to what it sent; nothing here needs it.
    (void)client_order_id;
    if (find(orig_client_order_id) == slots_.size()) {
        ++stats_.rejected;
        return Decision::reject(RejectReason::kUnknownOrder);
    }
    ++stats_.accepted;
    return Decision::accept();
}

void RiskEngine::on_exec_report(std::uint64_t client_order_id, std::uint64_t exchange_order_id,
                                ExecType type, std::int64_t price, std::uint32_t quantity,
                                std::uint32_t leaves_quantity) noexcept {
    const std::size_t i = find(client_order_id);
    if (i == slots_.size()) {
        // Not treated as an error. After a restart the engine legitimately
        // knows about orders this process has not rebuilt yet. Counted, though:
        // a number that climbs in steady state means something upstream is
        // reporting on orders that were never accepted here.
        ++stats_.unknown_order_reports;
        return;
    }
    OpenOrder& o = slots_[i].order;
    SymbolState& st = state_[o.symbol_id];

    if (exchange_order_id != 0) {
        o.exchange_order_id = exchange_order_id;
    }

    const bool is_fill = (type == ExecType::kFill || type == ExecType::kPartialFill);
    if (is_fill) {
        const auto filled = static_cast<std::int64_t>(quantity);
        if (o.side == Side::kBid) {
            st.position += filled;
            st.pending_buy -= filled;
        } else {
            st.position -= filled;
            st.pending_sell -= filled;
        }
        // The collar follows the market rather than the configuration: a
        // reference price frozen at startup rejects every order an hour later.
        if (price != 0) {
            cfg_.symbols[o.symbol_id].reference_price = price;
        }
        ++stats_.fills_applied;
        o.leaves_quantity = leaves_quantity;
    }

    const bool terminal = (type == ExecType::kFill && leaves_quantity == 0) ||
                          type == ExecType::kCanceled || type == ExecType::kRejected;
    if (terminal) {
        // Whatever never filled stops being pending. On a cancel or a reject
        // this is the whole remainder; on a full fill the fill above has
        // already taken it down and this is zero.
        const auto remaining = static_cast<std::int64_t>(o.leaves_quantity);
        if (!is_fill) {
            if (o.side == Side::kBid) {
                st.pending_buy -= remaining;
            } else {
                st.pending_sell -= remaining;
            }
        }
        if (type == ExecType::kCanceled) {
            ++stats_.cancels_applied;
        }
        release(i);
    }
}

std::int64_t RiskEngine::position(std::uint16_t symbol_id) const noexcept {
    return symbol_id < state_.size() ? state_[symbol_id].position : 0;
}

std::int64_t RiskEngine::pending_buy(std::uint16_t symbol_id) const noexcept {
    return symbol_id < state_.size() ? state_[symbol_id].pending_buy : 0;
}

std::int64_t RiskEngine::pending_sell(std::uint16_t symbol_id) const noexcept {
    return symbol_id < state_.size() ? state_[symbol_id].pending_sell : 0;
}

bool RiskEngine::adopt(const OpenOrder& order) noexcept {
    if (order.symbol_id >= cfg_.symbols.size() || live_ >= cfg_.max_open_orders) {
        return false;
    }
    if (find(order.client_order_id) != slots_.size()) {
        return false;
    }
    const std::size_t i = free_slot(order.client_order_id);
    slots_[i].order = order;
    slots_[i].occupied = true;
    ++live_;
    const auto qty = static_cast<std::int64_t>(order.leaves_quantity);
    if (order.side == Side::kBid) {
        state_[order.symbol_id].pending_buy += qty;
    } else {
        state_[order.symbol_id].pending_sell += qty;
    }
    return true;
}

}  // namespace risk
