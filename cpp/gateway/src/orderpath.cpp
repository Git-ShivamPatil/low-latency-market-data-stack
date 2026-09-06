#include "fix/orderpath.hpp"

#include <algorithm>
#include <charconv>
#include <ostream>

namespace fix {

namespace wire = mdstack::wire;
using wire::ExecType;
using wire::RejectReason;
using wire::Side;

namespace {

/// FIX `Side` (54): 1 = Buy, 2 = Sell.
std::optional<Side> parse_side(std::string_view v) {
    if (v == "1") return Side::kBid;
    if (v == "2") return Side::kAsk;
    return std::nullopt;
}

char fix_side(Side s) { return s == Side::kBid ? '1' : '2'; }

/// FIX prices are decimal strings; the wire is a scaled integer. `priceExponent`
/// is -4 in the schema, so 101.25 is 1012500.
///
/// Deliberately strict. A price this cannot represent exactly is refused rather
/// than rounded: rounding would rest an order at a price the client did not send,
/// and the client would have no way of knowing.
std::optional<std::int64_t> parse_price(std::string_view v) {
    bool negative = false;
    std::size_t i = 0;
    if (i < v.size() && (v[i] == '-' || v[i] == '+')) {
        negative = v[i] == '-';
        ++i;
    }
    std::int64_t whole = 0;
    bool any = false;
    for (; i < v.size() && v[i] >= '0' && v[i] <= '9'; ++i) {
        whole = whole * 10 + (v[i] - '0');
        any = true;
    }
    std::int64_t frac = 0;
    int frac_digits = 0;
    if (i < v.size() && v[i] == '.') {
        ++i;
        for (; i < v.size() && v[i] >= '0' && v[i] <= '9'; ++i) {
            if (frac_digits < 4) {
                frac = frac * 10 + (v[i] - '0');
                ++frac_digits;
            } else if (v[i] != '0') {
                // A fifth decimal place that is not zero cannot be represented.
                return std::nullopt;
            }
            any = true;
        }
    }
    if (!any || i != v.size()) {
        return std::nullopt;
    }
    for (; frac_digits < 4; ++frac_digits) {
        frac *= 10;
    }
    const std::int64_t scaled = whole * 10'000 + frac;
    return negative ? -scaled : scaled;
}

/// The scaled integer back to a decimal string, exactly.
std::string format_price(std::int64_t p) {
    const bool negative = p < 0;
    const std::uint64_t mag = negative ? static_cast<std::uint64_t>(-(p + 1)) + 1
                                       : static_cast<std::uint64_t>(p);
    const std::uint64_t whole = mag / 10'000;
    const std::uint64_t frac = mag % 10'000;
    char buf[32];
    auto [end, ec] = std::to_chars(buf, buf + sizeof buf, whole);
    std::string out(negative ? "-" : "");
    out.append(buf, end);
    out.push_back('.');
    for (std::uint64_t div = 1'000; div >= 1; div /= 10) {
        out.push_back(static_cast<char>('0' + (frac / div) % 10));
    }
    return out;
}

/// FIX `OrdStatus` (39) for an `ExecType`.
///
/// They are separate tags with separate meanings and FIX 4.4 requires both:
/// ExecType says what this message reports, OrdStatus says where the order now
/// stands. Collapsing them is a common shortcut and a counterparty will notice.
char ord_status(ExecType t, std::uint32_t leaves) {
    switch (t) {
        case ExecType::kAcknowledged: return '0';                     // New
        case ExecType::kPartialFill: return '1';                      // Partially filled
        case ExecType::kFill: return leaves == 0 ? '2' : '1';         // Filled / partial
        case ExecType::kCanceled: return '4';                         // Canceled
        case ExecType::kRejected: return '8';                         // Rejected
        case ExecType::kOrderStatus: return leaves == 0 ? '2' : '0';
        case ExecType::kStatusComplete: return '0';
    }
    return '0';
}

char exec_type_char(ExecType t) {
    switch (t) {
        case ExecType::kAcknowledged: return '0';   // New
        case ExecType::kPartialFill: return 'F';    // Trade
        case ExecType::kFill: return 'F';           // Trade
        case ExecType::kCanceled: return '4';       // Canceled
        case ExecType::kRejected: return '8';       // Rejected
        case ExecType::kOrderStatus: return 'I';    // Order status
        case ExecType::kStatusComplete: return 'I';
    }
    return '0';
}

/// FIX `OrdRejReason` (103) for one of ours. The mapping is lossy in FIX's
/// direction -- it has no code for "the queue to the risk engine was full" --
/// so the specific reason also goes in `Text` (58), which is where a human
/// looks anyway.
std::int64_t ord_rej_reason(RejectReason r) {
    switch (r) {
        case RejectReason::kUnknownSymbol: return 1;         // Unknown symbol
        case RejectReason::kMaxOrderQuantity: return 13;     // Incorrect quantity
        case RejectReason::kInvalidQuantity: return 13;
        case RejectReason::kMaxNotional: return 13;
        case RejectReason::kPriceCollar: return 16;          // Price exceeds current limit
        case RejectReason::kOpenOrderLimit: return 3;        // Order exceeds limit
        case RejectReason::kPositionLimit: return 3;
        case RejectReason::kDuplicateClientOrderId: return 6;  // Duplicate order
        case RejectReason::kUnknownOrder: return 5;          // Unknown order
        case RejectReason::kQueueFull: return 0;             // Broker option
        case RejectReason::kNotRejected: return 0;
    }
    return 0;
}

std::string_view reason_text(RejectReason r) {
    switch (r) {
        case RejectReason::kNotRejected: return "";
        case RejectReason::kUnknownSymbol: return "unknown symbol";
        case RejectReason::kMaxOrderQuantity: return "order quantity above the limit";
        case RejectReason::kMaxNotional: return "order notional above the limit";
        case RejectReason::kPriceCollar: return "price outside the collar";
        case RejectReason::kOpenOrderLimit: return "too many live orders";
        case RejectReason::kPositionLimit: return "position limit";
        case RejectReason::kDuplicateClientOrderId: return "duplicate ClOrdID";
        case RejectReason::kUnknownOrder: return "unknown order";
        case RejectReason::kQueueFull: return "the risk queue was full";
        case RejectReason::kInvalidQuantity: return "invalid quantity";
    }
    return "";
}

}  // namespace

std::string_view describe(DivergenceKind k) {
    switch (k) {
        case DivergenceKind::Agree: return "agree";
        case DivergenceKind::LeavesDiffer: return "leaves differ";
        case DivergenceKind::GatewayOnly: return "gateway only";
        case DivergenceKind::EngineOnly: return "engine only";
    }
    return "unknown";
}

std::size_t Divergence::count(DivergenceKind k) const {
    return static_cast<std::size_t>(
        std::count_if(entries.begin(), entries.end(),
                      [k](const DivergenceEntry& e) { return e.kind == k; }));
}

std::optional<std::string> OrderRouter::open(const Config& cfg) {
    symbols_ = cfg.symbols;
    if (auto e = orders_.open(cfg.orders_ring)) {
        return std::string("orders ring: ") + std::string(mdstack::ring::describe(*e));
    }
    if (auto e = reports_.open(cfg.reports_ring)) {
        return std::string("reports ring: ") + std::string(mdstack::ring::describe(*e));
    }
    if (auto e = store_.open(cfg.store_path, nullptr)) {
        return std::string("order store: ") + std::string(describe(*e));
    }
    return std::nullopt;
}

std::optional<std::uint16_t> OrderRouter::symbol_id(std::string_view name) const {
    for (const auto& s : symbols_) {
        if (s.name == name) {
            return s.id;
        }
    }
    return std::nullopt;
}

std::string_view OrderRouter::symbol_name(std::uint16_t id) const {
    for (const auto& s : symbols_) {
        if (s.id == id) {
            return s.name;
        }
    }
    // An id the gateway was never told a name for. It still has to appear on
    // the wire, because dropping the report would be worse than a name a client
    // does not recognise.
    return "UNKNOWN";
}

bool OrderRouter::on_application(const Message& m, Session& session, Sink& out,
                                 Clock::time_point now) {
    const auto type = m.msg_type();
    if (type == msg_type::NewOrderSingle) {
        const auto cl_ord_id = m.find(tag::ClOrdID);
        const auto sym = m.find(tag::Symbol);
        const auto side_s = m.find(tag::Side);
        const auto qty = m.find_int(tag::OrderQty);
        const auto px = m.find(tag::Price);
        if (!cl_ord_id || !sym || !side_s || !qty || !px) {
            send_reject(m, "missing a required field", RejectReason::kUnknownSymbol, session, out,
                        now);
            return true;
        }
        std::uint64_t id = 0;
        const auto& c = *cl_ord_id;
        if (std::from_chars(c.data(), c.data() + c.size(), id).ec != std::errc{}) {
            // This gateway numbers its own orders and requires the client to
            // use the same numbering. A string ClOrdID would need a map from
            // strings to ids on the hot path, which is a std::string per order.
            send_reject(m, "ClOrdID must be a number", RejectReason::kUnknownOrder, session, out,
                        now);
            return true;
        }
        const auto sid = symbol_id(*sym);
        const auto side = parse_side(*side_s);
        const auto price = parse_price(*px);
        if (!sid || !side || !price) {
            send_reject(m, !sid ? "unknown symbol" : "unparseable side or price",
                        RejectReason::kUnknownSymbol, session, out, now);
            return true;
        }

        // Durable first. If the process dies after this and before the push, the
        // order comes back as Pending and reconciliation says the engine does
        // not hold it -- which is true and recoverable. The other order would
        // leave an order working that nothing knows about.
        OrderRecord rec{id,
                        0,
                        *price,
                        static_cast<std::uint32_t>(*qty),
                        static_cast<std::uint32_t>(*qty),
                        *sid,
                        *side,
                        OrderState::Pending};
        if (store_.record(rec)) {
            send_reject(m, "could not persist the order", RejectReason::kQueueFull, session, out,
                        now);
            return true;
        }

        auto n = wire::encode_new_order(scratch_, sizeof scratch_, id, *price,
                                        static_cast<std::uint32_t>(*qty), *sid, *side);
        if (!n || orders_.push(scratch_, *n)) {
            ++stats_.queue_full;
            OrderRecord dead = rec;
            dead.state = OrderState::Rejected;
            dead.leaves_quantity = 0;
            (void)store_.record(dead);
            send_reject(m, "the risk queue was full", RejectReason::kQueueFull, session, out, now);
            return true;
        }
        ++stats_.orders_sent;
        return true;
    }

    if (type == msg_type::OrderCancelRequest) {
        const auto cl_ord_id = m.find(tag::ClOrdID);
        const auto orig = m.find(tag::OrigClOrdID);
        const auto sym = m.find(tag::Symbol);
        const auto side_s = m.find(tag::Side);
        if (!cl_ord_id || !orig || !sym || !side_s) {
            send_reject(m, "missing a required field", RejectReason::kUnknownOrder, session, out,
                        now);
            return true;
        }
        std::uint64_t id = 0;
        std::uint64_t orig_id = 0;
        std::from_chars(cl_ord_id->data(), cl_ord_id->data() + cl_ord_id->size(), id);
        std::from_chars(orig->data(), orig->data() + orig->size(), orig_id);
        const auto sid = symbol_id(*sym);
        const auto side = parse_side(*side_s);
        if (!sid || !side) {
            send_reject(m, "unknown symbol", RejectReason::kUnknownSymbol, session, out, now);
            return true;
        }
        auto n = wire::encode_cancel_order(scratch_, sizeof scratch_, id, orig_id, *sid, *side);
        if (!n || orders_.push(scratch_, *n)) {
            ++stats_.queue_full;
            send_reject(m, "the risk queue was full", RejectReason::kQueueFull, session, out, now);
            return true;
        }
        ++stats_.cancels_sent;
        return true;
    }

    return false;
}

void OrderRouter::send_reject(const Message& m, std::string_view text, RejectReason reason,
                              Session& session, Sink& out, Clock::time_point now) {
    ++stats_.rejected_locally;
    const auto cl_ord_id = m.find(tag::ClOrdID).value_or("0");
    const auto sym = m.find(tag::Symbol).value_or("UNKNOWN");
    const auto side_s = m.find(tag::Side).value_or("1");
    const auto qty = m.find_int(tag::OrderQty).value_or(0);
    const std::uint64_t exec_id = next_exec_id_++;
    session.send_application(
        msg_type::ExecutionReport,
        [&, exec_id](Builder& b) {
            b.add(tag::OrderID, "NONE");
            b.add(tag::ClOrdID, cl_ord_id);
            b.add(tag::ExecID, static_cast<std::int64_t>(exec_id));
            b.add(tag::ExecType, "8");
            b.add(tag::OrdStatus, "8");
            b.add(tag::Symbol, sym);
            b.add(tag::Side, side_s);
            b.add(tag::LeavesQty, std::int64_t{0});
            b.add(tag::CumQty, std::int64_t{0});
            b.add(tag::AvgPx, "0.0000");
            b.add(tag::OrderQty, qty);
            b.add(tag::OrdRejReason, ord_rej_reason(reason));
            // The reason FIX has no code for goes here, because this is where a
            // human looks and the numeric code is what a machine matches on.
            b.add(tag::Text, text);
        },
        out, now);
    ++stats_.exec_reports_sent;
}

std::size_t OrderRouter::pump(Session& session, Sink& out, Clock::time_point now) {
    std::size_t handled = 0;
    // Bounded, so a burst of reports cannot hold the session out of its timers.
    reports_.drain(64, [&](const std::byte* p, std::size_t n) {
        auto d = wire::ExecReportDecoder::wrap(p, n);
        if (!d) {
            return;
        }
        ++handled;
        ++stats_.reports_received;
        const auto type_opt = d->exec_type();
        if (!type_opt) {
            return;
        }
        const ExecType type = *type_opt;

        if (type == ExecType::kOrderStatus) {
            if (reconcile_state_ == ReconcileState::Awaiting) {
                DivergenceEntry e;
                e.exchange_order_id = d->exchange_order_id();
                e.engine_leaves = d->leaves_quantity();
                e.symbol_id = d->symbol_id();
                engine_view_.push_back(e);
            }
            // Not forwarded to the client. A status line is an answer to a
            // question the gateway asked, not an event on the client's order.
            return;
        }
        if (type == ExecType::kStatusComplete) {
            if (reconcile_state_ == ReconcileState::Awaiting &&
                d->client_order_id() == reconcile_request_id_) {
                divergence_.engine_named = d->quantity();
                finish_reconciliation();
            }
            return;
        }

        // A real event. Record it before telling the client, so a crash between
        // the two leaves the log ahead of the client rather than behind it.
        const auto& live = store_.live();
        auto it = std::find_if(live.begin(), live.end(), [&](const OrderRecord& r) {
            return r.client_order_id == d->client_order_id();
        });
        if (it == live.end()) {
            // A report for an order this gateway has no record of. Legitimate
            // straight after a restart; a symptom in steady state.
            ++stats_.reports_for_unknown_orders;
        } else {
            OrderRecord rec = *it;
            rec.exchange_order_id =
                d->exchange_order_id() != 0 ? d->exchange_order_id() : rec.exchange_order_id;
            rec.leaves_quantity = d->leaves_quantity();
            switch (type) {
                case ExecType::kAcknowledged:
                case ExecType::kPartialFill:
                    rec.state = OrderState::Live;
                    break;
                case ExecType::kFill:
                    rec.state = d->leaves_quantity() == 0 ? OrderState::Filled : OrderState::Live;
                    break;
                case ExecType::kCanceled:
                    rec.state = OrderState::Canceled;
                    rec.leaves_quantity = 0;
                    break;
                case ExecType::kRejected:
                    rec.state = OrderState::Rejected;
                    rec.leaves_quantity = 0;
                    break;
                default:
                    break;
            }
            (void)store_.record(rec);
        }

        if (type == ExecType::kFill || type == ExecType::kPartialFill) {
            ++stats_.fills;
            stats_.filled_quantity += d->quantity();
        }
        send_exec_report(*d, session, out, now);
    });
    return handled;
}

void OrderRouter::send_exec_report(const wire::ExecReportDecoder& d, Session& session, Sink& out,
                                   Clock::time_point now) {
    const auto type_opt = d.exec_type();
    if (!type_opt) {
        return;
    }
    const ExecType type = *type_opt;
    const auto side_opt = d.side();
    const std::uint32_t leaves = d.leaves_quantity();
    const bool is_fill = type == ExecType::kFill || type == ExecType::kPartialFill;

    // CumQty comes from the log rather than being accumulated here: the log is
    // the durable record and a running total in memory is a second one that
    // disagrees with it after a restart.
    std::uint32_t original = d.quantity();
    const auto& live = store_.live();
    auto it = std::find_if(live.begin(), live.end(), [&](const OrderRecord& r) {
        return r.client_order_id == d.client_order_id();
    });
    if (it != live.end()) {
        original = it->quantity;
    }
    const std::uint32_t cum = original >= leaves ? original - leaves : 0;

    const std::string price = format_price(d.price());
    const std::string sym(symbol_name(d.symbol_id()));
    const auto reason_opt = d.reject_reason();
    const RejectReason reason = reason_opt.value_or(RejectReason::kNotRejected);
    const std::uint64_t exec_id = next_exec_id_++;
    const std::uint64_t client_id = d.client_order_id();
    const std::uint64_t exch_id = d.exchange_order_id();
    const std::uint32_t last_qty = d.quantity();

    // FIX carries these as single characters, and Builder::add takes a
    // string_view, so they need storage that outlives the lambda.
    const char side_c = fix_side(side_opt.value_or(Side::kBid));
    const char exec_c = exec_type_char(type);
    const char status_c = ord_status(type, leaves);

    session.send_application(
        msg_type::ExecutionReport,
        [&, exec_id, client_id, exch_id, leaves, cum, last_qty](Builder& b) {
            b.add(tag::OrderID, static_cast<std::int64_t>(exch_id));
            b.add(tag::ClOrdID, static_cast<std::int64_t>(client_id));
            b.add(tag::ExecID, static_cast<std::int64_t>(exec_id));
            b.add(tag::ExecType, std::string_view(&exec_c, 1));
            b.add(tag::OrdStatus, std::string_view(&status_c, 1));
            b.add(tag::Symbol, sym);
            b.add(tag::Side, std::string_view(&side_c, 1));
            b.add(tag::LeavesQty, static_cast<std::int64_t>(leaves));
            b.add(tag::CumQty, static_cast<std::int64_t>(cum));
            b.add(tag::AvgPx, cum > 0 ? price : std::string_view("0.0000"));
            if (is_fill) {
                b.add(tag::LastPx, price);
                b.add(tag::LastQty, static_cast<std::int64_t>(last_qty));
            }
            if (type == ExecType::kRejected && reason != RejectReason::kNotRejected) {
                b.add(tag::OrdRejReason, ord_rej_reason(reason));
                b.add(tag::Text, reason_text(reason));
            }
        },
        out, now);
    ++stats_.exec_reports_sent;
}

bool OrderRouter::request_reconciliation() {
    // Counted rather than fixed. The id comes back on the StatusComplete, so a
    // late answer to an earlier request cannot be mistaken for this one's --
    // which matters precisely because reconciliation runs when things have
    // already gone wrong.
    ++reconcile_request_id_;
    auto n = wire::encode_reconcile_request(scratch_, sizeof scratch_, reconcile_request_id_);
    if (!n || orders_.push(scratch_, *n)) {
        return false;
    }
    engine_view_.clear();
    divergence_ = Divergence{};
    divergence_.gateway_had = store_.live_count();
    reconcile_state_ = ReconcileState::Awaiting;
    return true;
}

void OrderRouter::finish_reconciliation() {
    reconcile_state_ = ReconcileState::Done;
    divergence_.complete = true;

    std::vector<bool> matched(engine_view_.size(), false);
    for (const auto& mine : store_.live()) {
        DivergenceEntry e;
        e.client_order_id = mine.client_order_id;
        e.exchange_order_id = mine.exchange_order_id;
        e.gateway_leaves = mine.leaves_quantity;
        e.symbol_id = mine.symbol_id;

        // Matched by the exchange's id. An order the gateway recorded but never
        // saw acknowledged has none, and cannot match anything -- which is the
        // correct answer, not a limitation: the engine never told us it existed.
        std::size_t hit = engine_view_.size();
        if (mine.exchange_order_id != 0) {
            for (std::size_t i = 0; i < engine_view_.size(); ++i) {
                if (!matched[i] && engine_view_[i].exchange_order_id == mine.exchange_order_id) {
                    hit = i;
                    break;
                }
            }
        }
        if (hit == engine_view_.size()) {
            e.kind = DivergenceKind::GatewayOnly;
        } else {
            matched[hit] = true;
            e.engine_leaves = engine_view_[hit].engine_leaves;
            e.kind = e.engine_leaves == e.gateway_leaves ? DivergenceKind::Agree
                                                         : DivergenceKind::LeavesDiffer;
        }
        divergence_.entries.push_back(e);
    }
    for (std::size_t i = 0; i < engine_view_.size(); ++i) {
        if (!matched[i]) {
            DivergenceEntry e = engine_view_[i];
            e.kind = DivergenceKind::EngineOnly;
            divergence_.entries.push_back(e);
        }
    }
}

void OrderRouter::write_divergence(std::ostream& out) const {
    out << "reconciliation_complete=" << (divergence_.complete ? 1 : 0) << "\n"
        << "gateway_orders=" << divergence_.gateway_had << "\n"
        << "engine_orders=" << divergence_.engine_named << "\n"
        << "agree=" << divergence_.count(DivergenceKind::Agree) << "\n"
        << "leaves_differ=" << divergence_.count(DivergenceKind::LeavesDiffer) << "\n"
        << "gateway_only=" << divergence_.count(DivergenceKind::GatewayOnly) << "\n"
        << "engine_only=" << divergence_.count(DivergenceKind::EngineOnly) << "\n";
    for (const auto& e : divergence_.entries) {
        if (e.kind == DivergenceKind::Agree) {
            continue;
        }
        // Every difference is named. A summary count with no detail is a report
        // nobody can act on.
        out << "divergence " << describe(e.kind) << " clOrdID=" << e.client_order_id
            << " orderID=" << e.exchange_order_id << " symbol=" << e.symbol_id
            << " gatewayLeaves=" << e.gateway_leaves << " engineLeaves=" << e.engine_leaves
            << "\n";
    }
}

}  // namespace fix
