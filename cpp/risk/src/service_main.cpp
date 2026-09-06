// The risk service: the process in the middle of the order path.
//
//   risk-service --orders /tmp/o.ring --accepted /tmp/a.ring
//                --execs /tmp/e.ring --reports /tmp/r.ring
//                --symbol 7 --max-order-qty 1000 --max-position 100000
//                --run-seconds 30
//
// # It creates all four rings
//
// One process has to own their geometry, and it is the one that touches all
// four. A second creator would zero the indices under whoever was already
// attached -- silently, because a fresh ring looks exactly like an empty one.
// So: start the risk service first, then the gateway and the engine, both of
// which only ever open. `docs/ORDER-PATH.md` says the same thing.
//
// # It is in both directions
//
// Orders go gateway -> risk -> engine and reports come back engine -> risk ->
// gateway. It would be shorter to let the engine report straight to the
// gateway, but a position limit is a statement about fills, so risk has to see
// every fill anyway. Routing the reports through it means the position is
// updated by the same thread that enforces the limit, in order, with no
// synchronisation between them -- rather than there being a window where the
// limit is enforced against a position that has not finished updating.
//
// # What it does when a ring is full
//
// Rejects, and says which ring. It cannot block: blocking on the outbound ring
// would stop it draining the inbound one, and the gateway would see the order
// ring back up rather than get an answer. It cannot drop silently: an order
// that vanishes between the gateway and the engine is the worst outcome on this
// path, because nothing will ever notice on its own.

#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "ring/spsc.hpp"
#include "risk/counting_alloc.hpp"
#include "risk/limits.hpp"
#include "wire/generated.hpp"

namespace {

volatile std::sig_atomic_t stop_requested = 0;

void on_signal(int) { stop_requested = 1; }

using mdstack::ring::Consumer;
using mdstack::ring::Producer;
using mdstack::ring::Ring;
using mdstack::wire::ExecType;
using mdstack::wire::RejectReason;
using mdstack::wire::Side;

/// How many messages to take from one ring before looking at the other.
///
/// Bounded so that a burst on either side cannot starve the other. An unbounded
/// drain of the order ring would stop reports flowing back for as long as the
/// burst lasted, which is exactly when a client most wants them.
constexpr std::size_t kPerPass = 64;

struct Options {
    std::string orders;
    std::string accepted;
    std::string execs;
    std::string reports;
    std::vector<std::uint16_t> symbols;
    std::uint32_t max_order_qty = 10'000;
    std::int64_t max_notional = 100'000'000'000LL;
    std::int64_t max_position = 1'000'000;
    std::int64_t collar_bps = 0;  // 0 disables it; there is no sensible default
    std::int64_t reference_price = 0;
    std::size_t max_open_orders = 4096;
    std::uint32_t capacity = 4096;
    std::uint32_t slot_size = 128;
    int run_seconds = 30;
    bool no_create = false;
};

int usage() {
    std::cerr
        << "usage: risk-service --orders P --accepted P --execs P --reports P\n"
        << "                    [--symbol N]... [--max-order-qty N] [--max-notional N]\n"
        << "                    [--max-position N] [--collar-bps N] [--reference-price N]\n"
        << "                    [--max-open-orders N] [--capacity N] [--slot-size N]\n"
        << "                    [--run-seconds N] [--no-create]\n";
    return 2;
}

struct Counters {
    std::uint64_t orders_in = 0;
    std::uint64_t cancels_in = 0;
    std::uint64_t reconciles_in = 0;
    std::uint64_t forwarded = 0;
    std::uint64_t rejected = 0;
    std::uint64_t reports_in = 0;
    std::uint64_t reports_out = 0;
    /// Orders refused because the ring to the engine was full. A real condition
    /// with a real answer (a bigger ring, or a faster engine), and one the
    /// client is told about rather than left to wonder at.
    std::uint64_t queue_full = 0;
    /// Reports that could not be handed back to the gateway. **The bad one.**
    /// The gateway's view of those orders is stale until it reconciles.
    std::uint64_t reports_dropped = 0;
    std::uint64_t undecodable = 0;
};

Counters counters;
std::byte scratch[256];

/// Writes an ExecReport into `out`, counting a failure rather than hiding it.
void emit(Producer& out, std::uint64_t client_order_id, std::uint64_t exchange_order_id,
          std::int64_t price, std::uint32_t quantity, std::uint32_t leaves,
          std::uint16_t symbol_id, Side side, ExecType type, RejectReason reason) {
    auto n = mdstack::wire::encode_exec_report(scratch, sizeof scratch, client_order_id,
                                              exchange_order_id, 0, price, quantity, leaves,
                                              symbol_id, side, type, reason);
    if (!n) {
        ++counters.reports_dropped;
        return;
    }
    if (out.push(scratch, *n)) {
        ++counters.reports_dropped;
        return;
    }
    ++counters.reports_out;
}

/// Forwards a message verbatim. Re-encoding would be the same bytes on a good
/// day and a place for a transcription mistake on a bad one.
bool forward(Producer& out, const std::byte* p, std::size_t n) {
    return !out.push(p, n).has_value();
}

void handle_order_side(Consumer& orders, Producer& accepted, Producer& reports,
                       risk::RiskEngine& engine) {
    orders.drain(kPerPass, [&](const std::byte* p, std::size_t n) {
        auto hdr = mdstack::wire::MessageHeaderDecoder::wrap(p, n);
        if (!hdr) {
            ++counters.undecodable;
            return;
        }
        switch (hdr->template_id()) {
            case mdstack::wire::tmpl::kNewOrder: {
                auto d = mdstack::wire::NewOrderDecoder::wrap(p, n);
                if (!d) {
                    ++counters.undecodable;
                    return;
                }
                ++counters.orders_in;
                const auto side_opt = d->side();
                const Side side = side_opt ? *side_opt : Side::kBid;
                const auto decision = engine.on_new_order(d->client_order_id(), d->symbol_id(),
                                                          side, d->price(), d->quantity());
                if (!decision.accepted) {
                    // The rejected order never reaches the engine. That is the
                    // whole point of the limit, and the end-to-end test asserts
                    // it by looking at the engine's own counters.
                    ++counters.rejected;
                    emit(reports, d->client_order_id(), 0, d->price(), d->quantity(), 0,
                         d->symbol_id(), side, ExecType::kRejected, decision.reason);
                    return;
                }
                if (!forward(accepted, p, n)) {
                    // The engine's ring is full. The order has already been
                    // recorded as live by the limit check above, so it has to be
                    // taken back off before the client is told it was refused --
                    // otherwise the exposure sits there forever.
                    engine.on_exec_report(d->client_order_id(), 0, ExecType::kRejected, 0, 0, 0);
                    ++counters.queue_full;
                    ++counters.rejected;
                    emit(reports, d->client_order_id(), 0, d->price(), d->quantity(), 0,
                         d->symbol_id(), side, ExecType::kRejected, RejectReason::kQueueFull);
                    return;
                }
                ++counters.forwarded;
                return;
            }
            case mdstack::wire::tmpl::kCancelOrder: {
                auto d = mdstack::wire::CancelOrderDecoder::wrap(p, n);
                if (!d) {
                    ++counters.undecodable;
                    return;
                }
                ++counters.cancels_in;
                const auto side_opt = d->side();
                const Side side = side_opt ? *side_opt : Side::kBid;
                const auto decision =
                    engine.on_cancel(d->client_order_id(), d->orig_client_order_id());
                if (!decision.accepted || !forward(accepted, p, n)) {
                    ++counters.rejected;
                    const RejectReason reason =
                        decision.accepted ? RejectReason::kQueueFull : decision.reason;
                    if (decision.accepted) {
                        ++counters.queue_full;
                    }
                    emit(reports, d->client_order_id(), d->orig_client_order_id(), 0, 0, 0,
                         d->symbol_id(), side, ExecType::kRejected, reason);
                    return;
                }
                ++counters.forwarded;
                return;
            }
            case mdstack::wire::tmpl::kReconcileRequest: {
                ++counters.reconciles_in;
                // Passed straight through. The engine holds the only true
                // answer to "what is actually resting"; this service's view is
                // derived from reports it may have missed, which is the very
                // thing reconciliation exists to detect.
                if (!forward(accepted, p, n)) {
                    ++counters.queue_full;
                }
                return;
            }
            default:
                ++counters.undecodable;
                return;
        }
    });
}

void handle_report_side(Consumer& execs, Producer& reports, risk::RiskEngine& engine) {
    execs.drain(kPerPass, [&](const std::byte* p, std::size_t n) {
        auto d = mdstack::wire::ExecReportDecoder::wrap(p, n);
        if (!d) {
            ++counters.undecodable;
            return;
        }
        ++counters.reports_in;
        const auto type_opt = d->exec_type();
        if (type_opt) {
            const ExecType type = *type_opt;
            // Reconciliation answers are not events in this service's own state.
            // A status line names an order by the *engine's* id and carries no
            // client id, so feeding it to the limit engine would only produce
            // unknown-order noise.
            const bool is_reconciliation =
                type == ExecType::kOrderStatus || type == ExecType::kStatusComplete;
            if (!is_reconciliation) {
                engine.on_exec_report(d->client_order_id(), d->exchange_order_id(), type,
                                      d->price(), d->quantity(), d->leaves_quantity());
            }
        }
        if (!forward(reports, p, n)) {
            ++counters.reports_dropped;
            return;
        }
        ++counters.reports_out;
    });
}

}  // namespace

int main(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&]() -> std::string { return (i + 1 < argc) ? argv[++i] : std::string{}; };
        if (a == "--orders") {
            o.orders = next();
        } else if (a == "--accepted") {
            o.accepted = next();
        } else if (a == "--execs") {
            o.execs = next();
        } else if (a == "--reports") {
            o.reports = next();
        } else if (a == "--symbol") {
            o.symbols.push_back(static_cast<std::uint16_t>(std::atoll(next().c_str())));
        } else if (a == "--max-order-qty") {
            o.max_order_qty = static_cast<std::uint32_t>(std::atoll(next().c_str()));
        } else if (a == "--max-notional") {
            o.max_notional = std::atoll(next().c_str());
        } else if (a == "--max-position") {
            o.max_position = std::atoll(next().c_str());
        } else if (a == "--collar-bps") {
            o.collar_bps = std::atoll(next().c_str());
        } else if (a == "--reference-price") {
            o.reference_price = std::atoll(next().c_str());
        } else if (a == "--max-open-orders") {
            o.max_open_orders = static_cast<std::size_t>(std::atoll(next().c_str()));
        } else if (a == "--capacity") {
            o.capacity = static_cast<std::uint32_t>(std::atoll(next().c_str()));
        } else if (a == "--slot-size") {
            o.slot_size = static_cast<std::uint32_t>(std::atoll(next().c_str()));
        } else if (a == "--run-seconds") {
            o.run_seconds = std::atoi(next().c_str());
        } else if (a == "--no-create") {
            o.no_create = true;
        } else {
            return usage();
        }
    }
    if (o.orders.empty() || o.accepted.empty() || o.execs.empty() || o.reports.empty()) {
        return usage();
    }
    if (o.symbols.empty()) {
        std::cerr << "risk-service: at least one --symbol is required. There is no default "
                     "symbol, because an order naming an unconfigured one must be rejected "
                     "rather than waved through.\n";
        return 2;
    }

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    risk::RiskConfig cfg;
    std::uint16_t highest = 0;
    for (auto s : o.symbols) {
        highest = std::max(highest, s);
    }
    cfg.symbols.assign(static_cast<std::size_t>(highest) + 1, risk::SymbolLimits{});
    for (auto s : o.symbols) {
        cfg.symbols[s] = risk::SymbolLimits{true,           o.max_order_qty, o.max_notional,
                                            o.max_position, o.collar_bps,    o.reference_price};
    }
    cfg.max_open_orders = o.max_open_orders;
    risk::RiskEngine engine(std::move(cfg));

    Consumer orders;
    Producer accepted;
    Consumer execs;
    Producer reports;

    if (!o.no_create) {
        // All four, here, before anybody else attaches.
        Ring seed;
        for (const auto* path : {&o.orders, &o.accepted, &o.execs, &o.reports}) {
            if (auto e = seed.create(*path, o.capacity, o.slot_size)) {
                std::cerr << "risk-service: creating " << *path << ": "
                          << mdstack::ring::describe(*e) << "\n";
                return 1;
            }
        }
    }
    struct Attach {
        const char* what;
        std::string* path;
        mdstack::ring::Ring* ring;
    };
    const Attach attach[] = {
        {"orders", &o.orders, &orders},
        {"accepted", &o.accepted, &accepted},
        {"execs", &o.execs, &execs},
        {"reports", &o.reports, &reports},
    };
    for (const auto& a : attach) {
        if (auto e = a.ring->open(*a.path)) {
            std::cerr << "risk-service: opening the " << a.what << " ring at " << *a.path << ": "
                      << mdstack::ring::describe(*e) << "\n";
            return 1;
        }
    }

    std::cout << "ready=1\n" << std::flush;
    std::cerr << "risk-service: " << o.symbols.size() << " symbol(s), max order qty "
              << o.max_order_qty << ", max position " << o.max_position << ", collar "
              << o.collar_bps << "bps, " << o.capacity << " slots of " << o.slot_size
              << " bytes per ring\n";

    // Everything above this line may allocate. Nothing below it does, and the
    // count printed at exit is what says so.
    const risk::AllocCounts at_steady_state = risk::alloc_counts();

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(o.run_seconds);
    std::uint64_t idle_passes = 0;
    while (!stop_requested && std::chrono::steady_clock::now() < deadline) {
        const std::uint64_t before = counters.orders_in + counters.cancels_in +
                                     counters.reports_in + counters.reconciles_in;
        handle_order_side(orders, accepted, reports, engine);
        handle_report_side(execs, reports, engine);
        const std::uint64_t after = counters.orders_in + counters.cancels_in +
                                    counters.reports_in + counters.reconciles_in;
        if (after == before) {
            // Nothing on either ring. A spin here would burn a core for no
            // reason; this is not the latency-critical part of the system and
            // does not pretend to be.
            ++idle_passes;
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    }

    const risk::AllocCounts steady = at_steady_state.delta(risk::alloc_counts());

    std::cout << "orders_in=" << counters.orders_in << "\n"
              << "cancels_in=" << counters.cancels_in << "\n"
              << "reconciles_in=" << counters.reconciles_in << "\n"
              << "forwarded=" << counters.forwarded << "\n"
              << "rejected=" << counters.rejected << "\n"
              << "queue_full=" << counters.queue_full << "\n"
              << "reports_in=" << counters.reports_in << "\n"
              << "reports_out=" << counters.reports_out << "\n"
              << "reports_dropped=" << counters.reports_dropped << "\n"
              << "undecodable=" << counters.undecodable << "\n"
              << "open_orders=" << engine.open_orders() << "\n"
              << "accepted_total=" << engine.stats().accepted << "\n"
              << "rejected_total=" << engine.stats().rejected << "\n"
              << "fills_applied=" << engine.stats().fills_applied << "\n"
              << "unknown_order_reports=" << engine.stats().unknown_order_reports << "\n"
              << "idle_passes=" << idle_passes << "\n"
              // The claim, reported by the service itself rather than only by a
              // test. If this is ever non-zero in a real run, the allocation-free
              // path has acquired an allocation that the test did not reach.
              << "steady_state_allocations=" << steady.allocations << "\n"
              << "steady_state_deallocations=" << steady.deallocations << "\n"
              << std::flush;

    if (counters.reports_dropped > 0) {
        std::cerr << "risk-service: " << counters.reports_dropped
                  << " EXECUTION REPORTS WERE DROPPED. The gateway's view of those orders is "
                     "stale until it reconciles.\n";
    }
    return 0;
}
