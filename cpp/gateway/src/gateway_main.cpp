// The FIX gateway binary.
//
//   fix-gateway --connect 127.0.0.1:5001 --store /tmp/gw.seq --send-orders 5
//   fix-gateway --listen 5001 --store /tmp/acc.seq
//
// Deliberately small. The session is a state machine and the transport is a
// socket; this wires the two together and runs a loop. Everything worth testing
// lives in the pieces, which is why they are tested without this.

#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "fix/orderpath.hpp"
#include "fix/session.hpp"
#include "fix/transport.hpp"

namespace {

volatile std::sig_atomic_t stop_requested = 0;

void on_signal(int) { stop_requested = 1; }

struct Options {
    bool listen{false};
    std::string host{"127.0.0.1"};
    std::uint16_t port{5001};
    std::string store_path{"/tmp/mdstack-gateway.seq"};
    std::string sender{"GATEWAY"};
    std::string target{"EXCHANGE"};
    int heartbeat{30};
    int send_orders{0};
    int run_seconds{30};
    bool reset_seq{false};

    // --- the order path, milestone 8 ---------------------------------------
    //
    // All optional. Without them this binary is exactly what milestone 7 built:
    // a FIX session on a socket. With them it also routes orders.
    std::string orders_ring;
    std::string reports_ring;
    std::string order_store;
    std::vector<fix::SymbolMapping> symbols;
    /// Ask the engine what it is holding, once, after logon.
    bool reconcile{false};
    /// Orders this end sends as a client, as SYMBOL:SIDE:QTY:PRICE.
    std::vector<std::string> client_orders;
};

bool parse_host_port(const std::string& s, std::string& host, std::uint16_t& port) {
    auto colon = s.rfind(':');
    if (colon == std::string::npos) {
        return false;
    }
    host = s.substr(0, colon);
    port = static_cast<std::uint16_t>(std::stoi(s.substr(colon + 1)));
    return true;
}

int usage() {
    std::cerr << "usage: fix-gateway [--connect host:port | --listen port] --store PATH\n"
              << "                   [--sender ID] [--target ID] [--heartbeat N]\n"
              << "                   [--send-orders N] [--run-seconds N] [--reset-seq]\n"
              << "  order path:      [--orders-ring PATH --reports-ring PATH\n"
              << "                    --order-store PATH --symbol NAME=ID ... [--reconcile]]\n"
              << "  as a client:     [--client-order SYMBOL:SIDE:QTY:PRICE ...]\n";
    return 2;
}

/// One `SYMBOL:SIDE:QTY:PRICE` order specification.
struct ClientOrder {
    std::string symbol;
    std::string side;  ///< FIX Side: 1 buy, 2 sell
    std::string qty;
    std::string price;
};

std::optional<ClientOrder> parse_client_order(const std::string& spec) {
    ClientOrder o;
    std::size_t start = 0;
    std::string* fields[] = {&o.symbol, &o.side, &o.qty, &o.price};
    for (int i = 0; i < 4; ++i) {
        const std::size_t colon = spec.find(':', start);
        const bool last = i == 3;
        if ((colon == std::string::npos) != last) {
            return std::nullopt;
        }
        *fields[i] = spec.substr(start, last ? std::string::npos : colon - start);
        start = last ? start : colon + 1;
    }
    return o;
}

}  // namespace

int main(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> std::string { return (i + 1 < argc) ? argv[++i] : std::string{}; };
        if (a == "--connect") {
            if (!parse_host_port(next(), o.host, o.port)) return usage();
            o.listen = false;
        } else if (a == "--listen") {
            o.port = static_cast<std::uint16_t>(std::stoi(next()));
            o.listen = true;
        } else if (a == "--store") {
            o.store_path = next();
        } else if (a == "--sender") {
            o.sender = next();
        } else if (a == "--target") {
            o.target = next();
        } else if (a == "--heartbeat") {
            o.heartbeat = std::stoi(next());
        } else if (a == "--send-orders") {
            o.send_orders = std::stoi(next());
        } else if (a == "--run-seconds") {
            o.run_seconds = std::stoi(next());
        } else if (a == "--reset-seq") {
            o.reset_seq = true;
        } else if (a == "--orders-ring") {
            o.orders_ring = next();
        } else if (a == "--reports-ring") {
            o.reports_ring = next();
        } else if (a == "--order-store") {
            o.order_store = next();
        } else if (a == "--symbol") {
            // NAME=ID. The wire carries a uint16 and FIX carries a string, so
            // something has to map between them; making it configuration rather
            // than a convention is what turns an unrecognised symbol into a
            // reject with a reason instead of an id nobody meant.
            const std::string spec = next();
            const auto eq = spec.find('=');
            if (eq == std::string::npos) {
                std::cerr << "gateway: --symbol takes NAME=ID\n";
                return usage();
            }
            o.symbols.push_back(fix::SymbolMapping{
                spec.substr(0, eq),
                static_cast<std::uint16_t>(std::stoi(spec.substr(eq + 1)))});
        } else if (a == "--reconcile") {
            o.reconcile = true;
        } else if (a == "--client-order") {
            o.client_orders.push_back(next());
        } else {
            return usage();
        }
    }

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    // SIGKILL is deliberately not handled. scripts/kill-restart-test.sh uses it
    // precisely because it cannot be: no destructors, no flush, no chance to
    // tidy up. That is the point of the test.

    fix::SeqStore store;
    if (auto err = store.open(o.store_path)) {
        std::cerr << "gateway: " << fix::describe(*err) << "\n";
        return 1;
    }
    std::cerr << "gateway: sequences resume at outbound " << store.current().outbound
              << ", inbound " << store.current().inbound;
    if (store.torn_slots_recovered() > 0) {
        std::cerr << " (recovered from " << store.torn_slots_recovered()
                  << " torn slot(s), so a previous run was killed mid-write)";
    }
    std::cerr << "\n";

    // The reset is not done here. It goes through the session, so that
    // `ResetSeqNumFlag=Y` reaches the counterparty on the logon — clearing the
    // local store without telling the other end is not a reset, it is a
    // unilateral sequence reversal waiting to be reported.
    auto now = fix::Clock::now();
    const auto deadline = now + std::chrono::seconds(o.run_seconds);

    fix::SessionConfig cfg;
    cfg.sender_comp_id = o.sender;
    cfg.target_comp_id = o.target;
    cfg.heartbeat_interval_seconds = o.heartbeat;
    cfg.initiator = !o.listen;

    // An acceptor goes back to listening when its peer disappears; an initiator
    // does not retry. That asymmetry is what lets the kill-restart test kill one
    // end and leave the other standing, which is the only way the two sides ever
    // genuinely disagree about what has been sent -- and disagreeing is exactly
    // what the resend machinery exists for.
    fix::Session session(cfg, store);
    bool ran = false;
    bool first_connection = true;

    // The order path, if this run is wired for one. Opened before the socket:
    // a gateway that cannot reach its rings should say so and stop, rather than
    // log a client on and then discover it has nowhere to send their orders.
    std::optional<fix::OrderRouter> router;
    if (!o.orders_ring.empty()) {
        fix::OrderRouter::Config rc;
        rc.orders_ring = o.orders_ring;
        rc.reports_ring = o.reports_ring;
        rc.store_path = o.order_store;
        rc.symbols = o.symbols;
        router.emplace();
        if (auto err = router->open(rc)) {
            std::cerr << "gateway: " << *err
                      << "\n  the risk service creates the rings; start it first\n";
            return 1;
        }
        std::cerr << "gateway: order path attached, " << router->live_orders()
                  << " order(s) recovered from " << o.order_store;
        if (router->store().torn_tail_bytes() > 0) {
            std::cerr << " (discarded " << router->store().torn_tail_bytes()
                      << " torn tail byte(s), so the previous run was killed mid-write)";
        }
        std::cerr << "\n";
    }

    // Parsed once, before the loop, so a malformed spec fails at start-up
    // rather than in the middle of a session.
    std::vector<ClientOrder> to_send;
    for (const auto& spec : o.client_orders) {
        auto parsed = parse_client_order(spec);
        if (!parsed) {
            std::cerr << "gateway: --client-order takes SYMBOL:SIDE:QTY:PRICE, got " << spec
                      << "\n";
            return 2;
        }
        to_send.push_back(*parsed);
    }
    /// Execution reports this end received as a client, by ExecType.
    std::map<std::string, int> exec_reports_in;

    while (!stop_requested && fix::Clock::now() < deadline) {
        fix::Connection conn = o.listen ? fix::accept_one(o.port, 3000)
                                        : fix::connect_to(o.host, o.port);
        if (!conn.open()) {
            if (o.listen) {
                continue;  // nobody yet; keep listening until the deadline
            }
            std::cerr << "gateway: no connection\n";
            return ran ? 0 : 1;
        }
        ran = true;

        // A fresh session per connection, over the same durable store. The
        // sequence numbers survive; the in-memory resend ring does not, which is
        // legal -- anything no longer held is gap-filled rather than dropped.
        session.reset_for_new_connection();
        fix::Sink sink = [&conn](std::string_view bytes) {
            if (!conn.write_all(bytes)) {
                std::cerr << "gateway: write failed\n";
            }
        };

        // Only the first connection carries the reset; a reconnect must
        // resume from the persisted numbers, not start over.
        session.connect(sink, fix::Clock::now(), o.reset_seq && first_connection);
        first_connection = false;
        int orders_sent = 0;
        std::size_t client_orders_sent = 0;
        bool reconciliation_requested = false;

        // What to do with an application message that survived every session
        // check. A gateway with a router routes it; a client counts the
        // execution reports coming back, which is what the end-to-end test
        // asserts on.
        auto on_app = [&](const fix::Message& m) {
            if (router && router->on_application(m, session, sink, fix::Clock::now())) {
                return;
            }
            if (m.msg_type() == fix::msg_type::ExecutionReport) {
                const auto et = m.find(fix::tag::ExecType).value_or("?");
                ++exec_reports_in[std::string(et)];
            }
        };

        while (!stop_requested && !session.finished() && fix::Clock::now() < deadline) {
            if (!conn.read_some(50)) {
                std::cerr << "gateway: peer closed\n";
                break;
            }
            now = fix::Clock::now();
            if (!conn.drain([&](const fix::Message& m) {
                    session.on_message(m, sink, now, on_app);
                })) {
                std::cerr << "gateway: the stream stopped being FIX\n";
                break;
            }
            session.on_timer(sink, fix::Clock::now());

            if (router) {
                // Once, after the session is up. Before that there would be
                // nowhere to send the answer's consequences.
                if (o.reconcile && !reconciliation_requested &&
                    session.state() == fix::SessionState::Active) {
                    reconciliation_requested = router->request_reconciliation();
                }
                router->pump(session, sink, fix::Clock::now());
            }

            if (session.state() == fix::SessionState::Active) {
                if (client_orders_sent < to_send.size()) {
                    const auto& co = to_send[client_orders_sent];
                    // The ClOrdID is a number, because this gateway's order log
                    // keys on one -- see OrderRouter::on_application.
                    const std::int64_t id = 1000 + static_cast<std::int64_t>(client_orders_sent);
                    ++client_orders_sent;
                    session.send_application(
                        fix::msg_type::NewOrderSingle,
                        [&co, id](fix::Builder& b) {
                            b.add(fix::tag::ClOrdID, id);
                            b.add(fix::tag::Symbol, co.symbol);
                            b.add(fix::tag::Side, co.side);
                            b.add(fix::tag::OrderQty, std::stoll(co.qty));
                            b.add(fix::tag::OrdType, "2");  // Limit
                            b.add(fix::tag::Price, co.price);
                        },
                        sink, fix::Clock::now());
                } else if (orders_sent < o.send_orders) {
                    int n = ++orders_sent;
                    session.send_application(
                        fix::msg_type::NewOrderSingle,
                        [n](fix::Builder& b) {
                            b.add(fix::tag::ClOrdID, static_cast<std::int64_t>(n));
                            b.add(fix::tag::Symbol, "ACME");
                            b.add(fix::tag::Side, "1");
                            b.add(fix::tag::OrderQty, std::int64_t{100});
                            b.add(fix::tag::OrdType, "2");
                            b.add(fix::tag::Price, "10.00");
                        },
                        sink, fix::Clock::now());
                }
            }
        }
        if (!o.listen) {
            break;
        }
    }

    const auto& s = session.stats();
    std::cerr << "gateway: state " << fix::describe(session.state()) << ", sent " << s.sent
              << ", received " << s.received << ", resent " << s.resent << ", gap-filled "
              << s.gap_filled << ", resend requests sent " << s.resend_requests_sent
              << " served " << s.resend_requests_served << "\n";
    std::cerr << "gateway: sequences end at outbound " << store.current().outbound << ", inbound "
              << store.current().inbound << ", " << store.syncs() << " fsyncs\n";

    // key=value on stdout, which is the shape every script in this repo asserts
    // on. The human-readable lines go to stderr and stay there.
    if (router) {
        const auto& rs = router->stats();
        std::cout << "orders_sent=" << rs.orders_sent << "\n"
                  << "cancels_sent=" << rs.cancels_sent << "\n"
                  << "rejected_locally=" << rs.rejected_locally << "\n"
                  << "reports_received=" << rs.reports_received << "\n"
                  << "exec_reports_sent=" << rs.exec_reports_sent << "\n"
                  << "fills=" << rs.fills << "\n"
                  << "filled_quantity=" << rs.filled_quantity << "\n"
                  << "queue_full=" << rs.queue_full << "\n"
                  << "reports_for_unknown_orders=" << rs.reports_for_unknown_orders << "\n"
                  << "live_orders=" << router->live_orders() << "\n"
                  << "order_log_records=" << router->store().records_written() << "\n"
                  << "order_log_syncs=" << router->store().syncs() << "\n"
                  << "order_log_torn_tail_bytes=" << router->store().torn_tail_bytes() << "\n";
        if (o.reconcile) {
            router->write_divergence(std::cout);
            // The report is the deliverable, so it also goes where a human will
            // see it without going looking.
            if (!router->divergence().complete) {
                std::cerr << "gateway: RECONCILIATION DID NOT COMPLETE. The engine's answer "
                             "never arrived, so this gateway's view of what is working is "
                             "unverified.\n";
            } else if (!router->divergence().clean()) {
                std::cerr << "gateway: RECONCILIATION FOUND DIVERGENCE. Nothing has been "
                             "repaired; the differences are listed on stdout.\n";
            }
        }
    }
    if (!exec_reports_in.empty()) {
        for (const auto& kv : exec_reports_in) {
            std::cout << "exec_report_" << kv.first << "=" << kv.second << "\n";
        }
    }
    std::cout << std::flush;

    if (session.error() != fix::SessionError::None) {
        std::cerr << "gateway: ended badly: " << fix::describe(session.error()) << "\n";
        return 1;
    }
    return 0;
}
