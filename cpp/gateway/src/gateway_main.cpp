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
#include <string>

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
              << "                   [--send-orders N] [--run-seconds N] [--reset-seq]\n";
    return 2;
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
        while (!stop_requested && !session.finished() && fix::Clock::now() < deadline) {
            if (!conn.read_some(50)) {
                std::cerr << "gateway: peer closed\n";
                break;
            }
            now = fix::Clock::now();
            if (!conn.drain([&](const fix::Message& m) { session.on_message(m, sink, now); })) {
                std::cerr << "gateway: the stream stopped being FIX\n";
                break;
            }
            session.on_timer(sink, fix::Clock::now());

            if (orders_sent < o.send_orders && session.state() == fix::SessionState::Active) {
                int n = ++orders_sent;
                session.send_application(
                    fix::msg_type::NewOrderSingle,
                    [n](fix::Builder& b) {
                        b.add(11, "ORD" + std::to_string(n));  // ClOrdID
                        b.add(55, "ACME");                      // Symbol
                        b.add(54, "1");                         // Side=Buy
                        b.add(38, std::int64_t{100});           // OrderQty
                        b.add(40, "2");                         // OrdType=Limit
                        b.add(44, "10.00");                     // Price
                    },
                    sink, fix::Clock::now());
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
    if (session.error() != fix::SessionError::None) {
        std::cerr << "gateway: ended badly: " << fix::describe(session.error()) << "\n";
        return 1;
    }
    return 0;
}
