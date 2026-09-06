// The gateway's end of the order path: FIX on one side, rings on the other.
//
// # The ordering rule, in one sentence
//
// Nothing leaves for the ring until the record describing it is `fsync`'d.
// Everything else here follows from that.
//
// # Reconciliation, and why it only reports
//
// After a restart the gateway's log says what it believed; the engine says what
// is actually resting. They can disagree in four ways, and each means something
// different (see `Divergence`). None of them is repaired here.
//
// A gateway that quietly adopts the engine's view turns a bug into a shrug: the
// difference between "the order never arrived" and "the order filled while we
// were dead" is the difference between a client owing nothing and a client
// holding a position, and picking one silently is how a firm finds out about it
// from a statement rather than from its own software. The report is the
// deliverable.

#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "fix/message.hpp"
#include "fix/orderstore.hpp"
#include "fix/session.hpp"
#include "ring/spsc.hpp"
#include "wire/generated.hpp"

namespace fix {

/// A FIX `Symbol` string and the id the wire uses for it.
///
/// The wire carries a `uint16`; FIX carries a string. Something has to map
/// between them, and it is configuration rather than a convention, so that an
/// unrecognised symbol is a reject with a reason instead of an id nobody meant.
struct SymbolMapping {
    std::string name;
    std::uint16_t id{0};
};

/// One way the gateway's view and the engine's can differ.
enum class DivergenceKind {
    /// Both hold it and agree on what is left. Nothing to do.
    Agree,
    /// Both hold it, different leaves quantity. A fill happened that the
    /// gateway's log did not record.
    LeavesDiffer,
    /// The gateway thinks it is live; the engine does not hold it. Either it
    /// never arrived, or it was filled or cancelled while the gateway was dead.
    GatewayOnly,
    /// The engine holds it; the gateway has no record. **The serious one** --
    /// the gateway lost a record, and there is an order working that nothing on
    /// this side knows how to cancel.
    EngineOnly,
};

std::string_view describe(DivergenceKind k);

struct DivergenceEntry {
    DivergenceKind kind{DivergenceKind::Agree};
    std::uint64_t client_order_id{0};
    std::uint64_t exchange_order_id{0};
    std::uint32_t gateway_leaves{0};
    std::uint32_t engine_leaves{0};
    std::uint16_t symbol_id{0};
};

/// What the comparison found. Produced once, at startup, and reported.
struct Divergence {
    bool complete{false};
    std::uint64_t engine_named{0};
    std::uint64_t gateway_had{0};
    std::vector<DivergenceEntry> entries;

    [[nodiscard]] std::size_t count(DivergenceKind k) const;
    /// True when every order matched on both sides.
    [[nodiscard]] bool clean() const {
        return complete && count(DivergenceKind::Agree) == entries.size();
    }
};

/// Counters the end-to-end test asserts on.
struct RouterStats {
    std::uint64_t orders_sent = 0;
    std::uint64_t cancels_sent = 0;
    std::uint64_t rejected_locally = 0;
    std::uint64_t reports_received = 0;
    std::uint64_t exec_reports_sent = 0;
    std::uint64_t fills = 0;
    std::uint64_t filled_quantity = 0;
    /// Orders refused because the ring to risk was full. Reported to the client
    /// rather than retried: blocking here stalls the FIX session.
    std::uint64_t queue_full = 0;
    std::uint64_t reports_for_unknown_orders = 0;
};

/// Routes orders between a FIX session and the order-path rings.
class OrderRouter {
  public:
    struct Config {
        std::string orders_ring;
        std::string reports_ring;
        std::string store_path;
        std::vector<SymbolMapping> symbols;
    };

    /// Opens the rings and replays the log. The rings must already exist; the
    /// risk service creates them.
    std::optional<std::string> open(const Config& cfg);

    /// Handles one inbound application message. Returns false if it was not one
    /// this router deals with, which leaves it for whoever else wants it.
    bool on_application(const Message& m, Session& session, Sink& out, Clock::time_point now);

    /// Drains the reports ring, turning each report into a FIX
    /// `ExecutionReport`. Returns how many it handled.
    std::size_t pump(Session& session, Sink& out, Clock::time_point now);

    /// Asks the engine what it is holding. Called once, after a restart.
    bool request_reconciliation();

    [[nodiscard]] bool reconciliation_outstanding() const noexcept {
        return reconcile_state_ == ReconcileState::Awaiting;
    }
    [[nodiscard]] const Divergence& divergence() const noexcept { return divergence_; }
    [[nodiscard]] const RouterStats& stats() const noexcept { return stats_; }
    [[nodiscard]] const OrderStore& store() const noexcept { return store_; }
    [[nodiscard]] std::size_t live_orders() const noexcept { return store_.live_count(); }

    /// Writes the divergence report. Separate from producing it so the caller
    /// decides where it goes.
    void write_divergence(std::ostream& out) const;

  private:
    enum class ReconcileState { Idle, Awaiting, Done };

    [[nodiscard]] std::optional<std::uint16_t> symbol_id(std::string_view name) const;
    [[nodiscard]] std::string_view symbol_name(std::uint16_t id) const;
    void send_reject(const Message& m, std::string_view text, mdstack::wire::RejectReason reason,
                     Session& session, Sink& out, Clock::time_point now);
    void send_exec_report(const mdstack::wire::ExecReportDecoder& d, Session& session, Sink& out,
                          Clock::time_point now);
    void finish_reconciliation();

    mdstack::ring::Producer orders_;
    mdstack::ring::Consumer reports_;
    OrderStore store_;
    std::vector<SymbolMapping> symbols_;
    RouterStats stats_{};

    ReconcileState reconcile_state_{ReconcileState::Idle};
    std::uint64_t reconcile_request_id_{0};
    /// Status lines collected while a reconciliation is in flight. Startup only,
    /// so growing it is not on any path that claims to be allocation-free.
    std::vector<DivergenceEntry> engine_view_;
    Divergence divergence_{};

    std::uint64_t next_exec_id_{1};
    std::byte scratch_[256]{};
};

}  // namespace fix
