// The FIX 4.4 session layer.
//
// # Shape
//
// A pure state machine. It takes inbound messages and the clock, and emits
// outbound messages through a sink. It owns no socket, no thread and no timer.
//
// That is deliberate and it is what makes the conformance suite possible. The
// awkward cases this layer exists to handle — a resend request arriving during a
// resend, a sequence reset that moves backwards, a message with `PossDupFlag`
// and a number higher than expected — are all trivial to construct as a sequence
// of calls and nearly impossible to provoke reliably through a socket. A session
// that could only be tested by talking to something is a session whose hard
// paths are tested by hope.
//
// # What it decides, and what it refuses
//
// The full list, with the reasoning for each ambiguity, is in
// `docs/PROTOCOL.md`. That document was written before this code and is the
// contract; this file implements it and points at it rather than restating it.
//
// The two that matter most, because they are where sessions silently diverge:
//
// * A message with a sequence number **lower** than expected and no
//   `PossDupFlag` is fatal. Logout and disconnect. There is no safe recovery —
//   the counterparty's notion of what it has sent disagrees with ours, and
//   continuing means choosing one arbitrarily.
// * A `SequenceReset` with `GapFillFlag=N` that moves the sequence **backwards**
//   is refused. The spec permits it in principle; accepting it silently is how
//   two sides end up disagreeing about what has been sent with no way to notice.
//
// # Resending
//
// Sent messages are kept in a bounded ring. A resend request for something still
// held is answered with the original, flagged `PossDupFlag=Y` and carrying
// `OrigSendingTime`. Anything no longer held — and every administrative message,
// which is never resent — is **gap-filled**: a `SequenceReset` with
// `GapFillFlag=Y` covering exactly that range.
//
// A counterparty must never see a sequence number simply vanish. Gap fill is how
// a range is declined without lying about it.

#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "fix/message.hpp"
#include "fix/seqstore.hpp"

namespace fix {

using Clock = std::chrono::steady_clock;

/// Where outbound bytes go. The session never writes to a socket itself.
using Sink = std::function<void(std::string_view)>;

struct SessionConfig {
    std::string sender_comp_id{"GATEWAY"};
    std::string target_comp_id{"EXCHANGE"};
    /// Negotiated at logon. The initiator proposes; the acceptor echoes.
    int heartbeat_interval_seconds{30};
    /// How far past the interval a quiet counterparty is prodded with a
    /// `TestRequest`, and then how far past that it is disconnected. The spec
    /// suggests 20%; the exact value is a policy, so it is named rather than
    /// buried.
    double heartbeat_tolerance{0.2};
    /// Outbound messages kept for resend. Beyond this a request is gap-filled
    /// rather than answered, which is legal and is what the ring bounds.
    std::size_t resend_ring{4096};
    /// Whether this end initiates the logon.
    bool initiator{true};
};

enum class SessionState {
    Disconnected,
    /// Logon sent, waiting for the counterparty's.
    AwaitingLogon,
    /// Both sides have logged on. Normal traffic flows.
    Active,
    /// A gap was detected inbound and a `ResendRequest` is outstanding.
    AwaitingResend,
    /// Logout sent, waiting for the confirming logout.
    AwaitingLogout,
    /// Finished. The socket should be closed.
    LoggedOut,
};

std::string_view describe(SessionState s);

/// Why the session ended, when it ended badly.
enum class SessionError {
    None,
    /// A sequence number lower than expected, with no `PossDupFlag`.
    SequenceReversal,
    /// A `SequenceReset` that would move the sequence backwards.
    BackwardsSequenceReset,
    /// The counterparty stopped answering test requests.
    Unresponsive,
    /// Sequence state could not be persisted, so nothing may be sent.
    StoreFailure,
    /// The stream stopped being FIX.
    Corrupt,
};

std::string_view describe(SessionError e);

class Session {
  public:
    Session(SessionConfig cfg, SeqStore& store);

    /// Sends the logon. Initiator only.
    ///
    /// `reset_sequences` sets `ResetSeqNumFlag=Y`, which asks the counterparty
    /// to return both directions to 1 as well. Resetting the local store
    /// *without* it is not a reset at all — the other end keeps its numbers, and
    /// the next message it receives reads as a sequence reversal.
    void connect(Sink& out, Clock::time_point now, bool reset_sequences = false);

    /// Clears the per-connection state so the same session object can serve a
    /// reconnect.
    ///
    /// The durable store is deliberately untouched: sequence numbers survive a
    /// dropped connection, which is the entire point of persisting them. What
    /// does not survive is the in-memory resend ring, and that is legal --
    /// anything no longer held is gap-filled rather than dropped. Counters stay
    /// cumulative so a run's summary covers the whole run.
    void reset_for_new_connection();

    /// Feeds one decoded inbound message.
    void on_message(const Message& m, Sink& out, Clock::time_point now);

    /// `on_message`, plus delivery of application messages.
    ///
    /// The session layer's job ends once the sequence checks out; what an
    /// application message *means* is the caller's business. `on_app` is called
    /// only for messages that passed every session check, so the caller never
    /// sees one out of sequence, one from a session that is not up, or a
    /// duplicate the resend machinery already dealt with.
    void on_message(const Message& m, Sink& out, Clock::time_point now,
                    const std::function<void(const Message&)>& on_app);

    /// Drives heartbeats, test requests and the unresponsive-counterparty
    /// timeout. Call it regularly; it does nothing when nothing is due.
    void on_timer(Sink& out, Clock::time_point now);

    /// Sends an application message, assigning it the next sequence number and
    /// persisting that number before the bytes leave.
    ///
    /// `body` supplies the message-specific fields; the session writes the
    /// header and trailer. Returns false if the sequence could not be persisted,
    /// in which case **nothing was sent**.
    bool send_application(std::string_view msg_type,
                          const std::function<void(Builder&)>& body, Sink& out,
                          Clock::time_point now);

    /// Begins a graceful logout.
    void logout(std::string_view reason, Sink& out, Clock::time_point now);

    [[nodiscard]] SessionState state() const noexcept { return state_; }
    [[nodiscard]] SessionError error() const noexcept { return error_; }
    /// True once the socket should be closed.
    [[nodiscard]] bool finished() const noexcept { return state_ == SessionState::LoggedOut; }

    /// Counters the conformance suite asserts on.
    struct Stats {
        std::uint64_t sent{};
        std::uint64_t received{};
        std::uint64_t resent{};
        std::uint64_t gap_filled{};
        std::uint64_t resend_requests_sent{};
        std::uint64_t resend_requests_served{};
        std::uint64_t test_requests_sent{};
        std::uint64_t queued_during_resend{};
    };
    [[nodiscard]] const Stats& stats() const noexcept { return stats_; }

  private:
    struct Sent {
        std::uint64_t seq{};
        std::string bytes;
        std::string sending_time;
        bool administrative{};
    };

    // --- outbound ---------------------------------------------------------
    bool emit(std::string_view msg_type, const std::function<void(Builder&)>& body, Sink& out,
              Clock::time_point now, bool administrative);
    void send_heartbeat(Sink& out, Clock::time_point now, std::string_view test_req_id = {});
    void send_test_request(Sink& out, Clock::time_point now);
    void send_resend_request(std::uint64_t from, Sink& out, Clock::time_point now);
    void send_reject(const Message& m, std::string_view text, Sink& out, Clock::time_point now);
    void send_gap_fill(std::uint64_t from, std::uint64_t new_seq_no, Sink& out,
                       Clock::time_point now);

    // --- inbound ----------------------------------------------------------
    void handle_logon(const Message& m, Sink& out, Clock::time_point now);
    void handle_resend_request(const Message& m, Sink& out, Clock::time_point now);
    void handle_sequence_reset(const Message& m, Sink& out, Clock::time_point now);
    void handle_test_request(const Message& m, Sink& out, Clock::time_point now);
    void handle_logout(const Message& m, Sink& out, Clock::time_point now);

    /// Sequence checking, which is where the spec's teeth are.
    /// Returns false when the message must not be processed further.
    bool check_sequence(const Message& m, Sink& out, Clock::time_point now);

    void fail(SessionError e, std::string_view text, Sink& out, Clock::time_point now);
    void remember(std::uint64_t seq, std::string_view bytes, std::string_view sending_time,
                  bool administrative);
    [[nodiscard]] const Sent* recall(std::uint64_t seq) const;

    SessionConfig cfg_;
    SeqStore& store_;
    SessionState state_{SessionState::Disconnected};
    SessionError error_{SessionError::None};

    Clock::time_point last_sent_{};
    Clock::time_point last_received_{};
    bool test_request_outstanding_{false};
    std::uint64_t test_request_id_{};

    /// Set when *we* sent `ResetSeqNumFlag=Y`, cleared once the counterparty's
    /// Logon has been dealt with.
    ///
    /// The counterparty confirms a reset by echoing the flag back on its own
    /// Logon. That echo is an acknowledgement, not a second instruction: acting
    /// on it would clear a store we already cleared, and reissue the sequence
    /// number our own Logon has just spent. QuickFIX catches this immediately —
    /// "MsgSeqNum too low, expecting 2 but received 1" — and drops the session.
    bool reset_requested_{false};

    /// Live traffic that arrived while a resend was outstanding. Applied in
    /// order once the gap closes; see `AwaitingResend`.
    std::vector<std::string> queued_;

    std::vector<Sent> ring_;
    std::size_t ring_next_{};

    Stats stats_{};
};

}  // namespace fix
