// Session conformance: the resend / gap-fill / sequence-reset matrix.
//
// Driven as a state machine rather than over a socket. Every case here — a
// resend request for a range half of which is administrative, a sequence reset
// that moves backwards, a message with PossDupFlag and a number above the
// expected one — is trivial to construct as a sequence of calls and nearly
// impossible to provoke reliably through TCP. A session whose hard paths can
// only be tested by talking to something has its hard paths tested by hope.
//
// The same matrix runs against QuickFIX in the interop suite. This one is where
// the awkward inputs live, because QuickFIX will not produce them on demand.

#include <unistd.h>

#include <iostream>
#include <string>
#include <vector>

#include "fix/session.hpp"

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

std::string temp_path(const char* name) {
    return std::string("/tmp/mdstack-session-") + name + "-" + std::to_string(::getpid());
}

/// Collects what the session emits, parsed.
struct Captured {
    std::vector<std::string> raw;

    void clear() { raw.clear(); }

    [[nodiscard]] std::size_t count() const { return raw.size(); }

    [[nodiscard]] std::vector<fix::Message> parsed() const {
        std::vector<fix::Message> out;
        for (const auto& r : raw) {
            fix::Message m;
            std::size_t consumed = 0;
            if (!fix::Message::parse(r, m, consumed)) {
                out.push_back(m);
            }
        }
        return out;
    }

    /// The first emitted message of a given type, if any.
    [[nodiscard]] std::optional<fix::Message> first_of(std::string_view type) const {
        for (const auto& m : parsed()) {
            if (m.msg_type() == type) {
                return m;
            }
        }
        return std::nullopt;
    }

    [[nodiscard]] int count_of(std::string_view type) const {
        int n = 0;
        for (const auto& m : parsed()) {
            if (m.msg_type() == type) {
                ++n;
            }
        }
        return n;
    }
};

/// A session wired to a fresh store, plus the sink that captures its output.
struct Harness {
    std::string path;
    fix::SeqStore store;
    Captured captured;
    fix::Sink sink;
    std::unique_ptr<fix::Session> session;
    fix::Clock::time_point now{fix::Clock::now()};
    /// The interval the counterparty will echo. The session adopts whatever the
    /// logon carries -- that is what HeartBtInt negotiation means -- so a
    /// harness that hardcoded 30 here silently overrode its own configuration,
    /// which is how the first version of the timer tests failed.
    int hb_{30};

    explicit Harness(const char* name, bool initiator = true, int hb = 30) {
        path = temp_path(name);
        ::unlink(path.c_str());
        store.open(path);
        sink = [this](std::string_view b) { captured.raw.emplace_back(b); };
        fix::SessionConfig cfg;
        cfg.initiator = initiator;
        cfg.heartbeat_interval_seconds = hb;
        hb_ = hb;
        session = std::make_unique<fix::Session>(cfg, store);
    }

    ~Harness() { ::unlink(path.c_str()); }

    void advance(int seconds) { now += std::chrono::seconds(seconds); }

    /// Feeds an inbound message built from `fields`.
    void inbound(std::string_view type, std::int64_t seq,
                 const std::function<void(fix::Builder&)>& extra = nullptr) {
        std::vector<char> buf(4096);
        fix::Builder b(buf.data(), buf.size());
        b.begin();
        b.add(fix::tag::MsgType, type);
        b.add(fix::tag::SenderCompID, "EXCHANGE");
        b.add(fix::tag::TargetCompID, "GATEWAY");
        b.add(fix::tag::MsgSeqNum, seq);
        b.add(fix::tag::SendingTime, "20260906-12:00:00.000");
        if (extra) {
            extra(b);
        }
        std::string msg(b.finish());
        fix::Message m;
        std::size_t consumed = 0;
        if (fix::Message::parse(msg, m, consumed)) {
            std::cerr << "     harness built an unparseable message\n";
            ++failures;
            return;
        }
        session->on_message(m, sink, now);
    }

    /// Logs the session on so a test can start from a working session.
    void establish() {
        session->connect(sink, now);
        inbound(fix::msg_type::Logon, 1, [this](fix::Builder& b) {
            b.add(fix::tag::EncryptMethod, std::int64_t{0});
            b.add(fix::tag::HeartBtInt, static_cast<std::int64_t>(hb_));
        });
        captured.clear();
    }
};

// --- session establishment -------------------------------------------------

void logon_round_trip() {
    Harness h("logon");
    h.session->connect(h.sink, h.now);
    check(h.session->state() == fix::SessionState::AwaitingLogon,
          "an initiator that sent a logon is awaiting one");

    auto sent = h.captured.first_of(fix::msg_type::Logon);
    check(sent.has_value(), "a logon is emitted");
    check(sent && sent->seq_num() == 1, "the first outbound message is sequence 1");
    check(sent && sent->find_int(fix::tag::HeartBtInt) == 30, "HeartBtInt is proposed");

    h.inbound(fix::msg_type::Logon, 1, [](fix::Builder& b) {
        b.add(fix::tag::EncryptMethod, std::int64_t{0});
        b.add(fix::tag::HeartBtInt, std::int64_t{30});
    });
    check(h.session->state() == fix::SessionState::Active, "the answering logon activates it");
}

void reset_seq_num_flag_returns_both_directions_to_one() {
    Harness h("reset");
    h.establish();
    // Push the numbers up so a reset is visible.
    h.session->send_application(fix::msg_type::NewOrderSingle, nullptr, h.sink, h.now);
    check(h.store.current().outbound > 2, "the outbound sequence advanced");

    h.captured.clear();
    h.inbound(fix::msg_type::Logon, 1, [](fix::Builder& b) {
        b.add(fix::tag::EncryptMethod, std::int64_t{0});
        b.add(fix::tag::HeartBtInt, std::int64_t{30});
        b.add_bool(fix::tag::ResetSeqNumFlag, true);
    });
    check(h.store.current().inbound == 2,
          "after a reset the next expected inbound is 2, the logon having been 1");
}

// This one exists because the in-process suite did not have it and QuickFIX
// did. The case above is the counterparty asking for a reset. The case below is
// *us* asking — and the counterparty confirming by echoing the flag back, which
// is what the spec says it does and what every real engine actually does.
//
// The bug: `handle_logon` treated the echo as a fresh instruction and reset the
// store a second time, throwing away the sequence number `connect` had just
// spent on the Logon. Every subsequent message then went out one number too
// low. Against a simulator that never echoed the flag this was invisible.
// QuickFIX answered "MsgSeqNum too low, expecting 2 but received 1" and hung up.
void a_reset_we_asked_for_is_not_applied_twice() {
    Harness h("reset-echo");
    h.session->send_application(fix::msg_type::NewOrderSingle, nullptr, h.sink, h.now);
    h.captured.clear();

    h.session->connect(h.sink, h.now, /*reset_sequences=*/true);
    auto logon = h.captured.first_of(fix::msg_type::Logon);
    check(logon && logon->find_int(fix::tag::MsgSeqNum) == 1,
          "a reset logon goes out as sequence 1");
    check(h.store.current().outbound == 2, "and the store has moved on to 2");

    // The confirming logon. Same flag, coming back the other way.
    h.inbound(fix::msg_type::Logon, 1, [](fix::Builder& b) {
        b.add(fix::tag::EncryptMethod, std::int64_t{0});
        b.add(fix::tag::HeartBtInt, std::int64_t{30});
        b.add_bool(fix::tag::ResetSeqNumFlag, true);
    });
    check(h.session->state() == fix::SessionState::Active, "the confirmation activates the session");
    check(h.store.current().outbound == 2,
          "the confirmation does not reissue the number the logon already spent");

    h.captured.clear();
    h.session->send_application(fix::msg_type::NewOrderSingle, nullptr, h.sink, h.now);
    auto order = h.captured.first_of(fix::msg_type::NewOrderSingle);
    check(order && order->find_int(fix::tag::MsgSeqNum) == 2,
          "so the first message after the reset is 2, not 1 again");
}

// --- liveness --------------------------------------------------------------

void a_quiet_session_heartbeats() {
    Harness h("hb", true, 10);
    h.establish();
    h.advance(11);
    h.session->on_timer(h.sink, h.now);
    check(h.captured.count_of(fix::msg_type::Heartbeat) == 1,
          "a session quiet for its interval sends a heartbeat");
}

void a_silent_counterparty_is_prodded_then_dropped() {
    Harness h("test", true, 10);
    h.establish();

    h.advance(13);  // past interval + 20%
    h.session->on_timer(h.sink, h.now);
    check(h.captured.count_of(fix::msg_type::TestRequest) == 1,
          "a counterparty past the interval is sent a test request");

    h.advance(20);
    h.session->on_timer(h.sink, h.now);
    check(h.session->error() == fix::SessionError::Unresponsive,
          "a counterparty that never answers is declared unresponsive");
    check(h.session->finished(), "and the session ends");
}

void a_test_request_is_answered_with_its_id() {
    Harness h("answer");
    h.establish();
    h.inbound(fix::msg_type::TestRequest, 2,
              [](fix::Builder& b) { b.add(fix::tag::TestReqID, "PING-7"); });

    auto hb = h.captured.first_of(fix::msg_type::Heartbeat);
    check(hb.has_value(), "a test request is answered with a heartbeat");
    check(hb && hb->find(fix::tag::TestReqID) == "PING-7",
          "and the heartbeat echoes the TestReqID, which is what makes it an answer");
}

// --- the sequence rules ----------------------------------------------------

void a_gap_triggers_a_resend_request() {
    Harness h("gap");
    h.establish();
    // Expected 2, gets 5.
    h.inbound(fix::msg_type::NewOrderSingle, 5);

    auto rr = h.captured.first_of(fix::msg_type::ResendRequest);
    check(rr.has_value(), "a sequence gap produces a resend request");
    check(rr && rr->find_int(fix::tag::BeginSeqNo) == 2,
          "the request begins at the first missing sequence");
    check(h.session->state() == fix::SessionState::AwaitingResend,
          "and the session waits for the fill");
    check(h.session->stats().queued_during_resend == 1,
          "the message beyond the hole is held, not dropped");
}

void a_reversal_without_possdup_is_fatal() {
    // The spec requires this and it is not a judgement call: the two sides
    // disagree about what has been sent, and continuing means picking one.
    Harness h("reversal");
    h.establish();
    h.inbound(fix::msg_type::NewOrderSingle, 2);  // fine, expected 2
    h.captured.clear();
    h.inbound(fix::msg_type::NewOrderSingle, 2);  // again, no PossDup

    check(h.session->error() == fix::SessionError::SequenceReversal,
          "a repeated sequence with no PossDupFlag is a fatal reversal");
    check(h.captured.count_of(fix::msg_type::Logout) == 1, "the session logs out");
    check(h.session->finished(), "and finishes");
}

void a_duplicate_with_possdup_is_ignored_quietly() {
    Harness h("possdup");
    h.establish();
    h.inbound(fix::msg_type::NewOrderSingle, 2);
    h.captured.clear();
    h.inbound(fix::msg_type::NewOrderSingle, 2,
              [](fix::Builder& b) { b.add_bool(fix::tag::PossDupFlag, true); });

    check(h.session->error() == fix::SessionError::None,
          "a duplicate flagged PossDupFlag is not an error");
    check(h.session->state() == fix::SessionState::Active, "the session stays up");
    check(h.captured.count() == 0, "and nothing is sent in response");
}

void possdup_above_the_expected_number_is_still_a_gap() {
    // PossDupFlag says "this may be a repeat", not "trust this number". Reading
    // it as the latter would skip everything in between.
    Harness h("possdup-gap");
    h.establish();
    h.inbound(fix::msg_type::NewOrderSingle, 9,
              [](fix::Builder& b) { b.add_bool(fix::tag::PossDupFlag, true); });

    check(h.captured.first_of(fix::msg_type::ResendRequest).has_value(),
          "PossDupFlag on a number above the expected one is treated as a gap");
}

void a_backwards_sequence_reset_is_refused() {
    // The most opinionated decision in the session, recorded in PROTOCOL.md.
    Harness h("backwards");
    h.establish();
    h.inbound(fix::msg_type::NewOrderSingle, 2);
    h.inbound(fix::msg_type::NewOrderSingle, 3);
    h.captured.clear();

    h.inbound(fix::msg_type::SequenceReset, 4, [](fix::Builder& b) {
        b.add(fix::tag::NewSeqNo, std::int64_t{2});  // backwards
    });
    check(h.session->error() == fix::SessionError::BackwardsSequenceReset,
          "a SequenceReset that moves backwards is refused, not accepted silently");
    check(h.session->finished(), "and the session ends rather than continuing on a guess");
}

void a_forward_sequence_reset_closes_the_gap() {
    Harness h("forward");
    h.establish();
    h.inbound(fix::msg_type::NewOrderSingle, 7);  // gap: expected 2
    check(h.session->state() == fix::SessionState::AwaitingResend, "the gap opened");
    h.captured.clear();

    h.inbound(fix::msg_type::SequenceReset, 2, [](fix::Builder& b) {
        b.add_bool(fix::tag::GapFillFlag, true);
        b.add(fix::tag::NewSeqNo, std::int64_t{7});
    });
    check(h.store.current().inbound == 7, "a gap fill advances the expected sequence");
    check(h.session->state() == fix::SessionState::Active, "and the session resumes");
    check(h.session->error() == fix::SessionError::None, "without error");
}

// --- resend ----------------------------------------------------------------

void a_resend_request_replays_application_messages_with_possdup() {
    Harness h("resend");
    h.establish();
    h.session->send_application(fix::msg_type::NewOrderSingle, nullptr, h.sink, h.now);
    h.session->send_application(fix::msg_type::NewOrderSingle, nullptr, h.sink, h.now);
    auto first_app_seq = 2;  // logon was 1
    h.captured.clear();

    h.inbound(fix::msg_type::ResendRequest, 2, [first_app_seq](fix::Builder& b) {
        b.add(fix::tag::BeginSeqNo, static_cast<std::int64_t>(first_app_seq));
        b.add(fix::tag::EndSeqNo, std::int64_t{0});
    });

    check(h.session->stats().resend_requests_served == 1, "the resend request is served");
    check(h.session->stats().resent >= 2, "the application messages are resent");

    bool all_possdup = true;
    for (const auto& m : h.captured.parsed()) {
        if (m.msg_type() == fix::msg_type::NewOrderSingle && !m.flag(fix::tag::PossDupFlag)) {
            all_possdup = false;
        }
    }
    check(all_possdup, "every resent message carries PossDupFlag=Y");

    bool has_orig_time = false;
    for (const auto& m : h.captured.parsed()) {
        if (m.msg_type() == fix::msg_type::NewOrderSingle &&
            m.find(fix::tag::OrigSendingTime).has_value()) {
            has_orig_time = true;
        }
    }
    check(has_orig_time, "and OrigSendingTime, so the original time is not lost");
}

void administrative_messages_are_gap_filled_never_resent() {
    // Resending a heartbeat or a logon is meaningless — they describe a moment
    // that has passed. But the sequence numbers they consumed cannot simply
    // vanish, so they are gap-filled.
    Harness h("gapfill");
    h.establish();
    h.captured.clear();

    h.inbound(fix::msg_type::ResendRequest, 2, [](fix::Builder& b) {
        b.add(fix::tag::BeginSeqNo, std::int64_t{1});  // the logon
        b.add(fix::tag::EndSeqNo, std::int64_t{1});
    });

    auto sr = h.captured.first_of(fix::msg_type::SequenceReset);
    check(sr.has_value(), "an administrative message is gap-filled");
    check(sr && sr->flag(fix::tag::GapFillFlag), "with GapFillFlag=Y");
    check(sr && sr->flag(fix::tag::PossDupFlag),
          "and PossDupFlag=Y, because it stands in for something already sent");
    check(h.session->stats().resent == 0, "and it is not resent as itself");
}

void a_declined_range_is_never_silently_dropped() {
    // The invariant PROTOCOL.md states: anything not resent is gap-filled. A
    // counterparty must never see a sequence number simply disappear.
    Harness h("nodrop");
    h.establish();
    h.session->send_application(fix::msg_type::NewOrderSingle, nullptr, h.sink, h.now);
    h.captured.clear();

    // Ask for a range running past what was ever sent.
    h.inbound(fix::msg_type::ResendRequest, 2, [](fix::Builder& b) {
        b.add(fix::tag::BeginSeqNo, std::int64_t{1});
        b.add(fix::tag::EndSeqNo, std::int64_t{0});
    });

    auto messages = h.captured.parsed();
    check(!messages.empty(), "the request produces output");

    // Every sequence from 1 to the last sent must be covered by either a resend
    // or a gap fill. Nothing may be missing.
    std::uint64_t covered_to = 0;
    for (const auto& m : messages) {
        if (m.msg_type() == fix::msg_type::SequenceReset) {
            auto from = m.seq_num().value_or(0);
            auto to = m.find_int(fix::tag::NewSeqNo).value_or(0);
            if (static_cast<std::uint64_t>(from) <= covered_to + 1) {
                covered_to = std::max(covered_to, static_cast<std::uint64_t>(to) - 1);
            }
        } else if (m.flag(fix::tag::PossDupFlag)) {
            auto s = m.seq_num().value_or(0);
            if (static_cast<std::uint64_t>(s) == covered_to + 1) {
                covered_to = static_cast<std::uint64_t>(s);
            }
        }
    }
    check(covered_to >= 2,
          "every sequence in the requested range is covered by a resend or a gap fill");
}

// --- logout ----------------------------------------------------------------

void a_logout_is_confirmed() {
    Harness h("logout");
    h.establish();
    h.captured.clear();
    h.inbound(fix::msg_type::Logout, 2);
    check(h.captured.count_of(fix::msg_type::Logout) == 1, "an inbound logout is confirmed");
    check(h.session->finished(), "and the session finishes");
}

void our_logout_waits_for_confirmation() {
    Harness h("mylogout");
    h.establish();
    h.session->logout("done for the day", h.sink, h.now);
    check(h.session->state() == fix::SessionState::AwaitingLogout,
          "a logout we send waits for the confirmation");
    h.inbound(fix::msg_type::Logout, 2);
    check(h.session->finished(), "which finishes it");
}

// --- durability ------------------------------------------------------------

void sequence_numbers_are_persisted_before_the_bytes_leave() {
    // The ordering the whole gateway rests on. After emitting message N, the
    // store must already know about N — not after the socket write, and not at
    // shutdown.
    Harness h("durable");
    h.session->connect(h.sink, h.now);
    check(h.store.current().outbound == 2,
          "after sending sequence 1, the store already records 2 as next");
    check(h.store.syncs() >= 1, "and it cost an fsync");
}

}  // namespace

int main() {
    logon_round_trip();
    reset_seq_num_flag_returns_both_directions_to_one();
    a_reset_we_asked_for_is_not_applied_twice();
    a_quiet_session_heartbeats();
    a_silent_counterparty_is_prodded_then_dropped();
    a_test_request_is_answered_with_its_id();
    a_gap_triggers_a_resend_request();
    a_reversal_without_possdup_is_fatal();
    a_duplicate_with_possdup_is_ignored_quietly();
    possdup_above_the_expected_number_is_still_a_gap();
    a_backwards_sequence_reset_is_refused();
    a_forward_sequence_reset_closes_the_gap();
    a_resend_request_replays_application_messages_with_possdup();
    administrative_messages_are_gap_filled_never_resent();
    a_declined_range_is_never_silently_dropped();
    a_logout_is_confirmed();
    our_logout_waits_for_confirmation();
    sequence_numbers_are_persisted_before_the_bytes_leave();

    if (failures != 0) {
        std::cerr << failures << " failure(s)\n";
        return 1;
    }
    std::cout << "all session conformance checks passed\n";
    return 0;
}
