#include "fix/session.hpp"

#include <array>
#include <cstdio>
#include <ctime>

namespace fix {
namespace {

/// Fixed-width digits, written by hand rather than with `snprintf`.
///
/// Not micro-optimisation: `snprintf` with `%04d` cannot be proved by the
/// compiler to fit a fixed buffer, so `-Wformat-truncation` rejects it, and the
/// alternatives are sizing for an eleven-digit year or switching the warning
/// off. Writing the digits is shorter than either excuse.
void put_digits(char*& p, int value, int width) {
    for (int i = width - 1; i >= 0; --i) {
        p[i] = static_cast<char>('0' + value % 10);
        value /= 10;
    }
    p += width;
}

/// FIX `SendingTime`: UTC, `YYYYMMDD-HH:MM:SS.sss`.
///
/// Wall clock, not the steady clock the session uses for timers. The two are
/// different things, and conflating them is how a session ends up with
/// timestamps that go backwards over an NTP step.
std::string utc_timestamp() {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto secs = time_point_cast<seconds>(now);
    auto ms = static_cast<int>(duration_cast<milliseconds>(now - secs).count());
    std::time_t t = system_clock::to_time_t(secs);
    std::tm tm{};
    gmtime_r(&t, &tm);

    // YYYYMMDD-HH:MM:SS.sss
    std::array<char, 21> buf{};
    char* p = buf.data();
    put_digits(p, tm.tm_year + 1900, 4);
    put_digits(p, tm.tm_mon + 1, 2);
    put_digits(p, tm.tm_mday, 2);
    *p++ = '-';
    put_digits(p, tm.tm_hour, 2);
    *p++ = ':';
    put_digits(p, tm.tm_min, 2);
    *p++ = ':';
    put_digits(p, tm.tm_sec, 2);
    *p++ = '.';
    put_digits(p, ms, 3);
    return std::string(buf.data(), buf.size());
}

constexpr std::size_t MAX_MESSAGE = 8192;

}  // namespace

std::string_view describe(SessionState s) {
    switch (s) {
        case SessionState::Disconnected: return "disconnected";
        case SessionState::AwaitingLogon: return "awaiting logon";
        case SessionState::Active: return "active";
        case SessionState::AwaitingResend: return "awaiting resend";
        case SessionState::AwaitingLogout: return "awaiting logout";
        case SessionState::LoggedOut: return "logged out";
    }
    return "unknown";
}

std::string_view describe(SessionError e) {
    switch (e) {
        case SessionError::None:
            return "none";
        case SessionError::SequenceReversal:
            return "a message arrived with a sequence number below the expected one and no "
                   "PossDupFlag; the two sides disagree about what has been sent";
        case SessionError::BackwardsSequenceReset:
            return "a SequenceReset would have moved the sequence backwards, which this "
                   "gateway refuses rather than accepting silently";
        case SessionError::Unresponsive:
            return "the counterparty stopped answering test requests";
        case SessionError::StoreFailure:
            return "a sequence number could not be persisted, so nothing further may be sent";
        case SessionError::Corrupt:
            return "the stream stopped being FIX";
    }
    return "unknown";
}

Session::Session(SessionConfig cfg, SeqStore& store) : cfg_(std::move(cfg)), store_(store) {
    ring_.resize(cfg_.resend_ring);
}

// --- outbound --------------------------------------------------------------

bool Session::emit(std::string_view msg_type, const std::function<void(Builder&)>& body,
                   Sink& out, Clock::time_point now, bool administrative) {
    auto seq = store_.claim_outbound();
    if (!seq) {
        // The number was never durable. Sending would consume a sequence a
        // restart would hand out again, and the counterparty would treat the
        // repeat as a fatal reversal.
        error_ = SessionError::StoreFailure;
        state_ = SessionState::LoggedOut;
        return false;
    }

    std::string sending_time = utc_timestamp();
    std::array<char, MAX_MESSAGE> buf{};
    Builder b(buf.data(), buf.size());
    b.begin();
    b.add(tag::MsgType, msg_type);
    b.add(tag::SenderCompID, cfg_.sender_comp_id);
    b.add(tag::TargetCompID, cfg_.target_comp_id);
    b.add(tag::MsgSeqNum, static_cast<std::int64_t>(*seq));
    b.add(tag::SendingTime, sending_time);
    if (body) {
        body(b);
    }
    auto bytes = b.finish();
    if (bytes.empty()) {
        error_ = SessionError::Corrupt;
        return false;
    }

    remember(*seq, bytes, sending_time, administrative);
    out(bytes);
    last_sent_ = now;
    ++stats_.sent;
    return true;
}

void Session::remember(std::uint64_t seq, std::string_view bytes, std::string_view sending_time,
                       bool administrative) {
    if (ring_.empty()) {
        return;
    }
    Sent& slot = ring_[ring_next_];
    slot.seq = seq;
    slot.bytes.assign(bytes);
    slot.sending_time.assign(sending_time);
    slot.administrative = administrative;
    ring_next_ = (ring_next_ + 1) % ring_.size();
}

const Session::Sent* Session::recall(std::uint64_t seq) const {
    for (const auto& s : ring_) {
        if (s.seq == seq && !s.bytes.empty()) {
            return &s;
        }
    }
    return nullptr;
}

void Session::reset_for_new_connection() {
    state_ = SessionState::Disconnected;
    error_ = SessionError::None;
    last_sent_ = {};
    last_received_ = {};
    test_request_outstanding_ = false;
    reset_requested_ = false;
    queued_.clear();
    for (auto& slot : ring_) {
        slot = Sent{};
    }
    ring_next_ = 0;
}

void Session::connect(Sink& out, Clock::time_point now, bool reset_sequences) {
    if (!cfg_.initiator) {
        return;
    }
    if (reset_sequences && store_.reset()) {
        error_ = SessionError::StoreFailure;
        state_ = SessionState::LoggedOut;
        return;
    }
    reset_requested_ = reset_sequences;
    emit(msg_type::Logon,
         [this, reset_sequences](Builder& b) {
             b.add(tag::EncryptMethod, std::int64_t{0});
             b.add(tag::HeartBtInt, static_cast<std::int64_t>(cfg_.heartbeat_interval_seconds));
             if (reset_sequences) {
                 b.add_bool(tag::ResetSeqNumFlag, true);
             }
         },
         out, now, true);
    state_ = SessionState::AwaitingLogon;
    last_received_ = now;
}

void Session::send_heartbeat(Sink& out, Clock::time_point now, std::string_view test_req_id) {
    std::string id(test_req_id);
    emit(msg_type::Heartbeat,
         [id](Builder& b) {
             if (!id.empty()) {
                 // Echoing the id is what makes the heartbeat an *answer*. A
                 // bare heartbeat does not satisfy a test request.
                 b.add(tag::TestReqID, id);
             }
         },
         out, now, true);
}

void Session::send_test_request(Sink& out, Clock::time_point now) {
    ++test_request_id_;
    std::string id = "TR" + std::to_string(test_request_id_);
    emit(msg_type::TestRequest, [id](Builder& b) { b.add(tag::TestReqID, id); }, out, now, true);
    test_request_outstanding_ = true;
    ++stats_.test_requests_sent;
}

void Session::send_resend_request(std::uint64_t from, Sink& out, Clock::time_point now) {
    emit(msg_type::ResendRequest,
         [from](Builder& b) {
             b.add(tag::BeginSeqNo, static_cast<std::int64_t>(from));
             // 0 means "everything from BeginSeqNo". Some implementations write
             // 999999 for the same thing; both are accepted inbound, and 0 is
             // what this one sends because it cannot be mistaken for a real
             // sequence number.
             b.add(tag::EndSeqNo, std::int64_t{0});
         },
         out, now, true);
    ++stats_.resend_requests_sent;
}

void Session::send_gap_fill(std::uint64_t from, std::uint64_t new_seq_no, Sink& out,
                            Clock::time_point now) {
    // A gap fill is a SequenceReset that carries the sequence number of the
    // range it is replacing, flagged PossDup because it stands in for messages
    // that were already sent once.
    std::string sending_time = utc_timestamp();
    std::array<char, MAX_MESSAGE> buf{};
    Builder b(buf.data(), buf.size());
    b.begin();
    b.add(tag::MsgType, msg_type::SequenceReset);
    b.add(tag::SenderCompID, cfg_.sender_comp_id);
    b.add(tag::TargetCompID, cfg_.target_comp_id);
    b.add(tag::MsgSeqNum, static_cast<std::int64_t>(from));
    b.add(tag::SendingTime, sending_time);
    b.add_bool(tag::PossDupFlag, true);
    b.add(tag::OrigSendingTime, sending_time);
    b.add_bool(tag::GapFillFlag, true);
    b.add(tag::NewSeqNo, static_cast<std::int64_t>(new_seq_no));
    auto bytes = b.finish();
    if (bytes.empty()) {
        return;
    }
    // Deliberately not claimed from the store: a gap fill re-uses the sequence
    // numbers it is replacing rather than consuming new ones.
    out(bytes);
    last_sent_ = now;
    ++stats_.gap_filled;
}

void Session::send_reject(const Message& m, std::string_view text, Sink& out,
                          Clock::time_point now) {
    auto ref = m.seq_num().value_or(0);
    std::string t(text);
    emit(msg_type::Reject,
         [ref, t](Builder& b) {
             b.add(tag::RefSeqNum, static_cast<std::int64_t>(ref));
             b.add(tag::Text, t);
         },
         out, now, true);
}

bool Session::send_application(std::string_view msg_type_, const std::function<void(Builder&)>& body,
                               Sink& out, Clock::time_point now) {
    if (state_ != SessionState::Active) {
        return false;
    }
    return emit(msg_type_, body, out, now, false);
}

void Session::logout(std::string_view reason, Sink& out, Clock::time_point now) {
    std::string r(reason);
    emit(msg_type::Logout, [r](Builder& b) { if (!r.empty()) b.add(tag::Text, r); }, out, now,
         true);
    state_ = SessionState::AwaitingLogout;
}

void Session::fail(SessionError e, std::string_view text, Sink& out, Clock::time_point now) {
    error_ = e;
    std::string t(text);
    emit(msg_type::Logout, [t](Builder& b) { b.add(tag::Text, t); }, out, now, true);
    // No wait for a confirming logout. The session is not in a state where the
    // counterparty's reply could be trusted anyway.
    state_ = SessionState::LoggedOut;
}

// --- inbound ---------------------------------------------------------------

void Session::on_message(const Message& m, Sink& out, Clock::time_point now) {
    on_message(m, out, now, nullptr);
}

void Session::on_message(const Message& m, Sink& out, Clock::time_point now,
                         const std::function<void(const Message&)>& on_app) {
    ++stats_.received;
    last_received_ = now;

    auto type = m.msg_type();

    // Logon is the only message accepted before the session is up, and
    // SequenceReset is the only one whose sequence is not checked first — a
    // reset exists precisely to fix a sequence disagreement, so refusing it for
    // disagreeing would be circular.
    if (type == msg_type::Logon) {
        handle_logon(m, out, now);
        return;
    }
    if (state_ == SessionState::Disconnected) {
        return;
    }
    if (type == msg_type::SequenceReset) {
        handle_sequence_reset(m, out, now);
        return;
    }
    if (!check_sequence(m, out, now)) {
        return;
    }

    if (type == msg_type::Heartbeat) {
        // A heartbeat carrying the outstanding TestReqID is the answer we were
        // waiting for. One without it is just liveness.
        auto id = m.find(tag::TestReqID);
        if (test_request_outstanding_ && id && *id == ("TR" + std::to_string(test_request_id_))) {
            test_request_outstanding_ = false;
        }
        return;
    }
    if (type == msg_type::TestRequest) {
        handle_test_request(m, out, now);
        return;
    }
    if (type == msg_type::ResendRequest) {
        handle_resend_request(m, out, now);
        return;
    }
    if (type == msg_type::Logout) {
        handle_logout(m, out, now);
        return;
    }
    // Application messages: the session layer's job is done once the sequence
    // checks out. What the message *means* is the caller's business, and it
    // only ever sees one that passed every check above.
    if (on_app) {
        on_app(m);
    }
}

void Session::handle_logon(const Message& m, Sink& out, Clock::time_point now) {
    const bool we_asked = reset_requested_;
    reset_requested_ = false;
    if (m.flag(tag::ResetSeqNumFlag) && !we_asked) {
        // Both directions to 1, durably, before anything else happens.
        //
        // Only when the counterparty is the one asking. If we sent
        // `ResetSeqNumFlag=Y` ourselves, `connect` already reset the store and
        // spent sequence 1 on the Logon; this flag coming back is the other end
        // agreeing, and resetting again would reissue that number.
        if (store_.reset()) {
            fail(SessionError::StoreFailure, "could not persist a sequence reset", out, now);
            return;
        }
    }

    // The acceptor echoes the initiator's proposed interval rather than
    // imposing its own; that is what the spec means by negotiation.
    if (auto hb = m.find_int(tag::HeartBtInt)) {
        cfg_.heartbeat_interval_seconds = static_cast<int>(*hb);
    }

    if (!cfg_.initiator && state_ == SessionState::Disconnected) {
        emit(msg_type::Logon,
             [this, &m](Builder& b) {
                 b.add(tag::EncryptMethod, std::int64_t{0});
                 b.add(tag::HeartBtInt,
                       static_cast<std::int64_t>(cfg_.heartbeat_interval_seconds));
                 if (m.flag(tag::ResetSeqNumFlag)) {
                     b.add_bool(tag::ResetSeqNumFlag, true);
                 }
             },
             out, now, true);
    }

    if (!check_sequence(m, out, now)) {
        return;
    }
    state_ = SessionState::Active;
}

void Session::handle_test_request(const Message& m, Sink& out, Clock::time_point now) {
    auto id = m.find(tag::TestReqID);
    send_heartbeat(out, now, id.value_or(std::string_view{}));
}

void Session::handle_logout(const Message&, Sink& out, Clock::time_point now) {
    if (state_ == SessionState::AwaitingLogout) {
        // Our logout, confirmed.
        state_ = SessionState::LoggedOut;
        return;
    }
    // Theirs. Confirm and finish.
    emit(msg_type::Logout, nullptr, out, now, true);
    state_ = SessionState::LoggedOut;
}

void Session::handle_resend_request(const Message& m, Sink& out, Clock::time_point now) {
    auto begin = m.find_int(tag::BeginSeqNo);
    auto end = m.find_int(tag::EndSeqNo);
    if (!begin || !end) {
        send_reject(m, "ResendRequest without BeginSeqNo or EndSeqNo", out, now);
        return;
    }
    ++stats_.resend_requests_served;

    std::uint64_t from = static_cast<std::uint64_t>(*begin);
    // 0 and 999999 both mean "everything from BeginSeqNo".
    std::uint64_t to = (*end == 0 || *end == 999999) ? store_.current().outbound - 1
                                                     : static_cast<std::uint64_t>(*end);

    // Walk the range once, batching consecutive declines into a single gap fill.
    // Sending one gap fill per skipped message would be legal and would also be
    // a message storm during a large resend.
    std::uint64_t gap_start = 0;
    for (std::uint64_t seq = from; seq <= to; ++seq) {
        const Sent* s = recall(seq);
        bool resendable = s != nullptr && !s->administrative;
        if (!resendable) {
            if (gap_start == 0) {
                gap_start = seq;
            }
            continue;
        }
        if (gap_start != 0) {
            send_gap_fill(gap_start, seq, out, now);
            gap_start = 0;
        }
        // The original bytes with PossDupFlag and OrigSendingTime added. Rebuilt
        // rather than patched: inserting a field into an existing message means
        // recomputing BodyLength and CheckSum anyway.
        Message parsed;
        std::size_t consumed = 0;
        if (Message::parse(s->bytes, parsed, consumed)) {
            continue;
        }
        std::array<char, MAX_MESSAGE> buf{};
        Builder b(buf.data(), buf.size());
        b.begin();
        parsed.for_each([&](int t, std::string_view v) {
            // Header and trailer fields are rewritten, not copied.
            if (t == tag::BeginString || t == tag::BodyLength || t == tag::CheckSum) {
                return;
            }
            if (t == tag::SendingTime) {
                b.add(tag::SendingTime, v);
                b.add_bool(tag::PossDupFlag, true);
                b.add(tag::OrigSendingTime, s->sending_time);
                return;
            }
            b.add(t, v);
        });
        auto bytes = b.finish();
        if (!bytes.empty()) {
            out(bytes);
            last_sent_ = now;
            ++stats_.resent;
        }
    }
    if (gap_start != 0) {
        send_gap_fill(gap_start, to + 1, out, now);
    }
}

void Session::handle_sequence_reset(const Message& m, Sink& out, Clock::time_point now) {
    auto new_seq = m.find_int(tag::NewSeqNo);
    if (!new_seq) {
        send_reject(m, "SequenceReset without NewSeqNo", out, now);
        return;
    }
    auto target = static_cast<std::uint64_t>(*new_seq);
    auto expected = store_.current().inbound;

    if (target < expected) {
        // The most opinionated decision in this file, and it is recorded in
        // docs/PROTOCOL.md. The spec permits a reset to a lower number; taking
        // it silently is how two sides end up disagreeing about what has been
        // sent with nothing to notice it by.
        fail(SessionError::BackwardsSequenceReset,
             "SequenceReset would move the sequence backwards", out, now);
        return;
    }

    if (store_.accept_inbound(target - 1)) {
        fail(SessionError::StoreFailure, "could not persist a sequence reset", out, now);
        return;
    }

    // The gap the reset covered is closed, so any traffic held behind it can go.
    if (state_ == SessionState::AwaitingResend) {
        state_ = SessionState::Active;
        queued_.clear();
    }
}

bool Session::check_sequence(const Message& m, Sink& out, Clock::time_point now) {
    auto seq = m.seq_num();
    if (!seq) {
        send_reject(m, "message without MsgSeqNum", out, now);
        return false;
    }
    auto got = static_cast<std::uint64_t>(*seq);
    auto expected = store_.current().inbound;

    if (got == expected) {
        if (store_.accept_inbound(got)) {
            fail(SessionError::StoreFailure, "could not persist an inbound sequence", out, now);
            return false;
        }
        return true;
    }

    if (got < expected) {
        if (m.flag(tag::PossDupFlag)) {
            // A duplicate we already have. Ignoring it is correct and is not an
            // error — it is what PossDupFlag is for.
            return false;
        }
        fail(SessionError::SequenceReversal,
             "MsgSeqNum below the expected value with no PossDupFlag", out, now);
        return false;
    }

    // Higher than expected: a gap. Note that PossDupFlag does not make this a
    // duplicate — the flag says "this may be a repeat", not "trust this number".
    if (state_ != SessionState::AwaitingResend) {
        state_ = SessionState::AwaitingResend;
        send_resend_request(expected, out, now);
    }
    // Hold the message; it is beyond the hole and will be needed once filled.
    queued_.emplace_back(m.raw());
    ++stats_.queued_during_resend;
    return false;
}

// --- timers ----------------------------------------------------------------

void Session::on_timer(Sink& out, Clock::time_point now) {
    if (state_ == SessionState::Disconnected || state_ == SessionState::LoggedOut) {
        return;
    }
    auto interval = std::chrono::seconds(cfg_.heartbeat_interval_seconds);
    if (interval.count() <= 0) {
        return;
    }
    auto tolerance = std::chrono::duration_cast<Clock::duration>(
        std::chrono::duration<double>(static_cast<double>(interval.count()) *
                                      cfg_.heartbeat_tolerance));

    if (now - last_sent_ >= interval) {
        send_heartbeat(out, now);
    }

    auto quiet = now - last_received_;
    if (test_request_outstanding_) {
        if (quiet >= interval + tolerance * 2) {
            // Prodded and still silent. The counterparty is gone.
            error_ = SessionError::Unresponsive;
            state_ = SessionState::LoggedOut;
        }
        return;
    }
    if (quiet >= interval + tolerance) {
        send_test_request(out, now);
    }
}

}  // namespace fix
