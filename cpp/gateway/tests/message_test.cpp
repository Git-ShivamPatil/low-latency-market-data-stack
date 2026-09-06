// Framing tests for the FIX 4.4 codec.
//
// The interesting half is not "a good message parses". It is what happens to a
// byte stream that is not yet a message, or is two messages, or claims a length
// it does not have. A FIX session reads from TCP, which delivers bytes rather
// than messages, so every one of those arrives as a matter of routine and each
// needs a different response: read more, consume one and re-parse, or drop the
// session. Collapsing them into one "bad message" disconnects on every partial
// read.

#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "fix/message.hpp"

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

/// Readable form of a message, for failure output. SOH is invisible otherwise.
std::string readable(std::string_view s) {
    std::string out;
    for (char c : s) {
        out += (c == fix::SOH) ? '|' : c;
    }
    return out;
}

/// A minimal Logon, built the way the session layer will build it.
std::string logon(std::int64_t seq = 1) {
    std::vector<char> buf(512);
    fix::Builder b(buf.data(), buf.size());
    b.begin();
    b.add(fix::tag::MsgType, fix::msg_type::Logon);
    b.add(fix::tag::SenderCompID, "GATEWAY");
    b.add(fix::tag::TargetCompID, "EXCHANGE");
    b.add(fix::tag::MsgSeqNum, seq);
    b.add(fix::tag::SendingTime, "20260906-12:00:00.000");
    b.add(fix::tag::EncryptMethod, std::int64_t{0});
    b.add(fix::tag::HeartBtInt, std::int64_t{30});
    return std::string(b.finish());
}

void round_trip() {
    std::string msg = logon(7);
    check(!msg.empty(), "a built message is not empty");

    fix::Message m;
    std::size_t consumed = 0;
    auto err = fix::Message::parse(msg, m, consumed);
    check(!err, "a message this codec built, this codec parses");
    check(consumed == msg.size(), "parsing consumes exactly the message");
    check(m.msg_type() == fix::msg_type::Logon, "MsgType round-trips");
    check(m.seq_num() == 7, "MsgSeqNum round-trips as a number");
    check(m.find(fix::tag::SenderCompID) == "GATEWAY", "SenderCompID round-trips");
    check(m.find_int(fix::tag::HeartBtInt) == 30, "HeartBtInt round-trips");
    check(m.raw() == msg, "raw() gives back what was parsed");
}

void checksum_is_the_real_one() {
    // Computed independently of the builder: sum every byte before "10=",
    // modulo 256. If the builder and this agree, the builder is not simply
    // echoing its own arithmetic.
    std::string msg = logon();
    std::size_t ck_pos = msg.rfind("10=");
    check(ck_pos != std::string::npos, "a built message carries a checksum field");

    unsigned sum = 0;
    for (std::size_t i = 0; i < ck_pos; ++i) {
        sum += static_cast<unsigned char>(msg[i]);
    }
    unsigned expected = sum % 256U;

    unsigned stated = static_cast<unsigned>(std::stoi(msg.substr(ck_pos + 3, 3)));
    check(stated == expected, "the checksum matches an independent computation");
    check(msg.substr(ck_pos + 3, 3).size() == 3, "the checksum is three digits");
    check(msg.back() == fix::SOH, "the message ends with SOH");
}

void body_length_is_the_real_one() {
    std::string msg = logon();
    std::size_t nine = msg.find("9=");
    std::size_t after_nine = msg.find(fix::SOH, nine) + 1;
    std::size_t ck_pos = msg.rfind("10=");
    std::size_t actual = ck_pos - after_nine;

    std::string declared = msg.substr(nine + 2, after_nine - nine - 3);
    check(static_cast<std::size_t>(std::stoul(declared)) == actual,
          "BodyLength counts exactly the bytes between it and the checksum");
}

void a_partial_message_asks_for_more() {
    std::string msg = logon();
    fix::Message m;
    std::size_t consumed = 0;

    // Every proper prefix must say Incomplete, not "bad". A session that treats
    // a short read as corruption disconnects constantly under normal TCP.
    bool all_incomplete = true;
    for (std::size_t n = 1; n < msg.size(); ++n) {
        auto err = fix::Message::parse(std::string_view(msg).substr(0, n), m, consumed);
        if (err != fix::ParseError::Incomplete) {
            all_incomplete = false;
            std::cerr << "     at " << n << " bytes: " << fix::describe(*err) << "\n";
            std::cerr << "     prefix: " << readable(std::string_view(msg).substr(0, n)) << "\n";
            break;
        }
    }
    check(all_incomplete, "every prefix of a message reports Incomplete, never corruption");
}

void two_messages_in_one_buffer() {
    // The case that makes BodyLength load-bearing. A parser that scanned for
    // "10=" could stop in the right place here by luck; one that ignores the
    // boundary entirely would swallow both.
    std::string a = logon(1);
    std::string b = logon(2);
    std::string both = a + b;

    fix::Message m;
    std::size_t consumed = 0;
    auto err = fix::Message::parse(both, m, consumed);
    check(!err, "the first of two concatenated messages parses");
    check(consumed == a.size(), "exactly the first message is consumed");
    check(m.seq_num() == 1, "the first message is the one returned");

    auto err2 = fix::Message::parse(std::string_view(both).substr(consumed), m, consumed);
    check(!err2, "the remainder parses as the second message");
    check(m.seq_num() == 2, "and it is the second message");
}

void a_corrupt_checksum_is_refused() {
    std::string msg = logon();
    std::size_t ck_pos = msg.rfind("10=");
    msg[ck_pos + 3] = (msg[ck_pos + 3] == '0') ? '1' : '0';

    fix::Message m;
    std::size_t consumed = 0;
    auto err = fix::Message::parse(msg, m, consumed);
    check(err == fix::ParseError::BadCheckSum, "a wrong checksum is refused");
}

void a_flipped_payload_byte_is_caught_by_the_checksum() {
    // The reason the checksum is verified at all: a byte corrupted in flight
    // changes a field value, and without this the session would act on it.
    std::string msg = logon();
    std::size_t sender = msg.find("49=");
    msg[sender + 3] = 'X';

    fix::Message m;
    std::size_t consumed = 0;
    auto err = fix::Message::parse(msg, m, consumed);
    check(err == fix::ParseError::BadCheckSum,
          "a byte flipped in the payload is caught by the checksum");
}

void a_lying_body_length_is_refused() {
    // BodyLength that does not point at the checksum field. The message
    // disagrees with itself, and no amount of further reading fixes it.
    std::string msg = logon();
    std::size_t nine = msg.find("9=");
    // Shrink the declared length by one, keeping the width.
    std::size_t last_digit = msg.find(fix::SOH, nine) - 1;
    msg[last_digit] = static_cast<char>(msg[last_digit] - 1);

    fix::Message m;
    std::size_t consumed = 0;
    auto err = fix::Message::parse(msg, m, consumed);
    check(err == fix::ParseError::BadBodyLength || err == fix::ParseError::BadCheckSum,
          "a BodyLength that does not point at the checksum is refused");
}

void a_wrong_begin_string_is_refused_immediately() {
    std::string msg = logon();
    msg[7] = '2';  // FIX.4.4 -> FIX.2.4

    fix::Message m;
    std::size_t consumed = 0;
    auto err = fix::Message::parse(msg, m, consumed);
    check(err == fix::ParseError::BadBeginString, "a non-4.4 BeginString is refused");

    // And a buffer that is too short to judge, but already wrong, says so
    // rather than asking for bytes that cannot help.
    auto err2 = fix::Message::parse("8=FIX.4.2", m, consumed);
    check(err2 == fix::ParseError::BadBeginString,
          "a short buffer that already disagrees is refused, not awaited");
}

void repeated_tags_are_all_kept() {
    // FIX repeating groups are literally the same tag appearing again. A map
    // would discard all but one, silently.
    std::vector<char> buf(512);
    fix::Builder b(buf.data(), buf.size());
    b.begin();
    b.add(fix::tag::MsgType, fix::msg_type::NewOrderSingle);
    b.add(fix::tag::MsgSeqNum, std::int64_t{1});
    b.add(fix::tag::Text, "first");
    b.add(fix::tag::Text, "second");
    b.add(fix::tag::Text, "third");
    std::string msg(b.finish());

    fix::Message m;
    std::size_t consumed = 0;
    auto err = fix::Message::parse(msg, m, consumed);
    check(!err, "a message with repeated tags parses");
    check(m.find(fix::tag::Text) == "first", "find() returns the first occurrence");

    int seen = 0;
    m.for_each([&](int t, std::string_view) {
        if (t == fix::tag::Text) {
            ++seen;
        }
    });
    check(seen == 3, "for_each() sees every occurrence");
}

void a_non_numeric_number_is_not_zero() {
    // find_int returning 0 for "abc" would make a corrupt sequence number look
    // like sequence zero, which is a legal-looking value with a very different
    // meaning.
    std::vector<char> buf(512);
    fix::Builder b(buf.data(), buf.size());
    b.begin();
    b.add(fix::tag::MsgType, fix::msg_type::Heartbeat);
    b.add(fix::tag::MsgSeqNum, "not-a-number");
    std::string msg(b.finish());

    fix::Message m;
    std::size_t consumed = 0;
    fix::Message::parse(msg, m, consumed);
    check(!m.seq_num().has_value(), "a non-numeric MsgSeqNum reads as absent, not as zero");
    check(!m.find_int(fix::tag::HeartBtInt).has_value(), "an absent field reads as absent");
}

void a_builder_that_overflows_says_so() {
    // Silently truncating would produce a message with a valid checksum over
    // the wrong bytes, which is worse than producing nothing.
    std::vector<char> buf(32);
    fix::Builder b(buf.data(), buf.size());
    b.begin();
    b.add(fix::tag::MsgType, fix::msg_type::Logon);
    b.add(fix::tag::Text, std::string(200, 'x'));
    check(b.overflowed(), "a builder that runs out of room reports it");
    check(b.finish().empty(), "and returns nothing rather than a truncated message");
}

void flags_are_yes_or_no() {
    std::vector<char> buf(512);
    fix::Builder b(buf.data(), buf.size());
    b.begin();
    b.add(fix::tag::MsgType, fix::msg_type::SequenceReset);
    b.add(fix::tag::MsgSeqNum, std::int64_t{1});
    b.add_bool(fix::tag::GapFillFlag, true);
    b.add_bool(fix::tag::PossDupFlag, false);
    std::string msg(b.finish());

    fix::Message m;
    std::size_t consumed = 0;
    fix::Message::parse(msg, m, consumed);
    check(m.flag(fix::tag::GapFillFlag), "Y reads as true");
    check(!m.flag(fix::tag::PossDupFlag), "N reads as false");
    check(!m.flag(fix::tag::PossResend), "an absent flag reads as false");
}

}  // namespace

int main() {
    round_trip();
    checksum_is_the_real_one();
    body_length_is_the_real_one();
    a_partial_message_asks_for_more();
    two_messages_in_one_buffer();
    a_corrupt_checksum_is_refused();
    a_flipped_payload_byte_is_caught_by_the_checksum();
    a_lying_body_length_is_refused();
    a_wrong_begin_string_is_refused_immediately();
    repeated_tags_are_all_kept();
    a_non_numeric_number_is_not_zero();
    a_builder_that_overflows_says_so();
    flags_are_yes_or_no();

    if (failures != 0) {
        std::cerr << failures << " failure(s)\n";
        return 1;
    }
    std::cout << "all message framing checks passed\n";
    return 0;
}
