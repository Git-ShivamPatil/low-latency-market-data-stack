// FIX 4.4 message framing: parse, build, and refuse to be lied to.
//
// # What this is
//
// A tag=value codec over a caller-owned buffer. Parsing produces views into
// that buffer and never copies a field; building writes into a caller-owned
// buffer and never allocates. Both are what the session layer needs on a path
// that this project has spent three milestones keeping off the heap.
//
// # What "valid" means here, and why it is strict
//
// A FIX message carries two pieces of redundancy: `BodyLength` (9) says how long
// the body is, and `CheckSum` (10) is the sum of every byte before it. Both are
// cheap to verify and both are routinely skipped by implementations that assume
// a well-behaved counterparty.
//
// This one verifies both, because the failure they catch is the one that matters:
// a truncated or concatenated read off a TCP socket. TCP delivers a byte stream,
// not messages. Without `BodyLength` there is no way to know where a message
// ends, and a parser that scans for the next `10=` will happily find one inside a
// text field and produce a message that looks fine and is not.
//
// # Field storage
//
// A fixed-capacity array of (tag, view) pairs, not a map. Two reasons: a map
// allocates, and FIX allows repeated tags — a repeating group is literally the
// same tag appearing again — so a map would silently discard data. `find` returns
// the first occurrence, which is what the session layer wants; `for_each` walks
// them all, which is what a group parser wants.

#pragma once

#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <system_error>

namespace fix {

/// The field separator. FIX calls it SOH and writes it `|` in documentation.
inline constexpr char SOH = '\x01';

inline constexpr std::string_view BEGIN_STRING_44 = "FIX.4.4";

/// Tags this project uses. Not the whole dictionary — the session layer needs a
/// couple of dozen, and an enum of 900 would be noise.
namespace tag {
inline constexpr int BeginString = 8;
inline constexpr int BodyLength = 9;
inline constexpr int MsgType = 35;
inline constexpr int SenderCompID = 49;
inline constexpr int TargetCompID = 56;
inline constexpr int MsgSeqNum = 34;
inline constexpr int SendingTime = 52;
inline constexpr int CheckSum = 10;

inline constexpr int PossDupFlag = 43;
inline constexpr int OrigSendingTime = 122;
inline constexpr int PossResend = 97;

inline constexpr int HeartBtInt = 108;
inline constexpr int TestReqID = 112;
inline constexpr int ResetSeqNumFlag = 141;
inline constexpr int EncryptMethod = 98;

inline constexpr int BeginSeqNo = 7;
inline constexpr int EndSeqNo = 16;
inline constexpr int NewSeqNo = 36;
inline constexpr int GapFillFlag = 123;

// Application layer, milestone 8. Only the tags the order path actually uses;
// an unused constant is a claim that something handles it.
inline constexpr int ClOrdID = 11;
inline constexpr int OrigClOrdID = 41;
inline constexpr int OrderID = 37;
inline constexpr int ExecID = 17;
inline constexpr int ExecType = 150;
inline constexpr int OrdStatus = 39;
inline constexpr int OrdRejReason = 103;
inline constexpr int Symbol = 55;
inline constexpr int Side = 54;
inline constexpr int OrderQty = 38;
inline constexpr int OrdType = 40;
inline constexpr int Price = 44;
inline constexpr int LeavesQty = 151;
inline constexpr int CumQty = 14;
inline constexpr int AvgPx = 6;
inline constexpr int LastPx = 31;
inline constexpr int LastQty = 32;
inline constexpr int TransactTime = 60;

inline constexpr int Text = 58;
inline constexpr int RefSeqNum = 45;
inline constexpr int RefTagID = 371;
inline constexpr int SessionRejectReason = 373;
}  // namespace tag

/// Message types, as the single characters FIX 4.4 uses for session messages.
namespace msg_type {
inline constexpr std::string_view Heartbeat = "0";
inline constexpr std::string_view TestRequest = "1";
inline constexpr std::string_view ResendRequest = "2";
inline constexpr std::string_view Reject = "3";
inline constexpr std::string_view SequenceReset = "4";
inline constexpr std::string_view Logout = "5";
inline constexpr std::string_view Logon = "A";
inline constexpr std::string_view NewOrderSingle = "D";
inline constexpr std::string_view OrderCancelRequest = "F";
inline constexpr std::string_view OrderCancelReject = "9";
inline constexpr std::string_view ExecutionReport = "8";
}  // namespace msg_type

/// Why a buffer was not a message.
///
/// Distinguished rather than collapsed into one "bad message", because the
/// session layer's response differs: an `Incomplete` buffer means read more,
/// while a `BadChecksum` means the stream is corrupt and the session should go
/// down. Treating the first as the second disconnects on every partial read.
enum class ParseError {
    Incomplete,       ///< A complete message has not arrived yet. Read more.
    BadBeginString,   ///< Does not start with `8=FIX.4.4`.
    BadBodyLength,    ///< `9=` missing, unparseable, or pointing somewhere wrong.
    BadCheckSum,      ///< `10=` missing, malformed, or not matching the bytes.
    TooManyFields,    ///< More fields than the fixed capacity holds.
    MalformedField,   ///< A field with no `=`, or an unparseable tag.
};

constexpr std::string_view describe(ParseError e) {
    switch (e) {
        case ParseError::Incomplete:
            return "the buffer does not yet hold a complete message";
        case ParseError::BadBeginString:
            return "the message does not begin with 8=FIX.4.4";
        case ParseError::BadBodyLength:
            return "BodyLength(9) is missing, unparseable, or does not point at the checksum";
        case ParseError::BadCheckSum:
            return "CheckSum(10) is missing, malformed, or does not match the bytes";
        case ParseError::TooManyFields:
            return "the message has more fields than the parser holds";
        case ParseError::MalformedField:
            return "a field is missing its = or carries a non-numeric tag";
    }
    return "unknown";
}

/// Fields a single message may carry.
///
/// A session message needs a dozen. The ceiling is for an application message
/// with a repeating group, and exceeding it is an error rather than a silent
/// truncation — see `ParseError::TooManyFields`.
inline constexpr std::size_t MAX_FIELDS = 256;

/// The FIX checksum: every byte before the checksum field, summed, modulo 256.
inline std::uint8_t checksum(std::string_view bytes) noexcept {
    unsigned sum = 0;
    for (char c : bytes) {
        sum += static_cast<unsigned char>(c);
    }
    return static_cast<std::uint8_t>(sum % 256U);
}

/// One parsed message. Views point into the buffer that was parsed, so it must
/// outlive this.
class Message {
  public:
    struct Field {
        int tag{};
        std::string_view value;
    };

    /// The first value for `t`, or nothing.
    [[nodiscard]] std::optional<std::string_view> find(int t) const noexcept {
        for (std::size_t i = 0; i < count_; ++i) {
            if (fields_[i].tag == t) {
                return fields_[i].value;
            }
        }
        return std::nullopt;
    }

    /// The first value for `t` parsed as an integer, or nothing. A field that is
    /// present but not a number returns nothing rather than zero — the two mean
    /// very different things to a sequence number.
    [[nodiscard]] std::optional<std::int64_t> find_int(int t) const noexcept {
        auto v = find(t);
        if (!v) {
            return std::nullopt;
        }
        std::int64_t out{};
        auto* first = v->data();
        auto* last = v->data() + v->size();
        auto [ptr, ec] = std::from_chars(first, last, out);
        if (ec != std::errc{} || ptr != last) {
            return std::nullopt;
        }
        return out;
    }

    /// Whether `t` is present and equal to "Y". FIX booleans are Y/N.
    [[nodiscard]] bool flag(int t) const noexcept {
        auto v = find(t);
        return v && *v == "Y";
    }

    [[nodiscard]] std::string_view msg_type() const noexcept {
        return find(tag::MsgType).value_or(std::string_view{});
    }

    [[nodiscard]] std::optional<std::int64_t> seq_num() const noexcept {
        return find_int(tag::MsgSeqNum);
    }

    /// Every field, in wire order, including repeats.
    template <typename F>
    void for_each(F&& f) const {
        for (std::size_t i = 0; i < count_; ++i) {
            f(fields_[i].tag, fields_[i].value);
        }
    }

    [[nodiscard]] std::size_t field_count() const noexcept { return count_; }

    /// The whole message including header and trailer, as it appeared.
    [[nodiscard]] std::string_view raw() const noexcept { return raw_; }

    /// Parses one message from the front of `buffer` into `out`.
    ///
    /// Returns nothing on success and the reason on failure. On success
    /// `consumed` is set to the message's length, so a caller draining a TCP
    /// stream advances by exactly that and re-parses the remainder.
    ///
    /// Parses *into* an existing `Message` rather than returning one: the field
    /// array is a few kilobytes, and the session layer parses one message per
    /// read into the same object rather than paying to move that per message.
    static std::optional<ParseError> parse(std::string_view buffer, Message& out,
                                           std::size_t& consumed) noexcept;

  private:
    std::array<Field, MAX_FIELDS> fields_{};
    std::size_t count_{};
    std::string_view raw_;
};

/// Builds a message into a caller-owned buffer. Never allocates.
///
/// `BodyLength` and `CheckSum` cannot be written until the body is known, so the
/// builder reserves space for the first and appends the second in `finish()`.
class Builder {
  public:
    Builder(char* buf, std::size_t cap) noexcept : buf_(buf), cap_(cap) {}

    /// Starts a message. Writes `8=` and `9=` with the length left blank.
    ///
    /// The blank is a fixed six digits rather than a variable width, because
    /// rewriting it later must not move the body. Six digits covers any message
    /// this project sends and FIX permits leading zeros in `BodyLength`.
    bool begin(std::string_view begin_string = BEGIN_STRING_44) noexcept;

    bool add(int t, std::string_view value) noexcept;
    bool add(int t, std::int64_t value) noexcept;
    bool add_bool(int t, bool value) noexcept { return add(t, value ? "Y" : "N"); }

    /// Back-fills `BodyLength`, appends `CheckSum`, and returns the message.
    /// Empty if anything overflowed.
    [[nodiscard]] std::string_view finish() noexcept;

    [[nodiscard]] bool overflowed() const noexcept { return overflow_; }

  private:
    bool put(std::string_view s) noexcept;
    bool put(char c) noexcept;

    char* buf_;
    std::size_t cap_;
    std::size_t len_{};
    std::size_t body_length_pos_{};  ///< Where the six blank digits sit.
    std::size_t body_start_{};       ///< First byte counted by BodyLength.
    bool overflow_{false};
    bool begun_{false};
};

}  // namespace fix
