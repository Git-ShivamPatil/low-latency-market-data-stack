#include "fix/message.hpp"

#include <algorithm>

namespace fix {
namespace {

/// Six digits, zero-padded. See `Builder::begin` for why the width is fixed.
void write_six(char* dst, std::size_t value) noexcept {
    for (int i = 5; i >= 0; --i) {
        dst[i] = static_cast<char>('0' + static_cast<char>(value % 10U));
        value /= 10U;
    }
}

}  // namespace

std::optional<ParseError> Message::parse(std::string_view buffer, Message& out,
                                         std::size_t& consumed) noexcept {
    out.count_ = 0;
    out.raw_ = {};
    consumed = 0;

    // 8=FIX.4.4<SOH> is ten bytes; anything shorter cannot be judged yet.
    constexpr std::string_view prefix = "8=";
    if (buffer.size() < prefix.size() + BEGIN_STRING_44.size() + 1) {
        // Distinguish "not yet" from "never": if what we do have already
        // disagrees with the prefix, more bytes will not help.
        std::size_t n = std::min(buffer.size(), prefix.size() + BEGIN_STRING_44.size());
        std::string_view expect = "8=FIX.4.4";
        if (buffer.substr(0, n) != expect.substr(0, n)) {
            return ParseError::BadBeginString;
        }
        return ParseError::Incomplete;
    }
    if (buffer.substr(0, 9) != "8=FIX.4.4" || buffer[9] != SOH) {
        return ParseError::BadBeginString;
    }

    // 9=<len><SOH> must come second. The spec requires it, and the whole framing
    // rests on it: without BodyLength there is no way to know where the message
    // ends, and scanning for the next "10=" finds one inside a text field.
    std::size_t pos = 10;
    // "Not yet" and "never" have to stay distinct all the way down, not just at
    // the BeginString. A buffer holding exactly `8=FIX.4.4<SOH>` is a perfectly
    // ordinary short read, and an earlier version of this reported it as a bad
    // BodyLength — which would have dropped the session on a TCP boundary that
    // happened to land in the wrong place.
    {
        constexpr std::string_view nine = "9=";
        std::string_view have = buffer.substr(pos, nine.size());
        if (have.size() < nine.size()) {
            return nine.starts_with(have) ? ParseError::Incomplete
                                          : ParseError::BadBodyLength;
        }
        if (have != nine) {
            return ParseError::BadBodyLength;
        }
        pos += nine.size();
    }

    std::size_t body_len{};
    {
        auto* first = buffer.data() + pos;
        auto* last = buffer.data() + buffer.size();
        auto [ptr, ec] = std::from_chars(first, last, body_len);
        if (ec != std::errc{}) {
            // No digits at all. If the buffer simply ended, the digits have not
            // arrived; if something else is sitting there, they never will.
            return (first == last) ? ParseError::Incomplete : ParseError::BadBodyLength;
        }
        if (ptr == last) {
            // Digits so far, but the terminating SOH has not arrived, so the
            // number may not be finished.
            return ParseError::Incomplete;
        }
        if (*ptr != SOH) {
            return ParseError::BadBodyLength;
        }
        pos = static_cast<std::size_t>(ptr - buffer.data()) + 1;
    }

    // BodyLength counts from here to the byte before "10=".
    std::size_t body_start = pos;
    std::size_t checksum_pos = body_start + body_len;
    // "10=xxx<SOH>" is seven bytes.
    constexpr std::size_t CHECKSUM_FIELD_LEN = 7;
    if (checksum_pos + CHECKSUM_FIELD_LEN > buffer.size()) {
        return ParseError::Incomplete;
    }
    if (buffer.substr(checksum_pos, 3) != "10=") {
        // BodyLength pointed somewhere that is not the checksum field. The
        // message is not merely short — it disagrees with itself.
        return ParseError::BadBodyLength;
    }
    if (buffer[checksum_pos + 6] != SOH) {
        return ParseError::BadCheckSum;
    }

    // The checksum covers everything before the checksum field.
    {
        auto declared = buffer.substr(checksum_pos + 3, 3);
        unsigned stated{};
        auto* first = declared.data();
        auto* last = declared.data() + declared.size();
        auto [ptr, ec] = std::from_chars(first, last, stated);
        if (ec != std::errc{} || ptr != last) {
            return ParseError::BadCheckSum;
        }
        if (checksum(buffer.substr(0, checksum_pos)) != static_cast<std::uint8_t>(stated)) {
            return ParseError::BadCheckSum;
        }
    }

    consumed = checksum_pos + CHECKSUM_FIELD_LEN;
    out.raw_ = buffer.substr(0, consumed);

    // Every field, header and trailer included, so `find(8)` and `find(10)` work
    // and a re-serialiser can round-trip what it was given.
    std::size_t cursor = 0;
    while (cursor < consumed) {
        std::size_t soh = out.raw_.find(SOH, cursor);
        if (soh == std::string_view::npos) {
            return ParseError::MalformedField;
        }
        std::string_view field = out.raw_.substr(cursor, soh - cursor);
        std::size_t eq = field.find('=');
        if (eq == std::string_view::npos || eq == 0) {
            return ParseError::MalformedField;
        }
        int t{};
        auto* first = field.data();
        auto* last = field.data() + eq;
        auto [ptr, ec] = std::from_chars(first, last, t);
        if (ec != std::errc{} || ptr != last) {
            return ParseError::MalformedField;
        }
        if (out.count_ == MAX_FIELDS) {
            return ParseError::TooManyFields;
        }
        out.fields_[out.count_++] = Field{t, field.substr(eq + 1)};
        cursor = soh + 1;
    }

    return std::nullopt;
}

bool Builder::put(std::string_view s) noexcept {
    if (overflow_ || len_ + s.size() > cap_) {
        overflow_ = true;
        return false;
    }
    for (char c : s) {
        buf_[len_++] = c;
    }
    return true;
}

bool Builder::put(char c) noexcept {
    if (overflow_ || len_ + 1 > cap_) {
        overflow_ = true;
        return false;
    }
    buf_[len_++] = c;
    return true;
}

bool Builder::begin(std::string_view begin_string) noexcept {
    len_ = 0;
    overflow_ = false;
    put("8=");
    put(begin_string);
    put(SOH);
    put("9=");
    body_length_pos_ = len_;
    // Six blanks, back-filled in finish(). Fixed width so that back-filling
    // cannot move the body: a variable-width length would have to be written
    // before the body is known, which it is not.
    put("000000");
    put(SOH);
    body_start_ = len_;
    begun_ = !overflow_;
    return begun_;
}

bool Builder::add(int t, std::string_view value) noexcept {
    char num[16];
    auto [ptr, ec] = std::to_chars(num, num + sizeof(num), t);
    if (ec != std::errc{}) {
        overflow_ = true;
        return false;
    }
    put(std::string_view(num, static_cast<std::size_t>(ptr - num)));
    put('=');
    put(value);
    put(SOH);
    return !overflow_;
}

bool Builder::add(int t, std::int64_t value) noexcept {
    char num[24];
    auto [ptr, ec] = std::to_chars(num, num + sizeof(num), value);
    if (ec != std::errc{}) {
        overflow_ = true;
        return false;
    }
    return add(t, std::string_view(num, static_cast<std::size_t>(ptr - num)));
}

std::string_view Builder::finish() noexcept {
    if (!begun_ || overflow_) {
        return {};
    }
    write_six(buf_ + body_length_pos_, len_ - body_start_);

    std::uint8_t ck = checksum(std::string_view(buf_, len_));
    char num[4];
    // Always three digits, zero-padded. A two-digit checksum is a message some
    // counterparties accept and others reject, which is the worst of both.
    num[0] = static_cast<char>('0' + static_cast<char>(ck / 100U));
    num[1] = static_cast<char>('0' + static_cast<char>((ck / 10U) % 10U));
    num[2] = static_cast<char>('0' + static_cast<char>(ck % 10U));
    num[3] = '\0';
    put("10=");
    put(std::string_view(num, 3));
    put(SOH);
    if (overflow_) {
        return {};
    }
    return std::string_view(buf_, len_);
}

}  // namespace fix
