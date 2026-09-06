// TCP for a FIX session: accept, connect, and turn a byte stream into messages.
//
// # Why the buffer is the interesting part
//
// TCP delivers bytes. A `read()` can return half a message, three messages, or
// two and a half. The session layer above needs whole messages and nothing else,
// so this accumulates and drains: append what arrived, parse as many complete
// messages as the buffer holds, keep the remainder for next time.
//
// The codec already distinguishes `Incomplete` from corruption, which is what
// makes that loop safe. A parser that could not tell them apart would either
// drop the session on every short read or spin on a corrupt one.

#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

#include "fix/message.hpp"

namespace fix {

/// A connected FIX peer.
class Connection {
  public:
    Connection() = default;
    explicit Connection(int fd) : fd_(fd) {}
    ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    Connection(Connection&& other) noexcept;
    Connection& operator=(Connection&& other) noexcept;

    [[nodiscard]] bool open() const noexcept { return fd_ >= 0; }
    [[nodiscard]] int fd() const noexcept { return fd_; }

    /// Writes `bytes` in full. False on any error, at which point the session is
    /// over — a partial FIX message on the wire cannot be recovered from.
    bool write_all(std::string_view bytes);

    /// Reads whatever is available and appends it to the internal buffer.
    /// Returns false on EOF or error.
    ///
    /// `timeout_ms` of 0 polls without blocking, which is what a loop that also
    /// has to run timers needs.
    bool read_some(int timeout_ms);

    /// Parses every complete message in the buffer, calling `on_message` for
    /// each, and keeps the remainder.
    ///
    /// Returns false if the stream stopped being FIX — the caller should drop
    /// the session rather than resynchronise, because there is no way to know
    /// where the next message starts.
    bool drain(const std::function<void(const Message&)>& on_message);

    void close();

  private:
    int fd_{-1};
    std::string inbox_;
};

/// Connects to `host:port`. Returns a closed Connection on failure.
Connection connect_to(const std::string& host, std::uint16_t port);

/// Listens on `port` and accepts one connection.
///
/// One at a time, deliberately: a FIX session is a long-lived relationship
/// between two named parties, not a request/response service, and the multiple
/// concurrent sessions a real deployment needs are out of scope. See
/// `docs/PROTOCOL.md`.
Connection accept_one(std::uint16_t port, int timeout_ms);

}  // namespace fix
