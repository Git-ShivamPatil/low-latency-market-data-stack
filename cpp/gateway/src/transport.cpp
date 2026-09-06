#include "fix/transport.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace fix {
namespace {

/// Nagle batches small writes, which is exactly wrong for a protocol whose
/// messages are small and whose latency matters.
void disable_nagle(int fd) {
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

}  // namespace

Connection::~Connection() { close(); }

Connection::Connection(Connection&& other) noexcept
    : fd_(other.fd_), inbox_(std::move(other.inbox_)) {
    other.fd_ = -1;
}

Connection& Connection::operator=(Connection&& other) noexcept {
    if (this != &other) {
        close();
        fd_ = other.fd_;
        inbox_ = std::move(other.inbox_);
        other.fd_ = -1;
    }
    return *this;
}

void Connection::close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

bool Connection::write_all(std::string_view bytes) {
    if (fd_ < 0) {
        return false;
    }
    std::size_t sent = 0;
    while (sent < bytes.size()) {
        ssize_t n = ::send(fd_, bytes.data() + sent, bytes.size() - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false;
        }
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

bool Connection::read_some(int timeout_ms) {
    if (fd_ < 0) {
        return false;
    }
    struct pollfd p {};
    p.fd = fd_;
    p.events = POLLIN;
    int ready = ::poll(&p, 1, timeout_ms);
    if (ready < 0) {
        return errno == EINTR;  // interrupted, not broken
    }
    if (ready == 0) {
        return true;  // nothing to read, which is not an error
    }

    char buf[8192];
    ssize_t n = ::recv(fd_, buf, sizeof(buf), 0);
    if (n < 0) {
        return errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK;
    }
    if (n == 0) {
        return false;  // the peer closed
    }
    inbox_.append(buf, static_cast<std::size_t>(n));
    return true;
}

bool Connection::drain(const std::function<void(const Message&)>& on_message) {
    Message m;
    std::size_t offset = 0;
    while (offset < inbox_.size()) {
        std::size_t consumed = 0;
        auto err = Message::parse(std::string_view(inbox_).substr(offset), m, consumed);
        if (err == ParseError::Incomplete) {
            break;  // the rest has not arrived
        }
        if (err) {
            // Not a short read — the stream is not FIX any more. There is no
            // way to find where the next message starts, so the caller drops
            // the session rather than guessing.
            return false;
        }
        on_message(m);
        offset += consumed;
    }
    if (offset > 0) {
        inbox_.erase(0, offset);
    }
    return true;
}

Connection connect_to(const std::string& host, std::uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return {};
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        ::close(fd);
        return {};
    }
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return {};
    }
    disable_nagle(fd);
    return Connection(fd);
}

Connection accept_one(std::uint16_t port, int timeout_ms) {
    int listener = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listener < 0) {
        return {};
    }
    int one = 1;
    // Without this a restart within the TIME_WAIT window cannot rebind, which
    // is precisely what the kill-restart test does.
    ::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
        ::listen(listener, 1) != 0) {
        ::close(listener);
        return {};
    }

    struct pollfd p {};
    p.fd = listener;
    p.events = POLLIN;
    int ready = ::poll(&p, 1, timeout_ms);
    if (ready <= 0) {
        ::close(listener);
        return {};
    }
    int fd = ::accept(listener, nullptr, nullptr);
    ::close(listener);
    if (fd < 0) {
        return {};
    }
    disable_nagle(fd);
    return Connection(fd);
}

}  // namespace fix
