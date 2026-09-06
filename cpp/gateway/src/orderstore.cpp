#include "fix/orderstore.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>

namespace fix {

namespace {

/// "ORD!" — so `xxd` on the file says what it is.
constexpr std::uint32_t kMagic = 0x21445241;

// Byte layout of a record, written out once here and nowhere else. This log is
// read and written only by this file, in one language, so it does not go
// through the schema -- the schema exists for layouts that cross a boundary,
// and inventing one here would be ceremony rather than safety. `SeqStore` makes
// the same call for the same reason.
constexpr std::size_t kOffMagic = 0;
constexpr std::size_t kOffChecksum = 4;
constexpr std::size_t kOffClientOrderId = 8;
constexpr std::size_t kOffExchangeOrderId = 16;
constexpr std::size_t kOffPrice = 24;
constexpr std::size_t kOffQuantity = 32;
constexpr std::size_t kOffLeaves = 36;
constexpr std::size_t kOffSymbolId = 40;
constexpr std::size_t kOffSide = 42;
constexpr std::size_t kOffState = 43;
constexpr std::size_t kOffSequence = 48;
// 56..64 is reserved and written as zero.

/// FNV-1a over everything except the checksum field itself.
std::uint32_t checksum(const unsigned char* rec) {
    std::uint32_t h = 0x811c9dc5U;
    for (std::size_t i = 0; i < kOrderRecordSize; ++i) {
        if (i >= kOffChecksum && i < kOffChecksum + 4) {
            continue;
        }
        h ^= rec[i];
        h *= 0x01000193U;
    }
    return h;
}

template <typename T>
void put(unsigned char* p, std::size_t off, T v) {
    std::memcpy(p + off, &v, sizeof v);
}

template <typename T>
T get(const unsigned char* p, std::size_t off) {
    T v{};
    std::memcpy(&v, p + off, sizeof v);
    return v;
}

bool write_all(int fd, const unsigned char* buf, std::size_t n) {
    std::size_t done = 0;
    while (done < n) {
        const ssize_t w = ::write(fd, buf + done, n - done);
        if (w <= 0) {
            return false;
        }
        done += static_cast<std::size_t>(w);
    }
    return true;
}

}  // namespace

std::string_view describe(OrderState s) {
    switch (s) {
        case OrderState::Pending: return "pending";
        case OrderState::Live: return "live";
        case OrderState::Filled: return "filled";
        case OrderState::Canceled: return "canceled";
        case OrderState::Rejected: return "rejected";
    }
    return "unknown";
}

OrderStore::~OrderStore() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

std::optional<StoreError> OrderStore::open(
    const std::string& path, const std::function<void(const OrderRecord&)>& on_live) {
    path_ = path;
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT, 0600);
    if (fd_ < 0) {
        return StoreError::CannotOpen;
    }
    if (auto e = replay()) {
        return e;
    }
    // Compaction happens before the caller sees anything, so a crash during it
    // leaves either the old log or the new one and never a caller acting on a
    // half-rewritten view.
    if (auto e = compact()) {
        return e;
    }
    if (on_live) {
        for (const auto& r : live_) {
            on_live(r);
        }
    }
    return std::nullopt;
}

void OrderStore::apply(const OrderRecord& r) {
    auto it = std::find_if(live_.begin(), live_.end(), [&](const OrderRecord& x) {
        return x.client_order_id == r.client_order_id;
    });
    if (r.is_terminal()) {
        if (it != live_.end()) {
            live_.erase(it);
        }
        return;
    }
    if (it == live_.end()) {
        live_.push_back(r);
    } else {
        *it = r;
    }
}

std::optional<StoreError> OrderStore::replay() {
    if (::lseek(fd_, 0, SEEK_SET) < 0) {
        return StoreError::CannotRead;
    }
    live_.clear();
    replayed_ = 0;
    torn_tail_ = 0;

    const off_t end = ::lseek(fd_, 0, SEEK_END);
    if (end < 0 || ::lseek(fd_, 0, SEEK_SET) < 0) {
        return StoreError::CannotRead;
    }
    const auto file_size = static_cast<std::uint64_t>(end);

    unsigned char rec[kOrderRecordSize];
    std::uint64_t expect_sequence = 1;
    std::uint64_t offset = 0;
    for (;;) {
        std::size_t got = 0;
        bool eof = false;
        while (got < kOrderRecordSize) {
            const ssize_t n = ::read(fd_, rec + got, kOrderRecordSize - got);
            if (n < 0) {
                return StoreError::CannotRead;
            }
            if (n == 0) {
                eof = true;
                break;
            }
            got += static_cast<std::size_t>(n);
        }
        if (eof && got == 0) {
            break;  // a clean end
        }
        if (eof || got < kOrderRecordSize) {
            // A short read at the end of the file. The record was never
            // fsync'd, so the message it describes was never sent, so
            // discarding it is exactly right.
            torn_tail_ += got;
            break;
        }
        offset += kOrderRecordSize;
        if (get<std::uint32_t>(rec, kOffMagic) != kMagic ||
            get<std::uint32_t>(rec, kOffChecksum) != checksum(rec)) {
            if (offset < file_size) {
                // A bad record with good records *after* it. That is not a torn
                // tail -- a tail is by definition the end -- it is damage in
                // the middle of the file, and stopping here quietly would drop
                // every order after it while reporting success. Those orders
                // are live at the exchange.
                return StoreError::Corrupt;
            }
            // The last record does not verify. An fsync interrupted after the
            // bytes were queued but before they landed can leave a full-length
            // record of rubbish, and the message it described was never sent.
            // Counted rather than ignored.
            torn_tail_ += kOrderRecordSize;
            break;
        }
        const auto seq = get<std::uint64_t>(rec, kOffSequence);
        if (seq != expect_sequence) {
            // A gap in the middle means the file is not the sequence of writes
            // this process made. That is not a torn tail and must not be
            // guessed past.
            return StoreError::Corrupt;
        }
        ++expect_sequence;

        OrderRecord r;
        r.client_order_id = get<std::uint64_t>(rec, kOffClientOrderId);
        r.exchange_order_id = get<std::uint64_t>(rec, kOffExchangeOrderId);
        r.price = get<std::int64_t>(rec, kOffPrice);
        r.quantity = get<std::uint32_t>(rec, kOffQuantity);
        r.leaves_quantity = get<std::uint32_t>(rec, kOffLeaves);
        r.symbol_id = get<std::uint16_t>(rec, kOffSymbolId);
        r.side = static_cast<mdstack::wire::Side>(get<std::uint8_t>(rec, kOffSide));
        r.state = static_cast<OrderState>(get<std::uint8_t>(rec, kOffState));
        apply(r);
        ++replayed_;
    }
    return std::nullopt;
}

std::optional<StoreError> OrderStore::compact() {
    if (replayed_ == live_.size() && torn_tail_ == 0) {
        // Nothing to gain, and rewriting a file for no reason is a chance to
        // lose it. Seek to the end and carry on appending.
        if (::lseek(fd_, 0, SEEK_END) < 0) {
            return StoreError::CannotWrite;
        }
        return std::nullopt;
    }
    compacted_ = replayed_ - live_.size();

    const std::string tmp = path_ + ".compact";
    const int out = ::open(tmp.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (out < 0) {
        return StoreError::CannotWrite;
    }
    std::uint64_t seq = 1;
    for (const auto& r : live_) {
        unsigned char rec[kOrderRecordSize];
        std::memset(rec, 0, sizeof rec);
        put<std::uint32_t>(rec, kOffMagic, kMagic);
        put<std::uint64_t>(rec, kOffClientOrderId, r.client_order_id);
        put<std::uint64_t>(rec, kOffExchangeOrderId, r.exchange_order_id);
        put<std::int64_t>(rec, kOffPrice, r.price);
        put<std::uint32_t>(rec, kOffQuantity, r.quantity);
        put<std::uint32_t>(rec, kOffLeaves, r.leaves_quantity);
        put<std::uint16_t>(rec, kOffSymbolId, r.symbol_id);
        put<std::uint8_t>(rec, kOffSide, static_cast<std::uint8_t>(r.side));
        put<std::uint8_t>(rec, kOffState, static_cast<std::uint8_t>(r.state));
        put<std::uint64_t>(rec, kOffSequence, seq++);
        put<std::uint32_t>(rec, kOffChecksum, checksum(rec));
        if (!write_all(out, rec, sizeof rec)) {
            ::close(out);
            ::unlink(tmp.c_str());
            return StoreError::CannotWrite;
        }
    }
    if (::fsync(out) != 0) {
        ::close(out);
        ::unlink(tmp.c_str());
        return StoreError::CannotSync;
    }
    ::close(out);

    // Rename, then fsync the *directory*. Without the second fsync the rename
    // itself may not survive a power cut, and the whole point of the first one
    // is lost.
    if (::rename(tmp.c_str(), path_.c_str()) != 0) {
        ::unlink(tmp.c_str());
        return StoreError::CannotWrite;
    }
    const std::size_t slash = path_.find_last_of('/');
    const std::string dir = (slash == std::string::npos) ? std::string(".")
                                                         : path_.substr(0, slash);
    const int dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dfd >= 0) {
        ::fsync(dfd);
        ::close(dfd);
    }

    ::close(fd_);
    fd_ = ::open(path_.c_str(), O_RDWR, 0600);
    if (fd_ < 0) {
        return StoreError::CannotOpen;
    }
    if (::lseek(fd_, 0, SEEK_END) < 0) {
        return StoreError::CannotWrite;
    }
    // The log now holds exactly the live set, one record each, numbered from 1,
    // so the next appended record continues from there.
    replayed_ = live_.size();
    // `torn_tail_` is deliberately NOT cleared. It reports what was found at
    // open, not what is in the file now -- and it is the single most useful
    // line in a post-mortem, because it says the last run was killed mid-write.
    // Compaction erasing it would make the evidence disappear at exactly the
    // moment somebody needs it.
    return std::nullopt;
}

std::optional<StoreError> OrderStore::record(const OrderRecord& r) {
    unsigned char rec[kOrderRecordSize];
    std::memset(rec, 0, sizeof rec);
    put<std::uint32_t>(rec, kOffMagic, kMagic);
    put<std::uint64_t>(rec, kOffClientOrderId, r.client_order_id);
    put<std::uint64_t>(rec, kOffExchangeOrderId, r.exchange_order_id);
    put<std::int64_t>(rec, kOffPrice, r.price);
    put<std::uint32_t>(rec, kOffQuantity, r.quantity);
    put<std::uint32_t>(rec, kOffLeaves, r.leaves_quantity);
    put<std::uint16_t>(rec, kOffSymbolId, r.symbol_id);
    put<std::uint8_t>(rec, kOffSide, static_cast<std::uint8_t>(r.side));
    put<std::uint8_t>(rec, kOffState, static_cast<std::uint8_t>(r.state));
    put<std::uint64_t>(rec, kOffSequence, replayed_ + written_ + 1);
    put<std::uint32_t>(rec, kOffChecksum, checksum(rec));

    if (!write_all(fd_, rec, sizeof rec)) {
        return StoreError::CannotWrite;
    }
    // Before the caller sends anything. This is the expensive line and it is
    // the one the design is about.
    if (::fsync(fd_) != 0) {
        return StoreError::CannotSync;
    }
    ++written_;
    ++syncs_;
    apply(r);
    return std::nullopt;
}

}  // namespace fix
