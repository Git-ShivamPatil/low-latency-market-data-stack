#include "fix/seqstore.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <cstring>

namespace fix {
namespace {

/// Slots sit on separate 512-byte boundaries so a torn write to one cannot
/// reach the other. 512 is the smallest sector any disk this runs on presents;
/// a larger real sector only makes the separation safer, never worse.
constexpr off_t SLOT_SIZE = 512;
constexpr off_t SLOT_OFFSET[2] = {0, SLOT_SIZE};
constexpr std::uint64_t MAGIC = 0x4649585345510001ULL;  // "FIXSEQ" + version

struct Record {
    std::uint64_t magic;
    std::uint64_t generation;
    std::uint64_t outbound;
    std::uint64_t inbound;
    std::uint64_t checksum;
};

/// FNV-1a over the record's first four fields. Not cryptographic — this detects
/// a half-written record, not an adversary.
std::uint64_t compute_checksum(const Record& r) noexcept {
    std::uint64_t h = 0xcbf29ce484222325ULL;
    const std::uint64_t parts[4] = {r.magic, r.generation, r.outbound, r.inbound};
    const auto* bytes = reinterpret_cast<const unsigned char*>(parts);
    for (std::size_t i = 0; i < sizeof(parts); ++i) {
        h ^= bytes[i];
        h *= 0x100000001b3ULL;
    }
    return h;
}

}  // namespace

std::string_view describe(StoreError e) {
    switch (e) {
        case StoreError::CannotOpen:
            return "the sequence file could not be opened";
        case StoreError::CannotRead:
            return "the sequence file could not be read";
        case StoreError::CannotWrite:
            return "the sequence file could not be written";
        case StoreError::CannotSync:
            return "the sequence file could not be fsync'd, so nothing may be sent";
        case StoreError::Corrupt:
            return "both slots failed their checksum; the sequence state is unknown";
    }
    return "unknown";
}

SeqStore::~SeqStore() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

std::optional<StoreError> SeqStore::open(const std::string& path) {
    // Created explicitly rather than with a bare O_CREAT, because a file that
    // did not exist a moment ago needs its **directory entry** made durable as
    // well as its contents.
    //
    // Without that, every record can be fsync'd and the file can still vanish on
    // a power cut, because the entry naming it was never written -- which would
    // take this whole design down with it: a session that comes back with no
    // sequence file at all restarts at 1, and the counterparty reads that as a
    // reversal. It is the least intuitive line in the durability story and the
    // one most often left out. It was left out here too, until a review of the
    // milestone-8 code found the same omission in its sibling.
    fd_ = ::open(path.c_str(), O_RDWR);
    if (fd_ < 0) {
        fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0644);
        if (fd_ < 0) {
            // Somebody else created it in between. Not an error.
            fd_ = ::open(path.c_str(), O_RDWR);
            if (fd_ < 0) {
                return StoreError::CannotOpen;
            }
        } else {
            const std::size_t slash = path.find_last_of('/');
            const std::string dir =
                (slash == std::string::npos) ? std::string(".") : path.substr(0, slash);
            const int dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
            if (dfd >= 0) {
                ::fsync(dfd);
                ::close(dfd);
            }
        }
    }

    // Read both slots and take the valid one with the higher generation.
    Record best{};
    bool found = false;
    int seen = 0;
    for (int slot = 0; slot < 2; ++slot) {
        Record r{};
        ssize_t n = ::pread(fd_, &r, sizeof(r), SLOT_OFFSET[slot]);
        if (n != static_cast<ssize_t>(sizeof(r))) {
            // A short read is a slot that was never written, which is normal for
            // a new file and not a torn write.
            continue;
        }
        ++seen;
        if (r.magic != MAGIC || r.checksum != compute_checksum(r)) {
            // Written at some point but not intact. This is the case the second
            // slot exists for.
            ++torn_recovered_;
            continue;
        }
        if (!found || r.generation > best.generation) {
            best = r;
            found = true;
        }
    }

    if (!found) {
        if (seen > 0 && torn_recovered_ == seen) {
            // Every slot that exists is damaged. Guessing here would mean
            // inventing a sequence number, which is the one thing that must
            // never happen.
            return StoreError::Corrupt;
        }
        // A fresh file. FIX starts a new session at 1 in both directions.
        seq_ = SeqPair{};
        generation_ = 0;
        return std::nullopt;
    }

    seq_ = SeqPair{best.outbound, best.inbound};
    generation_ = best.generation;
    return std::nullopt;
}

std::optional<StoreError> SeqStore::write_slot(int slot, std::uint64_t generation, SeqPair v) {
    Record r{};
    r.magic = MAGIC;
    r.generation = generation;
    r.outbound = v.outbound;
    r.inbound = v.inbound;
    r.checksum = compute_checksum(r);

    ssize_t n = ::pwrite(fd_, &r, sizeof(r), SLOT_OFFSET[slot]);
    if (n != static_cast<ssize_t>(sizeof(r))) {
        return StoreError::CannotWrite;
    }
    // The whole guarantee. Without this the write sits in the page cache and a
    // power loss or a SIGKILL of the machine takes it with them.
    if (::fsync(fd_) != 0) {
        return StoreError::CannotSync;
    }
    ++syncs_;
    return std::nullopt;
}

std::optional<StoreError> SeqStore::store(SeqPair next) {
    if (fd_ < 0) {
        return StoreError::CannotOpen;
    }
    // Alternate slots so a torn write never damages the record we would fall
    // back to. Generation 1 goes to slot 1, 2 to slot 0, and so on; the reader
    // picks by generation, not by position.
    std::uint64_t generation = generation_ + 1;
    int slot = static_cast<int>(generation % 2);
    if (auto err = write_slot(slot, generation, next)) {
        return err;
    }
    seq_ = next;
    generation_ = generation;
    return std::nullopt;
}

std::optional<std::uint64_t> SeqStore::claim_outbound() {
    SeqPair next = seq_;
    std::uint64_t claimed = next.outbound;
    next.outbound += 1;
    if (store(next)) {
        // Persisting failed, so the number was never durable. The caller must
        // not send: doing so would consume a sequence number that a restart
        // would hand out again.
        return std::nullopt;
    }
    return claimed;
}

std::optional<StoreError> SeqStore::accept_inbound(std::uint64_t seq) {
    SeqPair next = seq_;
    next.inbound = seq + 1;
    return store(next);
}

std::optional<StoreError> SeqStore::reset() {
    return store(SeqPair{1, 1});
}

}  // namespace fix
