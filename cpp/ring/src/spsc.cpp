#include "ring/spsc.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <utility>

namespace mdstack::ring {

namespace hdr = wire::layout::ring_header;

std::string_view describe(RingError e) {
    switch (e) {
        case RingError::CapacityZero: return "capacity must be at least 1";
        case RingError::CapacityNotPowerOfTwo: return "capacity is not a power of two";
        case RingError::BadSlotSize:
            return "slot size must be a multiple of 64 and larger than the slot prefix";
        case RingError::BadMagic: return "not a ring: the magic does not match";
        case RingError::BadVersion: return "a ring layout version this build does not speak";
        case RingError::Truncated: return "the file is shorter than its own header describes";
        case RingError::CannotOpen: return "could not open the ring file";
        case RingError::CannotSize: return "could not size the ring file";
        case RingError::CannotMap: return "could not map the ring file";
    }
    return "unknown ring error";
}

std::string_view describe(PushError e) {
    switch (e) {
        case PushError::Full: return "the ring is full";
        case PushError::TooLarge: return "the message does not fit a slot";
    }
    return "unknown push error";
}

namespace {

std::optional<RingError> check_geometry(std::uint32_t capacity, std::uint32_t slot_size) {
    if (capacity == 0) {
        return RingError::CapacityZero;
    }
    if ((capacity & (capacity - 1)) != 0) {
        return RingError::CapacityNotPowerOfTwo;
    }
    const auto s = static_cast<std::size_t>(slot_size);
    if (s % kSlotAlign != 0 || s <= wire::layout::ring_slot::kLen) {
        return RingError::BadSlotSize;
    }
    return std::nullopt;
}

/// Opens `path`, sizing it to `len` when `create` is set, and maps it shared.
/// On success `out` is the mapping and the descriptor is already closed --
/// the mapping outlives it, which is what MAP_SHARED means.
std::optional<RingError> map_file(const std::string& path, std::size_t len, bool create,
                                  std::byte*& out) {
    const int flags = create ? (O_RDWR | O_CREAT) : O_RDWR;
    const int fd = ::open(path.c_str(), flags, 0600);
    if (fd < 0) {
        return RingError::CannotOpen;
    }

    std::optional<RingError> err;
    void* addr = MAP_FAILED;
    if (create && ::ftruncate(fd, static_cast<off_t>(len)) != 0) {
        err = RingError::CannotSize;
    }
    if (!err) {
        struct ::stat st {};
        if (::fstat(fd, &st) != 0) {
            err = RingError::CannotSize;
        } else if (static_cast<std::size_t>(st.st_size) < len) {
            err = RingError::Truncated;
        }
    }
    if (!err) {
        addr = ::mmap(nullptr, len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (addr == MAP_FAILED) {
            err = RingError::CannotMap;
        }
    }
    ::close(fd);
    if (err) {
        return err;
    }
    out = static_cast<std::byte*>(addr);
    return std::nullopt;
}

}  // namespace

Ring::~Ring() { close(); }

void Ring::close() noexcept {
    if (base_ != nullptr) {
        ::munmap(base_, mapped_);
        base_ = nullptr;
        mapped_ = 0;
        capacity_ = 0;
        slot_size_ = 0;
        cached_read_ = 0;
        cached_write_ = 0;
    }
}

Ring::Ring(Ring&& other) noexcept
    : cached_read_(std::exchange(other.cached_read_, 0)),
      cached_write_(std::exchange(other.cached_write_, 0)),
      base_(std::exchange(other.base_, nullptr)),
      mapped_(std::exchange(other.mapped_, 0)),
      capacity_(std::exchange(other.capacity_, 0)),
      slot_size_(std::exchange(other.slot_size_, 0)) {}

Ring& Ring::operator=(Ring&& other) noexcept {
    if (this != &other) {
        close();
        cached_read_ = std::exchange(other.cached_read_, 0);
        cached_write_ = std::exchange(other.cached_write_, 0);
        base_ = std::exchange(other.base_, nullptr);
        mapped_ = std::exchange(other.mapped_, 0);
        capacity_ = std::exchange(other.capacity_, 0);
        slot_size_ = std::exchange(other.slot_size_, 0);
    }
    return *this;
}

std::optional<RingError> Ring::create(const std::string& path, std::uint32_t capacity,
                                      std::uint32_t slot_size) {
    if (auto bad = check_geometry(capacity, slot_size)) {
        return bad;
    }
    close();
    const std::size_t len = file_size(capacity, slot_size);
    std::byte* addr = nullptr;
    if (auto e = map_file(path, len, true, addr)) {
        return e;
    }
    base_ = addr;
    mapped_ = len;
    capacity_ = capacity;
    slot_size_ = slot_size;

    // Zero the header first so a reopened file cannot carry an old index into a
    // new ring.
    std::memset(base_, 0, hdr::kLen);
    store_u64(hdr::kMagic, kRingMagic);
    store_u32(hdr::kVersion, kRingVersion);
    store_u32(hdr::kSlotSize, slot_size);
    store_u32(hdr::kCapacity, capacity);
    // The header is written first in program order but published last: the
    // release store is what a joining process's acquire load pairs with. Zero
    // here is not a no-op, it is the publication.
    write_index().store(0, std::memory_order_release);
    read_index().store(0, std::memory_order_release);
    refresh_cached_indices();
    return std::nullopt;
}

std::optional<RingError> Ring::open(const std::string& path) {
    close();
    // Map the header alone first: the geometry that says how big the file
    // should be is inside it, so it cannot be known before reading it.
    std::byte* probe = nullptr;
    if (auto e = map_file(path, hdr::kLen, false, probe)) {
        return e;
    }
    base_ = probe;
    mapped_ = hdr::kLen;

    // Pairs with the release store the creator finished with. Without it the
    // header fields below are read with no happens-before against the writes
    // that produced them, and the comment in `create` claiming a publication
    // would be describing something that is not there.
    (void)write_index().load(std::memory_order_acquire);

    const std::uint64_t magic = load_u64(hdr::kMagic);
    if (magic != kRingMagic) {
        close();
        return RingError::BadMagic;
    }
    const std::uint32_t version = load_u32(hdr::kVersion);
    if (version != kRingVersion) {
        close();
        return RingError::BadVersion;
    }
    const std::uint32_t capacity = load_u32(hdr::kCapacity);
    const std::uint32_t slot_size = load_u32(hdr::kSlotSize);
    close();

    if (auto bad = check_geometry(capacity, slot_size)) {
        return bad;
    }
    const std::size_t len = file_size(capacity, slot_size);
    std::byte* addr = nullptr;
    if (auto e = map_file(path, len, false, addr)) {
        return e;
    }
    base_ = addr;
    mapped_ = len;
    capacity_ = capacity;
    slot_size_ = slot_size;
    refresh_cached_indices();
    return std::nullopt;
}

}  // namespace mdstack::ring
