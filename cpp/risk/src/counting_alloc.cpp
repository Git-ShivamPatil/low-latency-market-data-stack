#include "risk/counting_alloc.hpp"

#include <cstdlib>
#include <new>

namespace {

// Plain PODs, so establishing the thread's TLS block cannot itself allocate.
thread_local std::uint64_t g_allocations = 0;
thread_local std::uint64_t g_deallocations = 0;
thread_local std::uint64_t g_bytes = 0;

void* counted_alloc(std::size_t n) {
    // Zero-size allocations still return a distinct pointer, and still count:
    // a loop doing a million of them is a million trips through the allocator
    // whatever the size says.
    void* p = std::malloc(n == 0 ? 1 : n);
    if (p != nullptr) {
        ++g_allocations;
        g_bytes += n;
    }
    return p;
}

void* counted_alloc_aligned(std::size_t n, std::size_t align) {
    // aligned_alloc requires a size that is a multiple of the alignment.
    const std::size_t size = ((n == 0 ? 1 : n) + align - 1) / align * align;
    void* p = std::aligned_alloc(align, size);
    if (p != nullptr) {
        ++g_allocations;
        g_bytes += n;
    }
    return p;
}

void counted_free(void* p) noexcept {
    if (p != nullptr) {
        ++g_deallocations;
        std::free(p);
    }
}

}  // namespace

namespace risk {

AllocCounts alloc_counts() noexcept {
    return AllocCounts{g_allocations, g_deallocations, g_bytes};
}

}  // namespace risk

// --- the replacements -------------------------------------------------------
//
// The whole set, not a convenient subset. Replacing `operator new` without
// replacing `operator new[]`, the sized deletes and the aligned forms leaves the
// program pairing this allocator's pointers with the default deallocator, which
// is undefined behaviour that usually appears to work.

void* operator new(std::size_t n) {
    void* p = counted_alloc(n);
    if (p == nullptr) {
        throw std::bad_alloc();
    }
    return p;
}

void* operator new[](std::size_t n) {
    void* p = counted_alloc(n);
    if (p == nullptr) {
        throw std::bad_alloc();
    }
    return p;
}

void* operator new(std::size_t n, const std::nothrow_t&) noexcept { return counted_alloc(n); }

void* operator new[](std::size_t n, const std::nothrow_t&) noexcept { return counted_alloc(n); }

void* operator new(std::size_t n, std::align_val_t a) {
    void* p = counted_alloc_aligned(n, static_cast<std::size_t>(a));
    if (p == nullptr) {
        throw std::bad_alloc();
    }
    return p;
}

void* operator new[](std::size_t n, std::align_val_t a) {
    void* p = counted_alloc_aligned(n, static_cast<std::size_t>(a));
    if (p == nullptr) {
        throw std::bad_alloc();
    }
    return p;
}

void* operator new(std::size_t n, std::align_val_t a, const std::nothrow_t&) noexcept {
    return counted_alloc_aligned(n, static_cast<std::size_t>(a));
}

void* operator new[](std::size_t n, std::align_val_t a, const std::nothrow_t&) noexcept {
    return counted_alloc_aligned(n, static_cast<std::size_t>(a));
}

void operator delete(void* p) noexcept { counted_free(p); }
void operator delete[](void* p) noexcept { counted_free(p); }
void operator delete(void* p, std::size_t) noexcept { counted_free(p); }
void operator delete[](void* p, std::size_t) noexcept { counted_free(p); }
void operator delete(void* p, const std::nothrow_t&) noexcept { counted_free(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { counted_free(p); }
void operator delete(void* p, std::align_val_t) noexcept { counted_free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { counted_free(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { counted_free(p); }
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept { counted_free(p); }
void operator delete(void* p, std::align_val_t, const std::nothrow_t&) noexcept {
    counted_free(p);
}
void operator delete[](void* p, std::align_val_t, const std::nothrow_t&) noexcept {
    counted_free(p);
}
