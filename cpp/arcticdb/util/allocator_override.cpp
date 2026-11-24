/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

// Replaces the global operator new/delete of the binary this object is linked into (arcticdb_ext and the C++ unit
// tests) with the allocator chosen by ARCTICDB_ALLOCATOR.

#include <arcticdb/util/allocator_backend.hpp>

#include <new>

namespace {

namespace backend = arcticdb::allocator_backend;

template<typename Allocate>
void* allocate_or_throw(Allocate&& allocate) {
    for (;;) {
        if (void* p = allocate())
            return p;
        std::new_handler handler = std::get_new_handler();
        if (handler == nullptr)
            throw std::bad_alloc();
        handler();
    }
}

void* new_impl(std::size_t size) {
    if (size == 0)
        size = 1;
    return allocate_or_throw([size] { return backend::allocate(size); });
}

void* new_aligned_impl(std::size_t size, std::align_val_t alignment) {
    if (size == 0)
        size = 1;
    return allocate_or_throw([size, alignment] {
        return backend::allocate_aligned(size, static_cast<std::size_t>(alignment));
    });
}

template<typename Allocate>
void* nothrow(Allocate&& allocate) noexcept {
    try {
        return allocate();
    } catch (...) {
        return nullptr;
    }
}

} // namespace

void* operator new(std::size_t size) { return new_impl(size); }
void* operator new[](std::size_t size) { return new_impl(size); }
void* operator new(std::size_t size, const std::nothrow_t&) noexcept {
    return nothrow([size] { return new_impl(size); });
}
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
    return nothrow([size] { return new_impl(size); });
}

void* operator new(std::size_t size, std::align_val_t alignment) { return new_aligned_impl(size, alignment); }
void* operator new[](std::size_t size, std::align_val_t alignment) { return new_aligned_impl(size, alignment); }
void* operator new(std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept {
    return nothrow([size, alignment] { return new_aligned_impl(size, alignment); });
}
void* operator new[](std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept {
    return nothrow([size, alignment] { return new_aligned_impl(size, alignment); });
}

void operator delete(void* p) noexcept { backend::deallocate(p); }
void operator delete[](void* p) noexcept { backend::deallocate(p); }
void operator delete(void* p, std::size_t) noexcept { backend::deallocate(p); }
void operator delete[](void* p, std::size_t) noexcept { backend::deallocate(p); }
void operator delete(void* p, const std::nothrow_t&) noexcept { backend::deallocate(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { backend::deallocate(p); }

// Each allocator frees aligned blocks with its ordinary free.
void operator delete(void* p, std::align_val_t) noexcept { backend::deallocate(p); }
void operator delete[](void* p, std::align_val_t) noexcept { backend::deallocate(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { backend::deallocate(p); }
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept { backend::deallocate(p); }
void operator delete(void* p, std::align_val_t, const std::nothrow_t&) noexcept { backend::deallocate(p); }
void operator delete[](void* p, std::align_val_t, const std::nothrow_t&) noexcept { backend::deallocate(p); }
