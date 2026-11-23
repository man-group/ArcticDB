/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstddef>
#include <cstdlib>

#if defined(_WIN32)
#include <malloc.h>
#endif

#if defined(__linux__) && defined(__GLIBC__)
#include <malloc.h>
#endif

#if defined(ARCTICDB_ALLOCATOR_MIMALLOC)
#ifdef _MSC_VER
#pragma warning(push, 0)
#endif
#include <mimalloc.h>
#ifdef _MSC_VER
#pragma warning(pop)
#endif
#elif defined(ARCTICDB_ALLOCATOR_JEMALLOC)
#include <jemalloc/jemalloc.h>
#elif defined(ARCTICDB_ALLOCATOR_TCMALLOC)
#include <gperftools/malloc_extension_c.h>
#include <gperftools/tcmalloc.h>
#endif

#if defined(__APPLE__) && (defined(ARCTICDB_ALLOCATOR_MIMALLOC) || defined(ARCTICDB_ALLOCATOR_JEMALLOC) ||             \
                           defined(ARCTICDB_ALLOCATOR_TCMALLOC))
#include <malloc/malloc.h>
// libc++ is a shared library on macOS and instantiates parts of std::basic_string itself, so memory allocated by the
// system operator new can reach arcticdb_ext's operator delete. Frees check ownership first.
#define ARCTICDB_ALLOCATOR_CHECK_OWNERSHIP
#endif

// The allocator named by ARCTICDB_ALLOCATOR_*, called by arcticdb::Allocator and by the operator new/delete in
// allocator_override.cpp. Without one, these are the C runtime functions.
namespace arcticdb::allocator_backend {

#if defined(ARCTICDB_ALLOCATOR_MIMALLOC)
inline constexpr const char* name = "mimalloc";
inline void* allocate(std::size_t size) { return mi_malloc(size); }
inline void* allocate_aligned(std::size_t size, std::size_t alignment) { return mi_malloc_aligned(size, alignment); }
inline void* reallocate(void* p, std::size_t size) { return mi_realloc(p, size); }
inline void backend_deallocate(void* p) { mi_free(p); }
inline bool owns(const void* p) { return mi_is_in_heap_region(p); }
inline void trim() { mi_collect(false); }
#elif defined(ARCTICDB_ALLOCATOR_JEMALLOC)
inline constexpr const char* name = "jemalloc";
inline void* allocate(std::size_t size) { return je_malloc(size); }
inline void* allocate_aligned(std::size_t size, std::size_t alignment) { return je_aligned_alloc(alignment, size); }
inline void* reallocate(void* p, std::size_t size) { return je_realloc(p, size); }
inline void backend_deallocate(void* p) { je_free(p); }
#if defined(__APPLE__)
// jemalloc is built without its malloc zone, so only system allocations belong to a zone.
inline bool owns(const void* p) { return malloc_zone_from_ptr(p) == nullptr; }
#endif
static_assert(MALLCTL_ARENAS_ALL == 4096);
inline void trim() { je_mallctl("arena.4096.purge", nullptr, nullptr, nullptr, 0); }
#elif defined(ARCTICDB_ALLOCATOR_TCMALLOC)
inline constexpr const char* name = "tcmalloc";
inline void* allocate(std::size_t size) { return tc_malloc(size); }
inline void* allocate_aligned(std::size_t size, std::size_t alignment) { return tc_memalign(alignment, size); }
inline void* reallocate(void* p, std::size_t size) { return tc_realloc(p, size); }
inline void backend_deallocate(void* p) { tc_free(p); }
inline bool owns(const void* p) { return MallocExtension_GetOwnership(p) == MallocExtension_kOwned; }
inline void trim() { MallocExtension_ReleaseFreeMemory(); }
#else
inline constexpr const char* name = "system";
inline void* allocate(std::size_t size) { return std::malloc(size); }
inline void* reallocate(void* p, std::size_t size) { return std::realloc(p, size); }
inline void backend_deallocate(void* p) { std::free(p); }
inline void trim() {
#if defined(__linux__) && defined(__GLIBC__)
    malloc_trim(0);
#endif
}
#endif

inline void deallocate(void* p) {
#if defined(ARCTICDB_ALLOCATOR_CHECK_OWNERSHIP)
    if (p != nullptr && !owns(p)) {
        std::free(p);
        return;
    }
#endif
    backend_deallocate(p);
}

} // namespace arcticdb::allocator_backend
