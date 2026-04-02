/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <aws/core/utils/memory/MemorySystemInterface.h>
#include <aws/common/allocator.h>

#include <array>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <mutex>

namespace arcticdb::storage::s3 {

/**
 * Thread-local freelist pool for small allocations.
 *
 * The AWS SDK and CRT make millions of small allocations per sync cycle
 * (cJSON nodes, aws_string, byte buffers, HTTP headers). Most are < 128 bytes
 * and have request-scoped lifetimes — allocated and freed within a single S3 API call.
 *
 * This allocator maintains per-thread freelists for common size classes.
 * When a block is freed, it goes back to the freelist instead of the system allocator.
 * When a block is needed, the freelist is checked first.
 *
 * Size classes: 16, 32, 64, 128, 256 bytes (covers ~90% of AWS SDK allocations).
 * Allocations > 256 bytes fall through to malloc/free directly.
 */

// Maximum number of blocks to cache per size class per thread.
// Beyond this, freed blocks go to the system allocator.
static constexpr size_t MAX_CACHED_BLOCKS = 256;

/// Global counters for pool allocation statistics.
/// These are atomic so they can be safely incremented from multiple threads.
struct PoolAllocStats {
    std::atomic<uint64_t> pool_hits{0};       // Returned from freelist (no malloc)
    std::atomic<uint64_t> pool_misses{0};     // Freelist empty, fell through to malloc
    std::atomic<uint64_t> pool_returns{0};    // Freed back to freelist (no free)
    std::atomic<uint64_t> pool_overflows{0};  // Freelist full, fell through to free
    std::atomic<uint64_t> large_allocs{0};    // Too large for pool, direct malloc
    std::atomic<uint64_t> large_frees{0};     // Too large for pool, direct free
    std::atomic<uint64_t> total_allocs{0};    // Total allocation requests
    std::atomic<uint64_t> total_frees{0};     // Total free requests

    void reset() {
        pool_hits.store(0, std::memory_order_relaxed);
        pool_misses.store(0, std::memory_order_relaxed);
        pool_returns.store(0, std::memory_order_relaxed);
        pool_overflows.store(0, std::memory_order_relaxed);
        large_allocs.store(0, std::memory_order_relaxed);
        large_frees.store(0, std::memory_order_relaxed);
        total_allocs.store(0, std::memory_order_relaxed);
        total_frees.store(0, std::memory_order_relaxed);
    }
};

inline PoolAllocStats& pool_alloc_stats() {
    static PoolAllocStats stats;
    return stats;
}

struct FreeBlock {
    FreeBlock* next;
};

struct SizeClassPool {
    FreeBlock* head = nullptr;
    size_t count = 0;

    void* pop() {
        if (!head)
            return nullptr;
        auto* block = head;
        head = block->next;
        --count;
        return block;
    }

    bool push(void* ptr) {
        if (count >= MAX_CACHED_BLOCKS)
            return false;
        auto* block = static_cast<FreeBlock*>(ptr);
        block->next = head;
        head = block;
        ++count;
        return true;
    }

    ~SizeClassPool() {
        while (head) {
            auto* next = head->next;
            std::free(head);
            head = next;
        }
    }
};

// Size classes: index 0=16, 1=32, 2=64, 3=128, 4=256
static constexpr size_t NUM_SIZE_CLASSES = 5;
static constexpr std::array<size_t, NUM_SIZE_CLASSES> SIZE_CLASSES = {16, 32, 64, 128, 256};

inline int size_class_index(size_t size) {
    // We need at least sizeof(FreeBlock*) = 8 bytes for the freelist pointer
    if (size <= 16) return 0;
    if (size <= 32) return 1;
    if (size <= 64) return 2;
    if (size <= 128) return 3;
    if (size <= 256) return 4;
    return -1; // Too large for pool
}

struct ThreadLocalPool {
    std::array<SizeClassPool, NUM_SIZE_CLASSES> pools;

    void* allocate(size_t size) {
        int idx = size_class_index(size);
        if (idx < 0)
            return std::malloc(size);

        void* ptr = pools[idx].pop();
        if (ptr)
            return ptr;

        // Pool empty — allocate from system at the size class granularity
        return std::malloc(SIZE_CLASSES[idx]);
    }

    void deallocate(void* ptr, size_t size) {
        int idx = size_class_index(size);
        if (idx < 0) {
            std::free(ptr);
            return;
        }
        if (!pools[idx].push(ptr)) {
            // Pool full — return to system
            std::free(ptr);
        }
    }

    void deallocate_unknown_size(void* ptr) {
        // When we don't know the size, we can't use the pool
        std::free(ptr);
    }
};

inline ThreadLocalPool& get_thread_pool() {
    thread_local ThreadLocalPool pool;
    return pool;
}

/**
 * AWS SDK MemorySystemInterface implementation that uses thread-local freelists.
 *
 * Hooks Aws::Malloc/Aws::Free for all SDK allocations (Aws::String, Aws::New, etc).
 *
 * NOTE: The SDK's Free() does not provide the allocation size, so we need to
 * store the size class alongside the allocation. We prepend a small header.
 */

static constexpr size_t HEADER_SIZE = 16; // Alignment-safe header

struct AllocHeader {
    uint32_t size_class_idx; // 0-4 for pooled, UINT32_MAX for large
    uint32_t padding;
    // 8 bytes total, but we use 16 for alignment
};

static_assert(sizeof(AllocHeader) <= HEADER_SIZE);

/// Log pool allocator stats. Call from anywhere (e.g. at process exit or between syncs).
inline void log_pool_alloc_stats(const char* label = "PoolAllocStats") {
    auto& s = pool_alloc_stats();
    auto total = s.total_allocs.load(std::memory_order_relaxed);
    auto hits = s.pool_hits.load(std::memory_order_relaxed);
    auto misses = s.pool_misses.load(std::memory_order_relaxed);
    auto large = s.large_allocs.load(std::memory_order_relaxed);
    auto returns = s.pool_returns.load(std::memory_order_relaxed);
    auto overflows = s.pool_overflows.load(std::memory_order_relaxed);
    auto frees = s.total_frees.load(std::memory_order_relaxed);
    auto large_f = s.large_frees.load(std::memory_order_relaxed);

    double hit_rate = (hits + misses > 0) ? 100.0 * hits / (hits + misses) : 0.0;
    uint64_t actual_mallocs = misses + large;
    double malloc_saved_pct = (total > 0) ? 100.0 * hits / total : 0.0;

    // Use fprintf+fflush to avoid any allocator re-entrancy issues and ensure output
    fprintf(stdout,
        "[%s] allocs=%lu frees=%lu | pool: hits=%lu misses=%lu hit_rate=%.1f%% | "
        "returns=%lu overflows=%lu | large: alloc=%lu free=%lu | "
        "actual_mallocs=%lu (%.1f%% saved)\n",
        label, total, frees, hits, misses, hit_rate,
        returns, overflows, large, large_f,
        actual_mallocs, malloc_saved_pct);
    fflush(stdout);
}

class PooledAwsMemorySystem : public Aws::Utils::Memory::MemorySystemInterface {
public:
    void Begin() override {}
    void End() override {
        log_pool_alloc_stats("PoolAllocStats-Shutdown");
    }

    void* AllocateMemory(std::size_t blockSize, std::size_t alignment, const char* /*allocationTag*/) override {
        (void)alignment; // We rely on malloc alignment (16-byte on x86-64) which is sufficient
        auto& stats = pool_alloc_stats();
        stats.total_allocs.fetch_add(1, std::memory_order_relaxed);

        int idx = size_class_index(blockSize);
        if (idx < 0) {
            stats.large_allocs.fetch_add(1, std::memory_order_relaxed);
            void* raw = std::malloc(HEADER_SIZE + blockSize);
            if (!raw) return nullptr;
            auto* hdr = static_cast<AllocHeader*>(raw);
            hdr->size_class_idx = UINT32_MAX;
            return static_cast<char*>(raw) + HEADER_SIZE;
        }

        auto& pool = get_thread_pool();
        void* block = pool.pools[idx].pop();
        if (!block) {
            stats.pool_misses.fetch_add(1, std::memory_order_relaxed);
            block = std::malloc(HEADER_SIZE + SIZE_CLASSES[idx]);
        } else {
            stats.pool_hits.fetch_add(1, std::memory_order_relaxed);
            auto* hdr = static_cast<AllocHeader*>(block);
            hdr->size_class_idx = static_cast<uint32_t>(idx);
            return static_cast<char*>(block) + HEADER_SIZE;
        }
        if (!block) return nullptr;
        auto* hdr = static_cast<AllocHeader*>(block);
        hdr->size_class_idx = static_cast<uint32_t>(idx);
        return static_cast<char*>(block) + HEADER_SIZE;
    }

    void FreeMemory(void* memoryPtr) override {
        if (!memoryPtr) return;
        auto& stats = pool_alloc_stats();
        stats.total_frees.fetch_add(1, std::memory_order_relaxed);

        void* raw = static_cast<char*>(memoryPtr) - HEADER_SIZE;
        auto* hdr = static_cast<AllocHeader*>(raw);

        if (hdr->size_class_idx == UINT32_MAX) {
            stats.large_frees.fetch_add(1, std::memory_order_relaxed);
            std::free(raw);
            return;
        }

        int idx = static_cast<int>(hdr->size_class_idx);
        auto& pool = get_thread_pool();
        if (pool.pools[idx].push(raw)) {
            stats.pool_returns.fetch_add(1, std::memory_order_relaxed);
        } else {
            stats.pool_overflows.fetch_add(1, std::memory_order_relaxed);
            std::free(raw);
        }
    }
};

/**
 * Custom aws_allocator for CRT allocations (aws_string, cJSON, byte buffers).
 *
 * Unlike the SDK MemorySystemInterface, the CRT's mem_release does not provide
 * the allocation size. We use the same header trick.
 */
inline void* crt_pool_acquire(struct aws_allocator* /*allocator*/, size_t size) {
    auto& stats = pool_alloc_stats();
    stats.total_allocs.fetch_add(1, std::memory_order_relaxed);

    int idx = size_class_index(size);
    if (idx < 0) {
        stats.large_allocs.fetch_add(1, std::memory_order_relaxed);
        void* raw = std::malloc(HEADER_SIZE + size);
        if (!raw) return nullptr;
        auto* hdr = static_cast<AllocHeader*>(raw);
        hdr->size_class_idx = UINT32_MAX;
        return static_cast<char*>(raw) + HEADER_SIZE;
    }

    auto& pool = get_thread_pool();
    void* block = pool.pools[idx].pop();
    if (!block) {
        stats.pool_misses.fetch_add(1, std::memory_order_relaxed);
        block = std::malloc(HEADER_SIZE + SIZE_CLASSES[idx]);
        if (!block) return nullptr;
    } else {
        stats.pool_hits.fetch_add(1, std::memory_order_relaxed);
    }
    auto* hdr = static_cast<AllocHeader*>(block);
    hdr->size_class_idx = static_cast<uint32_t>(idx);
    return static_cast<char*>(block) + HEADER_SIZE;
}

inline void crt_pool_release(struct aws_allocator* /*allocator*/, void* ptr) {
    if (!ptr) return;
    auto& stats = pool_alloc_stats();
    stats.total_frees.fetch_add(1, std::memory_order_relaxed);

    void* raw = static_cast<char*>(ptr) - HEADER_SIZE;
    auto* hdr = static_cast<AllocHeader*>(raw);

    if (hdr->size_class_idx == UINT32_MAX) {
        stats.large_frees.fetch_add(1, std::memory_order_relaxed);
        std::free(raw);
        return;
    }

    int idx = static_cast<int>(hdr->size_class_idx);
    auto& pool = get_thread_pool();
    if (pool.pools[idx].push(raw)) {
        stats.pool_returns.fetch_add(1, std::memory_order_relaxed);
    } else {
        stats.pool_overflows.fetch_add(1, std::memory_order_relaxed);
        std::free(raw);
    }
}

inline void* crt_pool_calloc(struct aws_allocator* allocator, size_t num, size_t size) {
    size_t total = num * size;
    void* ptr = crt_pool_acquire(allocator, total);
    if (ptr) std::memset(ptr, 0, total);
    return ptr;
}

inline void* crt_pool_realloc(struct aws_allocator* allocator, void* oldptr, size_t /*oldsize*/, size_t newsize) {
    if (!oldptr) return crt_pool_acquire(allocator, newsize);

    void* old_raw = static_cast<char*>(oldptr) - HEADER_SIZE;
    auto* old_hdr = static_cast<AllocHeader*>(old_raw);
    int old_idx = (old_hdr->size_class_idx == UINT32_MAX) ? -1 : static_cast<int>(old_hdr->size_class_idx);
    size_t old_usable = (old_idx >= 0) ? SIZE_CLASSES[old_idx] : 0; // unknown for large

    int new_idx = size_class_index(newsize);

    // If both old and new fit in the same size class, reuse the block
    if (old_idx >= 0 && new_idx == old_idx) {
        return oldptr;
    }

    // Allocate new, copy, free old
    void* newptr = crt_pool_acquire(allocator, newsize);
    if (!newptr) return nullptr;

    size_t copy_size = (old_idx >= 0) ? std::min(old_usable, newsize) : newsize;
    std::memcpy(newptr, oldptr, copy_size);

    crt_pool_release(allocator, oldptr);
    return newptr;
}

/// Global CRT allocator instance
inline aws_allocator* get_crt_pool_allocator() {
    static aws_allocator alloc = {
        crt_pool_acquire,
        crt_pool_release,
        crt_pool_realloc,
        crt_pool_calloc,
        nullptr // impl
    };
    return &alloc;
}

} // namespace arcticdb::storage::s3
