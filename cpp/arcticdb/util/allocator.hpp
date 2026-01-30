/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/clock.hpp>
#include <memory>
#include <sparrow/buffer/buffer.hpp>

#if USE_SLAB_ALLOCATOR
#include <arcticdb/util/slab_allocator.hpp>
#endif

// #define ARCTICDB_TRACK_ALLOCS
// #define USE_SLAB_ALLOCATOR

namespace arcticdb {

// The allocator type used for memory that will be passed to sparrow.
// We use sparrow's default allocator to ensure zero-copy behavior when buffers are moved
// internally within sparrow (sparrow compares allocators and copies data if they differ).
using DetachableAllocator = sparrow::buffer<uint8_t>::default_allocator;

// Must be used whenever deallocation COULD be the responsibility of sparrow.
// Uses sparrow's default allocator to ensure zero-copy when passing buffers to sparrow.
uint8_t* allocate_detachable_memory(size_t size);

// Must be used whenever memory was allocated with allocate_detachable_memory
void free_detachable_memory(uint8_t* ptr, size_t size);

// Returns the allocator used by allocate_detachable_memory(). Must be passed to sparrow when
// creating buffers from raw pointers to ensure zero-copy behavior.
DetachableAllocator get_detachable_allocator();

static constexpr uint64_t BYTES = 1;
static constexpr uint64_t KILOBYTES = 1024 * BYTES;
static constexpr uint64_t MEGABYTES = 1024 * KILOBYTES;
static constexpr uint64_t GIGABYTES = 1024 * MEGABYTES;
static constexpr uint64_t TERABYTES = 1024 * GIGABYTES;
static constexpr uint64_t page_size = 4096; // 4KB
bool use_slab_allocator();

static constexpr uint64_t ArcticNativeShmemSize = 30 * GIGABYTES;

typedef std::pair<uintptr_t, entity::timestamp> AddrIdentifier;

struct TracingData {
    TracingData();
    ~TracingData();

    static std::shared_ptr<TracingData> instance_;
    static std::once_flag init_flag_;
    static std::shared_ptr<TracingData> instance();
    static void destroy_instance();
    static void init();

    void track_alloc(AddrIdentifier addr_ts, size_t size);
    void track_free(AddrIdentifier addr_ts);
    void track_realloc(AddrIdentifier old_addr, AddrIdentifier new_addr, size_t size);
    size_t total_bytes() const;
    bool all_freed() const;

    void clear();

  private:
    struct Impl;

    std::unique_ptr<Impl> impl_;
    friend class InMemoryTracingPolicy;
};

class InMemoryTracingPolicy {
    static TracingData& data() { return *TracingData::instance(); }

  public:
    static void track_alloc(AddrIdentifier addr, size_t size);
    static void track_free(AddrIdentifier addr);
    static void track_realloc(AddrIdentifier old_addr, AddrIdentifier new_addr, size_t size);

    static size_t total_bytes();

    static bool deallocated();

    static void clear();
};

class NullTracingPolicy {
  public:
    static void track_alloc(AddrIdentifier, size_t) {}

    static void track_free(AddrIdentifier) {}

    static void track_realloc(AddrIdentifier, AddrIdentifier, size_t) {}

    static size_t total_bytes() { return 0; }

    static bool deallocated() { return true; }

    static void clear() {}
};

constexpr size_t alignment = 64;

constexpr size_t round_to_alignment(size_t size) {
    constexpr size_t mask = ~(alignment - 1);
    auto new_size = size & mask;
    if (new_size != size)
        new_size += alignment;

    return new_size;
}

constexpr size_t ArcticNativeMassiveAllocSize = 1000 * 1024 * 1024;

template<class TracingPolicy = NullTracingPolicy, class ClockType = util::LinearClock>
class AllocatorImpl {
  private:
    static uint8_t* get_alignment(size_t size);
    static entity::timestamp current_timestamp();

#ifdef USE_SLAB_ALLOCATOR

    static constexpr size_t page_slab_cacheline = 64;
    using SlabAllocatorType = SlabAllocator<std::byte[page_size], page_slab_cacheline>;

    inline static std::shared_ptr<SlabAllocatorType> page_size_slab_allocator_;
    inline static std::once_flag slab_init_flag_;

    static void init_slab() {
        static const size_t page_slab_capacity =
                ConfigsMap::instance()->get_int("Allocator.PageSlabCapacity", 1000 * 1000); // 4GB
        if (use_slab_allocator()) {
            page_size_slab_allocator_ = std::make_shared<SlabAllocatorType>(page_slab_capacity);
        }
    }
#endif

    static uint8_t* internal_alloc(size_t size);
    static void internal_free(uint8_t* p);
    static uint8_t* internal_realloc(uint8_t* p, std::size_t size);

  public:
    static std::shared_ptr<AllocatorImpl> instance_;
    static std::once_flag init_flag_;

    static void init();
    static std::shared_ptr<AllocatorImpl> instance();
    static void destroy_instance();
    static std::pair<uint8_t*, entity::timestamp> alloc(size_t size, bool no_realloc ARCTICDB_UNUSED = false);
    static void trim();
    static void maybe_trim();
    static std::pair<uint8_t*, entity::timestamp> aligned_alloc(size_t size, bool no_realloc = false);
    static std::pair<uint8_t*, entity::timestamp> realloc(std::pair<uint8_t*, entity::timestamp> ptr, size_t size);
    static void free(std::pair<uint8_t*, entity::timestamp> ptr);

#ifdef USE_SLAB_ALLOCATOR
    static size_t add_callback_when_slab_full(folly::Function<void()>&& func) {
        std::call_once(slab_init_flag_, &init_slab);
        return page_size_slab_allocator_->add_cb_when_full(std::move(func));
    }

    static void remove_callback_when_slab_full(size_t id) {
        std::call_once(slab_init_flag_, &init_slab);
        page_size_slab_allocator_->remove_cb_when_full(id);
    }

    static size_t get_slab_approx_free_blocks() { return page_size_slab_allocator_->get_approx_free_blocks(); }
#endif

    static size_t allocated_bytes();
    static size_t empty();
    static void clear();
};

#ifdef ARCTICDB_TRACK_ALLOCS
using Allocator = AllocatorImpl<InMemoryTracingPolicy>;
#else
using Allocator = AllocatorImpl<NullTracingPolicy>;
#endif

extern template class AllocatorImpl<InMemoryTracingPolicy, util::LinearClock>;
extern template class AllocatorImpl<InMemoryTracingPolicy, util::SysClock>;
extern template class AllocatorImpl<NullTracingPolicy, util::LinearClock>;
extern template class AllocatorImpl<NullTracingPolicy, util::SysClock>;

} // namespace arcticdb
