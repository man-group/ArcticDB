/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/memory_tracing.hpp>
#include <arcticdb/util/clock.hpp>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <arcticdb/util/slab_allocator.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <folly/ThreadCachedInt.h>
#include <arcticdb/util/timer.hpp>

#include <boost/interprocess/managed_shared_memory.hpp>

#include <mutex>
#include <unordered_set>

// for malloc_trim on linux
#if defined(__linux__) && defined(__GLIBC__) 
    #include <malloc.h>
#endif

//#define ARCTICDB_TRACK_ALLOCS
//#define USE_SLAB_ALLOCATOR

namespace arcticdb {

static constexpr uint64_t BYTES = 1;
static constexpr uint64_t KILOBYTES = 1024 * BYTES;
static constexpr uint64_t MEGABYTES = 1024 * KILOBYTES;
static constexpr uint64_t GIGABYTES = 1024 * MEGABYTES;
static constexpr uint64_t TERABYTES = 1024 * GIGABYTES;
static constexpr uint64_t page_size = 4096; // 4KB
static const bool use_slab_allocator = ConfigsMap::instance()->get_int("Allocator.UseSlabAllocator", 1);


static constexpr uint64_t ArcticNativeShmemSize = 30 * GIGABYTES;
static const char *ArcticNativeShmemName = "arctic_native_temp";

struct SharedMemorySegment {
    void init() {
        std::lock_guard lock(mutex_);
        if(!initialized()) {
            boost::interprocess::shared_memory_object::remove(ArcticNativeShmemName);
            segment_ = std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::create_only,
                                                                                    ArcticNativeShmemName,
                                                                                    ArcticNativeShmemSize);
        }
    }

    [[nodiscard]] bool initialized() const {
        return static_cast<bool>(segment_);
    }

    uint8_t *allocate(size_t size) {
        if(!initialized())
            init();

        return static_cast<uint8_t *>(segment_->allocate(size));
    }

    void deallocate(uint8_t* ptr) {
        util::check(static_cast<bool>(segment_), "Cannot deallocate on uninitialized segment");
        segment_->deallocate(ptr);
    }

    ~SharedMemorySegment() {
        boost::interprocess::shared_memory_object::remove(ArcticNativeShmemName);
    }

private:
    std::mutex mutex_;
    std::unique_ptr<boost::interprocess::managed_shared_memory> segment_;
};

struct SharedMemoryAllocator {
    static std::shared_ptr<SharedMemoryAllocator> instance_;
    static std::once_flag init_flag_;
    static void init();
    static std::shared_ptr<SharedMemoryAllocator> instance();
    static void destroy_instance();

    bool is_mapped_ptr(uint8_t *const ptr) const {
        return allocations_.find(ptr) != allocations_.end();
    }

    uint8_t *allocate(size_t size) {
        ARCTICDB_TRACE(log::inmem(), "shared memory allocating massive block of size {}", util::MemBytes{size});
        std::scoped_lock<std::mutex> lock(mutex_);
        auto ptr =segment_.allocate(size);
        if (!ptr)
            return nullptr;

        allocations_.insert(std::make_pair(ptr, size));
        return ptr;
    }

    bool deallocate(uint8_t *ptr) {
        std::scoped_lock<std::mutex> lock(mutex_);
        if (is_mapped_ptr(ptr)) {
            ARCTICDB_TRACE(arcticdb::log::inmem(), "shared memory de-allocating massive block of size {}",
                                       util::MemBytes{allocations_[ptr]});
            segment_.deallocate(ptr);
            allocations_.erase(ptr);
            return true;
        }
        return false;
    }

    SharedMemorySegment segment_;
    std::mutex mutex_;
    std::unordered_map<uint8_t *, size_t> allocations_;
};

typedef std::pair<uintptr_t, entity::timestamp> AddrIdentifier;

struct TracingData {
    TracingData() : total_allocs_(0),  total_irregular_allocs_(0), total_allocs_calls_(0){}
    static std::shared_ptr<TracingData> instance_;
    static std::once_flag init_flag_;
    static std::shared_ptr<TracingData> instance();
    static void destroy_instance();
    static void init();

    folly::ConcurrentHashMap<AddrIdentifier, size_t> allocs_;
    std::atomic<uint64_t> total_allocs_;
    std::atomic<uint64_t> total_irregular_allocs_;
    std::atomic<uint64_t> total_allocs_calls_;

    void track_alloc(AddrIdentifier addr_ts, size_t size) {
        util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
        allocs_.insert(std::make_pair(addr_ts, size));
        total_allocs_ += size;
        total_allocs_calls_++;
        if (size != page_size) {
            total_irregular_allocs_++;
        }
        ARCTICDB_DEBUG(log::codec(), "Allocated {} to {}:{}, total allocation size {}, total irregular allocs {}/{}",
                            util::MemBytes{size},
                            addr_ts.first,
                            addr_ts.second,
                            util::MemBytes{total_allocs_},
                            total_irregular_allocs_,
                            total_allocs_calls_);

    }

    void track_free(AddrIdentifier addr_ts) {
        util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
        auto it = allocs_.find(addr_ts);
        util::check(it != allocs_.end(), "Unrecognized address in free {}:{}", addr_ts.first, addr_ts.second);
        util::check(total_allocs_ >= it->second,
                    "Request to free {}  from {}:{} when only {} remain",
                    it->second,
                    addr_ts.first,
                    addr_ts.second,
                    total_allocs_.load());
        total_allocs_ -= it->second;
        ARCTICDB_DEBUG(log::codec(), "Freed {} at {}:{}, total allocation {}",
                            util::MemBytes{it->second},
                            addr_ts.first,
                            addr_ts.second,
                            util::MemBytes{total_allocs_.load()});
        allocs_.erase(it);
    }

    void track_realloc(AddrIdentifier old_addr, AddrIdentifier new_addr, size_t size) {
        if (old_addr.first != 0)
            track_free(old_addr);

        track_alloc(new_addr, size);
    }
};

class InMemoryTracingPolicy {
    static TracingData &data() {
        return *TracingData::instance();
    }

public:
    static void track_alloc(AddrIdentifier addr, size_t size) {
        data().track_alloc(addr, size);
    }

    static void track_free(AddrIdentifier addr) {
        data().track_free(addr);
    }

    static void track_realloc(AddrIdentifier old_addr, AddrIdentifier new_addr, size_t size) {
        data().track_realloc(old_addr, new_addr, size);
    }

    static size_t total_bytes() {
        return data().total_allocs_;
    }

    static bool deallocated() {
        auto &get_data = data();
        bool all_freed = get_data.allocs_.empty() && data().total_allocs_ == 0;
        if (!all_freed) {
            log::memory().warn("Allocator has not freed all data, {} bytes counted", get_data.total_allocs_);

            for (auto alloc : get_data.allocs_)
                log::memory().warn("Unfreed allocation: {}", uintptr_t(alloc.first.first));
        }
        return get_data.allocs_.empty() && data().total_allocs_ == 0;
    }

    static void clear() {
        data().total_allocs_ = 0;
        data().total_irregular_allocs_ = 0;
        data().total_allocs_calls_ = 0;
        data().allocs_.clear();
    }
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
    constexpr size_t mask = ~(alignment-1);
    auto new_size = size & mask;
    if(new_size != size)
        new_size += alignment;

    return new_size;
}

constexpr size_t ArcticNativeMassiveAllocSize = 1000 * 1024 * 1024;

template<class TracingPolicy = NullTracingPolicy, class ClockType = util::LinearClock>
class AllocatorImpl {
private:
    static folly::ThreadCachedInt<uint32_t> free_count_;
    static uint8_t *get_alignment(size_t size) {
#ifdef _WIN32
        return  static_cast<uint8_t*>(_aligned_malloc(size, alignment));
#else
        return static_cast<uint8_t *>(std::malloc(size));
#endif
    }
    static entity::timestamp current_timestamp() {
        return ClockType::nanos_since_epoch();
    }

#ifdef USE_SLAB_ALLOCATOR

    static constexpr size_t page_slab_cacheline = 64;
    using SlabAllocatorType = SlabAllocator<std::byte[page_size], page_slab_cacheline>;

    inline static std::shared_ptr<SlabAllocatorType> page_size_slab_allocator_;
    inline static std::once_flag slab_init_flag_;

    static void init_slab() {
        const size_t page_slab_capacity = ConfigsMap::instance()->get_int("Allocator.PageSlabCapacity", 1000 * 1000); // 4GB
        if (use_slab_allocator) {
            page_size_slab_allocator_ = std::make_shared<SlabAllocatorType>(page_slab_capacity);
        }
    }
#endif

    static uint8_t* internal_alloc(size_t size) {
        uint8_t* ret;
#ifdef USE_SLAB_ALLOCATOR
            std::call_once(slab_init_flag_, &init_slab);
            if (size == page_size && use_slab_allocator) {
                ARCTICDB_TRACE(log::codec(), "Doing slab allocation of page size");
                ret = reinterpret_cast<uint8_t *>(page_size_slab_allocator_->allocate());
            } else {
                ARCTICDB_TRACE(log::codec(), "Doing normal allocation of size {}", size);
                ret = static_cast<uint8_t *>(std::malloc(size));
            }
#else
            ret = static_cast<uint8_t *>(std::malloc(size));
#endif
        return ret;
    }

    static void internal_free(uint8_t* p) {
#ifdef USE_SLAB_ALLOCATOR
        std::call_once(slab_init_flag_, &init_slab);
        auto raw_pointer = reinterpret_cast<SlabAllocatorType::pointer>(p);
        if (use_slab_allocator && page_size_slab_allocator_->is_addr_in_slab(raw_pointer)) {
            ARCTICDB_TRACE(log::codec(), "Doing slab free of address {}", uintptr_t(p));
            page_size_slab_allocator_->deallocate(raw_pointer);
        } else {
            ARCTICDB_TRACE(log::codec(), "Doing normal free of address {}", uintptr_t(p));
            std::free(p);
        }
#else
        std::free(p);
        free_count_.increment(1);
        maybe_trim();
#endif
    }

    static uint8_t* internal_realloc(uint8_t* p, std::size_t size) {
        uint8_t* ret;
#ifdef USE_SLAB_ALLOCATOR
        std::call_once(slab_init_flag_, &init_slab);
        auto raw_pointer = reinterpret_cast<SlabAllocatorType::pointer>(p);
        if (use_slab_allocator && page_size_slab_allocator_->is_addr_in_slab(raw_pointer)) {
            ARCTICDB_TRACE(log::codec(), "Doing slab realloc of address {} and size {}", uintptr_t(p), size);
            if (size == page_size)
                return p;
            else {
                page_size_slab_allocator_->deallocate(raw_pointer);
                ret = static_cast<uint8_t *>(std::malloc(size));
            }
        } else {
            ARCTICDB_TRACE(log::codec(), "Doing normal realloc of address {} and size {}", uintptr_t(p), size);
            if (use_slab_allocator && size == page_size) {
                std::free(p);
                ret = reinterpret_cast<uint8_t *>(page_size_slab_allocator_->allocate());
            } else {
                ret = static_cast<uint8_t *>(std::realloc(p, size));
            }
        }
#else
        ret = static_cast<uint8_t *>(std::realloc(p, size));
#endif
        return ret;
    }

public:
    static std::shared_ptr<AllocatorImpl> instance_;
    static std::once_flag init_flag_;

    static void init(){
        instance_ = std::make_shared<AllocatorImpl>();
    }

    static std::shared_ptr<AllocatorImpl> instance();
    static void destroy_instance();

    static std::pair<uint8_t*, entity::timestamp> alloc(size_t size, bool no_realloc ARCTICDB_UNUSED = false) {
        util::check(size != 0, "Should not allocate zero bytes");
        auto ts = current_timestamp();
#ifdef SHMEM_ALLOC
        if (no_realloc && (size > ArcticNativeMassiveAllocSize)) {
            auto maybe_ptr = SharedMemoryAllocator::instance()->allocate(size);
            if (maybe_ptr != nullptr)
                return {maybe_ptr, ts};
        }
#endif
        uint8_t* ret = internal_alloc(size);
        util::check(ret != nullptr, "Failed to allocate {} bytes", size);
        TracingPolicy::track_alloc(std::make_pair(uintptr_t(ret), ts), size);
        return {ret, ts};
    }

    static bool is_mapped_ptr(uint8_t *const ptr) {
        return SharedMemoryAllocator::instance()->is_mapped_ptr(ptr);
    }

    static void trim() {
        /* malloc_trim is a glibc extension not available on Windows.It is possible
         * that we will end up with a larger memory footprint for not calling it, but
         * there are no windows alternatives.
         */
#if defined(__linux__) && defined(__GLIBC__) 
        malloc_trim(0);
#endif
    }

    static void maybe_trim() {
        static const uint32_t trim_count = ConfigsMap::instance()->get_int("Allocator.TrimCount", 250);
        if(free_count_.readFast() > trim_count && free_count_.readFastAndReset() > trim_count)
            trim();
    }

    static std::pair<uint8_t*, entity::timestamp> aligned_alloc(size_t size, bool no_realloc = false) {
        util::check(size != 0, "Should not allocate zero bytes");
        auto ts = current_timestamp();
        if (no_realloc && (size > ArcticNativeMassiveAllocSize)) {
            auto maybe_ptr = SharedMemoryAllocator::instance()->allocate(size);
            if (maybe_ptr != nullptr)
                return {maybe_ptr, ts};
        }

        util::check(size != 0, "Should not allocate zero bytes");
        auto ret = internal_alloc(size);
//        ARCTICDB_TRACE(log::codec(), "round_to_alignment got: {}, converted to: {} (alignment: {})", size, ret, alignment);
        util::check(ret != nullptr, "Failed to aligned allocate {} bytes", size);
        TracingPolicy::track_alloc(std::make_pair(uintptr_t(ret), ts), size);
        return std::make_pair(ret, ts);
    }

    static std::pair<uint8_t*, entity::timestamp> realloc(std::pair<uint8_t*, entity::timestamp> ptr, size_t size) {
        auto ret = internal_realloc(ptr.first, size);

        #ifdef ARCTICDB_TRACK_ALLOCS
        ARCTICDB_TRACE(log::codec(), "Reallocating {} bytes from {} to {}",
                            util::MemBytes{size},
                            uintptr_t(ptr.first),
                            uintptr_t(ret));
        #endif
        auto ts = current_timestamp();
        TracingPolicy::track_realloc(std::make_pair(uintptr_t(ptr.first), ptr.second), std::make_pair(uintptr_t(ret), ts), size);
        return {ret, ts};
    }

    static void free(std::pair<uint8_t*, entity::timestamp> ptr) {
        if (ptr.first == nullptr)
            return;

#ifdef SHMEM_ALLOC
        if (SharedMemoryAllocator::instance()->deallocate(ptr.first))
            return;
#endif

        TracingPolicy::track_free(std::make_pair(uintptr_t(ptr.first), ptr.second));
        internal_free(ptr.first);
    }

#ifdef USE_SLAB_ALLOCATOR
    static size_t add_callback_when_slab_full(folly::Function<void()>&& func) {
        std::call_once(slab_init_flag_, &init_slab);
        return page_size_slab_allocator_->add_cb_when_full(std::move(func));
    }

    static void remove_callback_when_slab_full(size_t id) {
        std::call_once(slab_init_flag_, &init_slab);
        page_size_slab_allocator_->remove_cb_when_full(id);
    }

    static size_t get_slab_approx_free_blocks() {
        return page_size_slab_allocator_->get_approx_free_blocks();
    }
#endif

    static size_t allocated_bytes() {
        return TracingPolicy::total_bytes();
    }

    static size_t empty() {
        return TracingPolicy::deallocated();
    }

    static void clear() {
        TracingPolicy::clear();
    }
};

template<typename TracingPolicy, typename Clocktype>
std::shared_ptr<AllocatorImpl<TracingPolicy, Clocktype>> AllocatorImpl<TracingPolicy, Clocktype>::instance_;

template<typename TracingPolicy, typename Clocktype>
std::once_flag AllocatorImpl<TracingPolicy, Clocktype>::init_flag_;

template<typename TracingPolicy, typename Clocktype>
std::shared_ptr< AllocatorImpl<TracingPolicy, Clocktype>>  AllocatorImpl<TracingPolicy, Clocktype>::instance() {
    std::call_once( AllocatorImpl<TracingPolicy,Clocktype>::init_flag_, & AllocatorImpl<TracingPolicy, Clocktype>::init);
    return instance_;
}

template<typename TracingPolicy, typename Clocktype>
folly::ThreadCachedInt<uint32_t> AllocatorImpl<TracingPolicy, Clocktype>::free_count_;

template<typename TracingPolicy, typename Clocktype>
void AllocatorImpl<TracingPolicy, Clocktype>::destroy_instance() {
    AllocatorImpl<TracingPolicy, Clocktype>::instance_.reset();
}

#ifdef ARCTICDB_TRACK_ALLOCS
using Allocator = AllocatorImpl<InMemoryTracingPolicy>;
#else
using Allocator = AllocatorImpl<NullTracingPolicy>;
#endif
} //namespace arcticdb
