/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/allocator.hpp>

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/memory_tracing.hpp>
#include <arcticdb/util/clock.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/util/thread_cached_int.hpp>
#include <folly/concurrency/ConcurrentHashMap.h>

namespace arcticdb {

uint8_t* allocate_detachable_memory(size_t size) { return get_detachable_allocator().allocate(size); }

void free_detachable_memory(uint8_t* ptr, size_t size) { get_detachable_allocator().deallocate(ptr, size); }

DetachableAllocator get_detachable_allocator() { return DetachableAllocator{}; }

bool use_slab_allocator() {
    static const bool use_it = ConfigsMap::instance()->get_int("Allocator.UseSlabAllocator", 1);
    return use_it;
}

void TracingData::init() { TracingData::instance_ = std::make_shared<TracingData>(); }

std::shared_ptr<TracingData> TracingData::instance() {
    std::call_once(TracingData::init_flag_, &TracingData::init);
    return TracingData::instance_;
}

void TracingData::destroy_instance() { TracingData::instance_.reset(); }

std::shared_ptr<TracingData> TracingData::instance_;
std::once_flag TracingData::init_flag_;

struct TracingData::Impl {
    folly::ConcurrentHashMap<AddrIdentifier, size_t> allocs_;
    std::atomic<uint64_t> total_allocs_{0};
    std::atomic<uint64_t> total_irregular_allocs_{0};
    std::atomic<uint64_t> total_allocs_calls_{0};
};

TracingData::TracingData() : impl_(std::make_unique<Impl>()) {}
TracingData::~TracingData() = default;

void TracingData::track_alloc(AddrIdentifier addr_ts, size_t size) {
    // util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
    impl_->allocs_.insert(std::make_pair(addr_ts, size));
    impl_->total_allocs_ += size;
    impl_->total_allocs_calls_++;
    if (size != page_size) {
        impl_->total_irregular_allocs_++;
    }
    ARCTICDB_TRACE(
            log::codec(),
            "Allocated {} to {}:{}, total allocation size {}, total irregular allocs {}/{}",
            util::MemBytes{size},
            addr_ts.first,
            addr_ts.second,
            util::MemBytes{impl_->total_allocs_},
            impl_->total_irregular_allocs_,
            impl_->total_allocs_calls_
    );
}

void TracingData::track_free(AddrIdentifier addr_ts) {
    //        util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
    auto it = impl_->allocs_.find(addr_ts);
    util::check(it != impl_->allocs_.end(), "Unrecognized address in free {}:{}", addr_ts.first, addr_ts.second);
    util::check(
            impl_->total_allocs_ >= it->second,
            "Request to free {}  from {}:{} when only {} remain",
            it->second,
            addr_ts.first,
            addr_ts.second,
            impl_->total_allocs_.load()
    );
    impl_->total_allocs_ -= it->second;
    ARCTICDB_TRACE(
            log::codec(),
            "Freed {} at {}:{}, total allocation {}",
            util::MemBytes{it->second},
            addr_ts.first,
            addr_ts.second,
            util::MemBytes{impl_->total_allocs_.load()}
    );
    impl_->allocs_.erase(it);
}

void TracingData::track_realloc(AddrIdentifier old_addr, AddrIdentifier new_addr, size_t size) {
    if (old_addr.first != 0)
        track_free(old_addr);

    track_alloc(new_addr, size);
}

size_t TracingData::total_bytes() const { return impl_->total_allocs_; }

bool TracingData::all_freed() const { return impl_->allocs_.empty() && impl_->total_allocs_ == 0; }

void TracingData::clear() {
    impl_->total_allocs_ = 0;
    impl_->total_irregular_allocs_ = 0;
    impl_->total_allocs_calls_ = 0;
    impl_->allocs_.clear();
}

void InMemoryTracingPolicy::track_alloc(AddrIdentifier addr, size_t size) { data().track_alloc(addr, size); }

void InMemoryTracingPolicy::track_free(AddrIdentifier addr) { data().track_free(addr); }

void InMemoryTracingPolicy::track_realloc(AddrIdentifier old_addr, AddrIdentifier new_addr, size_t size) {
    data().track_realloc(old_addr, new_addr, size);
}

size_t InMemoryTracingPolicy::total_bytes() { return data().total_bytes(); }

bool InMemoryTracingPolicy::deallocated() {
    auto& get_data = data();
    bool all_freed = get_data.all_freed();
    if (!all_freed) {
        log::memory().warn("Allocator has not freed all data, {} bytes counted", get_data.impl_->total_allocs_);

        for (auto alloc : get_data.impl_->allocs_)
            log::memory().warn("Unfreed allocation: {}", uintptr_t(alloc.first.first));
    }
    return get_data.all_freed();
}

void InMemoryTracingPolicy::clear() { data().clear(); }

namespace {
template<typename TracingPolicy, typename ClockType>
auto& free_count_of() {
    static ThreadCachedInt<uint32_t> free_count;
    return free_count;
};
} // namespace

template<typename TracingPolicy, typename ClockType>
std::shared_ptr<AllocatorImpl<TracingPolicy, ClockType>> AllocatorImpl<TracingPolicy, ClockType>::instance_;

template<typename TracingPolicy, typename ClockType>
std::once_flag AllocatorImpl<TracingPolicy, ClockType>::init_flag_;

template<class TracingPolicy, class ClockType>
uint8_t* AllocatorImpl<TracingPolicy, ClockType>::get_alignment(size_t size) {
#ifdef _WIN32
    return static_cast<uint8_t*>(_aligned_malloc(size, alignment));
#else
    return static_cast<uint8_t*>(std::malloc(size));
#endif
}

template<class TracingPolicy, class ClockType>
entity::timestamp AllocatorImpl<TracingPolicy, ClockType>::current_timestamp() {
    return ClockType::nanos_since_epoch();
}

template<class TracingPolicy, class ClockType>
uint8_t* AllocatorImpl<TracingPolicy, ClockType>::internal_alloc(size_t size) {
    uint8_t* ret;
#ifdef USE_SLAB_ALLOCATOR
    std::call_once(slab_init_flag_, &init_slab);
    if (size == page_size && use_slab_allocator()) {
        ARCTICDB_TRACE(log::codec(), "Doing slab allocation of page size");
        ret = reinterpret_cast<uint8_t*>(page_size_slab_allocator_->allocate());
    } else {
        ARCTICDB_TRACE(log::codec(), "Doing normal allocation of size {}", size);
        ret = static_cast<uint8_t*>(std::malloc(size));
    }
#else
    ret = static_cast<uint8_t*>(std::malloc(size));
#endif
    return ret;
}

template<class TracingPolicy, class ClockType>
void AllocatorImpl<TracingPolicy, ClockType>::internal_free(uint8_t* p) {
#ifdef USE_SLAB_ALLOCATOR
    std::call_once(slab_init_flag_, &init_slab);
    auto raw_pointer = reinterpret_cast<SlabAllocatorType::pointer>(p);
    if (use_slab_allocator() && page_size_slab_allocator_->is_addr_in_slab(raw_pointer)) {
        ARCTICDB_TRACE(log::codec(), "Doing slab free of address {}", uintptr_t(p));
        page_size_slab_allocator_->deallocate(raw_pointer);
    } else {
        ARCTICDB_TRACE(log::codec(), "Doing normal free of address {}", uintptr_t(p));
        std::free(p);
    }
#else
    std::free(p);
    free_count_of<TracingPolicy, ClockType>().increment(1);
    maybe_trim();
#endif
}

template<class TracingPolicy, class ClockType>
uint8_t* AllocatorImpl<TracingPolicy, ClockType>::internal_realloc(uint8_t* p, std::size_t size) {
    uint8_t* ret;
#ifdef USE_SLAB_ALLOCATOR
    std::call_once(slab_init_flag_, &init_slab);
    auto raw_pointer = reinterpret_cast<SlabAllocatorType::pointer>(p);
    if (use_slab_allocator() && page_size_slab_allocator_->is_addr_in_slab(raw_pointer)) {
        ARCTICDB_TRACE(log::codec(), "Doing slab realloc of address {} and size {}", uintptr_t(p), size);
        if (size == page_size)
            return p;
        else {
            page_size_slab_allocator_->deallocate(raw_pointer);
            ret = static_cast<uint8_t*>(std::malloc(size));
        }
    } else {
        ARCTICDB_TRACE(log::codec(), "Doing normal realloc of address {} and size {}", uintptr_t(p), size);
        if (use_slab_allocator && size == page_size) {
            std::free(p);
            ret = reinterpret_cast<uint8_t*>(page_size_slab_allocator_->allocate());
        } else {
            ret = static_cast<uint8_t*>(std::realloc(p, size));
        }
    }
#else
    ret = static_cast<uint8_t*>(std::realloc(p, size));
#endif
    return ret;
}

template<class TracingPolicy, class ClockType>
void AllocatorImpl<TracingPolicy, ClockType>::init() {
    instance_ = std::make_shared<AllocatorImpl>();
}

template<typename TracingPolicy, typename ClockType>
std::shared_ptr<AllocatorImpl<TracingPolicy, ClockType>> AllocatorImpl<TracingPolicy, ClockType>::instance() {
    std::call_once(AllocatorImpl<TracingPolicy, ClockType>::init_flag_, &AllocatorImpl<TracingPolicy, ClockType>::init);
    return instance_;
}

template<typename TracingPolicy, typename ClockType>
void AllocatorImpl<TracingPolicy, ClockType>::destroy_instance() {
    AllocatorImpl<TracingPolicy, ClockType>::instance_.reset();
}

template<typename TracingPolicy, typename ClockType>
std::pair<uint8_t*, entity::timestamp> AllocatorImpl<TracingPolicy, ClockType>::alloc(
        size_t size, bool no_realloc ARCTICDB_UNUSED
) {
    util::check(size != 0, "Should not allocate zero bytes");
    auto ts = current_timestamp();

    uint8_t* ret = internal_alloc(size);
    util::check(ret != nullptr, "Failed to allocate {} bytes", size);
    TracingPolicy::track_alloc(std::make_pair(uintptr_t(ret), ts), size);
    return {ret, ts};
}

template<typename TracingPolicy, typename ClockType>
void AllocatorImpl<TracingPolicy, ClockType>::trim() {
    /* malloc_trim is a glibc extension not available on Windows.It is possible
     * that we will end up with a larger memory footprint for not calling it, but
     * there are no windows alternatives.
     */
#if defined(__linux__) && defined(__GLIBC__)
    malloc_trim(0);
#endif
}

template<class TracingPolicy, class ClockType>
void AllocatorImpl<TracingPolicy, ClockType>::maybe_trim() {
    static const uint32_t trim_count = ConfigsMap::instance()->get_int("Allocator.TrimCount", 250);
    if (free_count_of<TracingPolicy, ClockType>().readFast() > trim_count &&
        free_count_of<TracingPolicy, ClockType>().readFastAndReset() > trim_count)
        trim();
}

template<typename TracingPolicy, typename ClockType>
std::pair<uint8_t*, entity::timestamp> AllocatorImpl<TracingPolicy, ClockType>::aligned_alloc(
        size_t size, bool no_realloc ARCTICDB_UNUSED
) {
    util::check(size != 0, "Should not allocate zero bytes");
    auto ts = current_timestamp();

    util::check(size != 0, "Should not allocate zero bytes");
    auto ret = internal_alloc(size);
    util::check(ret != nullptr, "Failed to aligned allocate {} bytes", size);
    TracingPolicy::track_alloc(std::make_pair(uintptr_t(ret), ts), size);
    return std::make_pair(ret, ts);
}

template<class TracingPolicy, class ClockType>
std::pair<uint8_t*, entity::timestamp> AllocatorImpl<TracingPolicy, ClockType>::realloc(
        std::pair<uint8_t*, entity::timestamp> ptr, size_t size
) {
    AddrIdentifier old_addr{uintptr_t(ptr.first), ptr.second};
    auto ret = internal_realloc(ptr.first, size);

#ifdef ARCTICDB_TRACK_ALLOCS
    ARCTICDB_TRACE(
            log::codec(),
            "Reallocating {} bytes from {} to {}",
            util::MemBytes{size},
            uintptr_t(ptr.first),
            uintptr_t(ret)
    );
#endif
    auto ts = current_timestamp();
    TracingPolicy::track_realloc(old_addr, std::make_pair(uintptr_t(ret), ts), size);
    return {ret, ts};
}

template<class TracingPolicy, class ClockType>
void AllocatorImpl<TracingPolicy, ClockType>::free(std::pair<uint8_t*, entity::timestamp> ptr) {
    if (ptr.first == nullptr)
        return;

    TracingPolicy::track_free(std::make_pair(uintptr_t(ptr.first), ptr.second));
    internal_free(ptr.first);
}

template<class TracingPolicy, class ClockType>
size_t AllocatorImpl<TracingPolicy, ClockType>::allocated_bytes() {
    return TracingPolicy::total_bytes();
}

template<class TracingPolicy, class ClockType>
size_t AllocatorImpl<TracingPolicy, ClockType>::empty() {
    return TracingPolicy::deallocated();
}

template<class TracingPolicy, class ClockType>
void AllocatorImpl<TracingPolicy, ClockType>::clear() {
    TracingPolicy::clear();
}

template class AllocatorImpl<InMemoryTracingPolicy, util::LinearClock>;
template class AllocatorImpl<InMemoryTracingPolicy, util::SysClock>;
template class AllocatorImpl<NullTracingPolicy, util::LinearClock>;
template class AllocatorImpl<NullTracingPolicy, util::SysClock>;

} // namespace arcticdb