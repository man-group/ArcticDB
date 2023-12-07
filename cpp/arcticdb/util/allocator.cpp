/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/allocator.hpp>

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/memory_tracing.hpp>
#include <arcticdb/util/clock.hpp>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/ThreadCachedInt.h>


namespace arcticdb {

    void TracingData::init() {
        TracingData::instance_ = std::make_shared<TracingData>();
    }

    std::shared_ptr<TracingData> TracingData::instance() {
        std::call_once(TracingData::init_flag_, &TracingData::init);
        return TracingData::instance_;
    }

    void TracingData::destroy_instance() {
        TracingData::instance_.reset();
    }

    std::shared_ptr<TracingData> TracingData::instance_;
    std::once_flag TracingData::init_flag_;

    template<> folly::ThreadCachedInt<uint32_t> AllocatorImpl<InMemoryTracingPolicy, util::LinearClock>::free_count_;
    template<> folly::ThreadCachedInt<uint32_t> AllocatorImpl<NullTracingPolicy, util::LinearClock>::free_count_;

    template<> folly::ThreadCachedInt<uint32_t> AllocatorImpl<InMemoryTracingPolicy, util::SysClock>::free_count_;
    template<> folly::ThreadCachedInt<uint32_t> AllocatorImpl<NullTracingPolicy, util::SysClock>::free_count_;


    struct TracingData::Impl
    {
        folly::ConcurrentHashMap<AddrIdentifier, size_t> allocs_;
        std::atomic<uint64_t> total_allocs_{ 0 };
        std::atomic<uint64_t> total_irregular_allocs_{ 0 };
        std::atomic<uint64_t> total_allocs_calls_{ 0 };

    };

    TracingData::TracingData() : impl_(std::make_unique<Impl>()) {}
    TracingData::~TracingData() = default;

    void TracingData::track_alloc(AddrIdentifier addr_ts, size_t size) {
        //util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
        impl_->allocs_.insert(std::make_pair(addr_ts, size));
        impl_->total_allocs_ += size;
        impl_->total_allocs_calls_++;
        if (size != page_size) {
            impl_->total_irregular_allocs_++;
        }
        ARCTICDB_TRACE(log::codec(), "Allocated {} to {}:{}, total allocation size {}, total irregular allocs {}/{}",
            util::MemBytes{ size },
            addr_ts.first,
            addr_ts.second,
            util::MemBytes{ total_allocs_ },
            total_irregular_allocs_,
            total_allocs_calls_);

    }

    void TracingData::track_free(AddrIdentifier addr_ts) {
        //        util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
        auto it = impl_->allocs_.find(addr_ts);
        util::check(it != impl_->allocs_.end(), "Unrecognized address in free {}:{}", addr_ts.first, addr_ts.second);
        util::check(impl_->total_allocs_ >= it->second,
            "Request to free {}  from {}:{} when only {} remain",
            it->second,
            addr_ts.first,
            addr_ts.second,
            impl_->total_allocs_.load());
        impl_->total_allocs_ -= it->second;
        ARCTICDB_TRACE(log::codec(), "Freed {} at {}:{}, total allocation {}",
            util::MemBytes{ it->second },
            addr_ts.first,
            addr_ts.second,
            util::MemBytes{ impl_->total_allocs_.load() });
        impl_->allocs_.erase(it);
    }

    void TracingData::track_realloc(AddrIdentifier old_addr, AddrIdentifier new_addr, size_t size) {
        if (old_addr.first != 0)
            track_free(old_addr);

        track_alloc(new_addr, size);
    }

    size_t TracingData::total_bytes() const
    {
        return impl_->total_allocs_;
    }

    bool TracingData::all_freed() const
    {
        return impl_->allocs_.empty() && impl_->total_allocs_ == 0;
    }

    void TracingData::clear()
    {
        impl_->total_allocs_ = 0;
        impl_->total_irregular_allocs_ = 0;
        impl_->total_allocs_calls_ = 0;
        impl_->allocs_.clear();
    }

    void InMemoryTracingPolicy::track_alloc(AddrIdentifier addr, size_t size) {
        data().track_alloc(addr, size);
    }

    void InMemoryTracingPolicy::track_free(AddrIdentifier addr) {
        data().track_free(addr);
    }

    void InMemoryTracingPolicy::track_realloc(AddrIdentifier old_addr, AddrIdentifier new_addr, size_t size) {
        data().track_realloc(old_addr, new_addr, size);
    }

    size_t InMemoryTracingPolicy::total_bytes() {
        return data().total_bytes();
    }

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

    void InMemoryTracingPolicy::clear() {
        data().clear();
    }



}