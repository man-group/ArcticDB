/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/util/allocator.hpp>

namespace arcticdb {

void SharedMemoryAllocator::init(){
    SharedMemoryAllocator::instance_ = std::make_shared<SharedMemoryAllocator>();
}

std::shared_ptr<SharedMemoryAllocator> SharedMemoryAllocator::instance() {
    std::call_once(SharedMemoryAllocator::init_flag_, &SharedMemoryAllocator::init);
    return SharedMemoryAllocator::instance_;
}

void SharedMemoryAllocator::destroy_instance() {
    SharedMemoryAllocator::instance_.reset();
}

std::shared_ptr<SharedMemoryAllocator> SharedMemoryAllocator::instance_;
std::once_flag SharedMemoryAllocator::init_flag_;

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
}