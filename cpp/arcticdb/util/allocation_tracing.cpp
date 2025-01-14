#include <arcticdb/util/allocation_tracing.hpp>

#ifdef ARCTICDB_COUNT_ALLOCATIONS

namespace arcticdb {

bool AllocationTracker::started_ = false;
thread_local bool top_level_ = true;

std::shared_ptr<AllocationTracker> AllocationTracker::instance() {
    std::call_once(AllocationTracker::init_flag_, &AllocationTracker::init);
    return AllocationTracker::instance_;
}

void AllocationTracker::destroy_instance() {
    if (instance_)
        instance_->print();

    instance_.reset();
}

void AllocationTracker::trace() {
    if(top_level_) {
        top_level_ = false;
        auto trace = unwind_stack(num_levels_);
        {
            std::lock_guard lock{mutex_};
            ++data_[trace];
        }
        top_level_ = true;
    }
}

void AllocationTracker::init() {
    instance_ = std::make_shared<AllocationTracker>();
}

std::shared_ptr<AllocationTracker> AllocationTracker::instance_;
std::once_flag AllocationTracker::init_flag_;

} // namespace arcticdb

void* operator new(std::size_t sz){
    void* ptr = std::malloc(sz);
    if(arcticdb::AllocationTracker::started())
        arcticdb::AllocationTracker::instance()->trace();
    return ptr;
}

void operator delete(void* ptr) noexcept{
    std::free(ptr);
}

void operator delete(void* ptr, std::size_t) noexcept{
    std::free(ptr);
}

#endif