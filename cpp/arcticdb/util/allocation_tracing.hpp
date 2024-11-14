#pragma once

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <new>
#include <string>
#include <memory>
#include <mutex>

#include <arcticdb/util/trace.hpp>
#include <arcticdb/util/constructors.hpp>

#include <ankerl/unordered_dense.h>
#include <iostream>

#ifdef ARCTICDB_COUNT_ALLOCATIONS

namespace arcticdb {

class AllocationTracker {
    static std::shared_ptr<AllocationTracker> instance_;
    static std::once_flag init_flag_;
    static bool started_;

    static void init();

    std::unordered_map<std::string, uint64_t> data_;
    std::recursive_mutex mutex_;
    constexpr static int num_levels_ = 3;

public:
    static std::shared_ptr<AllocationTracker> instance();
    static void destroy_instance();

    AllocationTracker() = default;
    ~AllocationTracker() {
        print();
    }

    ARCTICDB_NO_MOVE_OR_COPY(AllocationTracker)

    void trace();

    static void start() {
        started_ = true;
    }

    static bool started() {
        return started_;
    }

    void print() {
        for(const auto& [key, value] : data_)
            if(value > 100)
                std::cout << value << ": " << key << std::endl;
    }
};

}

void* operator new(std::size_t sz);

void operator delete(void* ptr) noexcept;

void operator delete(void* ptr, std::size_t) noexcept;

#endif