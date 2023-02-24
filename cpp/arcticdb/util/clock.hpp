/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/entity/types.hpp>

#include <folly/ClockGettimeWrappers.h>
#include <folly/portability/Time.h>
#ifndef CLOCK_REALTIME_COARSE
#include <chrono>
#endif

namespace arcticdb::util {

class SysClock {
  public:
    static entity::timestamp nanos_since_epoch() {
        return (*folly::chrono::clock_gettime_ns)(CLOCK_REALTIME);
    }
#ifdef _WIN32
    static entity::timestamp coarse_nanos_since_epoch() {
        auto t = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch()).count();
    }
#else
    static entity::timestamp coarse_nanos_since_epoch() {
        return (*folly::chrono::clock_gettime_ns)(CLOCK_REALTIME_COARSE);
    }
#endif
};


struct LinearClock {
    inline static std::atomic<entity::timestamp> time_{0};

    static entity::timestamp nanos_since_epoch() {
        return LinearClock::time_.fetch_add(1);
    }
    static entity::timestamp coarse_nanos_since_epoch() {
        return LinearClock::time_.fetch_add(1);
    }
};


} // namespace arcticdb::util



