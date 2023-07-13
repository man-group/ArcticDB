/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
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
#if defined(_WIN32) || defined(__APPLE__)
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

struct ManualClock {
    inline static std::atomic<entity::timestamp> time_{0};

    static entity::timestamp nanos_since_epoch() {
        return time_.load();
    }
    static entity::timestamp coarse_nanos_since_epoch() {
        return time_.load();
    }
};


} // namespace arcticdb::util
