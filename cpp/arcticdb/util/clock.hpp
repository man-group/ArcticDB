/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>

#include <cstdint>
#if defined(_WIN32)
#include <chrono>
#elif defined(__linux__)
#include <time.h>
#elif defined(__APPLE__)
#include <mach/mach_time.h>
#endif

namespace arcticdb::util {

class SysClock {
  public:
    static entity::timestamp nanos_since_epoch() {
        auto now = std::chrono::system_clock::now();
        auto now_ns = std::chrono::time_point_cast<std::chrono::nanoseconds>(now);
        return now_ns.time_since_epoch().count();
    }
    static entity::timestamp coarse_nanos_since_epoch() {
#if defined(__linux__)
        timespec ts;
        clock_gettime(CLOCK_REALTIME_COARSE, &ts);
        return static_cast<uint64_t>(ts.tv_sec) * 1000000000LL + ts.tv_nsec;
#elif defined(_WIN32)
        return nanos_since_epoch();
#elif defined(__APPLE__)
        static mach_timebase_info_data_t info = {0, 0};
        if (info.denom == 0) {
            mach_timebase_info(&info);
        }
        uint64_t now = mach_absolute_time();
        now *= info.numer;
        now /= info.denom;
        return now;
#endif
    }
};

struct LinearClock {
    inline static std::atomic<entity::timestamp> time_{0};

    static entity::timestamp nanos_since_epoch() { return LinearClock::time_.fetch_add(1); }
    static entity::timestamp coarse_nanos_since_epoch() { return LinearClock::time_.fetch_add(1); }
};

struct ManualClock {
    inline static std::atomic<entity::timestamp> time_{0};

    static entity::timestamp nanos_since_epoch() { return time_.load(); }
    static entity::timestamp coarse_nanos_since_epoch() { return time_.load(); }
};

} // namespace arcticdb::util
