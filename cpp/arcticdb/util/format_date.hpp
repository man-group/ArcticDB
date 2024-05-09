/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <fmt/format.h>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <arcticdb/entity/types.hpp>

namespace arcticdb::util {

using Clock = std::chrono::system_clock;
using TimePoint = std::chrono::time_point<Clock>;

inline std::string format_timestamp(arcticdb::entity::timestamp ts) {
    std::stringstream ss;
#if defined(_WIN32) || defined(__APPLE__)
    auto time_point = Clock::time_point(std::chrono::duration_cast<std::chrono::seconds>(std::chrono::nanoseconds(ts)));
#else
    auto time_point = Clock::time_point(std::chrono::nanoseconds(ts));
#endif
    std::time_t t = Clock::to_time_t(time_point);
    auto fraction = time_point - std::chrono::time_point_cast<std::chrono::seconds>(time_point);
    ss << std::put_time(std::gmtime(&t), "%F %T.")
       << std::chrono::duration_cast<std::chrono::milliseconds>(fraction).count();
    return ss.str();
}

}
