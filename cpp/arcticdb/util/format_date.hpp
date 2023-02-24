/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <fmt/format.h>
#include <chrono>
#include <ctime>
#include <iomanip>

namespace arcticdb::util {

using Clock = std::chrono::system_clock;
using TimePoint = std::chrono::time_point<Clock>;

inline std::string format_timestamp(entity::timestamp ts) {
    std::stringstream ss;
#ifdef _WIN32
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
