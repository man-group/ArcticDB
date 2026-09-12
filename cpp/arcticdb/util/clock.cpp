#include <arcticdb/util/clock.hpp>

#include <chrono>

namespace arcticdb::util {

entity::timestamp SysClock::nanos_since_epoch()  {
    auto now = std::chrono::system_clock::now();
    auto now_ns = std::chrono::time_point_cast<std::chrono::nanoseconds>(now);
    return now_ns.time_since_epoch().count();
}

}
