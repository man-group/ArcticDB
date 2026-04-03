#include <arcticdb/stream/piloted_clock.hpp>
#include <atomic>

namespace arcticdb {
std::atomic<entity::timestamp> PilotedClock::time_;
}