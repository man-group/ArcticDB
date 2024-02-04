#include <arcticdb/stream/piloted_clock.hpp>

namespace arcticdb {
std::atomic<entity::timestamp> PilotedClock::time_;
}