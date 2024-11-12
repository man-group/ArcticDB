#include <arcticdb/stream/piloted_clock.hpp>

namespace arcticdb {
std::atomic<entity::timestamp> PilotedClock::time_;
std::atomic<entity::timestamp> PilotedClockNoAutoIncrement::time_;
}