#include <arcticdb/entity/types.hpp>

namespace arcticdb {

struct PilotedClock {
    static std::atomic<entity::timestamp> time_;
    static entity::timestamp nanos_since_epoch() {
        return PilotedClock::time_++;
    }

    static void reset() {
        PilotedClock::time_ = 0;
    }
};

} //namespace arcticdb