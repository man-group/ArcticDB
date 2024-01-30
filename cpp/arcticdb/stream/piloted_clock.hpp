#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/preprocess.hpp>

namespace arcticdb {

struct PilotedClock {
    static std::atomic <timestamp> time_;
    static timestamp nanos_since_epoch() {
        return PilotedClock::time_++;
    }

    static void reset() {
        PilotedClock::time_ = 0;
    }
};

} //namespace arcticdb