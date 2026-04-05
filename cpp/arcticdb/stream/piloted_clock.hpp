/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <arcticdb/entity/typedefs.hpp>
#include <atomic>

namespace arcticdb {

struct PilotedClock {
    static std::atomic<entity::timestamp> time_;
    static entity::timestamp nanos_since_epoch() { return time_++; }

    static void reset() { time_ = 0; }
};

} // namespace arcticdb