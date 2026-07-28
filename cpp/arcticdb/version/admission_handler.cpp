/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/admission_handler.hpp>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <algorithm>
#include <cstdint>

namespace arcticdb::version_store {

// Residency budget: the number of processing units admitted-but-not-yet-processed at once, bounding decoded segments
// resident in memory. Always >= 1. The default converts the read window (in segments) into a processing-unit count by
// dividing by the largest unit size, so it does not impact normal reads (the read window then governs and the schedule
// reduces to folly::window) while still capping residency if processing is slow.
//
// Adding one unit per CPU thread means every thread can hold a unit without taking
// capacity from the window. It significantly helps performance for large processing units (eg resamples on large
// buckets) where benchmarking showed an order of magnitude performance regression without this term, as without it we
// effectively serialize work (for large processing units).
size_t max_resident_processing_units(const std::vector<std::vector<size_t>>& processing_unit_indexes) {
    size_t max_unit_size = 0;
    for (const auto& unit : processing_unit_indexes) {
        max_unit_size = std::max(max_unit_size, unit.size());
    }
    if (max_unit_size == 0) {
        return 1;
    }
    const int64_t read_window = static_cast<int64_t>(segment_read_window());
    const int64_t units_to_fill_window =
            (read_window + static_cast<int64_t>(max_unit_size) - 1) / static_cast<int64_t>(max_unit_size);
    const int64_t cpu_thread_count = static_cast<int64_t>(async::TaskScheduler::instance()->cpu_thread_count());
    const int64_t default_residency_limit = units_to_fill_window + cpu_thread_count;
    const int64_t configured =
            ConfigsMap::instance()->get_int("VersionStore.NumProcessingUnitsLive", default_residency_limit);
    if (configured == 0) {
        // A configured value of 0 is a kill switch: residency is unbounded, so the read window alone governs.
        return processing_unit_indexes.size();
    }
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            configured > 0, "VersionStore.NumProcessingUnitsLive must be >= 0, got {}", configured
    );
    return static_cast<size_t>(configured);
}

// Read window: the number of segment reads submitted but not completed at any given time. Always >= 1. Defaults to
// 2*io_thread_count.
size_t segment_read_window() {
    const int64_t io_thread_count = static_cast<int64_t>(async::TaskScheduler::instance()->io_thread_count());
    const int64_t default_window = 2 * io_thread_count;
    const int64_t configured = ConfigsMap::instance()->get_int("VersionStore.SegmentReadWindow", default_window);
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            configured >= 1, "VersionStore.SegmentReadWindow must be >= 1, got {}", configured
    );
    return static_cast<size_t>(configured);
}

} // namespace arcticdb::version_store
