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

#include <algorithm>
#include <cstdint>

namespace arcticdb::version_store {

// Residency budget: the number of processing units admitted-but-not-yet-processed at once, bounding decoded segments
// resident in memory. Always >= 1. The default is a multiple of the read window so that it does not impact
// normal reads (the read window then governs and the schedule reduces to folly::window), while still capping residency
// if processing is slow. The IO term ceil(4*io_thread_count / max_unit_size) is ~2x the read window's worth of
// segments. The 2*cpu_thread_count floor ensures a CPU worker that finishes a unit finds a decoded unit ready.
size_t max_resident_processing_units(const std::vector<std::vector<size_t>>& processing_unit_indexes) {
    size_t max_unit_size = 0;
    for (const auto& unit : processing_unit_indexes) {
        max_unit_size = std::max(max_unit_size, unit.size());
    }
    if (max_unit_size == 0) {
        return 1;
    }
    const int64_t io_thread_count = static_cast<int64_t>(async::TaskScheduler::instance()->io_thread_count());
    const int64_t cpu_thread_count = static_cast<int64_t>(async::TaskScheduler::instance()->cpu_thread_count());
    const int64_t io_read_ahead =
            (4 * io_thread_count + static_cast<int64_t>(max_unit_size) - 1) / static_cast<int64_t>(max_unit_size);
    const int64_t default_residency_limit = std::max(2 * cpu_thread_count, io_read_ahead);
    const int64_t configured =
            ConfigsMap::instance()->get_int("VersionStore.NumProcessingUnitsLive", default_residency_limit);
    if (configured == 0) {
        // A configured value of 0 is a kill switch: residency is unbounded, so the read window alone governs.
        return processing_unit_indexes.size();
    }
    return static_cast<size_t>(std::max<int64_t>(1, configured));
}

} // namespace arcticdb::version_store
