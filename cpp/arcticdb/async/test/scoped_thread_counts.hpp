/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/util/configs_map.hpp>

#include <cstdint>
#include <optional>
#include <utility>

namespace arcticdb::async {

// Resizes the task scheduler's pools for one scope. The config is restored first and the scheduler is then rebuilt
// with the defaults, on every exit path, so later tests in the binary don't inherit the resized pools.
class ScopedThreadCounts {
    std::optional<ScopedConfig> config_;

  public:
    explicit ScopedThreadCounts(int64_t cpu = 1, int64_t io = 1) :
        config_(std::in_place,
                ScopedConfig::ConfigOptions{{"VersionStore.NumCPUThreads", cpu}, {"VersionStore.NumIOThreads", io}}) {
        TaskScheduler::replace_instance_for_testing();
    }

    ~ScopedThreadCounts() {
        config_.reset();
        TaskScheduler::replace_instance_for_testing();
    }

    ScopedThreadCounts(const ScopedThreadCounts&) = delete;
    ScopedThreadCounts& operator=(const ScopedThreadCounts&) = delete;
    ScopedThreadCounts(ScopedThreadCounts&&) = delete;
    ScopedThreadCounts& operator=(ScopedThreadCounts&&) = delete;
};

} // namespace arcticdb::async
