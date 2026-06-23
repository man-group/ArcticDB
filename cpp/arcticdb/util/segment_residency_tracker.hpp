/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <atomic>
#include <cstdint>

namespace arcticdb::util {

// Test-only instrumentation for the number of decoded segments resident in the ComponentManager at once.
class SegmentResidencyTracker {
  public:
    static SegmentResidencyTracker& instance() {
        static SegmentResidencyTracker tracker;
        return tracker;
    }

    void set_enabled(bool enabled) { enabled_.store(enabled, std::memory_order_relaxed); }

    void reset() {
        live_.store(0, std::memory_order_relaxed);
        high_water_.store(0, std::memory_order_relaxed);
    }

    void on_segment_resident() {
        if (!enabled_.load(std::memory_order_relaxed))
            return;
        const auto now = live_.fetch_add(1, std::memory_order_relaxed) + 1;
        auto prev = high_water_.load(std::memory_order_relaxed);
        while (now > prev && !high_water_.compare_exchange_weak(prev, now, std::memory_order_relaxed)) {
        }
    }

    void on_segment_released() {
        if (!enabled_.load(std::memory_order_relaxed))
            return;
        live_.fetch_sub(1, std::memory_order_relaxed);
    }

    int64_t high_water() const { return high_water_.load(std::memory_order_relaxed); }

  private:
    std::atomic<bool> enabled_{false};
    std::atomic<int64_t> live_{0};
    std::atomic<int64_t> high_water_{0};
};

} // namespace arcticdb::util
