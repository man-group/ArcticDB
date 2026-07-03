#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace arcticdb::decoded_segment_gauge {

// Experiment-only gauge (see docs/claude/plans/.../instrument K=1 memory). Tracks the number and
// total num_bytes() of decoded input segments currently registered in the ComponentManager, with a
// running high-water mark. Lets us compare the peak *live* decoded working set against ru_maxrss to
// tell whether K=1's excess RSS is genuinely-live decoded segments or freed-but-retained pages.

inline std::atomic<int64_t>& live_bytes() {
    static std::atomic<int64_t> v{0};
    return v;
}

inline std::atomic<int64_t>& peak_bytes() {
    static std::atomic<int64_t> v{0};
    return v;
}

inline std::atomic<int64_t>& live_count() {
    static std::atomic<int64_t> v{0};
    return v;
}

inline std::atomic<int64_t>& peak_count() {
    static std::atomic<int64_t> v{0};
    return v;
}

inline void bump_peak(std::atomic<int64_t>& peak, int64_t candidate) {
    int64_t prev = peak.load(std::memory_order_relaxed);
    while (candidate > prev && !peak.compare_exchange_weak(prev, candidate, std::memory_order_relaxed)) {
    }
}

inline void add(int64_t bytes) {
    bump_peak(peak_bytes(), live_bytes().fetch_add(bytes, std::memory_order_relaxed) + bytes);
    bump_peak(peak_count(), live_count().fetch_add(1, std::memory_order_relaxed) + 1);
}

inline void sub(int64_t bytes) {
    live_bytes().fetch_sub(bytes, std::memory_order_relaxed);
    live_count().fetch_sub(1, std::memory_order_relaxed);
}

inline void reset() {
    live_bytes().store(0, std::memory_order_relaxed);
    peak_bytes().store(0, std::memory_order_relaxed);
    live_count().store(0, std::memory_order_relaxed);
    peak_count().store(0, std::memory_order_relaxed);
}

} // namespace arcticdb::decoded_segment_gauge
