#pragma once
#include <arcticdb/toolbox/query_stats.hpp>
#include <arcticdb/util/constants.hpp>
#include <pybind11/pybind11.h>

#include <algorithm>
#include <chrono>
#include <optional>

namespace arcticdb {

struct GILLock {
    PyGILState_STATE gstate;

    void lock() { gstate = PyGILState_Ensure(); }

    void unlock() { PyGILState_Release(gstate); }
};

class ScopedGILLock {
  public:
    ScopedGILLock() : acquired_gil_(false) { acquire(); }

    ~ScopedGILLock() { release(); }

    void acquire() {
        if (!acquired_gil_) {
            if (query_stats::is_active()) {
                const auto start = std::chrono::steady_clock::now();
                state_ = PyGILState_Ensure();
                acquired_gil_ = true;
                const auto wait_ns = static_cast<uint64_t>(std::max<int64_t>(
                        0,
                        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start)
                                .count()
                ));
                try {
                    query_stats::record_boundary_op(query_stats::BoundaryPhase::GilWait, wait_ns);
                } catch (...) {
                }
            } else {
                state_ = PyGILState_Ensure();
                acquired_gil_ = true;
            }
        }
    }

    void release() {
        if (acquired_gil_) {
            PyGILState_Release(state_);
            acquired_gil_ = false;
        }
    }

  private:
    bool acquired_gil_;
    PyGILState_STATE state_ = PyGILState_UNLOCKED;
};

class TimedGilAcquire {
  public:
    TimedGilAcquire() :
        active_(query_stats::is_active()),
        start_(active_ ? std::chrono::steady_clock::now() : query_stats::TimePoint{}),
        acquire_() {
        if (active_) {
            const auto wait_ns = static_cast<uint64_t>(std::max<int64_t>(
                    0,
                    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start_)
                            .count()
            ));
            try {
                query_stats::record_boundary_op(query_stats::BoundaryPhase::GilWait, wait_ns);
            } catch (...) {
            }
        }
    }

    ARCTICDB_NO_MOVE_OR_COPY(TimedGilAcquire);

  private:
    bool active_;
    query_stats::TimePoint start_;
    pybind11::gil_scoped_acquire acquire_;
};

class TimedGilRelease {
  public:
    TimedGilRelease() { release_.emplace(); }

    ~TimedGilRelease() noexcept {
        if (!Py_IsInitialized()) {
            if (release_) {
                release_->disarm();
                release_.reset();
            }
            return;
        }
        if (!query_stats::is_active()) {
            release_.reset();
            return;
        }
        const auto start = std::chrono::steady_clock::now();
        release_.reset();
        const auto wait_ns = static_cast<uint64_t>(std::max<int64_t>(
                0,
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count()
        ));
        try {
            query_stats::record_boundary_op(query_stats::BoundaryPhase::GilWait, wait_ns);
        } catch (...) {
        }
    }

    ARCTICDB_NO_MOVE_OR_COPY(TimedGilRelease);

  private:
    std::optional<pybind11::gil_scoped_release> release_;
};

} // namespace arcticdb