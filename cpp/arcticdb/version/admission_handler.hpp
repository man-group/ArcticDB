/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/stream/stream_source.hpp>
#include <arcticdb/toolbox/query_stats.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <folly/ExceptionWrapper.h>
#include <folly/futures/Future.h>
#include <folly/futures/Promise.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <exception>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

namespace arcticdb::version_store {

// Feeds segment reads to the IO pool under two independent controls.
//
// Residency budget (memory): at most `max_processing_units_in_flight` processing units are admitted but not yet
// processed at once. A unit is admitted initially or when an outstanding one finishes processing
// (on_processing_unit_complete), which bounds the decoded segments resident in memory. A value >= the number of units
// disables the bound.
//
// Read window (throughput): at most `read_window` reads are submitted-but-not-decoded at once. A slot frees on read
// completion, so an admitted unit's segments enter a queue that fill_read_window drains it into the IO pool
// the way folly::window does. When the residency budget is not the limiting factor, this
// reduces to folly::window(read_window). This is important to avoid scheduling all the IO work upfront,
// which can lead to very poor queue fairness as the Folly IO pool schedules work round robin across its threads.
//
// Never waits within a unit, so progress continues for any budget >= 1 and window >= 1 regardless of the processing
// unit structure (for example, if there is overlap between units). Construct via make_shared (reads capture
// shared_from_this to keep the handler alive across the async reads).
class ProcessingUnitAdmissionHandler : public std::enable_shared_from_this<ProcessingUnitAdmissionHandler> {
  public:
    ProcessingUnitAdmissionHandler(
            stream::SegmentReader reader, std::vector<pipelines::RangesAndKey>&& ranges_and_keys,
            std::vector<std::vector<size_t>>&& processing_units, size_t max_processing_units_in_flight,
            size_t read_window
    ) :
        reader_(std::move(reader)),
        ranges_and_keys_(std::make_shared<std::vector<pipelines::RangesAndKey>>(std::move(ranges_and_keys))),
        promises_(std::make_shared<std::vector<folly::Promise<pipelines::SegmentAndSlice>>>(ranges_and_keys_->size())),
        i_th_segment_enqueued_(ranges_and_keys_->size(), false),
        read_window_(read_window),
        processing_units_(std::move(processing_units)),
        max_processing_units_in_flight_(max_processing_units_in_flight),
        next_unit_(max_processing_units_in_flight) {
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                read_window >= 1, "ProcessingUnitAdmissionHandler read_window must be >= 1, got {}", read_window
        );
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                max_processing_units_in_flight >= 1,
                "ProcessingUnitAdmissionHandler max_processing_units_in_flight must be >= 1, got {}",
                max_processing_units_in_flight
        );
    }

    // Used to chain downstream processing, possibly before any of the work is actually executing.
    // The work then gets kicked off by admit_initial_processing_units or on_processing_unit_complete.
    std::vector<folly::Future<pipelines::SegmentAndSlice>> futures() const {
        std::vector<folly::Future<pipelines::SegmentAndSlice>> res;
        res.reserve(promises_->size());
        for (auto& promise : *promises_) {
            res.emplace_back(promise.getFuture());
        }
        return res;
    }

    const std::vector<std::vector<size_t>>& processing_units() const { return processing_units_; }

    void admit_initial_processing_units() {
        const bool track = query_stats::is_active();
        const auto now = track ? std::chrono::steady_clock::now() : query_stats::TimePoint{};
        if (track) {
            admit_start_ns_.store(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count(),
                    std::memory_order_release
            );
        }
        const size_t initial = std::min(max_processing_units_in_flight_, processing_units_.size());
        std::vector<PendingRead> to_launch;
        {
            std::lock_guard lock{window_mutex_};
            for (size_t u = 0; u < initial; ++u) {
                enqueue_processing_unit_locked(u, to_launch, track, now);
            }
        }
        if (track) {
            for (size_t u = 0; u < initial; ++u) {
                query_stats::record_admission_unit(/*waited=*/false, 0);
            }
        }
        dispatch_reads(std::move(to_launch));
    }

    void on_processing_unit_complete() {
        const auto u = next_unit_.fetch_add(1);
        if (u >= processing_units_.size()) {
            return;
        }
        const bool track = query_stats::is_active();
        const auto now = track ? std::chrono::steady_clock::now() : query_stats::TimePoint{};
        if (track) {
            const int64_t start_ns = admit_start_ns_.load(std::memory_order_acquire);
            if (start_ns > 0) {
                const int64_t now_ns =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
                const uint64_t wait_ns = static_cast<uint64_t>(std::max<int64_t>(0, now_ns - start_ns));
                query_stats::record_admission_unit(/*waited=*/true, wait_ns);
            } else {
                query_stats::record_admission_unit(/*waited=*/false, 0);
            }
        }
        std::vector<PendingRead> to_launch;
        {
            std::lock_guard lock{window_mutex_};
            enqueue_processing_unit_locked(u, to_launch, track, now);
        }
        dispatch_reads(std::move(to_launch));
    }

  private:
    struct PendingRead {
        size_t seg_idx;
        bool waited{false};
        query_stats::TimePoint enqueued_at{};
    };

    // Make the u-th unit's segments eligible to read. A segment may belong to two units (e.g. resample boundaries), so
    // it is enqueued at most once. Segments are enqueued unit-contiguously so folly::collect on a unit is not left
    // waiting on segments submitted far apart. Caller must hold window_mutex_.
    void enqueue_processing_unit_locked(
            const size_t u, std::vector<PendingRead>& to_launch, bool track, query_stats::TimePoint now
    ) {
        for (const auto i : processing_units_.at(u)) {
            if (!i_th_segment_enqueued_.at(i)) {
                i_th_segment_enqueued_.at(i) = true;
                if (active_reads_ < read_window_ && segments_queued_for_read_.empty()) {
                    ++active_reads_;
                    to_launch.push_back(PendingRead{i, /*waited=*/false, {}});
                } else {
                    segments_queued_for_read_.push_back(
                            PendingRead{i, /*waited=*/true, track ? now : query_stats::TimePoint{}}
                    );
                }
            }
        }
    }

    // Pop queued reads into to_launch until the read window is full or the queue is empty.
    // Caller must hold window_mutex_.
    void drain_queue_locked(std::vector<PendingRead>& to_launch) {
        while (active_reads_ < read_window_ && !segments_queued_for_read_.empty()) {
            to_launch.push_back(segments_queued_for_read_.front());
            segments_queued_for_read_.pop_front();
            ++active_reads_;
        }
    }

    void dispatch_reads(std::vector<PendingRead> to_launch) {
        while (!to_launch.empty()) {
            if (query_stats::is_active()) {
                query_stats::TimePoint now{};
                for (const auto& item : to_launch) {
                    if (item.waited && item.enqueued_at != query_stats::TimePoint{}) {
                        if (now == query_stats::TimePoint{}) {
                            now = std::chrono::steady_clock::now();
                        }
                        const uint64_t wait_ns = static_cast<uint64_t>(std::max<int64_t>(
                                0, std::chrono::duration_cast<std::chrono::nanoseconds>(now - item.enqueued_at).count()
                        ));
                        query_stats::record_admission_segment(/*waited=*/true, wait_ns);
                    } else {
                        query_stats::record_admission_segment(/*waited=*/false, 0);
                    }
                }
            }

            std::vector<PendingRead> next_launch;
            for (const auto& item : to_launch) {
                // A reader that throws instead of returning a failed future would otherwise leave the window slot used
                // and the promise unfulfilled, hanging every consumer of this segment. Catch everything, since losing a
                // promise here cannot be recovered from.
                try {
                    dispatch_read(item.seg_idx);
                } catch (...) {
                    {
                        std::lock_guard lock{window_mutex_};
                        --active_reads_;
                        drain_queue_locked(next_launch);
                    }
                    promises_->at(item.seg_idx).setException(folly::exception_wrapper{std::current_exception()});
                }
            }
            to_launch = std::move(next_launch);
        }
    }

    void dispatch_read(size_t i) {
        auto self = shared_from_this();
        reader_(pipelines::RangesAndKey{ranges_and_keys_->at(i)})
                .thenTryInline([self, i](folly::Try<pipelines::SegmentAndSlice>&& t) {
                    self->promises_->at(i).setTry(std::move(t));
                    self->on_read_complete();
                });
    }

    void on_read_complete() {
        std::vector<PendingRead> to_launch;
        {
            std::lock_guard lock{window_mutex_};
            --active_reads_;
            drain_queue_locked(to_launch);
        }
        dispatch_reads(std::move(to_launch));
    }

    stream::SegmentReader reader_;
    std::shared_ptr<std::vector<pipelines::RangesAndKey>> ranges_and_keys_;

    // We accumulate promises for all segments up front, but they do not start to execute until the read is dispatched.
    // This lets us set up chains of futures while controlling how many are actually executing at a given time.
    std::shared_ptr<std::vector<folly::Promise<pipelines::SegmentAndSlice>>> promises_;

    // State to hand-roll a folly::window over the segment loads on the IO pool
    // We do this so that when the processing unit budget is much higher than the window size, we still
    // get sensible queuing behaviour on the IO pools. Benchmarking on PR #3191 showed a major performance
    // penalty without this windowing, due to the round-robin scheduling on the IO thread pools.
    std::mutex window_mutex_; // protects the fields below
    std::vector<bool> i_th_segment_enqueued_;
    std::deque<PendingRead> segments_queued_for_read_;
    size_t active_reads_{0};
    size_t read_window_;

    // State to manage the number of processing units being processed at a time
    std::vector<std::vector<size_t>> processing_units_;
    size_t max_processing_units_in_flight_;
    std::atomic<size_t> next_unit_;
    std::atomic<int64_t> admit_start_ns_{0};
};

size_t max_resident_processing_units(const std::vector<std::vector<size_t>>& processing_unit_indexes);

// Read window: the number of segment reads submitted but not completed at any given time. Always >= 1. Defaults to
// 2*io_thread_count.
size_t segment_read_window();

} // namespace arcticdb::version_store
