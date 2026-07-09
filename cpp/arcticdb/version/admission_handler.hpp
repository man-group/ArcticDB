/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/pipeline/frame_slice.hpp>
#include <folly/futures/Future.h>
#include <folly/futures/Promise.h>
#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

namespace arcticdb::version_store {

using SegmentReader = std::function<folly::Future<pipelines::SegmentAndSlice>(pipelines::RangesAndKey&&)>;

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
            SegmentReader reader, std::vector<pipelines::RangesAndKey>&& ranges_and_keys,
            std::vector<std::vector<size_t>>&& processing_units, size_t max_processing_units_in_flight,
            size_t read_window
    ) :
        reader_(std::move(reader)),
        ranges_and_keys_(std::make_shared<std::vector<pipelines::RangesAndKey>>(std::move(ranges_and_keys))),
        promises_(std::make_shared<std::vector<folly::Promise<pipelines::SegmentAndSlice>>>(ranges_and_keys_->size())),
        i_th_segment_enqueued_(ranges_and_keys_->size(), false),
        read_window_(std::max<size_t>(1, read_window)),
        processing_units_(std::move(processing_units)),
        max_processing_units_in_flight_(max_processing_units_in_flight),
        next_unit_(max_processing_units_in_flight) {}

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
        for (size_t u = 0; u < std::min(max_processing_units_in_flight_, processing_units_.size()); ++u) {
            enqueue_processing_unit(u);
        }
        fill_read_window();
    }

    void on_processing_unit_complete() {
        const auto u = next_unit_.fetch_add(1);
        if (u < processing_units_.size()) {
            enqueue_processing_unit(u);
        }
        fill_read_window();
    }

  private:
    // Make the u-th unit's segments eligible to read. A segment may belong to two units (e.g. resample boundaries), so
    // it is enqueued at most once. Segments are enqueued unit-contiguously so folly::collect on a unit is not left
    // waiting on segments submitted far apart.
    void enqueue_processing_unit(const size_t u) {
        std::lock_guard lock{window_mutex_};
        for (const auto i : processing_units_.at(u)) {
            if (!i_th_segment_enqueued_.at(i)) {
                i_th_segment_enqueued_.at(i) = true;
                segments_queued_for_read_.push_back(i);
            }
        }
    }

    // Launch eligible reads until the read window is full. Called whenever a read completes or a new processing unit
    // is admitted.
    void fill_read_window() {
        std::vector<size_t> to_launch;
        {
            std::lock_guard lock{window_mutex_};
            while (active_reads_ < read_window_ && !segments_queued_for_read_.empty()) {
                to_launch.push_back(segments_queued_for_read_.front());
                segments_queued_for_read_.pop_front();
                ++active_reads_;
            }
        }
        for (const auto i : to_launch) {
            dispatch_read(i);
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
        {
            std::lock_guard lock{window_mutex_};
            --active_reads_;
        }
        fill_read_window();
    }

    SegmentReader reader_;
    std::shared_ptr<std::vector<pipelines::RangesAndKey>> ranges_and_keys_;

    // We accumulate promises for all segments up front, but they do not start to execute until the read is dispatched.
    // This lets us set up chains of futures while controlling how many are actually executing at a given time.
    std::shared_ptr<std::vector<folly::Promise<pipelines::SegmentAndSlice>>> promises_;

    // State to hand-roll a folly::window over the segment loads on the IO pool
    std::mutex window_mutex_; // protects the 3 fields below
    std::vector<bool> i_th_segment_enqueued_;
    std::deque<size_t> segments_queued_for_read_;
    size_t active_reads_{0};
    size_t read_window_;

    // State to manage the number of processing units being processed at a time
    std::vector<std::vector<size_t>> processing_units_;
    size_t max_processing_units_in_flight_;
    std::atomic<size_t> next_unit_;
};

size_t max_resident_processing_units(const std::vector<std::vector<size_t>>& processing_unit_indexes);

} // namespace arcticdb::version_store
