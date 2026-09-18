/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <deque>
#include <memory>
#include <stdexcept>
#include <utility>

#include <arcticdb/version/version_store_api.hpp>
#include <arcticdb/version/version_core.hpp>
#include <arcticdb/version/admission_handler.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/write_frame.hpp>
#include <arcticdb/pipeline/slicing.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/util/segment_residency_tracker.hpp>
#include <arcticdb/column_store/memory_segment_impl.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/util/error_code.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/processing/clause_utils.hpp>
#include <arcticdb/async/task_scheduler.hpp>
#include <folly/executors/QueuedImmediateExecutor.h>

namespace arcticdb {

namespace {
constexpr auto PROCESSING_UNITS_BOUND_KEY = "VersionStore.NumProcessingUnitsLive";

void write_sliced_symbol(
        version_store::PythonVersionStore& pvs, const StreamId& stream_id, size_t num_cols, size_t num_rows,
        size_t col_per_slice, size_t row_per_slice
) {
    std::vector<std::string> names;
    names.reserve(num_cols);
    std::vector<entity::FieldRef> fields;
    fields.reserve(num_cols);
    for (size_t i = 0; i < num_cols; ++i) {
        names.emplace_back(fmt::format("col_{}", i));
        fields.emplace_back(entity::scalar_field(entity::DataType::FLOAT64, names[i]));
    }
    auto frame = get_test_frame<stream::TimeseriesIndex>(stream_id, fields, num_rows, 0);
    pipelines::SlicingPolicy slicing = pipelines::FixedSlicer{col_per_slice, row_per_slice};
    pipelines::IndexPartialKey pk{stream_id, 0};
    auto store = pvs._test_get_store();
    auto key = pipelines::write_frame(std::move(pk), frame.frame_, slicing, store).get();
    pvs.write_version_and_prune_previous(false, key, std::nullopt);
}

struct ResidencyGuard {
    ResidencyGuard() {
        // A non-zero count here means a previous test leaked a counted segment past its own guard. Left unchecked, the
        // stray release drives live_ negative and every upper-bound assertion below passes vacuously.
        EXPECT_EQ(util::SegmentResidencyTracker::instance().live(), 0) << "counted segment leaked from an earlier test";
        util::SegmentResidencyTracker::instance().reset();
        util::SegmentResidencyTracker::instance().set_enabled(true);
    }
    ~ResidencyGuard() {
        util::SegmentResidencyTracker::instance().set_enabled(false);
        util::SegmentResidencyTracker::instance().reset();
    }
};

// Runs create_column_stats with admission ceiling k and returns the peak number of resident segments.
int64_t high_water_for_create_column_stats(
        version_store::PythonVersionStore& pvs, const StreamId& stream_id, int64_t k
) {
    ScopedConfig config_guard{PROCESSING_UNITS_BOUND_KEY, k};
    ResidencyGuard residency_guard;
    pvs.create_column_stats_version(stream_id, VersionQuery{}, std::make_shared<ReadQuery>());
    return util::SegmentResidencyTracker::instance().high_water();
}
} // namespace

TEST(SegmentResidencyTracking, HighWaterIsPeakNotTotal) {
    ResidencyGuard residency_guard;
    auto& tracker = util::SegmentResidencyTracker::instance();
    tracker.on_segment_resident();
    tracker.on_segment_resident();
    tracker.on_segment_released();
    tracker.on_segment_resident();
    tracker.on_segment_released();
    tracker.on_segment_resident();
    EXPECT_EQ(tracker.high_water(), 2);
}

TEST(SegmentResidencyTracking, SelfMoveAssignmentKeepsSegmentCounted) {
    ResidencyGuard residency_guard;
    {
        SegmentInMemoryImpl segment{};
        segment.mark_from_disk();
        // -Wpragmas first because GCC before 13 does not know -Wself-move and -Werror is on
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic ignored "-Wself-move"
        segment = std::move(segment);
#pragma GCC diagnostic pop
    }
    SegmentInMemoryImpl later{};
    later.mark_from_disk();
    EXPECT_EQ(util::SegmentResidencyTracker::instance().high_water(), 1);
}

TEST(SegmentResidencyTracking, MarkedWhileDisabledIsNotReleased) {
    util::SegmentResidencyTracker::instance().set_enabled(false);
    auto marked_while_disabled = std::make_unique<SegmentInMemoryImpl>();
    marked_while_disabled->mark_from_disk();

    ResidencyGuard residency_guard;
    marked_while_disabled.reset();

    SegmentInMemoryImpl resident{};
    resident.mark_from_disk();
    EXPECT_EQ(util::SegmentResidencyTracker::instance().high_water(), 1);
}

// A clone of a counted segment is a second copy of the decoded data, so it is counted too. Otherwise destroying the
// source would take the count back to zero while the data is still resident in the clone.
TEST(SegmentResidencyTracking, CloneOfMarkedSegmentIsCounted) {
    ResidencyGuard residency_guard;
    auto& tracker = util::SegmentResidencyTracker::instance();
    {
        auto source = std::make_unique<SegmentInMemoryImpl>();
        source->mark_from_disk();
        auto clone = source->clone();
        EXPECT_EQ(tracker.high_water(), 2);
        source.reset();
        EXPECT_EQ(tracker.live(), 1);
    }
    EXPECT_EQ(tracker.live(), 0);
    EXPECT_EQ(tracker.high_water(), 2);
}

TEST(SegmentResidencyTracking, CloneOfUnmarkedSegmentIsNotCounted) {
    ResidencyGuard residency_guard;
    SegmentInMemoryImpl source{};
    auto clone = source.clone();
    EXPECT_EQ(util::SegmentResidencyTracker::instance().high_water(), 0);
}

struct ColumnStatsStoreTest : TestStore {
  protected:
    std::string get_name() override { return "test.column_stats"; }
};

TEST_F(ColumnStatsStoreTest, ResidencyBoundedByAdmission) {
    const StreamId stream_id{"bounded"};
    const size_t num_cols = 4;
    const size_t col_per_slice = 2;
    const size_t num_rows = 40;
    const size_t row_per_slice = 2;
    const size_t max_unit_size = num_cols / col_per_slice;
    const size_t num_segments = (num_rows / row_per_slice) * max_unit_size;
    const int64_t k = 2;

    write_sliced_symbol(*test_store_, stream_id, num_cols, num_rows, col_per_slice, row_per_slice);

    const int64_t high_water_mark = high_water_for_create_column_stats(*test_store_, stream_id, k);

    ASSERT_GT(num_segments, static_cast<size_t>(k) * max_unit_size);
    EXPECT_GE(high_water_mark, static_cast<int64_t>(max_unit_size));
    EXPECT_LE(high_water_mark, static_cast<int64_t>(k * max_unit_size));
}

namespace {

// Builds num_units processing units of unit_size segments each.
std::shared_ptr<version_store::ProcessingUnitAdmissionHandler> make_admission_handler(
        size_t num_units, size_t unit_size, size_t max_processing_units_in_flight, size_t read_window,
        std::vector<int>& fire_count, std::deque<folly::Promise<pipelines::SegmentAndSlice>>& kept
) {
    using namespace arcticdb::pipelines;
    std::vector<RangesAndKey> ranges;
    std::vector<std::vector<size_t>> units;
    for (size_t u = 0; u < num_units; ++u) {
        std::vector<size_t> unit;
        for (size_t c = 0; c < unit_size; ++c) {
            const size_t idx = ranges.size();
            ranges.emplace_back(RowRange{idx, idx + 1}, ColRange{0, 1}, entity::AtomKey{});
            unit.push_back(idx);
        }
        units.push_back(std::move(unit));
    }
    fire_count.assign(num_units * unit_size, 0);
    version_store::SegmentReader reader = [&fire_count, &kept](RangesAndKey&& rk) {
        ++fire_count.at(rk.row_range().first);
        kept.emplace_back();
        return kept.back().getFuture();
    };
    return std::make_shared<version_store::ProcessingUnitAdmissionHandler>(
            std::move(reader), std::move(ranges), std::move(units), max_processing_units_in_flight, read_window
    );
}
} // namespace

TEST(ProcessingUnitAdmissionHandlerTest, FiresAtMostKUnitsThenAdvancesOnCompletion) {
    const size_t num_units = 5;
    const size_t unit_size = 2;
    const size_t k = 2;
    // Read window larger than all work, so the residency budget is the only limiter
    const size_t read_window = num_units * unit_size;
    std::vector<int> fire_count;
    std::deque<folly::Promise<pipelines::SegmentAndSlice>> kept;
    auto admission = make_admission_handler(num_units, unit_size, k, read_window, fire_count, kept);

    admission->admit_initial_processing_units();
    // Only the first k units are loaded up front.
    for (size_t i = 0; i < num_units * unit_size; ++i) {
        EXPECT_EQ(fire_count.at(i), i < k * unit_size ? 1 : 0) << "index " << i << " after admit_initial";
    }

    // Each completion admits exactly one further unit, in order.
    for (size_t completed = 0; completed < num_units; ++completed) {
        admission->on_processing_unit_complete();
        const size_t expected_fired = std::min(k + completed + 1, num_units) * unit_size;
        for (size_t i = 0; i < num_units * unit_size; ++i) {
            EXPECT_EQ(fire_count.at(i), i < expected_fired ? 1 : 0)
                    << "index " << i << " after " << completed + 1 << " completions";
        }
    }

    // Extra completions are no-ops
    admission->on_processing_unit_complete();
    for (auto count : fire_count) {
        EXPECT_EQ(count, 1);
    }
}

// A segment referenced by two units is loaded once, even though both units are admitted.
TEST(ProcessingUnitAdmissionHandlerTest, SharedSegmentFiredOnce) {
    using namespace arcticdb::pipelines;
    std::vector<RangesAndKey> ranges;
    for (size_t i = 0; i < 3; ++i) {
        ranges.emplace_back(RowRange{i, i + 1}, ColRange{0, 1}, entity::AtomKey{});
    }
    std::vector<std::vector<size_t>> units{{0, 1}, {1, 2}}; // segment 1 shared

    std::vector<int> fire_count(3, 0);
    std::deque<folly::Promise<SegmentAndSlice>> kept;
    version_store::SegmentReader reader = [&fire_count, &kept](RangesAndKey&& rk) {
        ++fire_count.at(rk.row_range().first);
        kept.emplace_back();
        return kept.back().getFuture();
    };
    auto admission = std::make_shared<version_store::ProcessingUnitAdmissionHandler>(
            std::move(reader),
            std::move(ranges),
            std::move(units),
            /*max_processing_units_in_flight=*/2,
            /*read_window=*/3
    );

    admission->admit_initial_processing_units();
    EXPECT_EQ(fire_count.at(0), 1);
    EXPECT_EQ(fire_count.at(1), 1); // shared, fired once despite two referencing units
    EXPECT_EQ(fire_count.at(2), 1);
}

// With the residency budget non-binding (all units eligible up front), the read window caps the number of reads in
// flight, and each completed read frees exactly one slot so the next eligible segment is launched.
TEST(ProcessingUnitAdmissionHandlerTest, ReadWindowLimitsInFlightReads) {
    using namespace arcticdb::pipelines;
    const size_t num_units = 4;
    const size_t unit_size = 2;
    const size_t total_segments = num_units * unit_size;
    const size_t k = num_units; // residency non-binding: every unit eligible at once
    const size_t read_window = 3;

    std::vector<int> fire_count;
    // setValue below holds a reference into this deque while its continuation dispatches the next read, which appends
    // here. A vector would reallocate under that reference; a deque never invalidates references to existing elements.
    std::deque<folly::Promise<SegmentAndSlice>> kept;
    auto admission = make_admission_handler(num_units, unit_size, k, read_window, fire_count, kept);

    auto total_fired = [&fire_count]() {
        return static_cast<size_t>(std::count_if(fire_count.begin(), fire_count.end(), [](int c) { return c > 0; }));
    };

    admission->admit_initial_processing_units();
    EXPECT_EQ(total_fired(), read_window); // window caps reads in flight

    // Completing a read frees exactly one window slot, launching exactly one more until the eligible work runs out.
    for (size_t completed = 0; completed < total_segments; ++completed) {
        EXPECT_EQ(total_fired(), std::min(read_window + completed, total_segments))
                << "before completion " << completed;
        kept.at(completed).setValue(
                SegmentAndSlice(RangesAndKey{RowRange{0, 1}, ColRange{0, 1}, entity::AtomKey{}}, SegmentInMemory{})
        );
    }
    EXPECT_EQ(total_fired(), total_segments);
}

namespace {
struct TinyThreadPool {
    TinyThreadPool() {
        ConfigsMap::instance()->set_int("VersionStore.NumIOThreads", 1);
        ConfigsMap::instance()->set_int("VersionStore.NumCPUThreads", 1);
        async::TaskScheduler::reattach_instance();
    }
    ~TinyThreadPool() {
        ConfigsMap::instance()->unset_int("VersionStore.NumIOThreads");
        ConfigsMap::instance()->unset_int("VersionStore.NumCPUThreads");
        async::TaskScheduler::reattach_instance();
    }
};
} // namespace

// Overlapping units with a ceiling of 1, and thread pools of size 1. The first unit loads the
// shared segment, the second is admitted only when the first completes, and it does not reload the shared segment.
// Reaching .get() shows that the admit -> process -> advance loop does not deadlock.
TEST(ProcessingUnitAdmissionHandlerTest, OverlappingUnitsAdvanceWithCeilingOne) {
    using namespace arcticdb::pipelines;
    TinyThreadPool tiny_pool;

    const std::vector<std::vector<size_t>> units{{0, 1}, {1, 2}}; // segment 1 shared between the two units
    const size_t num_segments = 3;

    std::vector<RangesAndKey> ranges;
    for (size_t i = 0; i < num_segments; ++i) {
        ranges.emplace_back(RowRange{i, i + 1}, ColRange{0, 1}, entity::AtomKey{});
    }

    std::vector<std::atomic<int>> fire_count(num_segments);
    for (auto& count : fire_count) {
        count.store(0);
    }

    version_store::SegmentReader reader = [&fire_count](RangesAndKey&& rk) {
        const auto idx = rk.row_range().first;
        return folly::via(&async::io_executor(), [&fire_count, idx, rk = std::move(rk)]() mutable {
            fire_count[idx].fetch_add(1, std::memory_order_relaxed);
            return SegmentAndSlice(std::move(rk), SegmentInMemory{});
        });
    };

    auto admission = std::make_shared<version_store::ProcessingUnitAdmissionHandler>(
            std::move(reader),
            std::move(ranges),
            std::vector<std::vector<size_t>>(units),
            /*max_processing_units_in_flight=*/1,
            /*read_window=*/num_segments
    );

    // Mirror schedule_first_iteration: split the shared segment's future, then build one future per unit that processes
    // on the CPU pool and advances admission on completion.
    auto segment_futures = admission->futures();
    std::vector<EntityFetchCount> fetch_counts{1, 2, 1}; // segment 1 is referenced by both units
    auto splitters = split_futures(std::move(segment_futures), fetch_counts);

    std::vector<folly::Future<folly::Unit>> unit_futures;
    for (const auto& unit : units) {
        std::vector<folly::Future<SegmentAndSlice>> local;
        for (auto i : unit) {
            util::variant_match(
                    splitters.at(i),
                    [&local](folly::Future<SegmentAndSlice>& fut) { local.emplace_back(std::move(fut)); },
                    [&local](folly::FutureSplitter<SegmentAndSlice>& splitter) {
                        local.emplace_back(splitter.getFuture());
                    }
            );
        }
        unit_futures.emplace_back(folly::collect(local)
                                          .via(&async::cpu_executor())
                                          .thenValue([](auto&&) { return folly::Unit{}; })
                                          .ensure([admission]() { admission->on_processing_unit_complete(); }));
    }

    auto all_done = folly::collect(unit_futures).via(&async::io_executor());
    admission->admit_initial_processing_units();
    std::move(all_done).get();

    EXPECT_EQ(fire_count.at(0).load(), 1);
    EXPECT_EQ(fire_count.at(1).load(), 1);
    EXPECT_EQ(fire_count.at(2).load(), 1);
}

TEST(ProcessingUnitAdmissionHandlerTest, AdvancesWithReadWindowOfOne) {
    using namespace arcticdb::pipelines;
    TinyThreadPool tiny_pool;

    const size_t num_units = 3;
    const size_t unit_size = 2;
    const size_t num_segments = num_units * unit_size;

    std::vector<RangesAndKey> ranges;
    std::vector<std::vector<size_t>> units;
    for (size_t u = 0; u < num_units; ++u) {
        std::vector<size_t> unit;
        for (size_t c = 0; c < unit_size; ++c) {
            const size_t idx = ranges.size();
            ranges.emplace_back(RowRange{idx, idx + 1}, ColRange{0, 1}, entity::AtomKey{});
            unit.push_back(idx);
        }
        units.push_back(std::move(unit));
    }

    std::vector<std::atomic<int>> fire_count(num_segments);
    for (auto& count : fire_count) {
        count.store(0);
    }

    version_store::SegmentReader reader = [&fire_count](RangesAndKey&& rk) {
        const auto idx = rk.row_range().first;
        return folly::via(&async::io_executor(), [&fire_count, idx, rk = std::move(rk)]() mutable {
            fire_count[idx].fetch_add(1, std::memory_order_relaxed);
            return SegmentAndSlice(std::move(rk), SegmentInMemory{});
        });
    };

    auto admission = std::make_shared<version_store::ProcessingUnitAdmissionHandler>(
            std::move(reader),
            std::move(ranges),
            std::vector<std::vector<size_t>>(units),
            /*max_processing_units_in_flight=*/1,
            /*read_window=*/1
    );

    auto segment_futures = admission->futures();
    std::vector<folly::Future<folly::Unit>> unit_futures;
    for (const auto& unit : units) {
        std::vector<folly::Future<SegmentAndSlice>> local;
        for (auto i : unit) {
            local.emplace_back(std::move(segment_futures.at(i)));
        }
        unit_futures.emplace_back(folly::collect(local)
                                          .via(&async::cpu_executor())
                                          .thenValue([](auto&&) { return folly::Unit{}; })
                                          .ensure([admission]() { admission->on_processing_unit_complete(); }));
    }

    auto all_done = folly::collect(unit_futures).via(&async::io_executor());
    admission->admit_initial_processing_units();
    std::move(all_done).get(std::chrono::seconds(30));

    for (size_t i = 0; i < num_segments; ++i) {
        EXPECT_EQ(fire_count.at(i).load(), 1) << "segment " << i;
    }
}

namespace {
// Two units of two segments each, a ceiling and window of 1, and a reader that throws synchronously for
// throwing_segment. Mirrors schedule_first_iteration, including the .ensure that advances admission whether the unit
// succeeded or failed.
std::vector<folly::Try<folly::Unit>> run_synchronous_read_throw_case(size_t throwing_segment) {
    using namespace arcticdb::pipelines;
    const std::vector<std::vector<size_t>> units{{0, 1}, {2, 3}};
    const size_t num_segments = 4;

    std::vector<RangesAndKey> ranges;
    for (size_t i = 0; i < num_segments; ++i) {
        ranges.emplace_back(RowRange{i, i + 1}, ColRange{0, 1}, entity::AtomKey{});
    }

    version_store::SegmentReader reader = [throwing_segment](RangesAndKey&& rk) -> folly::Future<SegmentAndSlice> {
        if (rk.row_range().first == throwing_segment) {
            throw std::runtime_error("synchronous read failure");
        }
        return folly::via(&async::io_executor(), [rk = std::move(rk)]() mutable {
            return SegmentAndSlice(std::move(rk), SegmentInMemory{});
        });
    };

    auto admission = std::make_shared<version_store::ProcessingUnitAdmissionHandler>(
            std::move(reader),
            std::move(ranges),
            std::vector<std::vector<size_t>>(units),
            /*max_processing_units_in_flight=*/1,
            /*read_window=*/1
    );

    auto segment_futures = admission->futures();
    std::vector<folly::Future<folly::Unit>> unit_futures;
    for (const auto& unit : units) {
        std::vector<folly::Future<SegmentAndSlice>> local;
        for (auto i : unit) {
            local.emplace_back(std::move(segment_futures.at(i)));
        }
        unit_futures.emplace_back(folly::collect(local)
                                          .via(&async::cpu_executor())
                                          .thenValue([](auto&&) { return folly::Unit{}; })
                                          .ensure([admission]() { admission->on_processing_unit_complete(); }));
    }

    auto all_done = folly::collectAll(unit_futures).via(&async::io_executor());
    admission->admit_initial_processing_units();
    return std::move(all_done).get(std::chrono::seconds(30));
}
} // namespace

// A reader that throws synchronously rather than returning a failed future must still free its window slot and complete
// its promise. Otherwise the slot is taken forever and nothing ever satisfies the collect for that segment, so with a
// window of 1 the whole pipeline stops. Unit 1 completing shows that the window recovered, because with a
// ceiling of 1 it is only admitted once unit 0 has finished.
TEST(ProcessingUnitAdmissionHandlerTest, SynchronousReadThrowDoesNotHang) {
    TinyThreadPool tiny_pool;
    const auto results = run_synchronous_read_throw_case(/*throwing_segment=*/1); // second segment of unit 0
    EXPECT_TRUE(results.at(0).hasException());
    EXPECT_TRUE(results.at(1).hasValue());
}

TEST(ProcessingUnitAdmissionHandlerTest, SynchronousReadThrowOnFirstSegmentOfUnitDoesNotHang) {
    TinyThreadPool tiny_pool;
    const auto results = run_synchronous_read_throw_case(/*throwing_segment=*/0); // first segment of unit 0
    EXPECT_TRUE(results.at(0).hasException());
    EXPECT_TRUE(results.at(1).hasValue());
}

TEST(ProcessingUnitAdmissionHandlerTest, SynchronousReadThrowStillDrainsRestOfUnit) {
    using namespace arcticdb::pipelines;
    const std::vector<std::vector<size_t>> units{{0, 1}, {2, 3}};
    std::vector<RangesAndKey> ranges;
    for (size_t i = 0; i < 4; ++i) {
        ranges.emplace_back(RowRange{i, i + 1}, ColRange{0, 1}, entity::AtomKey{});
    }

    std::vector<int> fire_count(4, 0);
    version_store::SegmentReader reader = [&fire_count](RangesAndKey&& rk) -> folly::Future<SegmentAndSlice> {
        const auto i = rk.row_range().first;
        ++fire_count.at(i);
        if (i == 0) {
            throw std::runtime_error("synchronous read failure");
        }
        return folly::makeFuture(SegmentAndSlice(std::move(rk), SegmentInMemory{}));
    };

    auto admission = std::make_shared<version_store::ProcessingUnitAdmissionHandler>(
            std::move(reader),
            std::move(ranges),
            std::vector<std::vector<size_t>>(units),
            /*max_processing_units_in_flight=*/1,
            /*read_window=*/1
    );

    auto& executor = folly::QueuedImmediateExecutor::instance();
    auto segment_futures = admission->futures();
    std::vector<folly::Future<folly::Unit>> unit_futures;
    for (const auto& unit : units) {
        std::vector<folly::Future<SegmentAndSlice>> local;
        for (auto i : unit) {
            local.emplace_back(std::move(segment_futures.at(i)));
        }
        unit_futures.emplace_back(folly::collectAll(local)
                                          .via(&executor)
                                          .thenValue([](std::vector<folly::Try<SegmentAndSlice>>&& tries) {
                                              for (const auto& t : tries) {
                                                  t.throwUnlessValue();
                                              }
                                              return folly::Unit{};
                                          })
                                          .ensure([admission]() { admission->on_processing_unit_complete(); }));
    }
    auto all_done = folly::collectAll(unit_futures).via(&executor);

    admission->admit_initial_processing_units();

    const std::vector<int> expected_fire_counts(4, 1);
    EXPECT_EQ(expected_fire_counts, fire_count);
    // Before the get, which has no timeout and so would hang rather than fail if a unit never completed.
    ASSERT_TRUE(all_done.isReady()) << "stalled: unit 0 never completed, so unit 1 was never admitted";
    const auto results = std::move(all_done).get();
    EXPECT_TRUE(results.at(0).hasException());
    EXPECT_TRUE(results.at(1).hasValue());
}

// Static schema with column_group_size changed between the initial write and a later append, so the appended row slices
// have a different number of column slices than the original ones. The processing units therefore have different sizes,
// and the bound must use the max across units to avoid deadlock.
TEST(ColumnStatsMixedColSlicing, ResidencyBoundedWithUnevenUnits) {
    using namespace arcticdb::pipelines;

    const std::array fields{
            scalar_field(entity::DataType::FLOAT64, "col_0"),
            scalar_field(entity::DataType::FLOAT64, "col_1"),
            scalar_field(entity::DataType::FLOAT64, "col_2"),
            scalar_field(entity::DataType::FLOAT64, "col_3")
    };

    arcticdb::proto::storage::VersionStoreConfig cfg;
    cfg.mutable_write_options()->set_segment_row_size(2);
    cfg.mutable_write_options()->set_column_group_size(2); // 4 cols -> 2 column slices per row slice
    auto engine = get_test_engine<version_store::PythonVersionStore>({cfg});

    const StreamId stream_id{"uneven"};
    const size_t num_rows = 8;

    auto initial = get_test_frame<stream::TimeseriesIndex>(stream_id, fields, num_rows, 0);
    engine.write_versioned_dataframe_internal(stream_id, std::move(initial.frame_), false, false, false);

    // Widen the column group so the appended row slices hold all 4 cols in a single column slice (unit size 1), whereas
    // the original row slices have 2 column slices (unit size 2).
    auto wide_cfg = engine.cfg();
    wide_cfg.mutable_write_options()->set_column_group_size(4);
    engine.configure(std::move(wide_cfg));

    auto appended = get_test_frame<stream::TimeseriesIndex>(stream_id, fields, num_rows, num_rows);
    engine.append_internal(stream_id, std::move(appended.frame_), version_store::AppendOptions{});

    // Confirm the slicing: 4 original row slices x 2 column slices + 4 appended row slices x 1 column
    // slice = 12 data segments. A uniform layout would have 16
    size_t data_segments = 0;
    engine._test_get_store()->iterate_type(KeyType::TABLE_DATA, [&data_segments](VariantKey&&) { ++data_segments; });
    ASSERT_EQ(data_segments, 12u);

    const int64_t k = 2;
    const size_t max_unit_size = 2; // original row slices span 2 column slices; appended ones span 1
    const int64_t bound = k * static_cast<int64_t>(max_unit_size);

    int64_t high_water = high_water_for_create_column_stats(engine, stream_id, k);

    EXPECT_GT(high_water, 0);
    EXPECT_LE(high_water, bound);

    auto info = engine.get_column_stats_info_version(stream_id, VersionQuery{});
    const std::unordered_map<std::string, std::unordered_set<std::string>> expected{
            {"col_0", {"MINMAX"}},
            {"col_1", {"MINMAX"}},
            {"col_2", {"MINMAX"}},
            {"col_3", {"MINMAX"}},
            {"time", {"MINMAX"}}
    };
    EXPECT_EQ(info.to_map(), expected);
}

// As above but with the wider column slice written first, so the largest processing units are the later row
// slices.
TEST(ColumnStatsMixedColSlicing, ResidencyBoundedWithUnevenUnitsWiderFirst) {
    using namespace arcticdb::pipelines;

    const std::array fields{
            scalar_field(entity::DataType::FLOAT64, "col_0"),
            scalar_field(entity::DataType::FLOAT64, "col_1"),
            scalar_field(entity::DataType::FLOAT64, "col_2"),
            scalar_field(entity::DataType::FLOAT64, "col_3")
    };

    arcticdb::proto::storage::VersionStoreConfig cfg;
    cfg.mutable_write_options()->set_segment_row_size(2);
    cfg.mutable_write_options()->set_column_group_size(4); // 4 cols -> 1 column slice per row slice
    auto engine = get_test_engine<version_store::PythonVersionStore>({cfg});

    const StreamId stream_id{"uneven_wider_first"};
    const size_t num_rows = 8;

    auto initial = get_test_frame<stream::TimeseriesIndex>(stream_id, fields, num_rows, 0);
    engine.write_versioned_dataframe_internal(stream_id, std::move(initial.frame_), false, false, false);

    // Narrow the column group so the appended row slices have 2 column slices (unit size 2), whereas the original row
    // slices have 1 (unit size 1).
    auto narrow_cfg = engine.cfg();
    narrow_cfg.mutable_write_options()->set_column_group_size(2);
    engine.configure(std::move(narrow_cfg));

    auto appended = get_test_frame<stream::TimeseriesIndex>(stream_id, fields, num_rows, num_rows);
    engine.append_internal(stream_id, std::move(appended.frame_), version_store::AppendOptions{});

    // 4 original row slices x 1 column slice + 4 appended row slices x 2 column slices = 12 data segments.
    size_t data_segments = 0;
    engine._test_get_store()->iterate_type(KeyType::TABLE_DATA, [&data_segments](VariantKey&&) { ++data_segments; });
    ASSERT_EQ(data_segments, 12u);

    const int64_t k = 2;
    const size_t max_unit_size = 2; // appended row slices span 2 column slices
    const int64_t bound = k * static_cast<int64_t>(max_unit_size);

    int64_t high_water = high_water_for_create_column_stats(engine, stream_id, k);

    EXPECT_GT(high_water, 0);
    EXPECT_LE(high_water, bound);

    auto info = engine.get_column_stats_info_version(stream_id, VersionQuery{});
    const std::unordered_map<std::string, std::unordered_set<std::string>> expected{
            {"col_0", {"MINMAX"}},
            {"col_1", {"MINMAX"}},
            {"col_2", {"MINMAX"}},
            {"col_3", {"MINMAX"}},
            {"time", {"MINMAX"}}
    };
    EXPECT_EQ(info.to_map(), expected);
}

namespace {
struct ScopedThreadCounts {
    ScopedThreadCounts(int64_t cpu, int64_t io) {
        ConfigsMap::instance()->set_int("VersionStore.NumCPUThreads", cpu);
        ConfigsMap::instance()->set_int("VersionStore.NumIOThreads", io);
        async::TaskScheduler::reattach_instance();
    }
    ~ScopedThreadCounts() {
        ConfigsMap::instance()->unset_int("VersionStore.NumCPUThreads");
        ConfigsMap::instance()->unset_int("VersionStore.NumIOThreads");
        async::TaskScheduler::reattach_instance();
    }
};
} // namespace

// Dividing the window by a wide unit size rounds down to almost nothing, so the per-CPU-thread term is what keeps
// enough units admitted to occupy the CPU pool.
TEST(MaxResidentProcessingUnits, CpuTermDominatesForWideUnits) {
    ScopedThreadCounts threads{/*cpu=*/4, /*io=*/2};
    const std::vector<std::vector<size_t>> wide_units{{0, 1, 2, 3, 4, 5, 6, 7}}; // max_unit_size = 8
    // read_window = 2*2 = 4; ceil(4 / 8) = 1, plus cpu_thread_count = 4
    EXPECT_EQ(version_store::max_resident_processing_units(wide_units), 5u);
    EXPECT_GE(version_store::max_resident_processing_units(wide_units), 4u);
}

// For single-segment units the budget must exceed the read window, so segments stay queued ahead of it and read
// completion keeps dispatching without waiting on a processing unit to finish.
TEST(MaxResidentProcessingUnits, ExceedsReadWindowForNarrowUnits) {
    ScopedThreadCounts threads{/*cpu=*/2, /*io=*/8};
    const std::vector<std::vector<size_t>> narrow_units{{0}, {1}, {2}}; // max_unit_size = 1
    // read_window = 2*8 = 16; ceil(16 / 1) = 16, plus cpu_thread_count = 2
    EXPECT_EQ(version_store::max_resident_processing_units(narrow_units), 18u);
    EXPECT_GT(version_store::max_resident_processing_units(narrow_units), version_store::segment_read_window());
}

// An explicit config override takes precedence over the default.
TEST(MaxResidentProcessingUnits, ConfigOverrideTakesPrecedence) {
    ScopedThreadCounts threads{/*cpu=*/4, /*io=*/8};
    ScopedConfig override_guard{"VersionStore.NumProcessingUnitsLive", 3};
    const std::vector<std::vector<size_t>> units{{0, 1}, {2, 3}};
    EXPECT_EQ(version_store::max_resident_processing_units(units), 3u);
}

// A configured value of 0 is a kill switch: residency is unbounded (the limit becomes the total number of processing
// units, so every unit is eligible at once) and the read window alone governs.
TEST(MaxResidentProcessingUnits, ZeroAdmitsAllUnits) {
    ScopedThreadCounts threads{/*cpu=*/4, /*io=*/8};
    ScopedConfig override_guard{"VersionStore.NumProcessingUnitsLive", 0};
    const std::vector<std::vector<size_t>> units{{0, 1}, {2, 3}, {4, 5}};
    EXPECT_EQ(version_store::max_resident_processing_units(units), units.size());
}

// A negative value is neither a valid budget nor the 0 kill switch, so it is rejected rather than silently floored.
TEST(MaxResidentProcessingUnits, NegativeConfigThrows) {
    ScopedThreadCounts threads{/*cpu=*/4, /*io=*/8};
    ScopedConfig override_guard{"VersionStore.NumProcessingUnitsLive", -1};
    const std::vector<std::vector<size_t>> units{{0, 1}, {2, 3}};
    EXPECT_THROW(version_store::max_resident_processing_units(units), UserInputException);
}

// The read window defaults to 2*io_thread_count, matching the old folly::window(2*io) read path.
TEST(SegmentReadWindow, DefaultsToTwiceIoThreads) {
    ScopedThreadCounts threads{/*cpu=*/4, /*io=*/8};
    EXPECT_EQ(version_store::segment_read_window(), 16u);
}

// An explicit config override takes precedence.
TEST(SegmentReadWindow, ConfigOverrideTakesPrecedence) {
    ScopedThreadCounts threads{/*cpu=*/4, /*io=*/8};
    ScopedConfig override_guard{"VersionStore.SegmentReadWindow", 5};
    EXPECT_EQ(version_store::segment_read_window(), 5u);
}

// Zero and negative values are rejected rather than silently floored to 1, since unlike NumProcessingUnitsLive there
// is no kill-switch meaning for 0 here.
TEST(SegmentReadWindow, NonPositiveConfigThrows) {
    ScopedThreadCounts threads{/*cpu=*/4, /*io=*/8};
    {
        ScopedConfig override_guard{"VersionStore.SegmentReadWindow", 0};
        EXPECT_THROW(version_store::segment_read_window(), UserInputException);
    }
    {
        ScopedConfig override_guard{"VersionStore.SegmentReadWindow", -1};
        EXPECT_THROW(version_store::segment_read_window(), UserInputException);
    }
}

} // namespace arcticdb
