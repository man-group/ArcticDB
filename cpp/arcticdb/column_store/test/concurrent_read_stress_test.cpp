#include <gtest/gtest.h> // googletest header file
#include <string>
#include <algorithm>
#include <fmt/format.h>

#include <util/random.h>
#include <util/timer.hpp>
#include <util/test/generators.hpp>
#include "stream/row_builder.hpp"
#include <stream/test/stream_test_common.hpp>

#include <stream/aggregator.hpp>
#include <pipeline/query.hpp>

#include <version/version_store_api.hpp>

#include <folly/executors/FutureExecutor.h>

#include <util/clock.hpp>
#include <entity/read_result.hpp>

#define GTEST_COUT std::cerr << "[          ] [ INFO ] "

using namespace arcticdb;
using namespace folly;
namespace as = arcticdb::stream;

#define NANOS_IN_ONE_SECOND 1'000'000'000

bool duration_elapsed(size_t start_ns, size_t now_ns, size_t interval_ns) {
    return now_ns - start_ns > interval_ns;
}

struct ReadTask {
    std::shared_ptr<version_store::PythonVersionStore> store_;
    size_t lifetime_ns_;
    StreamId symbol_;

    ReadTask(std::shared_ptr<version_store::PythonVersionStore> store, size_t lifetime_s, StreamId symbol) :
            store_(std::move(store)),
            lifetime_ns_(lifetime_s * NANOS_IN_ONE_SECOND),
            symbol_(symbol){
    }

    void assert_read_result_cols(size_t cols) const { // gtest macros must be used in a function that returns void....
        ASSERT_EQ(cols, 1U + 5U); // 1 for the index, 5 for the columns
    }

    folly::Future<folly::Unit> operator()() {
        size_t start_time_ns = util::SysClock::coarse_nanos_since_epoch();
        auto last_metrics_ns = start_time_ns;
        int count_metric = 0;
        int key_not_found_metric = 0;
        while (!duration_elapsed(start_time_ns, util::SysClock::coarse_nanos_since_epoch(), lifetime_ns_)) {
            using namespace arcticdb::pipelines;

            auto ro = ReadOptions{};
            ro.allow_sparse_ = true;
            ro.set_dynamic_schema(false);
            ro.set_incompletes(true);
            ReadQuery read_query;
            read_query.row_filter = universal_range();

            try {
                auto read_result = store_->read_dataframe_version(symbol_, VersionQuery{}, read_query, ro);

                assert_read_result_cols(read_result.frame_data.frame().num_columns());
                count_metric++;
            } catch (const storage::KeyNotFoundException&) {
                // The append data keys are added to the pipeline context after the index keys, and then all
                // are read and added to the output frame segment. In the intermediate time the compact could
                // delete the append data keys. Therefore, the solution is to read the append data keys first, then read
                // their segments (and find a way to skip any that are missing, but this is difficult as it is a chained
                // list of folly futures which all expect return values), and **then** read the data keys and
                // the data segments, de-duplicating as needed.
                key_not_found_metric++;
            }

            size_t now = util::SysClock::coarse_nanos_since_epoch();
            if(duration_elapsed(last_metrics_ns, now, NANOS_IN_ONE_SECOND)) {
                GTEST_COUT << "In last second, read " << count_metric << " times, and saw KeyNotFoundException " << key_not_found_metric << " times" << std::endl;
                last_metrics_ns = now;
                count_metric = key_not_found_metric = 0;
            }
        }
        return makeFuture(Unit{});
    }
};


struct AppendTask {
    std::shared_ptr<version_store::PythonVersionStore> store_;
    size_t lifetime_ns_;
    StreamId symbol_;

    AppendTask(std::shared_ptr<version_store::PythonVersionStore> store, size_t lifetime_s, StreamId symbol) :
            store_(std::move(store)),
            lifetime_ns_(lifetime_s * NANOS_IN_ONE_SECOND),
            symbol_(symbol){
    }

    void append_data() {
        using namespace arcticdb;
        const uint64_t NumColumns = 5;
        const uint64_t NumRows = 2000;
        const uint64_t SegmentPolicyRows = 1000;


        // generate vals
        FieldCollection columns;
        for (auto i = 0u; i < NumColumns; ++i) {
            columns.add_field(scalar_field(DataType::UINT64, fmt::format("col_{}", i)));
        }

        const auto index = as::TimeseriesIndex::default_index();
        auto desc = index.create_stream_descriptor(symbol_, fields_from_range(columns));
        as::FixedSchema schema{desc, index};

        SegmentsSink sink;
        as::FixedTimestampAggregator agg(std::move(schema), [&](SegmentInMemory &&mem) {
            sink.segments_.push_back(std::move(mem));
        }, as::RowCountSegmentPolicy{SegmentPolicyRows});

        for (auto i = 0u; i < NumRows; ++i) {
            agg.start_row(timestamp{i})([&](auto &rb) {
                for (timestamp j = 1u; j <= timestamp(NumColumns); ++j)
                    rb.set_scalar(j, uint64_t(i + j));
            });
        }
        agg.commit();
        for(auto &seg : sink.segments_) {
            arcticdb::append_incomplete_segment(store_->_test_get_store(), symbol_, std::move(seg));
        }
    }

    folly::Future<folly::Unit> operator()() {
        size_t start_time_ns = util::SysClock::coarse_nanos_since_epoch();
        auto last_metrics_ns = start_time_ns;
        int count_metric = 0;
        while (!duration_elapsed(start_time_ns, util::SysClock::coarse_nanos_since_epoch(), lifetime_ns_)) {
            append_data();
            count_metric++;
            size_t now = util::SysClock::coarse_nanos_since_epoch();
            if(duration_elapsed(last_metrics_ns, now, NANOS_IN_ONE_SECOND)) {
                GTEST_COUT << "In last second, appended " << count_metric << " times." << std::endl;
                last_metrics_ns = now;
                count_metric = 0;
            }
        }
        return makeFuture(Unit{});
    }
};

struct CompactTask {
    std::shared_ptr<version_store::PythonVersionStore> store_;
    size_t lifetime_ns_;
    StreamId symbol_;

    CompactTask(std::shared_ptr<version_store::PythonVersionStore> store, size_t lifetime_s, StreamId symbol) :
            store_(std::move(store)),
            lifetime_ns_(lifetime_s * NANOS_IN_ONE_SECOND),
            symbol_(symbol){
    }

    void compact() {
        store_->compact_incomplete(symbol_, true, false, false, true, std::nullopt, true); // last is prune previous
    }

    folly::Future<folly::Unit> operator()() {
        size_t start_time_ns = util::SysClock::coarse_nanos_since_epoch();
        auto last_metrics_ns = start_time_ns;
        int count_metric = 0;
        int no_incompletes_metric = 0;
        while (!duration_elapsed(start_time_ns, util::SysClock::coarse_nanos_since_epoch(), lifetime_ns_)) {
            try {
                compact();
                count_metric++;
            } catch (std::runtime_error& e) {
                if (std::string(e.what()).find("No incomplete segments found") != std::string::npos) {
                    no_incompletes_metric++;
                } else {
                    throw e;
                }
            }
            size_t now = util::SysClock::coarse_nanos_since_epoch();
            if(duration_elapsed(last_metrics_ns, now, NANOS_IN_ONE_SECOND)) {
                GTEST_COUT << "In last second, compacted " << count_metric << " times"
                    << " and saw no incompletes error " << no_incompletes_metric << "times." << std::endl;
                last_metrics_ns = now;
                count_metric = 0;
                no_incompletes_metric = 0;
            }
        }
        return makeFuture(Unit{});
    }
};

// Comment in stream_test_common.hpp says to override inline (i.e manually copy code of the function), but since
// these are still useful as separate functions in this test case, so just define new names for the shared_cfg case.
inline std::shared_ptr<storage::Library> test_library_shared_cfg(const std::string &lib_name) {
    static auto path_and_config = test_config(lib_name);
    auto [lib_path, config] = path_and_config;
    return test_library_from_config(lib_path, config);
}

inline auto test_store_shared_cfg(const std::string &lib_name) {
    auto library = test_library_shared_cfg(lib_name);
    auto version_store = std::make_shared<version_store::PythonVersionStore>(library);
    return version_store;
}

TEST(ConcurrentReadStress, ScalarIntAppendJoeChange) {

    std::string lib_name("test.compact_stress_test");

    //ScopedConfig reload_interval("VersionMap.ReloadInterval", 0);

    folly::FutureExecutor<folly::CPUThreadPoolExecutor> exec{4};

    std::vector<Future<Unit>> futures;
    size_t lifetime_s = 120; // time to spend reading
    StreamId symbol{"symbol"};
    auto read_task = ReadTask{test_store_shared_cfg(lib_name), lifetime_s, symbol};
    auto append_task = AppendTask{test_store_shared_cfg(lib_name), lifetime_s, symbol};
    auto compact_task = CompactTask{test_store_shared_cfg(lib_name), lifetime_s, symbol};
    // We must do these first so that the reader has a version to read. Could just give it append data,
    // but then during the first compaction, it will see there's no versions and will have a race condition
    // where then there will also be no append data since the compactor will sweep them up into version keys.
    // So we **need** the re-ordering refactor described above where reader reads the append keys first, and reads
    // the segments, then moves onto the index keys with some good dedup logic before we can avoid having to do these
    // first steps in this unit test.
    append_task.append_data();
    compact_task.compact();

    futures.emplace_back(exec.addFuture(read_task));
    futures.emplace_back(exec.addFuture(append_task));
    futures.emplace_back(exec.addFuture(compact_task));
    collect(futures).get();
}