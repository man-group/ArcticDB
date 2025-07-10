/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <string>
#include <algorithm>
#include <arcticdb/stream/incompletes.hpp>

#include <arcticdb/util/random.h>
#include <arcticdb/util/timer.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/stream/row_builder.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/util/native_handler.hpp>

using namespace arcticdb;
namespace as = arcticdb::stream;

#define GTEST_COUT std::cerr << "[          ] [ INFO ]"

struct IngestionStressStore : TestStore {
protected:
    std::string get_name() override {
        return "ingestion_stress";
    }
};

TEST(IngestionStress, ScalarInt) {
    const int64_t NumColumns = 20;
    const int64_t NumRows = 10000;
    const uint64_t SegmentPolicyRows = 1000;

    std::vector<FieldRef> columns;
    for (auto i = 0; i < NumColumns; ++i)
        columns.emplace_back(scalar_field(DataType::UINT64, "uint64"));

    const auto index = as::TimeseriesIndex::default_index();
    as::FixedSchema schema{
        index.create_stream_descriptor(NumericId{123}, fields_from_range(std::move(columns))), index
    };

    SegmentsSink sink;
    as::FixedTimestampAggregator agg(std::move(schema), [&](SegmentInMemory &&mem) {
        sink.segments_.push_back(std::move(mem));
    }, as::RowCountSegmentPolicy{SegmentPolicyRows});

    std::string timer_name("ingestion_stress");
    interval_timer timer(timer_name);
    size_t x = 0;
    for (auto i = 0; i < NumRows; ++i) {
        agg.start_row(timestamp{i})([&](auto &rb) {
            for (timestamp j = 1u; j <= timestamp(NumColumns); ++j)
                rb.set_scalar(j, uint64_t(i + j));
        });
    }
    timer.stop_timer(timer_name);
    GTEST_COUT << x << " " << timer.display_all() << std::endl;
}

TEST_F(IngestionStressStore, ScalarIntAppend) {
    using namespace arcticdb;
    const uint64_t NumColumns = 1;
    const uint64_t NumRows = 2;
    const uint64_t SegmentPolicyRows = 1000;

    StreamId symbol{"stable"};
    std::vector<SegmentToInputFrameAdapter> data;
    // generate vals
    FieldCollection columns;
    for (timestamp i = 0; i < timestamp(NumColumns); ++i) {
        columns.add_field(scalar_field(DataType::UINT64, fmt::format("col_{}", i)));
    }

    const auto index = as::TimeseriesIndex::default_index();
    auto desc = index.create_stream_descriptor(symbol, {});
    as::DynamicSchema schema{desc, index};

    SegmentsSink sink;
    as::DynamicTimestampAggregator agg(std::move(schema), [&](SegmentInMemory &&mem) {
        sink.segments_.push_back(std::move(mem));
    }, as::RowCountSegmentPolicy{SegmentPolicyRows});


    std::string timer_name("ingestion_stress");
    interval_timer timer(timer_name);
    size_t x = 0;
    for (timestamp i = 0; i < timestamp(NumRows); ++i) {
        agg.start_row(timestamp{i})([&](auto &rb) {
            for (timestamp j = 1u; j <= timestamp(NumColumns); ++j)
                rb.set_scalar_by_name(columns[j-1].name(), uint64_t(i + j), columns[j-1].type().data_type());
        });
    }
    timer.stop_timer(timer_name);
    GTEST_COUT << x << " " << timer.display_all() << std::endl;


    FieldCollection columns_second;
    for (auto i = 0; i < 2; ++i) {
        columns_second.add_field(scalar_field(DataType::UINT64, fmt::format("col_{}", i)));
    }

    auto new_descriptor = index.create_stream_descriptor(symbol, fields_from_range(columns_second));
    for (timestamp i = 0u; i < timestamp(NumRows); ++i) {
        agg.start_row(timestamp(i + NumRows))([&](auto &rb) {
            for (uint64_t j = 1u; j <= 2; ++j)
                rb.set_scalar_by_name(columns_second[j-1].name(), uint64_t(i + j), columns_second[j-1].type().data_type());
        });
    }
    GTEST_COUT << " 2 done";

    agg.finalize();

    for(auto &seg : sink.segments_)
        arcticdb::append_incomplete_segment(test_store_->_test_get_store(), symbol, std::move(seg));

    using namespace arcticdb::pipelines;

    auto ro = ReadOptions{};
    ro.set_allow_sparse(true);
    ro.set_dynamic_schema(true);
    ro.set_incompletes(true);
    auto read_query = std::make_shared<ReadQuery>();
    read_query->row_filter = universal_range();
    register_native_handler_data_factory();
    auto handler_data = TypeHandlerRegistry::instance()->get_handler_data(OutputFormat::NATIVE);
    auto read_result = test_store_->read_dataframe_version(symbol, VersionQuery{}, read_query, ro, handler_data);
    GTEST_COUT << "columns in res: " << std::get<PandasOutputFrame>(read_result.frame_data).index_columns().size();
}

TEST_F(IngestionStressStore, ScalarIntDynamicSchema) {
    const uint64_t NumColumnsFirstWrite = 5;
    const uint64_t NumColumnsSecondWrite = 10;
    const int64_t NumRows = 10;
    const uint64_t SegmentPolicyRows = 100;
    StreamId symbol{"blah"};

    std::vector<SegmentToInputFrameAdapter> data;

    // generate vals
    FieldCollection columns_first;
    FieldCollection columns_second;
    for (timestamp i = 0; i < timestamp(NumColumnsFirstWrite); ++i) {
        columns_first.add_field(scalar_field(DataType::UINT64,  fmt::format("col_{}", i)));
    }

    const auto index = as::TimeseriesIndex::default_index();
    as::DynamicSchema schema{index.create_stream_descriptor(symbol, {}), index};

    SegmentsSink sink;
    as::DynamicTimestampAggregator agg(std::move(schema), [&](SegmentInMemory &&mem) {
        sink.segments_.push_back(std::move(mem));
    }, as::RowCountSegmentPolicy{SegmentPolicyRows});

    std::string timer_name("ingestion_stress");
    interval_timer timer(timer_name);
    for (timestamp i = 0; i < timestamp(NumRows); ++i) {
        agg.start_row(timestamp{i})([&](auto &rb) {
            for (uint64_t j = 1u; j < NumColumnsFirstWrite; ++j)
                rb.set_scalar_by_name(columns_first[j-1].name(), uint64_t(i + j), columns_first[j-1].type().data_type());
        });
    }
    timer.stop_timer(timer_name);
    GTEST_COUT << " 1 done";

    // Now try and write rows with more columns
    for (timestamp i = 0; i < timestamp(NumColumnsSecondWrite); ++i) {
        columns_second.add_field(scalar_field(DataType::UINT64,  fmt::format("col_{}", i)));
    }
    auto new_descriptor = index.create_stream_descriptor(symbol, columns_second.clone());

    // Now write again.
    for (timestamp i = 0; i < NumRows; ++i) {
        agg.start_row(timestamp{i + NumRows})([&](auto &rb) {
            for (uint64_t j = 1u; j < NumColumnsSecondWrite; ++j)
                rb.set_scalar_by_name(columns_second[j-1].name(), uint64_t(i + j), columns_second[j-1].type().data_type());
        });
    }
    GTEST_COUT << " 2 done";


    // now write 5 columns
    for (auto i = 0u; i < NumRows; ++i) {
        agg.start_row(timestamp{i + NumRows * 2})([&](auto &rb) {
            for (uint64_t j = 1u; j < NumColumnsFirstWrite; ++j)
                rb.set_scalar_by_name(columns_first[j].name(), uint64_t(i + j), columns_first[j].type().data_type());
        });
    }
    GTEST_COUT << " 3 done";

    // now write 10
    for (auto i = 0u; i < NumRows; ++i) {
        agg.start_row(timestamp{i + NumRows * 3})([&](auto &rb) {
            for (uint64_t j = 1u; j < NumColumnsSecondWrite; ++j)
                rb.set_scalar_by_name(columns_second[j].name(), uint64_t(i + j), columns_second[j].type().data_type());
        });
    }

    agg.finalize();
    for(auto &seg : sink.segments_) {
        ARCTICDB_DEBUG(log::version(), "Writing to symbol: {}", symbol);
        arcticdb::append_incomplete_segment(test_store_->_test_get_store(), symbol, std::move(seg));
    }

    using namespace arcticdb::pipelines;

    ReadOptions read_options;
    read_options.set_dynamic_schema(true);
    read_options.set_allow_sparse(true);
    read_options.set_incompletes(true);
    auto read_query = std::make_shared<ReadQuery>();
    read_query->row_filter = universal_range();
    register_native_handler_data_factory();
    auto handler_data = TypeHandlerRegistry::instance()->get_handler_data(OutputFormat::NATIVE);
    auto read_result = test_store_->read_dataframe_version_internal(symbol, VersionQuery{}, read_query, read_options, handler_data);
}

TEST_F(IngestionStressStore, DynamicSchemaWithStrings) {
    const uint64_t NumRows = 10;
    const uint64_t SegmentPolicyRows = 100;
    StreamId symbol{"blah_string"};

    std::vector<SegmentToInputFrameAdapter> data;

    const auto index = as::TimeseriesIndex::default_index();
    as::DynamicSchema schema{
        index.create_stream_descriptor(symbol, {
                    scalar_field(DataType::INT64, "INT64"),
                    scalar_field(DataType::ASCII_FIXED64, "ASCII"),
            }), index
    };

    SegmentsSink sink;
    as::DynamicTimestampAggregator agg(std::move(schema), [&](SegmentInMemory &&mem) {
        sink.segments_.push_back(std::move(mem));
    }, as::RowCountSegmentPolicy{SegmentPolicyRows});

    std::string timer_name("ingestion_stress");
    interval_timer timer(timer_name);

    for (auto i = 0u; i < NumRows; ++i) {
        agg.start_row(timestamp{i})([&](auto &rb) {
            rb.set_scalar_by_name("INT64", uint64_t(i), DataType::INT64);
            auto val = fmt::format("hi_{}", i);
            rb.set_scalar_by_name("ASCII", std::string_view{val}, DataType::ASCII_FIXED64);
        });
    }
    timer.stop_timer(timer_name);
    GTEST_COUT << " 1 done";

    agg.finalize();

    for(auto &seg : sink.segments_) {
        ARCTICDB_DEBUG(log::version(), "Writing to symbol: {}", symbol);
        arcticdb::append_incomplete_segment(test_store_->_test_get_store(), symbol, std::move(seg));
    }

    using namespace arcticdb::pipelines;

    ReadOptions read_options;
    read_options.set_dynamic_schema(true);
    read_options.set_allow_sparse(true);
    read_options.set_incompletes(true);
    auto read_query = std::make_shared<ReadQuery>();
    read_query->row_filter = universal_range();
    register_native_handler_data_factory();
    auto handler_data = TypeHandlerRegistry::instance()->get_handler_data(OutputFormat::NATIVE);
    auto read_result = test_store_->read_dataframe_version(symbol, VersionQuery{}, read_query, read_options, handler_data);
    ARCTICDB_DEBUG(log::version(), "result columns: {}", std::get<PandasOutputFrame>(read_result.frame_data).names());
}
