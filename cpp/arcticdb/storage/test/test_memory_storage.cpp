/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>
#include <arcticdb/storage/memory/memory_storage.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>


TEST(InMemory, ReadTwice) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    std::string symbol{"read_twice"};
    size_t num_rows{100};
    size_t start_val{0};

    auto version_store = get_test_engine();
    std::vector<FieldDescriptor::Proto> fields{
        scalar_field_proto(DataType::UINT8, "thing1"),
    };

    auto test_frame =  get_test_frame<stream::TimeseriesIndex>(symbol, fields, num_rows, start_val);
    version_store.write_versioned_dataframe_internal(symbol, std::move(test_frame.frame_), false, false);

    ReadQuery read_query;
    auto read_result1 = version_store.read_dataframe_version_internal(symbol, VersionQuery{}, read_query, ReadOptions{});
    auto read_result2 = version_store.read_dataframe_version_internal(symbol, VersionQuery{}, read_query, ReadOptions{});
}