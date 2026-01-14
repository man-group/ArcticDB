/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/storage/memory/memory_storage.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/util/native_handler.hpp>

TEST(InMemory, ReadTwice) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    std::string symbol{"read_twice"};
    size_t num_rows{100};
    size_t start_val{0};

    auto version_store = get_test_engine();
    std::vector<FieldRef> fields{
            scalar_field(DataType::UINT8, "thing1"),
    };

    auto test_frame = get_test_frame<stream::TimeseriesIndex>(symbol, fields, num_rows, start_val);
    version_store.write_versioned_dataframe_internal(symbol, std::move(test_frame.frame_), false, false, false);

    auto read_query = std::make_shared<ReadQuery>();
    register_native_handler_data_factory();
    auto handler_data = TypeHandlerRegistry::instance()->get_handler_data(OutputFormat::NATIVE);
    auto read_result1 = version_store.read_dataframe_version_internal(
            symbol, VersionQuery{}, read_query, ReadOptions{}, handler_data
    );
    auto read_result2 = version_store.read_dataframe_version_internal(
            symbol, VersionQuery{}, read_query, ReadOptions{}, handler_data
    );
}