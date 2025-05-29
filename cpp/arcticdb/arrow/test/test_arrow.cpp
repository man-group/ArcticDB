/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/arrow/array_from_block.hpp>

using namespace arcticdb;

void allocate_and_fill_chunked_column(Column& column, size_t num_rows, size_t chunk_size) {
    // Allocate column in chunks
    for (size_t row = 0; row < num_rows; row+=chunk_size) {
        auto data_size = data_type_size(column.type(), OutputFormat::ARROW, DataTypeMode::EXTERNAL);
        auto current_block_size = std::min(chunk_size, num_rows - row); // TODO: Likely needs some extra handling for strings?
        auto bytes = current_block_size * data_size;
        column.allocate_data(bytes);
        column.advance_data(bytes);
    }

    // Actually fill the data
    column.type().visit_tag([&num_rows, &column](auto &&impl) {
        using TagType = std::decay_t<decltype(impl)>;
        using RawType = typename TagType::DataTypeTag::raw_type;
        for (size_t row = 0; row < num_rows; ++row) {
            column.reference_at<RawType>(row) = static_cast<RawType>(row);
        }
    });
}

SegmentInMemory get_detachable_segment(StreamId symbol, std::span<const FieldRef> fields, size_t num_rows, size_t chunk_size) {
    auto num_columns = fields.size();
    SegmentInMemory segment(get_test_descriptor<stream::TimeseriesIndex>(symbol, fields), 0, AllocationType::DETACHABLE);

    for (auto i=0u; i < num_columns+1; ++i) {
        allocate_and_fill_chunked_column(segment.column(i), num_rows, chunk_size);
    }

    return segment;
}

TEST(Arrow, ConvertColumnBasic) {
    const size_t num_rows = 100;
    const size_t chunk_size = 5;
    const size_t num_chunks = num_rows / chunk_size;
    auto column = Column(TypeDescriptor{DataType::FLOAT32, Dimension::Dim0}, 0, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED);
    allocate_and_fill_chunked_column(column, num_rows, chunk_size);
    auto arrow_arrays = arrow_arrays_from_column(column, "col");
    EXPECT_EQ(arrow_arrays.size(), num_chunks);
    for (const auto& arr : arrow_arrays) {
        EXPECT_EQ(arr.name(), "col");
    }
    for (auto row=0u; row < num_rows; ++row) {
        auto chunk = row / chunk_size;
        auto pos = row % chunk_size;
        EXPECT_EQ(std::get<sparrow::nullable<const float&>>(arrow_arrays[chunk][pos]).get(), static_cast<float>(row));
    }
}
// TODO: Add string column test and bool column test

TEST(Arrow, ConvertSegmentBasic) {
    const auto symbol = "symbol";
    const auto num_rows = 100u;
    const auto chunk_size = 10u;
    const auto num_chunks = num_rows / chunk_size;
    const auto fields = std::array{
        scalar_field(DataType::UINT8, "smallints"),
        scalar_field(DataType::INT64, "bigints"),
        scalar_field(DataType::FLOAT64, "floats"),
    };
    auto segment = get_detachable_segment(symbol, fields, num_rows, chunk_size);
    // Verify the index column has the expected number of chunks
    EXPECT_EQ(segment.column(0).num_blocks(), num_chunks);

    auto arrow_data = segment_to_arrow_data(segment);
    // We expect to see num_chunks record batches
    EXPECT_EQ(arrow_data->size(), num_chunks);
    for (const auto& record_batch : *arrow_data) {
        auto names = record_batch.names();
        auto columns = record_batch.columns();
        // Each record batch should have all columns for the row range (including the index)
        EXPECT_EQ(names.size(), fields.size() + 1);
        EXPECT_EQ(columns.size(), fields.size() + 1);
        EXPECT_EQ(names[0], "time");
        EXPECT_EQ(columns[0].data_type(), sparrow::data_type::INT64);
        EXPECT_EQ(names[1], "smallints");
        EXPECT_EQ(columns[1].data_type(), sparrow::data_type::UINT8);
        EXPECT_EQ(names[2], "bigints");
        EXPECT_EQ(columns[2].data_type(), sparrow::data_type::INT64);
        EXPECT_EQ(names[3], "floats");
        EXPECT_EQ(columns[3].data_type(), sparrow::data_type::DOUBLE);
        for (const auto& col : columns) {
            EXPECT_EQ(col.size(), chunk_size);
        }
    }
}

// TODO: Add string handling tests