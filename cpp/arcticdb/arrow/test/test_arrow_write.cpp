/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <sparrow/record_batch.hpp>

#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/allocator.hpp>

using namespace arcticdb;

template<typename T>
requires std::integral<T> || std::floating_point<T>
sparrow::array create_array(const std::vector<T>& data) {
    sparrow::u8_buffer<T> u8_buffer(data);
    sparrow::primitive_array<T> primitive_array{std::move(u8_buffer), data.size()};
    return sparrow::array{std::move(primitive_array)};
}

sparrow::record_batch create_record_batch(const std::vector<std::pair<std::string, sparrow::array>>& columns) {
    sparrow::record_batch record_batch{};
    for (const auto& column: columns) {
        record_batch.add_column(column.first, column.second);
    }
    return record_batch;
}

template<typename types>
class ArrowDataToSegmentNumeric : public testing::Test {};

using test_types = ::testing::Types<
        uint8_t,
        uint16_t,
        uint32_t,
        uint64_t,
        int8_t,
        int16_t,
        int32_t,
        int64_t,
        float,
        double
        >;

TYPED_TEST_SUITE(ArrowDataToSegmentNumeric, test_types);

TYPED_TEST(ArrowDataToSegmentNumeric, Simple) {
    size_t num_rows = 10;
    std::vector<TypeParam> data(num_rows);
    std::iota(data.begin(), data.end(), 0UL);
    auto array = create_array(data);
    auto record_batch = create_record_batch({{"col", array}});

    std::vector<sparrow::record_batch> record_batches;
    record_batches.emplace_back(std::move(record_batch));
    auto seg = arrow_data_to_segment(record_batches);

    ASSERT_EQ(seg.fields().size(), 1);
    ASSERT_EQ(seg.num_columns(), 1);
    ASSERT_EQ(seg.row_count(), num_rows);
    const auto column_index = seg.column_index("col");
    ASSERT_TRUE(column_index.has_value());
    ASSERT_EQ(*column_index, 0);
    const auto& col = seg.column(0);
    ASSERT_EQ(col.type(), make_scalar_type(data_type_from_raw_type<TypeParam>()));
    ASSERT_EQ(col.row_count(), num_rows);
    ASSERT_EQ(col.last_row(), num_rows - 1);
    ASSERT_FALSE(col.is_sparse());
    for (size_t idx = 0; idx < num_rows; ++idx) {
        ASSERT_EQ(*col.scalar_at<TypeParam>(idx), data.at(idx));
    }
}

TYPED_TEST(ArrowDataToSegmentNumeric, MultiColumn) {
    size_t num_rows = 10;
    size_t num_columns = 10;
    std::vector<std::pair<std::string, sparrow::array>> columns;
    for (size_t idx = 0; idx < num_columns; ++idx) {
        std::vector<TypeParam> data(num_rows);
        std::iota(data.begin(), data.end(), num_rows * idx);
        auto array = create_array(data);
        columns.emplace_back(fmt::format("col{}", idx), array);
    }
    auto record_batch = create_record_batch(columns);

    std::vector<sparrow::record_batch> record_batches;
    record_batches.emplace_back(std::move(record_batch));
    auto seg = arrow_data_to_segment(record_batches);

    ASSERT_EQ(seg.fields().size(), num_columns);
    ASSERT_EQ(seg.num_columns(), num_columns);
    ASSERT_EQ(seg.row_count(), num_rows);
    for (size_t idx = 0; idx < num_columns; ++idx) {
        const auto column_index = seg.column_index(fmt::format("col{}", idx));
        ASSERT_TRUE(column_index.has_value());
        ASSERT_EQ(*column_index, idx);
        const auto& col = seg.column(idx);
        ASSERT_EQ(col.type(), make_scalar_type(data_type_from_raw_type<TypeParam>()));
        ASSERT_EQ(col.row_count(), num_rows);
        ASSERT_EQ(col.last_row(), num_rows - 1);
        ASSERT_FALSE(col.is_sparse());
        for (size_t row = 0; row < num_rows; ++row) {
            ASSERT_EQ(*col.scalar_at<TypeParam>(row), (idx * num_rows) + row);
        }
    }
}

TYPED_TEST(ArrowDataToSegmentNumeric, MultipleRecordBatches) {
    std::vector<sparrow::record_batch> record_batches;
    std::vector<size_t> rows_per_batch{1, 10, 100};
    size_t total_rows{0};
    for (auto num_rows: rows_per_batch) {
        std::vector<TypeParam> data(num_rows);
        std::iota(data.begin(), data.end(), total_rows);
        total_rows += num_rows;
        auto array = create_array(data);
        record_batches.emplace_back(create_record_batch({{"col", array}}));
    }
    auto seg = arrow_data_to_segment(record_batches);

    ASSERT_EQ(seg.fields().size(), 1);
    ASSERT_EQ(seg.num_columns(), 1);
    ASSERT_EQ(seg.row_count(), total_rows);
    const auto column_index = seg.column_index("col");
    ASSERT_TRUE(column_index.has_value());
    ASSERT_EQ(*column_index, 0);
    const auto& col = seg.column(0);
    ASSERT_EQ(col.type(), make_scalar_type(data_type_from_raw_type<TypeParam>()));
    ASSERT_EQ(col.row_count(), total_rows);
    ASSERT_EQ(col.last_row(), total_rows - 1);
    ASSERT_FALSE(col.is_sparse());
    for (size_t idx = 0; idx < total_rows; ++idx) {
        ASSERT_EQ(*col.scalar_at<TypeParam>(idx), idx);
    }
    const auto& buffer = col.data().buffer();
    ASSERT_EQ(buffer.bytes(), total_rows * sizeof(TypeParam));
    ASSERT_EQ(buffer.blocks().size(), rows_per_batch.size());
    ASSERT_EQ(buffer.block_offsets().size(), rows_per_batch.size() + 1);
    size_t bytes{0};
    for (size_t idx = 0; idx < rows_per_batch.size(); ++idx) {
        ASSERT_TRUE(buffer.blocks()[idx]->is_external());
        ASSERT_EQ(buffer.blocks()[idx]->bytes(), rows_per_batch[idx] * sizeof(TypeParam));
        ASSERT_EQ(buffer.blocks()[idx]->offset_, bytes);
        ASSERT_EQ(buffer.block_offsets()[idx], bytes);
        bytes += buffer.blocks()[idx]->bytes();
    }
    ASSERT_EQ(buffer.block_offsets().back(), bytes);
}
