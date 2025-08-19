/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <sparrow/record_batch.hpp>

#include <arcticdb/arrow/array_from_block.hpp>
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/allocator.hpp>

using namespace arcticdb;

TEST(ArrowWrite, SingleRecordBatch) {
    size_t num_rows = 10;
    std::vector<uint64_t> data(num_rows);
    std::iota(data.begin(), data.end(), 0UL);
    sparrow::u8_buffer<uint64_t> u8_buffer(data);
    sparrow::primitive_array<uint64_t> primitive_array{std::move(u8_buffer), num_rows};
    sparrow::array array{std::move(primitive_array)};
    sparrow::record_batch record_batch{};
    record_batch.add_column("col", array);
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
    ASSERT_EQ(col.type(), make_scalar_type(DataType::UINT64));
    ASSERT_EQ(col.row_count(), num_rows);
    ASSERT_EQ(col.last_row(), num_rows - 1);
    ASSERT_FALSE(col.is_sparse());
    for (size_t idx = 0; idx < num_rows; ++idx) {
        ASSERT_EQ(*col.scalar_at<uint64_t>(idx), idx);
    }
}

//TEST(ArrowWrite, ExampleForQs) {
//    size_t num_rows = 10;
//    uint8_t* data_ptr = std::allocator<uint8_t>().allocate(sizeof(uint64_t) * num_rows);
//    auto cast_ptr = reinterpret_cast<uint64_t*>(data_ptr);
//    for (size_t idx = 0; idx < num_rows; ++idx) {
//        cast_ptr[idx] = idx;
//    }
//    sparrow::u8_buffer<uint64_t> u8_buffer(reinterpret_cast<uint64_t*>(data_ptr), num_rows);
//    for (size_t idx = 0; idx < num_rows; ++idx) {
//        ASSERT_EQ(cast_ptr[idx], idx);
//    }
//    sparrow::primitive_array<uint64_t> primitive_array{std::move(u8_buffer), num_rows};
//    for (size_t idx = 0; idx < num_rows; ++idx) {
//        ASSERT_EQ(cast_ptr[idx], idx);
//    }

//    sparrow::array array{std::move(primitive_array)};
//    auto arrow_structures = sparrow::get_arrow_structures(array);
//    auto arrow_array_buffers = sparrow::get_arrow_array_buffers(*arrow_structures.first, *arrow_structures.second);
//    const auto* ptr = reinterpret_cast<uint64_t*>(arrow_array_buffers.at(1).data<uint8_t>());
//    for (size_t idx = 0; idx < num_rows; ++idx) {
//        ASSERT_EQ(ptr[idx], idx);
//    }

//    sparrow::record_batch record_batch{};
//    record_batch.add_column("col", array);
//
//    auto seg = arrow_data_to_segment({record_batch});
//}