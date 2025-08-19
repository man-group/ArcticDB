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
    uint8_t* raw_buffer = allocate_detachable_memory(sizeof(uint64_t) * num_rows);
    sparrow::array array{create_primitive_array(reinterpret_cast<uint64_t*>(raw_buffer), num_rows, std::nullopt)};

    sparrow::record_batch record_batch{};
    record_batch.add_column("col", array);

    auto seg = arrow_data_to_segment({record_batch});
}