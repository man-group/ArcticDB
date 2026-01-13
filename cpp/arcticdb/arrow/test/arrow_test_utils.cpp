/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/arrow/test/arrow_test_utils.hpp>

void allocate_chunked_column(Column& column, size_t num_rows, size_t chunk_size) {
    for (size_t row = 0; row < num_rows; row += chunk_size) {
        auto data_size = data_type_size(column.type());
        auto current_block_size = std::min(chunk_size, num_rows - row);
        auto bytes = current_block_size * data_size;
        column.allocate_data(bytes);
        column.advance_data(bytes);
    }
}
