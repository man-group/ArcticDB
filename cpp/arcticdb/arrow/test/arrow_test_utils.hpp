/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <sparrow/record_batch.hpp>

#include <arcticdb/util/allocator.hpp>
#include <arcticdb/column_store/column.hpp>

using namespace arcticdb;

template<typename T>
requires std::integral<T> || std::floating_point<T>
sparrow::array create_array(const std::vector<T>& data) {
    if constexpr (std::is_same_v<T, bool>) {
        auto data_ptr = reinterpret_cast<bool*>(allocate_detachable_memory(data.size()));
        for (size_t idx = 0; idx < data.size(); ++idx) {
            data_ptr[idx] = data[idx];
        }
        auto buffer = sparrow::details::primitive_data_access<bool>::make_data_buffer(std::span{data_ptr, data.size()});
        sparrow::primitive_array<bool> primitive_array{std::move(buffer), data.size()};
        return sparrow::array{std::move(primitive_array)};
    } else {
        sparrow::u8_buffer<T> u8_buffer(data);
        sparrow::primitive_array<T> primitive_array{std::move(u8_buffer), data.size()};
        return sparrow::array{std::move(primitive_array)};
    }
}

inline sparrow::record_batch create_record_batch(const std::vector<std::pair<std::string, sparrow::array>>& columns) {
    sparrow::record_batch record_batch{};
    for (const auto& column : columns) {
        record_batch.add_column(column.first, column.second);
    }
    return record_batch;
}

template<typename RawType>
void allocate_and_fill_chunked_column(
        Column& column, size_t num_rows, size_t chunk_size, std::optional<std::span<RawType>> values = std::nullopt
) {
    // Allocate column in chunks
    for (size_t row = 0; row < num_rows; row += chunk_size) {
        auto data_size = data_type_size(column.type(), OutputFormat::ARROW, DataTypeMode::EXTERNAL);
        auto current_block_size = std::min(chunk_size, num_rows - row);
        auto bytes = current_block_size * data_size;
        column.allocate_data(bytes);
        column.advance_data(bytes);
    }

    // Actually fill the data
    for (size_t row = 0; row < num_rows; ++row) {
        if (values.has_value()) {
            column.reference_at<RawType>(row) = values.value()[row];
        } else {
            column.reference_at<RawType>(row) = static_cast<RawType>(row);
        }
    }
}
