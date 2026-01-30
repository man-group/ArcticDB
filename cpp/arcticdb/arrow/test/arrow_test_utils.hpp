/* Copyright 2026 Man Group Operations Limited
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
requires std::integral<T> || std::floating_point<T> || std::is_same_v<T, std::string>
sparrow::array create_array(const std::vector<T>& data) {
    if constexpr (std::is_same_v<T, bool>) {
        auto data_ptr = reinterpret_cast<bool*>(allocate_detachable_memory(data.size()));
        for (size_t idx = 0; idx < data.size(); ++idx) {
            data_ptr[idx] = data[idx];
        }
        auto buffer = sparrow::details::primitive_data_access<bool>::make_data_buffer(std::span{data_ptr, data.size()});
        sparrow::primitive_array<bool> primitive_array{std::move(buffer), data.size()};
        return sparrow::array{std::move(primitive_array)};
    } else if constexpr (std::integral<T> || std::floating_point<T>) {
        sparrow::u8_buffer<T> u8_buffer(data);
        sparrow::primitive_array<T> primitive_array{std::move(u8_buffer), data.size()};
        return sparrow::array{std::move(primitive_array)};
    } else { // Strings
        auto offsets_buffer = new int32_t[data.size() + 1];
        size_t idx{0};
        offsets_buffer[idx] = 0;
        auto strings_buffer_size =
                std::accumulate(data.cbegin(), data.cend(), size_t(0), [](const size_t& accum, const std::string& str) {
                    return accum + str.size();
                });
        auto strings_buffer = new char[strings_buffer_size];
        size_t str_idx{0};
        for (const auto& str : data) {
            offsets_buffer[idx + 1] = offsets_buffer[idx] + str.size();
            ++idx;
            for (auto c : str) {
                strings_buffer[str_idx++] = c;
            }
        }
        sparrow::u8_buffer<int32_t> sparrow_offsets_buffer(offsets_buffer, data.size() + 1, get_detachable_allocator());
        sparrow::u8_buffer<char> sparrow_strings_buffer(
                strings_buffer, strings_buffer_size, get_detachable_allocator()
        );
        return sparrow::array{
                sparrow::string_array{std::move(sparrow_strings_buffer), std::move(sparrow_offsets_buffer)}
        };
    }
}

inline sparrow::record_batch create_record_batch(const std::vector<std::pair<std::string, sparrow::array>>& columns) {
    sparrow::record_batch record_batch{};
    for (const auto& column : columns) {
        record_batch.add_column(column.first, column.second);
    }
    return record_batch;
}

void allocate_chunked_column(Column& column, size_t num_rows, size_t chunk_size);

template<typename RawType>
void allocate_and_fill_chunked_column(
        Column& column, size_t num_rows, size_t chunk_size, std::optional<std::span<RawType>> values = std::nullopt
) {
    // Allocate column in chunks
    allocate_chunked_column(column, num_rows, chunk_size);

    // Actually fill the data
    for (size_t row = 0; row < num_rows; ++row) {
        if (values.has_value()) {
            column.reference_at<RawType>(row) = values.value()[row];
        } else {
            column.reference_at<RawType>(row) = static_cast<RawType>(row);
        }
    }
}
