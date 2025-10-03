/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <sparrow/record_batch.hpp>

#include <arcticdb/util/allocator.hpp>

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

sparrow::record_batch create_record_batch(const std::vector<std::pair<std::string, sparrow::array>>& columns) {
    sparrow::record_batch record_batch{};
    for (const auto& column : columns) {
        record_batch.add_column(column.first, column.second);
    }
    return record_batch;
}
