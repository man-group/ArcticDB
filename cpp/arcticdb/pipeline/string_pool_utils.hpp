/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/offset_string.hpp>

#include <string_view>

namespace arcticdb {

namespace pipelines {
struct SliceAndKey;
struct PipelineContextRow;
} // namespace pipelines

inline auto get_offset_string_at(size_t offset, const ChunkedBuffer& src) {
    return *(src.ptr_cast<typename entity::position_t>(offset * sizeof(entity::position_t), sizeof(entity::position_t))
    );
}

inline auto get_offset_ptr_at(size_t offset, const ChunkedBuffer& src) {
    return src.ptr_cast<typename entity::position_t>(offset * sizeof(entity::position_t), sizeof(entity::position_t));
}

inline void set_offset_string_at(size_t offset, ChunkedBuffer& target, entity::position_t str) {
    *(target.ptr_cast<typename entity::position_t>(offset * sizeof(entity::position_t), sizeof(entity::position_t))) =
            str;
}

inline auto get_string_from_pool(entity::position_t offset_val, const StringPool& string_pool) {
    return string_pool.get_const_view(offset_val);
}

/// @brief Get the i-th string from a buffer
/// @param string_pos The index of the string to be returned
/// @return If the string at @p string_pos is actual string inside @p string_pool return a
///  string view, otherwise return an (integer) placeholder representing not a string.
inline std::variant<std::string_view, entity::position_t> get_string_from_buffer(
        size_t string_pos, const ChunkedBuffer& src, const StringPool& string_pool
) {
    auto offset_val = get_offset_string_at(string_pos, src);
    if (offset_val == nan_placeholder() || offset_val == not_a_string())
        return offset_val;
    else
        return get_string_from_pool(offset_val, string_pool);
}

size_t first_context_row(const pipelines::SliceAndKey& slice_and_key, size_t first_row_in_frame);

position_t get_offset_string(
        const pipelines::PipelineContextRow& context_row, ChunkedBuffer& src, std::size_t first_row_in_frame
);

inline size_t get_first_string_size(
        size_t num_rows, ChunkedBuffer& src, std::size_t first_row_in_frame, const StringPool& string_pool
) {
    entity::position_t offset_val{0};

    for (auto row = 0u; row < num_rows; ++row) {
        offset_val = get_offset_string_at(first_row_in_frame + row, src);
        if (offset_val != nan_placeholder() && offset_val != not_a_string())
            return get_string_from_pool(offset_val, string_pool).size();
    }

    return 0u;
}

} // namespace arcticdb
