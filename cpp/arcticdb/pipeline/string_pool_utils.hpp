/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/offset_string.hpp>

#include <string>

namespace arcticdb {

namespace pipelines {
    struct SliceAndKey;
    struct PipelineContextRow;
}

inline auto get_offset_string_at(size_t offset, const ChunkedBuffer& src) {
    return *(src.ptr_cast<typename StringPool::offset_t>(offset * sizeof(StringPool::offset_t), sizeof(StringPool::offset_t)));
}

inline auto get_offset_ptr_at(size_t offset, const ChunkedBuffer& src) {
    return src.ptr_cast<typename StringPool::offset_t>(offset * sizeof(StringPool::offset_t), sizeof(StringPool::offset_t));
}

inline void set_offset_string_at(size_t offset, ChunkedBuffer& target,  StringPool::offset_t str) {
    *(target.ptr_cast<typename StringPool::offset_t>(offset * sizeof(StringPool::offset_t), sizeof(StringPool::offset_t))) = str;
}

inline auto get_string_from_pool(StringPool::offset_t offset_val, const StringPool& string_pool) {
    return string_pool.get_const_view(offset_val);
}

inline std::variant<std::string_view, StringPool::offset_t> get_string_from_buffer(size_t offset, const ChunkedBuffer& src, const StringPool& string_pool) {
    auto offset_val = get_offset_string_at(offset, src);
    if (offset_val == nan_placeholder() || offset_val == not_a_string())
        return offset_val;
    else
        return get_string_from_pool(offset_val, string_pool);
}

size_t first_context_row(const pipelines::SliceAndKey& slice_and_key, size_t first_row_in_frame);

position_t get_offset_string(const pipelines::PipelineContextRow& context_row, ChunkedBuffer &src, std::size_t first_row_in_frame);

inline size_t get_first_string_size(size_t num_rows, ChunkedBuffer &src, std::size_t first_row_in_frame, const StringPool& string_pool) {
    StringPool::offset_t offset_val{0};

    for(auto row = 0u; row < num_rows; ++row) {
        offset_val = get_offset_string_at(first_row_in_frame + row, src);
        if (offset_val != nan_placeholder() && offset_val != not_a_string())
            return get_string_from_pool(offset_val, string_pool).size();
    }

    return 0u;
}

} //namespace arcticdb