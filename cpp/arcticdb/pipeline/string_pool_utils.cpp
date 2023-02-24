/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/pipeline/string_pool_utils.hpp>
#include <arcticdb/pipeline/pipeline_context.hpp>

namespace arcticdb {
size_t first_context_row(const pipelines::SliceAndKey& slice_and_key, size_t first_row_in_frame) {
    return slice_and_key.slice_.row_range.first - first_row_in_frame;
}

position_t get_offset_string(const pipelines::PipelineContextRow& context_row, ChunkedBuffer &src, std::size_t first_row_in_frame) {
    auto offset = first_context_row(context_row.slice_and_key(), first_row_in_frame);
    auto offset_val = get_offset_string_at(offset, src);
    util::check(offset_val != nan_placeholder() && offset_val != not_a_string(), "NaN or None placeholder in get_offset_string");
    return offset_val;
}
}