/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/stream/stream_source.hpp>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/util/bitset.hpp>
#include <arcticdb/util/shared_future.hpp>
#include <arcticdb/util/buffer_holder.hpp>

#include <folly/futures/Future.h>

#include <memory>

namespace arcticdb::pipelines {

SegmentInMemory allocate_frame(const std::shared_ptr<PipelineContext>& context);

template <typename KeySliceContainer>
std::optional<util::BitSet> check_and_mark_slices(
    const KeySliceContainer& slice_and_keys,
    bool dynamic_schema,
    bool return_bitset,
    std::optional<size_t> incompletes_after,
    bool has_column_groups) {
    ARCTICDB_SAMPLE_DEFAULT(MarkIndexSlices)
    std::optional<util::BitSet> output = return_bitset ? std::make_optional<util::BitSet>(0u) : std::nullopt;
    if (slice_and_keys.empty())
        return output;

    auto &index_slice = slice_and_keys.begin()->slice_;
    ColRange col_range;
    RowRange row_range;
    bool is_first = true;
    size_t count = 0u;
    std::set<RowRange> row_ranges;
    for (auto[opt_seg, slice, key] : slice_and_keys) {
        if(has_column_groups) {
          is_first = row_ranges.insert(slice.row_range).second;
        } else if (slice.col_range != col_range) {
            if(!dynamic_schema)
                is_first = slice.col_range == index_slice.col_range;
            
            col_range = slice.col_range;
        }
        row_ranges.insert(slice.row_range);
        if(return_bitset)
            output.value()[output.value().size()] = (dynamic_schema && !has_column_groups) || is_first || (incompletes_after && count >= incompletes_after.value());

        ++count;
    }
    util::check(!return_bitset || slice_and_keys.size() == output.value().size(),
                "Index fetch vector size should match slice and key size");

    if(!row_ranges.empty()) {
        auto pos = row_ranges.begin();
        RowRange current = *pos;
        std::advance(pos, 1);
        for(; pos != row_ranges.end(); ++pos){
            sorting::check<ErrorCode::E_UNSORTED_DATA>(pos->start() == current.end(), "Non-contiguous rows, range search on unsorted data? {} {}", current, *pos);
            current = *pos;
        }
    }

    return output;
}

void mark_index_slices(
    const std::shared_ptr<PipelineContext>& context,
    bool dynamic_schema,
    bool column_groups);

folly::Future<std::vector<VariantKey>> fetch_data(
    const SegmentInMemory& frame,
    const std::shared_ptr<PipelineContext> &context,
    const std::shared_ptr<stream::StreamSource>& ssource,
    bool dynamic_schema,
    std::shared_ptr<BufferHolder> buffers
    );

void decode_into_frame_static(
    SegmentInMemory &frame,
    PipelineContextRow &context,
    Segment &&seg,
    const std::shared_ptr<BufferHolder>& buffers
    );

void decode_into_frame_dynamic(
        SegmentInMemory &frame,
        PipelineContextRow &context,
        Segment &&seg,
        const std::shared_ptr<BufferHolder>& buffers
);

void reduce_and_fix_columns(
        std::shared_ptr<PipelineContext> &context,
        SegmentInMemory &frame,
        const ReadOptions& read_options
);

size_t get_index_field_count(const SegmentInMemory& frame);

StreamDescriptor get_filtered_descriptor(
        const StreamDescriptor& desc,
        const std::shared_ptr<FieldCollection>& filter_columns);
} // namespace  arcticdb::pipelines