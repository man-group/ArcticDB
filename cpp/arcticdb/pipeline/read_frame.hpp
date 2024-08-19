/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/stream/stream_source.hpp>
#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/util/bitset.hpp>

#include <folly/futures/Future.h>

#include <memory>

namespace arcticdb {
    struct BufferHolder;
}

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

    bool is_first = true;
    size_t count = 0u;
    std::set<RowRange> row_ranges;
    for (auto[opt_seg, slice, key] : slice_and_keys) {
        is_first = row_ranges.insert(slice.row_range).second;
        if(return_bitset) {
            util::check(static_cast<bool>(output), "Expected output bitset to be none-null");
            output.value()[output->size()] = (dynamic_schema && !has_column_groups) || is_first
                || (incompletes_after && count >= *incompletes_after);
        }

        ++count;
    }
    util::check(!return_bitset || (output && slice_and_keys.size() == output->size()),
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
    DecodePathData shared_data,
    std::any& handler_data);

void decode_into_frame_static(
    SegmentInMemory &frame,
    PipelineContextRow &context,
    Segment &&seg,
    const DecodePathData& shared_data,
    std::any& handler_data);

void decode_into_frame_dynamic(
    SegmentInMemory &frame,
    PipelineContextRow &context,
    Segment &&seg,
    const DecodePathData& shared_data,
    std::any& handler_data);

void reduce_and_fix_columns(
    std::shared_ptr<PipelineContext> &context,
    SegmentInMemory &frame,
    const ReadOptions& read_options,
    std::any& handler_data);

StreamDescriptor get_filtered_descriptor(
    const StreamDescriptor& desc,
    const std::shared_ptr<FieldCollection>& filter_columns);

size_t get_index_field_count(const SegmentInMemory& frame);




} // namespace  arcticdb::pipelines
