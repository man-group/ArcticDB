/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/read_frame.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/pipeline/read_pipeline.hpp>
#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/type_handler.hpp>
#include <arcticdb/python/python_handler_data.hpp>

namespace arcticdb::pipelines {

inline void apply_type_handlers(SegmentInMemory seg, std::any& handler_data, OutputFormat output_format) {
    DecodePathData shared_data;
    if (seg.empty())
        return;

    for (auto i = 0U; i < seg.num_columns(); ++i) {
        auto& column = seg.column(i);
        if (auto handler = get_type_handler(output_format, column.type()); handler) {
            // TODO: To support arrow output format we'll need to change the allocation logic for the dest_column.
            // We'll need to consider what arrow layout we want to output the data in.
            util::check(
                    output_format == OutputFormat::PANDAS,
                    "Only Pandas output format is supported for read_result_from_single_frame"
            );
            ColumnMapping mapping{column.type(), column.type(), seg.field(i), 0, seg.row_count(), 0, 0, 0, i};
            Column dest_column(column.type(), seg.row_count(), AllocationType::DETACHABLE, Sparsity::PERMITTED);
            handler->convert_type(column, dest_column, mapping, shared_data, handler_data, seg.string_pool_ptr());
            std::swap(column, dest_column);
            // dest_column now holds the original column. This was allocated with detachable blocks, which should be
            // manually freed before the column goes out of scope to avoid triggering debug logs in ~MemBlock
            // complaining about freeing external data
            util::check(dest_column.blocks().size() == 1, "Unexpected multi-block column when reading single frame");
            dest_column.blocks().at(0)->abandon();
        }
    }
}

inline ReadResult read_result_from_single_frame(
        FrameAndDescriptor& frame_and_desc, const AtomKey& key, std::any& handler_data, OutputFormat output_format
) {
    auto pipeline_context = std::make_shared<PipelineContext>(frame_and_desc.frame_.descriptor());
    SliceAndKey sk{FrameSlice{frame_and_desc.frame_}, key};
    pipeline_context->slice_and_keys_.emplace_back(std::move(sk));
    util::BitSet bitset(1);
    bitset.flip();
    pipeline_context->fetch_index_ = std::move(bitset);
    pipeline_context->ensure_vectors();

    generate_filtered_field_descriptors(pipeline_context, {});
    pipeline_context->begin()->set_string_pool(frame_and_desc.frame_.string_pool_ptr());
    auto descriptor = std::make_shared<StreamDescriptor>(frame_and_desc.frame_.descriptor());
    pipeline_context->begin()->set_descriptor(std::move(descriptor));
    reduce_and_fix_columns(pipeline_context, frame_and_desc.frame_, ReadOptions{}, handler_data).get();
    apply_type_handlers(frame_and_desc.frame_, handler_data, output_format);
    return create_python_read_result(VersionedItem{key}, output_format, std::move(frame_and_desc));
}

} // namespace arcticdb::pipelines
