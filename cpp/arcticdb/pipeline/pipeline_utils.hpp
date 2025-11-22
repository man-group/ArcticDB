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
            handler->convert_type(column, dest_column, mapping, shared_data, handler_data, seg.string_pool_ptr(), {});
            std::swap(column, dest_column);
            // dest_column now holds the original column. This was allocated with detachable blocks, which should be
            // manually freed before the column goes out of scope to avoid triggering debug logs in ~MemBlock
            // complaining about freeing external data
            util::check(dest_column.blocks().size() == 1, "Unexpected multi-block column when reading single frame");
            dest_column.blocks().at(0)->abandon();
        }
    }
}

inline ReadResult create_python_read_result(
        const std::variant<VersionedItem, std::vector<VersionedItem>>& version, OutputFormat output_format,
        FrameAndDescriptor&& fd,
        std::optional<std::vector<arcticdb::proto::descriptors::UserDefinedMetadata>>&& user_meta = std::nullopt,
        std::vector<version_store::ReadVersionOutput>&& node_outputs = {}
) {
    auto result = std::move(fd);

    // If version is a vector then this was a multi-symbol join, so the user_meta vector should have a value
    // Otherwise, there is a single piece of metadata on the frame descriptor
    util::check(
            std::holds_alternative<VersionedItem>(version) ^ user_meta.has_value(),
            "Unexpected argument combination to create_python_read_result"
    );

    // Very old (pre Nov-2020) PandasIndex protobuf messages had no "start" or "step" fields. If is_physically_stored
    // (renamed from is_not_range_index) was false, the index was always RangeIndex(num_rows, 1)
    // This used to be handled in the Python layer by passing None to the DataFrame index parameter, which would then
    // default to RangeIndex(num_rows, 1). However, the empty index also has is_physically_stored as false, and because
    // integer protobuf fields default to zero if they are not present on the wire, it is impossible to tell from
    // the normalization metadata alone if the data was written with an empty index, or with a very old range index.
    // We therefore patch the normalization metadata here in this case
    auto norm_meta = result.desc_.mutable_proto().mutable_normalization();
    if (norm_meta->has_df() || norm_meta->has_series()) {
        auto common = norm_meta->has_df() ? norm_meta->mutable_df()->mutable_common()
                                          : norm_meta->mutable_series()->mutable_common();
        if (common->has_index()) {
            auto index = common->mutable_index();
            if (result.desc_.index().type() == IndexDescriptor::Type::ROWCOUNT && !index->is_physically_stored() &&
                index->start() == 0 && index->step() == 0) {
                index->set_step(1);
            }
        }
    }

    auto get_python_frame = [output_format](auto& result) -> OutputFrame {
        if (output_format == OutputFormat::ARROW) {
            return ArrowOutputFrame{segment_to_arrow_data(result.frame_)};
        } else {
            return pipelines::PandasOutputFrame{result.frame_};
        }
    };
    auto python_frame = get_python_frame(result);
    util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);

    const auto& desc_proto = result.desc_.proto();
    std::variant<
            arcticdb::proto::descriptors::UserDefinedMetadata,
            std::vector<arcticdb::proto::descriptors::UserDefinedMetadata>>
            metadata;
    if (user_meta.has_value()) {
        metadata = std::move(*user_meta);
    } else {
        metadata = std::move(desc_proto.user_meta());
    }

    std::vector<NodeReadResult> node_results;
    for (auto& node_output : node_outputs) {
        auto& node_fd = node_output.frame_and_descriptor_;
        auto node_python_frame = get_python_frame(node_fd);
        auto node_metadata = node_fd.desc_.proto().normalization();
        node_results.emplace_back(
                node_output.versioned_item_.symbol(), std::move(node_python_frame), std::move(node_metadata)
        );
    }
    return {version,
            std::move(python_frame),
            output_format,
            desc_proto.normalization(),
            metadata,
            desc_proto.multi_key_meta(),
            std::move(node_results)};
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
