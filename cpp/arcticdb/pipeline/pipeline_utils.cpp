/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/pipeline_utils.hpp>
#include <arcticdb/arrow/arrow_schema_utils.hpp>

#include <sparrow/record_batch.hpp>

namespace arcticdb::pipelines {

ReadResult create_python_read_result(
        const std::variant<VersionedItem, std::vector<VersionedItem>>& version, OutputFormat output_format,
        FrameAndDescriptor&& fd,
        std::optional<std::vector<arcticdb::proto::descriptors::UserDefinedMetadata>>&& user_meta,
        std::vector<version_store::ReadVersionOutput>&& node_outputs
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
            if (result.desc_.index().type() == IndexDescriptor::Type::ROWCOUNT && index->start() == 0 &&
                index->step() == 0) {
                index->set_step(1);
            }
        }
    }

    const auto prepare_frame_for_arrow_output = [](FrameAndDescriptor& frame_and_descriptor) {
        prepare_for_arrow_output(
                frame_and_descriptor.frame_, *frame_and_descriptor.desc_.mutable_proto().mutable_normalization()
        );
    };
    // For OutputFormat::ARROW we convert pandas metadata to arrow before `segment_to_arrow_data` is called.
    // This way all timezone operations and column renames for arrow output are handled in C++ layer.
    // Ideally for OutputFormat::PANDAS we would convert arrow metadata to pandas here as well. However that is
    // impossible since this conversion is inherently lossy. Pandas metadata doesn't have a structure to specify
    // timezones for non index columns.
    if (output_format == OutputFormat::ARROW) {
        prepare_frame_for_arrow_output(result);
        for (auto& node_output : node_outputs) {
            prepare_frame_for_arrow_output(node_output.frame_and_descriptor_);
        }
    }

    auto get_python_frame = [](auto& result, OutputFormat requested_format) -> OutputFrame {
        const auto& norm = result.desc_.proto().normalization();
        if (requested_format == OutputFormat::ARROW) {
            return ArrowOutputFrame{segment_to_arrow_data(result.frame_, norm)};
        } else {
            return pipelines::PandasOutputFrame{result.frame_};
        }
    };
    auto python_frame = get_python_frame(result, output_format);
    util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);

    const auto& desc_proto = result.desc_.proto();
    std::variant<
            arcticdb::proto::descriptors::UserDefinedMetadata,
            std::vector<arcticdb::proto::descriptors::UserDefinedMetadata>>
            metadata;
    if (user_meta.has_value()) {
        metadata = std::move(*user_meta);
    } else {
        // Monday 13266183500 should try to move this data
        metadata = desc_proto.user_meta();
    }

    auto sorted = result.desc_.sorted();

    std::vector<NodeReadResult> node_results;
    for (auto& node_output : node_outputs) {
        auto& node_fd = node_output.frame_and_descriptor_;
        auto node_python_frame = get_python_frame(node_fd, output_format);
        auto node_metadata = node_fd.desc_.proto().normalization();
        auto node_sorted = node_fd.desc_.sorted();
        node_results.emplace_back(
                node_output.versioned_item_.symbol(),
                std::move(node_python_frame),
                std::move(node_metadata),
                node_sorted
        );
    }
    return {version,
            std::move(python_frame),
            output_format,
            desc_proto.normalization(),
            metadata,
            desc_proto.multi_key_meta(),
            std::move(node_results),
            sorted};
}

} // namespace arcticdb::pipelines
