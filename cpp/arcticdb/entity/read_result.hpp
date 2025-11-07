/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/util/preprocess.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/frame_and_descriptor.hpp>
#include <arcticdb/pipeline/pandas_output_frame.hpp>
#include <arcticdb/util/memory_tracing.hpp>
#include <arcticdb/arrow/arrow_output_frame.hpp>
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/version/version_core.hpp>

#include <vector>

namespace arcticdb {

using OutputFrame = std::variant<pipelines::PandasOutputFrame, ArrowOutputFrame>;

struct ARCTICDB_VISIBILITY_HIDDEN NodeReadResult {
    NodeReadResult(
            const std::string& symbol, OutputFrame&& frame_data,
            const arcticdb::proto::descriptors::NormalizationMetadata& norm_meta
    ) :
        symbol_(symbol),
        frame_data_(std::move(frame_data)),
        norm_meta_(norm_meta) {};
    std::string symbol_;
    OutputFrame frame_data_;
    arcticdb::proto::descriptors::NormalizationMetadata norm_meta_;

    ARCTICDB_MOVE_ONLY_DEFAULT(NodeReadResult)
};

struct ARCTICDB_VISIBILITY_HIDDEN ReadResult {
    ReadResult(
            const std::variant<VersionedItem, std::vector<VersionedItem>>& versioned_item, OutputFrame&& frame_data,
            OutputFormat output_format, const arcticdb::proto::descriptors::NormalizationMetadata& norm_meta,
            const std::variant<
                    arcticdb::proto::descriptors::UserDefinedMetadata,
                    std::vector<arcticdb::proto::descriptors::UserDefinedMetadata>>& user_meta,
            const arcticdb::proto::descriptors::UserDefinedMetadata& multi_key_meta,
            std::vector<NodeReadResult>&& node_results = {}
    ) :
        item(versioned_item),
        frame_data(std::move(frame_data)),
        output_format(output_format),
        norm_meta(norm_meta),
        user_meta(user_meta),
        multi_key_meta(multi_key_meta),
        node_results(std::move(node_results)) {}
    std::variant<VersionedItem, std::vector<VersionedItem>> item;
    OutputFrame frame_data;
    OutputFormat output_format;
    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    std::variant<
            arcticdb::proto::descriptors::UserDefinedMetadata,
            std::vector<arcticdb::proto::descriptors::UserDefinedMetadata>>
            user_meta;
    arcticdb::proto::descriptors::UserDefinedMetadata multi_key_meta;
    std::vector<NodeReadResult> node_results;

    ARCTICDB_MOVE_ONLY_DEFAULT(ReadResult)
};

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
        node_results.emplace_back(node_output.versioned_item_.symbol(), std::move(node_python_frame), node_metadata);
    }
    return {version,
            std::move(python_frame),
            output_format,
            desc_proto.normalization(),
            metadata,
            desc_proto.multi_key_meta(),
            std::move(node_results)};
}

} // namespace arcticdb