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

#include <vector>

namespace arcticdb {

using OutputFrame = std::variant<pipelines::PandasOutputFrame, ArrowOutputFrame>;

struct ARCTICDB_VISIBILITY_HIDDEN NodeReadResult {
    NodeReadResult(
            const StreamId& symbol, OutputFrame&& frame_data,
            arcticdb::proto::descriptors::NormalizationMetadata&& norm_meta
    ) :
        symbol_(symbol),
        frame_data_(std::move(frame_data)),
        norm_meta_(std::move(norm_meta)) {};
    StreamId symbol_;
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

namespace version_store {

struct SymbolProcessingResult {
    VersionedItem versioned_item_;
    proto::descriptors::UserDefinedMetadata metadata_;
    OutputSchema output_schema_;
    std::vector<EntityId> entity_ids_;
};

struct ReadVersionOutput {
    ReadVersionOutput() = delete;
    ReadVersionOutput(VersionedItem&& versioned_item, FrameAndDescriptor&& frame_and_descriptor) :
        versioned_item_(std::move(versioned_item)),
        frame_and_descriptor_(std::move(frame_and_descriptor)) {}

    ARCTICDB_MOVE_ONLY_DEFAULT(ReadVersionOutput)

    VersionedItem versioned_item_;
    FrameAndDescriptor frame_and_descriptor_;
};

struct ReadVersionWithNodesOutput {
    ReadVersionOutput root_;
    std::vector<ReadVersionOutput> nodes_;
};

struct MultiSymbolReadOutput {
    MultiSymbolReadOutput() = delete;
    MultiSymbolReadOutput(
            std::vector<VersionedItem>&& versioned_items,
            std::vector<proto::descriptors::UserDefinedMetadata>&& metadatas, FrameAndDescriptor&& frame_and_descriptor
    ) :
        versioned_items_(std::move(versioned_items)),
        metadatas_(std::move(metadatas)),
        frame_and_descriptor_(std::move(frame_and_descriptor)) {}

    ARCTICDB_MOVE_ONLY_DEFAULT(MultiSymbolReadOutput)

    std::vector<VersionedItem> versioned_items_;
    std::vector<proto::descriptors::UserDefinedMetadata> metadatas_;
    FrameAndDescriptor frame_and_descriptor_;
};
} // namespace version_store

} // namespace arcticdb