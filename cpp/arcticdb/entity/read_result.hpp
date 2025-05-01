/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/util/preprocess.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/frame_and_descriptor.hpp>
#include <arcticdb/pipeline/pandas_output_frame.hpp>
#include <arcticdb/util/memory_tracing.hpp>
#include <arcticdb/arrow/arrow_utils.hpp>

#include <vector>

namespace arcticdb {

using OutputFrame = std::variant<pipelines::PandasOutputFrame, ArrowOutputFrame>;

struct ARCTICDB_VISIBILITY_HIDDEN ReadResult {
    ReadResult(
            const VersionedItem& versioned_item,
            OutputFrame&& frame_data,
            OutputFormat output_format,
            const arcticdb::proto::descriptors::NormalizationMetadata& norm_meta,
            const arcticdb::proto::descriptors::UserDefinedMetadata& user_meta,
            const arcticdb::proto::descriptors::UserDefinedMetadata& multi_key_meta,
            std::vector<entity::AtomKey>&& multi_keys) :
            item(versioned_item),
            frame_data(std::move(frame_data)),
            output_format(output_format),
            norm_meta(norm_meta),
            user_meta(user_meta),
            multi_key_meta(multi_key_meta),
            multi_keys(std::move(multi_keys)) {

    }
    VersionedItem item;
    OutputFrame frame_data;
    OutputFormat output_format;
    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    arcticdb::proto::descriptors::UserDefinedMetadata user_meta;
    arcticdb::proto::descriptors::UserDefinedMetadata multi_key_meta;
    std::vector <entity::AtomKey> multi_keys;

    ARCTICDB_MOVE_ONLY_DEFAULT(ReadResult)
};

inline ReadResult create_python_read_result(
    const VersionedItem& version,
    OutputFormat output_format,
    FrameAndDescriptor&& fd) {
    auto result = std::move(fd);

    // Very old (pre Nov-2020) PandasIndex protobuf messages had no "start" or "step" fields. If is_physically_stored
    // (renamed from is_not_range_index) was false, the index was always RangeIndex(num_rows, 1)
    // This used to be handled in the Python layer by passing None to the DataFrame index parameter, which would then
    // default to RangeIndex(num_rows, 1). However, the empty index also has is_physically_stored as false, and because
    // integer protobuf fields default to zero if they are not present on the wire, it is impossible to tell from
    // the normalization metadata alone if the data was written with an empty index, or with a very old range index.
    // We therefore patch the normalization metadata here in this case
    auto norm_meta = result.desc_.mutable_proto().mutable_normalization();
    if (norm_meta->has_df() || norm_meta->has_series()) {
        auto common = norm_meta->has_df() ? norm_meta->mutable_df()->mutable_common() : norm_meta->mutable_series()->mutable_common();
        if (common->has_index()) {
            auto index = common->mutable_index();
            if (result.desc_.index().type() == IndexDescriptor::Type::ROWCOUNT &&
                !index->is_physically_stored()
                && index->start() == 0 &&
                index->step() == 0) {
                index->set_step(1);
            }
        }
    }

    // Is that the principal way to init a variant?
    auto python_frame = [&]() -> OutputFrame {
        if (output_format == OutputFormat::ARROW) {
            return ArrowOutputFrame{segment_to_arrow_data(result.frame_), names_from_segment(result.frame_)};
        } else {
            return pipelines::PandasOutputFrame{result.frame_, output_format};
        }
    }();
    util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);

    const auto& desc_proto = result.desc_.proto();
    return {version, std::move(python_frame), output_format, desc_proto.normalization(),
            desc_proto.user_meta(), desc_proto.multi_key_meta(), std::move(result.keys_)};
}

} //namespace arcticdb