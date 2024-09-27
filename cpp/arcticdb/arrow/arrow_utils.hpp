/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <string_view>
#include <vector>

#include <arcticdb/arrow/arrow_output_frame.hpp>
#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/util/preprocess.hpp>

namespace arcticdb {

class SegmentInMemory;
struct ArrowData;
class Column;
struct FrameAndDescriptor;

struct ArrowReadResult {

    ArrowReadResult(
        const VersionedItem& versioned_item,
        ArrowOutputFrame&& frame_data,
        const arcticdb::proto::descriptors::UserDefinedMetadata& user_meta) :
        versioned_item_(versioned_item),
        frame_(std::move(frame_data)),
        user_meta_(user_meta) {
    }

    VersionedItem versioned_item_;
    ArrowOutputFrame frame_;
    arcticdb::proto::descriptors::UserDefinedMetadata user_meta_;
};

ArrowReadResult create_arrow_read_result(
    const VersionedItem& version,
    FrameAndDescriptor&& fd);

std::vector<ArrowData> arrow_data_from_column(const Column& column, std::string_view name);

std::vector<std::vector<ArrowData>> segment_to_arrow_data(SegmentInMemory& segment);

std::vector<std::string> names_from_segment(const SegmentInMemory& segment);

} // namespace arcticdb