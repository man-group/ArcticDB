/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <arcticdb/arrow/arrow_output_frame.hpp>
#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/util/preprocess.hpp>
#include <arcticdb/arrow/include_sparrow.hpp>

#include <string_view>
#include <vector>

namespace arcticdb {

class SegmentInMemory;
struct ArrowData;
struct FrameAndDescriptor;
struct DecodePathData;
class Column;

struct ArrowReadResult {

    ArrowReadResult() = default;

    ArrowReadResult(
        VersionedItem versioned_item,
        ArrowOutputFrame&& frame_data,
        arcticdb::proto::descriptors::UserDefinedMetadata user_meta) :
        versioned_item_(std::move(versioned_item)),
        frame_(std::move(frame_data)),
        user_meta_(std::move(user_meta)) {
    }

    VersionedItem versioned_item_;
    ArrowOutputFrame frame_;
    arcticdb::proto::descriptors::UserDefinedMetadata user_meta_;
};

ArrowReadResult create_arrow_read_result(
    const VersionedItem& version,
    FrameAndDescriptor&& fd);


std::shared_ptr<std::vector<sparrow::record_batch>> segment_to_arrow_data(SegmentInMemory& segment);

std::vector<std::string> names_from_segment(const SegmentInMemory& segment);

} // namespace arcticdb