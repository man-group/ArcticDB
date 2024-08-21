/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/pipeline/frame_data_wrapper.hpp>
#include <arcticdb/util/buffer_holder.hpp>

#include <memory>

namespace arcticdb {

class SegmentInMemory;

class ArrowOutputFrame {
    ArrowOutputFrame(const SegmentInMemory& frame, std::shared_ptr<BufferHolder> buffers);

private:
    std::shared_ptr<ModuleData> module_data_;
    SegmentInMemory frame_;
    std::vector<std::string> names_;
    std::vector<std::string> index_columns_;
    std::weak_ptr<pipelines::FrameDataWrapper> arrays_;
    std::shared_ptr<BufferHolder> buffers_;
};
}
