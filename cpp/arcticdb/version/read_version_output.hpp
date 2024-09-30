#pragma once

#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/entity/frame_and_descriptor.hpp>

namespace arcticdb {

struct ReadVersionOutput {
    ReadVersionOutput() = delete;
    ReadVersionOutput(VersionedItem&& versioned_item, FrameAndDescriptor&& frame_and_descriptor):
        versioned_item_(std::move(versioned_item)),
        frame_and_descriptor_(std::move(frame_and_descriptor)) {}

    ARCTICDB_MOVE_ONLY_DEFAULT(ReadVersionOutput)

    VersionedItem versioned_item_;
    FrameAndDescriptor frame_and_descriptor_;
};

}