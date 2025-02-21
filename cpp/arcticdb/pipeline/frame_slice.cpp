/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/pipeline/read_frame.hpp>
#include <arcticdb/storage/store.hpp>

namespace arcticdb::pipelines {
FrameSlice::FrameSlice(const SegmentInMemory& seg) :
    col_range(get_index_field_count(seg), seg.descriptor().field_count()),
    row_range(0, seg.row_count()),
    desc_(std::make_shared<entity::StreamDescriptor>(seg.descriptor())) {}

void SliceAndKey::ensure_segment(const std::shared_ptr<Store>& store) const {
    if (!segment_)
        segment_ = store->read_sync(*key_).second;
}

SegmentInMemory& SliceAndKey::segment(const std::shared_ptr<Store>& store) {
    ensure_segment(store);
    return *segment_;
}

SegmentInMemory&& SliceAndKey::release_segment(const std::shared_ptr<Store>& store) const {
    ensure_segment(store);
    return std::move(*segment_);
}

const SegmentInMemory& SliceAndKey::segment(const std::shared_ptr<Store>& store) const {
    ensure_segment(store);
    return *segment_;
}

} // namespace arcticdb::pipelines