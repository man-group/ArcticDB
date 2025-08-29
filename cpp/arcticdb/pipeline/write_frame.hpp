/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/index_range.hpp>


#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/stream/index.hpp>
#include <folly/futures/Future.h>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/slicing.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/pipeline/pipeline_common.hpp>

namespace arcticdb::pipelines {

using namespace arcticdb::stream;

struct WriteToSegmentTask : public async::BaseTask {

    std::shared_ptr<InputFrame> frame_;
    const FrameSlice slice_;
    const SlicingPolicy slicing_;
    folly::Function<stream::StreamSink::PartialKey(const FrameSlice&)> partial_key_gen_;
    size_t slice_num_for_column_;
    Index index_;
    bool sparsify_floats_;
    util::MagicNum<'W', 's', 'e', 'g'> magic_;

    WriteToSegmentTask(
        std::shared_ptr<InputFrame> frame,
        FrameSlice slice,
        const SlicingPolicy& slicing,
        folly::Function<stream::StreamSink::PartialKey(const FrameSlice&)>&& partial_key_gen,
        size_t slice_num_for_column,
        Index index,
        bool sparsify_floats);

    std::tuple<stream::StreamSink::PartialKey, SegmentInMemory, FrameSlice> operator()();
};

folly::Future<std::vector<SliceAndKey>> slice_and_write(
        const std::shared_ptr<InputFrame> &frame,
        const SlicingPolicy &slicing,
        IndexPartialKey&& partial_key,
        const std::shared_ptr<stream::StreamSink> &sink,
        const std::shared_ptr<DeDupMap>& de_dup_map = std::make_shared<DeDupMap>(),
        bool allow_sparse = false
);

int64_t write_window_size();

folly::Future<std::vector<SliceAndKey>> write_slices(
        const std::shared_ptr<InputFrame> &frame,
        std::vector<FrameSlice>&& slices,
        const SlicingPolicy &slicing,
        TypedStreamVersion&& partial_key,
        const std::shared_ptr<stream::StreamSink>& sink,
        const std::shared_ptr<DeDupMap>& de_dup_map,
        bool sparsify_floats);

folly::Future<entity::AtomKey> write_frame(
    IndexPartialKey &&key,
    const std::shared_ptr<InputFrame>& frame,
    const SlicingPolicy &slicing,
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<DeDupMap>& de_dup_map = std::make_shared<DeDupMap>(),
    bool allow_sparse = false
);

folly::Future<entity::AtomKey> append_frame(
        IndexPartialKey&& key,
        const std::shared_ptr<InputFrame>& frame,
        const SlicingPolicy& slicing,
        index::IndexSegmentReader &index_segment_reader,
        const std::shared_ptr<Store>& store,
        bool dynamic_schema,
        bool ignore_sort_order
);

enum class AffectedSegmentPart {
    START,
    END
};

folly::Future<std::optional<SliceAndKey>> async_rewrite_partial_segment(
        const SliceAndKey& existing,
        const IndexRange& index_range,
        VersionId version_id,
        AffectedSegmentPart affected_part,
        const std::shared_ptr<Store>& store);


/// Used, when updating a segment, to convert all 5 affected groups into a single list of slices
/// The 5 groups are:
/// * Segments before the update range which do not intersect with it and are not affected by
///   the update
/// * Segments before the update range which are intersecting with it and are partially affected
///   by the update.
/// * Segments which are fully contained inside the update range
/// * Segments after the update range which are intersecting with it and are partially affected
///   by the update
/// * Segments after the update range which do not intersect with it and are not affected by the
///   update
std::vector<SliceAndKey> flatten_and_fix_rows(
    const std::array<std::vector<SliceAndKey>, 5>& groups,
    size_t& global_count
);

std::vector<std::pair<FrameSlice, size_t>> get_slice_and_rowcount(
        const std::vector<FrameSlice>& slices);

} //namespace arcticdb::pipelines