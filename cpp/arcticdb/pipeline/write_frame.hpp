/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/stream/index.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/index_range.hpp>

#include <vector>
#include <variant>
#include <arcticdb/pipeline/input_tensor_frame.hpp>
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

    std::shared_ptr<InputTensorFrame> frame_;
    const FrameSlice slice_;
    const SlicingPolicy slicing_;
    folly::Function<stream::StreamSink::PartialKey(const FrameSlice&)> partial_key_gen_;
    size_t slice_num_for_column_;
    Index index_;
    bool sparsify_floats_;
    util::MagicNum<'W', 's', 'e', 'g'> magic_;

    WriteToSegmentTask(
        std::shared_ptr<InputTensorFrame> frame,
        FrameSlice slice,
        const SlicingPolicy& slicing,
        folly::Function<stream::StreamSink::PartialKey(const FrameSlice&)>&& partial_key_gen,
        size_t slice_num_for_column,
        Index index,
        bool sparsify_floats);

    std::tuple<stream::StreamSink::PartialKey, SegmentInMemory, FrameSlice> operator()();
};

folly::Future<std::vector<SliceAndKey>> slice_and_write(
        const std::shared_ptr<InputTensorFrame> &frame,
        const SlicingPolicy &slicing,
        IndexPartialKey&& partial_key,
        const std::shared_ptr<stream::StreamSink> &sink,
        const std::shared_ptr<DeDupMap>& de_dup_map = std::make_shared<DeDupMap>(),
        bool allow_sparse = false
);

folly::Future<std::vector<SliceAndKey>> write_slices(
        const std::shared_ptr<InputTensorFrame> &frame,
        std::vector<FrameSlice>&& slices,
        const SlicingPolicy &slicing,
        IndexPartialKey&& partial_key,
        const std::shared_ptr<stream::StreamSink>& sink,
        const std::shared_ptr<DeDupMap>& de_dup_map,
        bool sparsify_floats);

folly::Future<entity::AtomKey> write_frame(
    IndexPartialKey &&key,
    const std::shared_ptr<InputTensorFrame>& frame,
    const SlicingPolicy &slicing,
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<DeDupMap>& de_dup_map = std::make_shared<DeDupMap>(),
    bool allow_sparse = false
);

folly::Future<entity::AtomKey> append_frame(
        IndexPartialKey&& key,
        const std::shared_ptr<InputTensorFrame>& frame,
        const SlicingPolicy& slicing,
        index::IndexSegmentReader &index_segment_reader,
        const std::shared_ptr<Store>& store,
        bool dynamic_schema,
        bool ignore_sort_order
);

std::optional<SliceAndKey> rewrite_partial_segment(
        const SliceAndKey& existing,
        IndexRange index_range,
        VersionId version_id,
        bool before,
        const std::shared_ptr<Store>& store);

std::vector<SliceAndKey> flatten_and_fix_rows(
        const std::vector<std::vector<SliceAndKey>>& groups,
        size_t& global_count);

std::vector<std::pair<FrameSlice, size_t>> get_slice_and_rowcount(
        const std::vector<FrameSlice>& slices);

} //namespace arcticdb::pipelines