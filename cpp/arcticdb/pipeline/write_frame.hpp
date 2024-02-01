/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/index_range.hpp>

#include <vector>
#include <arcticdb/pipeline/input_tensor_frame.hpp>
#include <arcticdb/stream/index.hpp>
#include <folly/futures/Future.h>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/slicing.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/pipeline/pipeline_common.hpp>

#include<boost/core/span.hpp>

namespace arcticdb::pipelines {

using namespace arcticdb::stream;

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

enum class AffectedSegmentPart {
    START,
    END
};

std::optional<SliceAndKey> rewrite_partial_segment(
        const SliceAndKey& existing,
        IndexRange index_range,
        VersionId version_id,
        AffectedSegmentPart affected_part,
        const std::shared_ptr<Store>& store);

// TODO: Use std::span when C++20 is enabled
std::vector<SliceAndKey> flatten_and_fix_rows(boost::span<const std::vector<SliceAndKey>> groups, size_t& global_count);

} //namespace arcticdb::pipelines