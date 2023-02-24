/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
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

std::vector<folly::Future<SliceAndKey>> slice_and_write(
        InputTensorFrame &frame,
        const SlicingPolicy &slicing,
        folly::Function<stream::StreamSink::PartialKey(const FrameSlice &)> &&partial_key_gen,
        const std::shared_ptr<stream::StreamSink> &sink,
        const std::shared_ptr<DeDupMap>& de_dup_map = std::make_shared<DeDupMap>(),
        bool allow_sparse = false
);


folly::Future<entity::AtomKey> write_frame(
    const IndexPartialKey &key,
    InputTensorFrame&& frame,
    const SlicingPolicy &slicing,
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<DeDupMap>& de_dup_map = std::make_shared<DeDupMap>(),
    bool allow_sparse = false
);

folly::Future<entity::AtomKey> append_frame(
        const IndexPartialKey& key,
        InputTensorFrame&& frame,
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

} //namespace arcticdb::pipelines