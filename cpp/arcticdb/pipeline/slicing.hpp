/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/stream/index.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/pipeline/pipeline_common.hpp>
#include <arcticdb/pipeline/index_segment_reader.hpp>
#include <arcticdb/pipeline/input_tensor_frame.hpp>
#include <arcticdb/pipeline/write_options.hpp>

#include <folly/futures/Future.h>

#include <optional>
#include <vector>
#include <cstddef>

namespace arcticdb::pipelines {

class FixedSlicer {
public:
    explicit FixedSlicer(std::size_t col_per_slice = 127, std::size_t row_per_slice = 100'000) :
            col_per_slice_(col_per_slice), row_per_slice_(row_per_slice) { }

    std::vector<FrameSlice> operator() (const InputTensorFrame &frame) const;

    auto row_per_slice() const { return row_per_slice_; }

private:
    size_t col_per_slice_;
    size_t row_per_slice_;
};

class HashedSlicer {
public:
    explicit HashedSlicer(std::size_t num_buckets, std::size_t row_per_slice) :
        num_buckets_(num_buckets), row_per_slice_(row_per_slice) { }

    std::vector<FrameSlice> operator() (const InputTensorFrame &frame) const;

    size_t num_buckets() const { return num_buckets_; }

    auto row_per_slice() const { return row_per_slice_; }

private:
    size_t num_buckets_;
    size_t row_per_slice_;
};

class NoSlicing {
};

using SlicingPolicy = std::variant<NoSlicing, FixedSlicer, HashedSlicer>;

SlicingPolicy get_slicing_policy(
    const WriteOptions& options,
    const arcticdb::pipelines::InputTensorFrame& frame);

SlicingPolicy get_slicing_policy(
        const WriteOptions& options,
        const SegmentInMemory& segment);

std::vector<FrameSlice> slice(InputTensorFrame &frame, const SlicingPolicy& slicer);

inline auto slice_begin_pos(const FrameSlice& slice, const InputTensorFrame& frame) {
    return slice.row_range.first - frame.offset;
}

inline auto slice_end_pos(const FrameSlice& slice, const InputTensorFrame& frame) {
    return (slice.row_range.second-1) - frame.offset;
}

template <typename T>
inline auto end_index_generator(T end_index){//works for both rawtype and rawtype encapsulated in variant
    if constexpr(std::is_same_v<T, stream::IndexValue>){
        std::visit(
            [](auto &index){
                if constexpr(std::is_same_v<std::remove_reference_t<decltype(index)>, entity::NumericIndex>){
                    index += timestamp(1);
                }
            }
            , end_index);
        return end_index;
    }
    else if constexpr(std::is_same_v<T, timestamp>){
        return end_index + timestamp(1);
    }
    else{
        return end_index;
    }
}

inline auto get_partial_key_gen(std::shared_ptr<InputTensorFrame> frame, TypedStreamVersion key) {
    using PartialKey = stream::StreamSink::PartialKey;

    return [frame=std::move(frame), key = std::move(key)](const FrameSlice& s) {
        if (frame->has_index()) {
            util::check(static_cast<bool>(frame->index_tensor), "Got null index tensor in get_partial_key_gen");
            auto& idx = frame->index_tensor.value();
            auto start = *idx.ptr_cast<timestamp>(slice_begin_pos(s, *frame));
            auto end = *idx.ptr_cast<timestamp>(slice_end_pos(s, *frame));
            return PartialKey{
                    key.type, key.version_id, key.id, start, end_index_generator(end)};
        }
        else {
            return PartialKey{
                    key.type, key.version_id, key.id,
                    entity::safe_convert_to_numeric_index(s.row_range.first, "Rows"),
                    entity::safe_convert_to_numeric_index(s.row_range.second, "Rows")};
        }
    };
}

} //arcticdb::pipelines

