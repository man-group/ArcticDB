/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
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
  public:
    std::shared_ptr<InputFrame> frame_;
    const FrameSlice slice_;
    const SlicingPolicy slicing_;
    folly::Function<PartialKey(const FrameSlice&)> partial_key_gen_;
    size_t slice_num_for_column_;
    Index index_;
    bool sparsify_floats_;
    util::MagicNum<'W', 's', 'e', 'g'> magic_;

    WriteToSegmentTask(
            std::shared_ptr<InputFrame> frame, FrameSlice slice, const SlicingPolicy& slicing,
            folly::Function<PartialKey(const FrameSlice&)>&& partial_key_gen, size_t slice_num_for_column, Index index,
            bool sparsify_floats
    );

    std::tuple<PartialKey, SegmentInMemory, FrameSlice> operator()();

  private:
    SegmentInMemory slice_tensors() const;
    SegmentInMemory slice_segment() const;
    Column slice_column(const SegmentInMemory& frame, size_t col_idx, size_t offset, StringPool& string_pool) const;
};

folly::Future<std::vector<SliceAndKey>> slice_and_write(
        const std::shared_ptr<InputFrame>& frame, const SlicingPolicy& slicing, IndexPartialKey&& partial_key,
        const std::shared_ptr<stream::StreamSink>& sink,
        const std::shared_ptr<DeDupMap>& de_dup_map = std::make_shared<DeDupMap>(), bool allow_sparse = false
);

int64_t write_window_size();

folly::SemiFuture<std::vector<folly::Try<SliceAndKey>>> write_slices(
        const std::shared_ptr<InputFrame>& frame, std::vector<FrameSlice>&& slices, const SlicingPolicy& slicing,
        TypedStreamVersion&& partial_key, const std::shared_ptr<stream::StreamSink>& sink,
        const std::shared_ptr<DeDupMap>& de_dup_map, bool sparsify_floats
);

folly::Future<entity::AtomKey> write_frame(
        IndexPartialKey&& key, const std::shared_ptr<InputFrame>& frame, const SlicingPolicy& slicing,
        const std::shared_ptr<Store>& store, const std::shared_ptr<DeDupMap>& de_dup_map = std::make_shared<DeDupMap>(),
        bool allow_sparse = false
);

folly::Future<entity::AtomKey> append_frame(
        IndexPartialKey&& key, const std::shared_ptr<InputFrame>& frame, const SlicingPolicy& slicing,
        index::IndexSegmentReader& index_segment_reader, const std::shared_ptr<Store>& store, bool dynamic_schema,
        bool ignore_sort_order
);

enum class AffectedSegmentPart { START, END };

folly::Future<SliceAndKey> async_rewrite_partial_segment(
        const SliceAndKey& existing, const IndexRange& index_range, VersionId version_id,
        AffectedSegmentPart affected_part, const std::shared_ptr<Store>& store
);

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
        const std::array<std::vector<SliceAndKey>, 5>& groups, size_t& global_count
);

std::vector<std::pair<FrameSlice, size_t>> get_slice_and_rowcount(const std::vector<FrameSlice>& slices);

template<typename T>
requires std::is_same_v<T, SliceAndKey> || std::is_same_v<T, std::vector<SliceAndKey>>
folly::SemiFuture<std::vector<T>> rollback_on_quota_exceeded(
        std::vector<folly::Try<T>>&& try_slices,
        folly::Function<folly::Future<StreamSink::RemoveKeyResultType>(std::vector<T>&&)>&& remove_future
) {
    std::vector<T> succeeded;
    std::optional<folly::exception_wrapper> exception;
    bool has_quota_limit_exceeded = false;
    succeeded.reserve(try_slices.size());
    for (auto& try_slice : try_slices) {
        if (try_slice.hasException()) {
            if (!exception.has_value()) {
                exception = try_slice.exception();
            }

            has_quota_limit_exceeded = has_quota_limit_exceeded ||
                                       try_slice.exception().template is_compatible_with<QuotaExceededException>();
        } else if (try_slice.hasValue()) {
            succeeded.emplace_back(std::move(try_slice.value()));
        }
    }

    if (has_quota_limit_exceeded) {
        return std::move(remove_future)(std::move(succeeded)).via(&async::cpu_executor()).thenValue([](auto&&) {
            return folly::makeSemiFuture<std::vector<T>>(
                    QuotaExceededException("Quota has been exceeded. Orphaned keys have been deleted.")
            );
        });
    }

    if (exception.has_value()) {
        exception->throw_exception();
    }

    return succeeded;
}

folly::SemiFuture<std::vector<SliceAndKey>> rollback_slices_on_quota_exceeded(
        std::vector<folly::Try<SliceAndKey>>&& try_slices, const std::shared_ptr<stream::StreamSink>& sink
);

folly::SemiFuture<std::vector<std::vector<SliceAndKey>>> rollback_batches_on_quota_exceeded(
        std::vector<folly::Try<std::vector<SliceAndKey>>>&& try_slice_batches,
        const std::shared_ptr<stream::StreamSink>& sink
);

folly::Future<StreamSink::RemoveKeyResultType> remove_slice_and_keys(
        std::vector<SliceAndKey>&& slices, StreamSink& sink
);

} // namespace arcticdb::pipelines