/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/pipeline/index_writer.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>

namespace arcticdb::pipelines::index {

template <class IndexType>
folly::Future<entity::AtomKey> write_index(
    TimeseriesDescriptor &&metadata,
    std::vector<SliceAndKey> &&sk,
    const IndexPartialKey &partial_key,
    const std::shared_ptr<stream::StreamSink> &sink
    ) {
    auto slice_and_keys = std::move(sk);
    IndexWriter<IndexType> writer(sink, partial_key, std::move(metadata));
    for (const auto &slice_and_key : slice_and_keys) {
        writer.add(slice_and_key.key(), slice_and_key.slice_);
    }
    return writer.commit();
}

folly::Future<entity::AtomKey> write_index(
    const stream::Index& index,
    TimeseriesDescriptor &&metadata,
    std::vector<SliceAndKey> &&sk,
    const IndexPartialKey &partial_key,
    const std::shared_ptr<stream::StreamSink> &sink
    ) {
    return util::variant_match(index, [&] (auto idx) {
        using IndexType = decltype(idx);
        return write_index<IndexType>(std::move(metadata), std::move(sk), partial_key, sink);
    });
}

folly::Future<entity::AtomKey> write_index(
    InputTensorFrame&& frame,
    std::vector<SliceAndKey> &&slice_and_keys,
    const IndexPartialKey &partial_key,
    const std::shared_ptr<stream::StreamSink> &sink
    ) {
    auto offset = frame.offset;
    auto index = stream::index_type_from_descriptor(frame.desc);
    auto timeseries_desc = index_descriptor_from_frame(std::move(frame), offset);
    return write_index(index, std::move(timeseries_desc), std::move(slice_and_keys), partial_key, sink);
}

folly::Future<entity::AtomKey> write_index(
    InputTensorFrame&& frame,
    std::vector<folly::Future<SliceAndKey>> &&slice_and_keys,
    const IndexPartialKey &partial_key,
    const std::shared_ptr<stream::StreamSink> &sink
    ) {
    auto keys_fut = folly::collect(std::move(slice_and_keys)).via(&async::cpu_executor());
    return std::move(keys_fut)
    .thenValue([frame = std::move(frame), &partial_key, &sink](auto&& slice_and_keys_vals) mutable {
        return write_index(std::move(frame), std::move(slice_and_keys_vals), partial_key, sink);
    });
}

std::pair<index::IndexSegmentReader, std::vector<SliceAndKey>> read_index_to_vector(
    const std::shared_ptr<Store> &store,
    const AtomKey &index_key) {
    auto [_, index_seg] = store->read_sync(index_key);
    index::IndexSegmentReader index_segment_reader(std::move(index_seg));
    std::vector<SliceAndKey> slice_and_keys;
    for (const auto& row : index_segment_reader)
        slice_and_keys.push_back(row);

    return {std::move(index_segment_reader), std::move(slice_and_keys)};
}

} //namespace arcticdb::pipelines::index