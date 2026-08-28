/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/index_utils.hpp>

#include <arcticdb/python/normalization_utils.hpp>

#include <arcticdb/storage/store.hpp>
#include <arcticdb/pipeline/index_writer.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>
#include <arcticdb/version/version_utils.hpp>

namespace arcticdb::pipelines::index {

template<class IndexType>
folly::Future<entity::AtomKey> write_index(
        const TimeseriesDescriptor& metadata, std::vector<SliceAndKey>&& sk, const IndexPartialKey& partial_key,
        const std::shared_ptr<stream::StreamSink>& sink
) {
    auto slice_and_keys = std::move(sk);
    IndexWriter<IndexType> writer(sink, partial_key, metadata);
    for (const auto& slice_and_key : slice_and_keys) {
        writer.add(slice_and_key.key(), slice_and_key.slice_);
    }

    return writer.commit();
}

folly::Future<entity::AtomKey> write_index(
        const stream::Index& index, const TimeseriesDescriptor& metadata, std::vector<SliceAndKey>&& sk,
        const IndexPartialKey& partial_key, const std::shared_ptr<stream::StreamSink>& sink
) {
    return util::variant_match(index, [&](auto idx) {
        using IndexType = decltype(idx);
        return write_index<IndexType>(metadata, std::move(sk), partial_key, sink);
    });
}

folly::Future<entity::AtomKey> write_index(
        const std::shared_ptr<InputFrame>& frame, std::vector<SliceAndKey>&& slice_and_keys,
        const IndexPartialKey& partial_key, const std::shared_ptr<stream::StreamSink>& sink
) {
    auto offset = frame->offset;
    auto index = stream::index_type_from_descriptor(frame->desc());
    auto timeseries_desc = index_descriptor_from_frame(frame, offset);
    return write_index(index, timeseries_desc, std::move(slice_and_keys), partial_key, sink);
}

folly::Future<entity::AtomKey> write_index(
        const std::shared_ptr<InputFrame>& frame, std::vector<folly::Future<SliceAndKey>>&& slice_and_keys,
        const IndexPartialKey& partial_key, const std::shared_ptr<stream::StreamSink>& sink
) {
    auto keys_fut = folly::collect(std::move(slice_and_keys)).via(&async::cpu_executor());
    return std::move(keys_fut).thenValue([frame = frame, &partial_key, &sink](auto&& slice_and_keys_vals) mutable {
        return write_index(frame, std::move(slice_and_keys_vals), partial_key, sink);
    });
}

std::pair<index::IndexSegmentReader, std::vector<SliceAndKey>> read_index_to_vector(
        const std::shared_ptr<Store>& store, const AtomKey& index_key
) {
    auto [_, index_seg] = store->read_sync(index_key);
    index::IndexSegmentReader index_segment_reader(std::move(index_seg));
    std::vector<SliceAndKey> slice_and_keys;
    for (const auto& row : index_segment_reader)
        slice_and_keys.push_back(row);

    return {std::move(index_segment_reader), std::move(slice_and_keys)};
}

bool is_timeseries_or_empty_index(const IndexDescriptorImpl& index_desc) {
    return index_desc.type() == IndexDescriptor::Type::TIMESTAMP || index_desc.type() == IndexDescriptor::Type::EMPTY;
}

RequiredFieldInfo required_fields_info(const proto::descriptors::NormalizationMetadata& norm_meta) {
    RequiredFieldInfo info;
    info.has_series_value_column = norm_meta.has_series();
    if (const auto* common = pandas_common(norm_meta); common != nullptr) {
        info.has_multi_index = common->has_multi_index();
        // The field count in the norm metadata is one less than the actual number of levels in the multi-index.
        // See index_norm.field_count = len(index.levels) - 1 in _normalization.py::_PandasNormalizer::_index_to_records
        info.num_physical_indices = info.has_multi_index                     ? common->multi_index().field_count() + 1
                                    : common->index().is_physically_stored() ? 1
                                                                             : 0;
    } else if (norm_meta.has_experimental_arrow()) {
        info.num_physical_indices = norm_meta.experimental_arrow().has_index() ? 1 : 0;
    }
    return info;
}

RequiredFieldInfo required_fields_info(
        const StreamDescriptor& stream_desc, const std::optional<proto::descriptors::NormalizationMetadata>& norm_meta
) {
    if (!norm_meta.has_value()) {
        return {.num_physical_indices = stream_desc.index().field_count()};
    }
    auto info = required_fields_info(*norm_meta);
    if (pandas_common(*norm_meta) == nullptr && !norm_meta->has_experimental_arrow()) {
        info.num_physical_indices = stream_desc.index().field_count();
    }
    return info;
}

} // namespace arcticdb::pipelines::index