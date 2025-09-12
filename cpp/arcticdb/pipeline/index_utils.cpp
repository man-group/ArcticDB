/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/pipeline/index_writer.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>
#include <arcticdb/version/version_utils.hpp>
#include <arcticdb/entity/merge_descriptors.hpp>

namespace arcticdb::pipelines::index {

template<class IndexType>
folly::Future<entity::AtomKey> write_index(
        const TimeseriesDescriptor& metadata, std::vector<SliceAndKey>&& sk, const IndexPartialKey& partial_key,
        const std::shared_ptr<stream::StreamSink>& sink
) {
    auto slice_and_keys = std::move(sk);
    IndexWriter<IndexType> writer(sink, partial_key, metadata);
    uint64_t total = 0;
    for (const auto &slice_and_key : slice_and_keys) {
        total += slice_and_key.slice_.row_range.diff();
        writer.add(slice_and_key.key(), slice_and_key.slice_);
    }
    util::check(metadata.total_rows() == total,
     "The row count of the metadata is not the same as the row count of the slice and keys: {} != {}",
     metadata.total_rows(), total);

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
        const std::shared_ptr<InputTensorFrame>& frame, std::vector<SliceAndKey>&& slice_and_keys,
        const IndexPartialKey& partial_key, const std::shared_ptr<stream::StreamSink>& sink
) {
    auto offset = frame->offset;
    auto index = stream::index_type_from_descriptor(frame->desc);
    auto timeseries_desc = index_descriptor_from_frame(frame, offset);
    return write_index(index, timeseries_desc, std::move(slice_and_keys), partial_key, sink);
}

folly::Future<entity::AtomKey> write_index(
        const std::shared_ptr<InputTensorFrame>& frame, std::vector<folly::Future<SliceAndKey>>&& slice_and_keys,
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

TimeseriesDescriptor get_merged_tsd(
        size_t row_count, bool dynamic_schema, const TimeseriesDescriptor& existing_tsd,
        const std::shared_ptr<pipelines::InputTensorFrame>& new_frame
) {
    auto existing_descriptor = existing_tsd.as_stream_descriptor();
    auto merged_descriptor = existing_descriptor;
    if (existing_tsd.total_rows() == 0) {
        // If the existing dataframe is empty, we use the descriptor of the new_frame
        merged_descriptor = new_frame->desc;
    } else if (dynamic_schema) {
        // In case of dynamic schema
        const std::array fields_ptr = {new_frame->desc.fields_ptr()};
        merged_descriptor = merge_descriptors(existing_descriptor, fields_ptr, {});
    } else {
        // In case of static schema, we only promote empty types and fixed->dynamic strings
        const auto& new_fields = new_frame->desc.fields();
        for (size_t i = 0; i < new_fields.size(); ++i) {
            const auto& new_type = new_fields.at(i).type();
            TypeDescriptor& result_type = merged_descriptor.mutable_field(i).mutable_type();
            // We allow promoting empty types
            if (is_empty_type(result_type.data_type()) && !is_empty_type(new_type.data_type())) {
                result_type = new_type;
            }
            // We allow promoting fixed strings to dynamic strings
            else if (is_sequence_type(result_type.data_type()) && is_sequence_type(new_type.data_type()) &&
                     !is_dynamic_string_type(result_type.data_type()) && is_dynamic_string_type(new_type.data_type()) &&
                     !is_arrow_output_only_type(new_type.data_type())) {
                result_type = new_type;
            }
        }
    }
    merged_descriptor.set_sorted(deduce_sorted(existing_descriptor.sorted(), new_frame->desc.sorted()));
    return make_timeseries_descriptor(
            row_count,
            std::move(merged_descriptor),
            std::move(new_frame->norm_meta),
            std::move(new_frame->user_meta),
            std::nullopt,
            std::nullopt,
            new_frame->bucketize_dynamic
    );
}

bool is_timeseries_index(const IndexDescriptorImpl& index_desc) {
    return index_desc.type() == IndexDescriptor::Type::TIMESTAMP || index_desc.type() == IndexDescriptor::Type::EMPTY;
}

uint32_t required_fields_count(
        const StreamDescriptor& stream_desc, const std::optional<proto::descriptors::NormalizationMetadata>& norm_meta
) {
    if (norm_meta.has_value() && norm_meta->has_df() && norm_meta->df().common().has_multi_index()) {
        // The field count in the norm metadata is one less than the actual number of levels in the multiindex
        // See index_norm.field_count = len(index.levels) - 1 in _normalization.py::_PandasNormalizer::_index_to_records
        return norm_meta->df().common().multi_index().field_count() + 1;
    } else if (norm_meta.has_value() && norm_meta->has_series() && norm_meta->series().common().has_multi_index()) {
        return norm_meta->series().common().multi_index().field_count() + 2;
    } else {
        return stream_desc.index().field_count() + ((norm_meta.has_value() && norm_meta->has_series()) ? 1 : 0);
    }
}

} // namespace arcticdb::pipelines::index