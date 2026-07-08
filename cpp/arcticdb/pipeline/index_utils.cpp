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
#include <arcticdb/entity/merge_descriptors.hpp>

namespace {
using namespace arcticdb;

template<typename T>
concept common_normalization = util::any_of<
        T, proto::descriptors::NormalizationMetadata_Pandas,
        proto::descriptors::NormalizationMetadata_NormalisedTimeSeries>;

template<common_normalization CommonNormalization>
void set_index(CommonNormalization& dest_common, const CommonNormalization& source_common) {
    if (dest_common.has_index()) {
        *dest_common.mutable_index() = source_common.index();
    } else if (dest_common.has_multi_index()) {
        *dest_common.mutable_multi_index() = source_common.multi_index();
    }
}

template<common_normalization CommonNormalization>
void set_tz(CommonNormalization& dest, const CommonNormalization& source) {
    if (dest.has_multi_index()) {
        dest.mutable_multi_index()->set_tz(source.multi_index().tz());
    } else {
        dest.mutable_index()->set_tz(source.index().tz());
    }
}

void merge_rowrange_index(
        proto::descriptors::NormalizationMetadata& dest, const proto::descriptors::NormalizationMetadata& source
) {
    // The current behavior is the last modification operation is setting the index name. See Monday 9797097831, it
    // would be best to require that index names are always matching. This is the case for datetime index because
    // it's a physical column. It's a potentially breaking change. Covered in:
    // test_append.py::test_append_series_with_different_row_range_index_name
    if (dest.has_series()) {
        set_index(*dest.mutable_series()->mutable_common(), source.series().common());
    } else if (dest.has_df()) {
        set_index(*dest.mutable_df()->mutable_common(), source.df().common());
    }
}

void merge_timeseries_index(
        proto::descriptors::NormalizationMetadata& dest, const proto::descriptors::NormalizationMetadata& source
) {
    // See Monday 12029540807
    // It's a known bug that the new metadata overwrites the timezone but it's an API break to fix it.
    if (dest.has_series()) {
        set_tz(*dest.mutable_series()->mutable_common(), source.series().common());
    } else if (dest.has_df()) {
        set_tz(*dest.mutable_df()->mutable_common(), source.df().common());
    }
}

} // namespace

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

proto::descriptors::NormalizationMetadata merge_normalization_metadata(
        const TimeseriesDescriptor& existing_tsd, const InputFrame& new_frame
) {
    if (existing_tsd.index().type() == IndexDescriptor::Type::EMPTY) {
        return new_frame.norm_meta;
    }

    proto::descriptors::NormalizationMetadata result = existing_tsd.normalization();
    accumulate_norm_metadata_column_names(result, new_frame.norm_meta);
    if (result.has_np()) {
        // Only append is allowed for numpy arrays, and it's checked up the callstack.
        (*result.mutable_np()->mutable_shape())[0] += new_frame.norm_meta.np().shape()[0];
    }

    if (existing_tsd.index().type() == IndexDescriptor::Type::ROWCOUNT) {
        merge_rowrange_index(result, new_frame.norm_meta);
    } else if (existing_tsd.index().type() == IndexDescriptor::Type::TIMESTAMP) {
        merge_timeseries_index(result, new_frame.norm_meta);
    }
    return result;
}

TimeseriesDescriptor get_merged_tsd(
        size_t row_count, bool dynamic_schema, const TimeseriesDescriptor& existing_tsd,
        const std::shared_ptr<InputFrame>& new_frame
) {
    auto existing_descriptor = existing_tsd.as_stream_descriptor();
    auto merged_descriptor = existing_descriptor;
    const auto new_frame_desc = new_frame->compute_desc_for_tsd();
    if (existing_tsd.total_rows() == 0) {
        // If the existing dataframe is empty, we use the descriptor of the new_frame
        merged_descriptor = new_frame_desc;
    } else if (dynamic_schema) {
        // In case of dynamic schema
        const std::array fields_ptr = {new_frame_desc.fields_ptr()};
        merged_descriptor = merge_descriptors(existing_descriptor, fields_ptr, {});
    } else {
        // In case of static schema, we only promote empty types and fixed->dynamic strings
        const auto& new_fields = new_frame_desc.fields();
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
    merged_descriptor.set_sorted(deduce_sorted(existing_descriptor.sorted(), new_frame_desc.sorted()));
    return make_timeseries_descriptor(
            row_count,
            std::move(merged_descriptor),
            merge_normalization_metadata(existing_tsd, *new_frame),
            std::move(new_frame->user_meta),
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