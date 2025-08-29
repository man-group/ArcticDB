/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/index_segment_reader.hpp>
#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/pipeline/pipeline_common.hpp>
#include <arcticdb/stream/index.hpp>
#include <folly/futures/Future.h>

namespace arcticdb {
namespace stream {
struct StreamSink;
}

namespace pipelines {
struct InputTensorFrame;
}
}

namespace arcticdb::pipelines::index {
inline std::vector<SliceAndKey> unfiltered_index(const index::IndexSegmentReader &index_segment_reader) {
    ARCTICDB_SAMPLE_DEFAULT(FilterIndex)
    std::vector<SliceAndKey> output;
    std::copy(std::cbegin(index_segment_reader), std::cend(index_segment_reader), std::back_inserter(output));
    return output;
}

template<typename RowType>
std::optional<IndexValue> index_value_from_row(const RowType &row, IndexDescriptorImpl::Type index_type, int field_num) {
    switch (index_type) {
    case IndexDescriptorImpl::Type::TIMESTAMP:
    case IndexDescriptorImpl::Type::ROWCOUNT: return row.template scalar_at<timestamp>(field_num);
    case IndexDescriptorImpl::Type::STRING: {
        auto opt = row.string_at(field_num);
        return opt ? std::make_optional<IndexValue>(std::string(opt.value())) : std::nullopt;
    }
    default:
        util::raise_rte("Unknown index type {} for column {}", int(index_type), field_num);
    }
    return std::nullopt;
}

template<typename RowType>
std::optional<IndexValue> index_start_from_row(const RowType &row, IndexDescriptorImpl::Type index_type) {
    return index_value_from_row(row, index_type, 0);
}

template<typename SegmentType, typename FieldType=pipelines::index::Fields>
IndexValue index_value_from_segment(const SegmentType &seg, size_t row_id, FieldType field) {
    auto index_type = seg.template scalar_at<uint8_t>(row_id, int(FieldType::index_type));
    IndexValue index_value;
    auto type = IndexDescriptor::Type(index_type.value());
    switch (type) {
    case IndexDescriptorImpl::Type::TIMESTAMP:
        case IndexDescriptorImpl::Type::ROWCOUNT:
        index_value = seg.template scalar_at<timestamp>(row_id, int(field)).value();
        break;
    case IndexDescriptorImpl::Type::STRING:
        index_value = std::string(seg.string_at(row_id, int(field)).value());
        break;
    default:
        util::raise_rte("Unknown index type {} for column {} and row {}", uint32_t(index_type.value()), uint32_t(field), row_id);
    }
    return index_value;
}

template<typename SegmentType, typename FieldType>
IndexValue index_start_from_segment(const SegmentType &seg, size_t row_id) {
    return index_value_from_segment(seg, row_id, FieldType::start_index);
}

template<typename SegmentType, typename FieldType>
IndexValue index_end_from_segment(const SegmentType &seg, size_t row_id) {
    return index_value_from_segment(seg, row_id, FieldType::end_index);
}

template<class IndexType>
folly::Future<entity::AtomKey> write_index(
    const TimeseriesDescriptor& metadata,
    std::vector<SliceAndKey>&& slice_and_keys,
    const IndexPartialKey& partial_key,
    const std::shared_ptr<stream::StreamSink>& sink);

folly::Future<entity::AtomKey> write_index(
    const stream::Index& index,
    const TimeseriesDescriptor& metadata,
    std::vector<SliceAndKey> &&sk,
    const IndexPartialKey &partial_key,
    const std::shared_ptr<stream::StreamSink> &sink);

folly::Future<entity::AtomKey> write_index(
    const std::shared_ptr<InputTensorFrame>& frame,
    std::vector<folly::Future<SliceAndKey>> &&slice_and_keys,
    const IndexPartialKey &partial_key,
    const std::shared_ptr<stream::StreamSink> &sink);

folly::Future<entity::AtomKey> write_index(
    const std::shared_ptr<InputTensorFrame>& frame,
    std::vector<SliceAndKey> &&slice_and_keys,
    const IndexPartialKey &partial_key,
    const std::shared_ptr<stream::StreamSink> &sink);

inline folly::Future<VersionedItem> index_and_version(
        const stream::Index& index,
        const std::shared_ptr<stream::StreamSink>& store,
        TimeseriesDescriptor time_series,
        std::vector<SliceAndKey> slice_and_keys,
        const StreamId& stream_id,
        VersionId version_id) {
    return write_index(
        index,
        std::move(time_series),
        std::move(slice_and_keys),
        IndexPartialKey{stream_id, version_id},
        store).thenValue([] (AtomKey&& version_key) {
            return VersionedItem(std::move(version_key));
        });
}

std::pair<index::IndexSegmentReader, std::vector<SliceAndKey>> read_index_to_vector(
    const std::shared_ptr<Store>& store,
    const AtomKey& index_key);

// Combines the stream descriptors of an existing index key and a new frame.
// Can be used to get the metadata for [write_index] when updating or appending.
TimeseriesDescriptor get_merged_tsd(
    size_t row_count,
    bool dynamic_schema,
    const TimeseriesDescriptor& existing_tsd,
    const std::shared_ptr<pipelines::InputTensorFrame>& new_frame);

[[nodiscard]] bool is_timeseries_index(const IndexDescriptorImpl& index_desc);

// If NormalizationMetadata is not provided, or specifies the return type is not a Series or DataFrame, returns the
// index field count from the stream descriptor.
// Otherwise, the behaviour depends on whether the data has a multiindex, and whether or not the data is a Series.
// - DataFrame + non multiindex = index field count from the stream descriptor
// - DataFrame + multiindex = number of levels in the multiindex
// - Series + non multiindex = index field count from the stream descriptor PLUS ONE. This is to handle joins of Series
//   with different names, which are stored as dataframes with different column names internally.
// - Series + multiindex = number of levels in the multiindex PLUS ONE for the same reason as above
uint32_t required_fields_count(
        const StreamDescriptor& stream_desc,
        const std::optional<proto::descriptors::NormalizationMetadata>& norm_meta = std::nullopt);

} //namespace arcticdb::pipelines::index