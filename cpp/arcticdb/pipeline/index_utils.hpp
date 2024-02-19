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
#include <arcticdb/entity/variant_key.hpp>
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
std::optional<IndexValue> index_value_from_row(const RowType &row, IndexDescriptor::Type index_type, int field_num) {
    std::optional<IndexValue> index_value;
    switch (index_type) {
    case IndexDescriptor::TIMESTAMP:
        case IndexDescriptor::ROWCOUNT:
            index_value = row.template scalar_at<timestamp>(field_num);
            break;
            case IndexDescriptor::STRING: {
                auto opt = row.string_at(field_num);
                index_value = opt ? std::make_optional<IndexValue>(std::string(opt.value())) : std::nullopt;
                break;
            }
            default:
                util::raise_rte("Unknown index type {} for column {}", int(index_type), field_num);
    }
    return index_value;
}

template<typename RowType>
std::optional<IndexValue> index_start_from_row(const RowType &row, IndexDescriptor::Type index_type) {
    return index_value_from_row(row, index_type, 0);
}

template<typename SegmentType, typename FieldType=pipelines::index::Fields>
    IndexValue index_value_from_segment(const SegmentType &seg, size_t row_id, FieldType field) {
    auto index_type = seg.template scalar_at<uint8_t>(row_id, int(FieldType::index_type));
    IndexValue index_value;
    switch (index_type.value()) {
    case IndexDescriptor::TIMESTAMP:
        case IndexDescriptor::ROWCOUNT:
            index_value = seg.template scalar_at<timestamp>(row_id, int(field)).value();
            break;
            case IndexDescriptor::STRING:
                index_value = std::string(seg.string_at(row_id, int(field)).value());
                break;
                default:
                    util::raise_rte("Unknown index type {} for column {} and row {}",
                                    uint32_t(index_type.value()), uint32_t(field), row_id);
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
    TimeseriesDescriptor&& metadata,
    std::vector<SliceAndKey>&& slice_and_keys,
    const IndexPartialKey& partial_key,
    const std::shared_ptr<stream::StreamSink>& sink);

folly::Future<entity::AtomKey> write_index(
    const stream::Index& index,
    TimeseriesDescriptor &&metadata,
    std::vector<SliceAndKey> &&sk,
    const IndexPartialKey &partial_key,
    const std::shared_ptr<stream::StreamSink> &sink);

folly::Future<entity::AtomKey> write_index(
    const std::shared_ptr<InputTensorFrame>& frame,
    std::vector<folly::Future<SliceAndKey>> &&slice_and_keys,
    const IndexPartialKey &partial_key,
    const std::shared_ptr<stream::StreamSink> &sink
    );

folly::Future<entity::AtomKey> write_index(
    const std::shared_ptr<InputTensorFrame>& frame,
    std::vector<SliceAndKey> &&slice_and_keys,
    const IndexPartialKey &partial_key,
    const std::shared_ptr<stream::StreamSink> &sink
    );

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
        store).thenValue([] (auto&& version_key) {
            return VersionedItem(to_atom(std::move(version_key)));
        });
}

std::pair<index::IndexSegmentReader, std::vector<SliceAndKey>> read_index_to_vector(
    const std::shared_ptr<Store>& store,
    const AtomKey& index_key);

} //namespace arcticdb::pipelines::index