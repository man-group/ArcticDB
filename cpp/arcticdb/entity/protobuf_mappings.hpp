/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/atom_key.hpp>

// Forward-declare generated protobuf namespace and message classes so the alias below compiles
namespace arcticc {
namespace pb2 {
namespace descriptors_pb2 {
class AtomKey;
class StreamDescriptor;
class TimeSeriesDescriptor;
class IndexDescriptor;
class NormalizationMetadata;
} // namespace descriptors_pb2
} // namespace pb2
} // namespace arcticc

namespace arcticdb {
namespace proto {
namespace descriptors = arcticc::pb2::descriptors_pb2;
}
namespace entity {
struct StreamDescriptor;
struct IndexDescriptorImpl;
} // namespace entity

struct TimeseriesDescriptor;

proto::descriptors::AtomKey key_to_proto(const entity::AtomKey& key);

entity::AtomKey key_from_proto(const proto::descriptors::AtomKey& input);

void copy_stream_descriptor_to_proto(const entity::StreamDescriptor& desc, proto::descriptors::StreamDescriptor& proto);

proto::descriptors::TimeSeriesDescriptor copy_time_series_descriptor_to_proto(const TimeseriesDescriptor& tsd);

inline void set_id(proto::descriptors::StreamDescriptor& pb_desc, StreamId id);

[[nodiscard]] proto::descriptors::IndexDescriptor index_descriptor_to_proto(
        const entity::IndexDescriptorImpl& index_descriptor
);

[[nodiscard]] entity::IndexDescriptorImpl index_descriptor_from_proto(
        const proto::descriptors::IndexDescriptor& index_descriptor
);

template<typename SourceType, typename DestType>
void exchange_timeseries_proto(const SourceType& source, DestType& destination) {
    if (source.has_normalization())
        *destination.mutable_normalization() = source.normalization();

    if (source.has_user_meta())
        *destination.mutable_user_meta() = source.user_meta();

    if (source.has_next_key())
        *destination.mutable_next_key() = source.next_key();

    if (source.has_multi_key_meta())
        *destination.mutable_multi_key_meta() = source.multi_key_meta();
}

} // namespace arcticdb