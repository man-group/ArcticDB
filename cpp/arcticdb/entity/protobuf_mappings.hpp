/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/types.hpp>

namespace arcticdb {

namespace entity {
struct StreamDescriptor;
} // namespace entity

struct TimeseriesDescriptor;

arcticdb::proto::descriptors::AtomKey key_to_proto(const entity::AtomKey& key);

entity::AtomKey key_from_proto(const arcticdb::proto::descriptors::AtomKey& input);

void copy_stream_descriptor_to_proto(
        const entity::StreamDescriptor& desc, arcticdb::proto::descriptors::StreamDescriptor& proto
);

arcticdb::proto::descriptors::TimeSeriesDescriptor copy_time_series_descriptor_to_proto(const TimeseriesDescriptor& tsd
);

inline void set_id(arcticdb::proto::descriptors::StreamDescriptor& pb_desc, StreamId id);

[[nodiscard]] arcticdb::proto::descriptors::IndexDescriptor index_descriptor_to_proto(
        const entity::IndexDescriptorImpl& index_descriptor
);

[[nodiscard]] entity::IndexDescriptorImpl index_descriptor_from_proto(
        const arcticdb::proto::descriptors::IndexDescriptor& index_descriptor
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