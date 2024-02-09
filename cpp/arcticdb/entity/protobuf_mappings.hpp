/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/atom_key.hpp>

namespace arcticdb {

namespace entity {
struct StreamDescriptor;
} //namespace arcticdb::entity

struct TimeseriesDescriptor;

arcticdb::proto::descriptors::AtomKey encode_key(const entity::AtomKey &key);

entity::AtomKey decode_key(const arcticdb::proto::descriptors::AtomKey& input);

[[nodiscard]] arcticdb::proto::descriptors::StreamDescriptor copy_stream_descriptor_to_proto(const entity::StreamDescriptor& desc);

arcticdb::proto::descriptors::TimeSeriesDescriptor copy_time_series_descriptor_to_proto(const TimeseriesDescriptor& tsd);

static StreamId id_from_proto(const arcticdb::proto::descriptors::StreamDescriptor& proto);

inline void set_id(arcticdb::proto::descriptors::StreamDescriptor& pb_desc, StreamId id);

} //namespace arcticdb