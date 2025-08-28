/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/column_store/statistics.hpp>

namespace arcticdb {

struct FrameDescriptorImpl;

namespace entity {
struct SegmentDescriptorImpl;
}

inline arcticdb::proto::descriptors::NormalizationMetadata make_rowcount_norm_meta(const StreamId& stream_id);

arcticdb::proto::descriptors::NormalizationMetadata make_timeseries_norm_meta(const StreamId& stream_id);

arcticdb::proto::descriptors::NormalizationMetadata make_rowcount_norm_meta(const StreamId& stream_id);

void ensure_timeseries_norm_meta(arcticdb::proto::descriptors::NormalizationMetadata& norm_meta, const StreamId& stream_id, bool set_tz);

void ensure_rowcount_norm_meta(arcticdb::proto::descriptors::NormalizationMetadata& norm_meta, const StreamId& stream_id);

FrameDescriptorImpl frame_descriptor_from_proto(arcticdb::proto::descriptors::TimeSeriesDescriptor& tsd);

entity::SegmentDescriptorImpl segment_descriptor_from_proto(const arcticdb::proto::descriptors::StreamDescriptor& desc);

StreamId stream_id_from_proto(const arcticdb::proto::descriptors::StreamDescriptor& desc);

size_t num_blocks(const arcticdb::proto::encoding::EncodedField& field);

void field_stats_to_proto(const FieldStatsImpl& stats, arcticdb::proto::encoding::FieldStats& msg);

void field_stats_from_proto(const arcticdb::proto::encoding::FieldStats& msg, FieldStatsImpl& stats);

FieldStatsImpl create_from_proto(const arcticdb::proto::encoding::FieldStats& msg);

} //namespace arcticdb