/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/codec/encode_common.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/codec/segment_header.hpp>

namespace arcticdb {

using ShapesBlockTDT = entity::TypeDescriptorTag<
        entity::DataTypeTag<entity::DataType::INT64>, entity::DimensionTag<entity::Dimension::Dim0>>;

Segment encode_dispatch(
        SegmentInMemory&& in_mem_seg, const arcticdb::proto::encoding::VariantCodec& codec_opts,
        EncodingVersion encoding_version
);

Segment encode_v2(SegmentInMemory&& in_mem_seg, const arcticdb::proto::encoding::VariantCodec& codec_opts);

Segment encode_v1(SegmentInMemory&& in_mem_seg, const arcticdb::proto::encoding::VariantCodec& codec_opts);

void decode_v1(
        const Segment& segment, const SegmentHeader& hdr, SegmentInMemory& res, const StreamDescriptor& desc,
        bool is_decoding_incompletes = false
);

void decode_v2(const Segment& segment, const SegmentHeader& hdr, SegmentInMemory& res, const StreamDescriptor& desc);

SizeResult max_compressed_size_dispatch(
        const SegmentInMemory& in_mem_seg, const arcticdb::proto::encoding::VariantCodec& codec_opts,
        EncodingVersion encoding_version
);

EncodedFieldCollection decode_encoded_fields(
        const SegmentHeader& hdr, const uint8_t* data, const uint8_t* begin ARCTICDB_UNUSED
);

SegmentInMemory decode_segment(Segment& segment, AllocationType allocation_type = AllocationType::DYNAMIC);

void decode_into_memory_segment(
        const Segment& segment, SegmentHeader& hdr, SegmentInMemory& res, const entity::StreamDescriptor& desc
);

template<class DataSink>
std::size_t decode_field(
        const entity::TypeDescriptor& td, const EncodedFieldImpl& field, const uint8_t* input, DataSink& data_sink,
        std::optional<util::BitMagic>& bv, arcticdb::EncodingVersion encoding_version
);

std::optional<google::protobuf::Any> decode_metadata_from_segment(const Segment& segment);

std::pair<std::optional<google::protobuf::Any>, StreamDescriptor> decode_metadata_and_descriptor_fields(Segment& segment
);

std::optional<TimeseriesDescriptor> decode_timeseries_descriptor(Segment& segment);

std::optional<TimeseriesDescriptor> decode_timeseries_descriptor_for_incompletes(Segment& segment);

HashedValue get_segment_hash(Segment& seg);

SegmentDescriptorImpl read_segment_descriptor(const uint8_t*& data);

} // namespace arcticdb

#define ARCTICDB_SEGMENT_ENCODER_H_
#include "codec-inl.hpp"
