/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/codec/core.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/codec/encoding_version.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/codec/segment_header.hpp>

namespace arcticdb {

class Segment;
class SegmentInMemory;

using ShapesBlockTDT = entity::TypeDescriptorTag<entity::DataTypeTag<entity::DataType::INT64>, entity::DimensionTag<entity::Dimension::Dim0>>;

Segment encode_dispatch(
    SegmentInMemory&& in_mem_seg,
    const arcticdb::proto::encoding::VariantCodec &codec_opts,
    EncodingVersion encoding_version);

Buffer decode_encoded_fields(
    const SegmentHeader& hdr,
    const uint8_t* data,
    const uint8_t* begin ARCTICDB_UNUSED);

SegmentInMemory decode_segment(Segment&& segment);

void decode_into_memory_segment(
    const Segment& segment,
    SegmentHeader& hdr,
    SegmentInMemory& res,
    entity::StreamDescriptor& desc);

template<class DataSink, typename EncodedFieldType>
std::size_t decode_field(
    const entity::TypeDescriptor &td,
    const EncodedFieldType &field,
    const uint8_t *input,
    DataSink &data_sink,
    std::optional<util::BitMagic>& bv,
    arcticdb::EncodingVersion encoding_version);

std::optional<google::protobuf::Any> decode_metadata_from_segment(
    const Segment& segment);

std::pair<std::optional<google::protobuf::Any>, StreamDescriptor> decode_metadata_and_descriptor_fields(
    Segment& segment);

std::optional<std::tuple<google::protobuf::Any, arcticdb::proto::descriptors::TimeSeriesDescriptor, FieldCollection>> decode_timeseries_descriptor(
    Segment& segment);

HashedValue hash_segment_header(const arcticdb::proto::encoding::SegmentHeader &hdr);
} // namespace arcticdb

#define ARCTICDB_SEGMENT_ENCODER_H_
#include "codec-inl.hpp"
