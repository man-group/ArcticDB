/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/codec/core.hpp>
#include <arcticdb/codec/variant_encoded_field_collection.hpp>
#include <arcticdb/codec/segment.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/buffer.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/util/hash.hpp>

#include <cstdlib>
#include <cstdint>
#include <memory>
#include <variant>

namespace arcticdb {

using DescriptorMagic = util::MagicNum<'D','e','s','c'>;
using EncodedMagic = util::MagicNum<'E','n','c','d'>;
using StringPoolMagic = util::MagicNum<'S','t','r','p'>;
using MetadataMagic = util::MagicNum<'M','e','t','a'>;
using IndexMagic = util::MagicNum<'I','n','d','x'>;
using ColumnMagic = util::MagicNum<'C','l','m','n'>;

/// @brief This should be the block data type descriptor when the shapes array is encoded as a block
using ShapesBlockTDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension::Dim0>>;

/// @brief Options used by default to encode the shapes array of a column
arcticdb::proto::encoding::VariantCodec shapes_encoding_opts();

template<typename MagicType>
void check_magic_in_place(const uint8_t* data) {
    const auto magic = reinterpret_cast<const MagicType*>(data);
    magic->check();
}

template<typename MagicType>
void check_magic(const uint8_t*& data) {
    check_magic_in_place<MagicType>(data);
    data += sizeof(MagicType);
}

Segment encode_v2(
    SegmentInMemory&& in_mem_seg,
    const arcticdb::proto::encoding::VariantCodec &codec_opts);

Segment encode_v1(
    SegmentInMemory&& in_mem_seg,
    const arcticdb::proto::encoding::VariantCodec &codec_opts);


inline Segment encode_dispatch(
    SegmentInMemory&& in_mem_seg,
    const arcticdb::proto::encoding::VariantCodec &codec_opts,
    EncodingVersion encoding_version) {
    if(encoding_version == EncodingVersion::V2) {
        return encode_v2(std::move(in_mem_seg), codec_opts);
    } else {
        return encode_v1(std::move(in_mem_seg), codec_opts);
    }
}

Buffer decode_encoded_fields(
    const arcticdb::proto::encoding::SegmentHeader& hdr,
    const uint8_t* data,
    const uint8_t* begin ARCTICDB_UNUSED);

SegmentInMemory decode_segment(Segment&& segment);

void decode_into_memory_segment(
    const Segment& segment,
    arcticdb::proto::encoding::SegmentHeader& hdr,
    SegmentInMemory& res,
    StreamDescriptor& desc);

template<class DataSink, typename EncodedFieldType>
std::size_t decode_field(
    const TypeDescriptor &td,
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

inline void hash_field(const arcticdb::proto::encoding::EncodedField &field, HashAccum &accum) {
    auto &n = field.ndarray();
    for(auto i = 0; i < n.shapes_size(); ++i) {
        auto v = n.shapes(i).hash();
        accum(&v);
    }

    for(auto j = 0; j < n.values_size(); ++j) {
        auto v = n.values(j).hash();
        accum(&v);
    }
}

inline HashedValue hash_segment_header(const arcticdb::proto::encoding::SegmentHeader &hdr) {
    HashAccum accum;
    if (hdr.has_metadata_field()) {
        hash_field(hdr.metadata_field(), accum);
    }
    for (int i = 0; i < hdr.fields_size(); ++i) {
        hash_field(hdr.fields(i), accum);
    }
    if(hdr.has_string_pool_field()) {
        hash_field(hdr.string_pool_field(), accum);
    }
    return accum.digest();
}
} // namespace arcticdb

#define ARCTICDB_SEGMENT_ENCODER_H_
#include "codec-inl.hpp"
