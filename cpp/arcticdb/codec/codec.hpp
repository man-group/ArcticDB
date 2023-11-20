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

template<typename MagicType>
void write_magic(Buffer& buffer, std::ptrdiff_t& pos) {
    new (buffer.data() + pos) MagicType{};
    pos += sizeof(MagicType);
}

/// @brief This should be the block data type descriptor when the shapes array is encoded as a block
using ShapesBlockTDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension::Dim0>>;
/// @brief The type to be used for a block which represents shapes
using ShapesBlock = TypedBlockData<ShapesBlockTDT>;

//TODO  Provide tuning parameters to choose the encoding through encoding tags (also acting as config options)
template<template<typename> class TypedBlock, class TD, EncodingVersion encoder_version>
struct TypedBlockEncoderImpl;

template<template<typename> class F, class TD, EncodingVersion v>
struct BlockEncoderHelper : public TypedBlockEncoderImpl<F, TD, v> {
    using ValuesBlockType = F<TD>;
    using TypeDescriptorTag = TD;
    using DimTag = typename TD::DimensionTag;
    using DataTag = typename TD::DataTypeTag;
};

template<typename TD, EncodingVersion v>
using BlockEncoder = BlockEncoderHelper<TypedBlockData, TD, v>;

/// @brief Options used by default to encode the shapes array of a column
arcticdb::proto::encoding::VariantCodec shapes_encoding_opts();

/// @brief Utility class used to encode and compute the max encoding size for regular data columns for V1 encoding
struct ColumnEncoderV1 {
    static std::pair<size_t, size_t> max_compressed_size(
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        ColumnData& column_data);

    static void encode(
        const arcticdb::proto::encoding::VariantCodec &codec_opts,
        ColumnData& column_data,
        MutableVariantField variant_field,
        Buffer& out,
        std::ptrdiff_t& pos);
};

/// @brief Utility class used to encode and compute the max encoding size for regular data columns for V2 encoding
/// What differs this from the already existing ColumnEncoder is that ColumnEncoder encodes the shapes of
/// multidimensional data as part of each block. ColumnEncoder2 uses a better strategy and encodes the shapes for the
/// whole column upfront (before all blocks).
/// @note Although ArcticDB did not support multidimensional user data prior to creating ColumnEncoder2 some of the
/// internal data was multidimensional and used ColumnEncoder. More specifically: string pool and metadata.
/// @note This should be used for V2 encoding. V1 encoding can't use it as there is already data written the other
///	way and it will be hard to distinguish both.
struct ColumnEncoderV2 {
public:
    static void encode(
        const arcticdb::proto::encoding::VariantCodec &codec_opts,
        ColumnData& column_data,
        MutableVariantField variant_field,
        Buffer& out,
        std::ptrdiff_t& pos);
    static std::pair<size_t, size_t> max_compressed_size(
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        ColumnData& column_data);
private:
    template<typename TypeDescriptor>
    using Encoder = BlockEncoder<TypeDescriptor, EncodingVersion::V2>;
	using ShapesEncoder = Encoder<ShapesBlockTDT>;

	static void encode_shapes(
        const ColumnData& column_data,
        MutableVariantField variant_field,
        Buffer& out,
        std::ptrdiff_t& pos_in_buffer);
    static void encode_blocks(
        const arcticdb::proto::encoding::VariantCodec &codec_opts,
        ColumnData& column_data,
        MutableVariantField variant_field,
        Buffer& out,
        std::ptrdiff_t& pos);
};

template<EncodingVersion v>
using ColumnEncoder = std::conditional_t<v == EncodingVersion::V1, ColumnEncoderV1, ColumnEncoderV2>;

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
