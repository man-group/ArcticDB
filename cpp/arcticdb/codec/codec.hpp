/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/buffer.hpp>

#include <arcticdb/codec/segment.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/util/hash.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

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

//TODO  Provide tuning parameters to choose the encoding through encoding tags (also acting as config options)
template<template<typename> class F, class TD>
struct TypedBlockEncoderImpl;

template<template<typename> class F, class TD>
struct BlockEncoderHelper : public TypedBlockEncoderImpl<F, TD> {
    using FieldType = F<TD>;
    using TypeDescriptorTag = TD;
    using DimTag = typename TD::DimensionTag;
    using DataTag = typename TD::DataTypeTag;
};

size_t encode_bitmap(
    const util::BitMagic &sparse_map,
    Buffer &out,
    std::ptrdiff_t &pos);

template<typename TD>
using BlockEncoder = BlockEncoderHelper<TypedBlockData, TD>;

struct ColumnEncoder {
    static std::pair<size_t, size_t> max_compressed_size(
        const arcticdb::proto::encoding::VariantCodec &codec_opts,
        ColumnData &column_data);

    template <typename EncodedFieldType>
    void encode(
        const arcticdb::proto::encoding::VariantCodec &codec_opts,
        ColumnData &column_data,
        EncodedFieldType &field,
        Buffer &out,
        std::ptrdiff_t &pos) {
        column_data.type().visit_tag([&codec_opts, &column_data, &field, &out, &pos](auto type_desc_tag) {
            using TDT = decltype(type_desc_tag);
            using Encoder = BlockEncoder<TDT>;
            ARCTICDB_TRACE(log::codec(), "Column data has {} blocks", column_data.num_blocks());

            while (auto block = column_data.next<TDT>()) {
                if constexpr(!is_empty_type(TDT::DataTypeTag::data_type)) {
                    util::check(block.value().nbytes() > 0, "Zero-sized block");
                }
                Encoder::encode(codec_opts, block.value(), field, out, pos);
            }
        });

        if (column_data.bit_vector() != nullptr && column_data.bit_vector()->count() > 0)   {
            ARCTICDB_DEBUG(log::codec(), "Sparse map count = {} pos = {}", column_data.bit_vector()->count(), pos);
            auto sparse_bm_bytes = encode_bitmap(*column_data.bit_vector(), out, pos);
            field.mutable_ndarray()->set_sparse_map_bytes(static_cast<int>(sparse_bm_bytes));
        }
    }
};


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

SegmentInMemory decode_segment(
    Segment&& segment);

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
    std::optional<util::BitMagic>& bv);

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
