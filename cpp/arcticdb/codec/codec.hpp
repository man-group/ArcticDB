/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
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

template<typename TD>
using BlockEncoder = BlockEncoderHelper<TypedBlockData, TD>;

struct ColumnEncoder {
    static std::pair<size_t, size_t> max_compressed_size(
        const arcticdb::proto::encoding::VariantCodec &codec_opts,
        ColumnData &column_data);

    static void encode(
        const arcticdb::proto::encoding::VariantCodec &codec_opts,
        ColumnData &f,
        arcticdb::proto::encoding::EncodedField &field,
        Buffer &out,
        std::ptrdiff_t &pos);
};

size_t encode_bitmap(
    const util::BitMagic &sparse_map,
    Buffer &out,
    std::ptrdiff_t &pos);

Segment encode(
    SegmentInMemory&& in_mem_seg,
    const arcticdb::proto::encoding::VariantCodec &codec_opts);

SegmentInMemory decode(
    Segment&& segment);

void decode(
    const Segment& segment,
    const arcticdb::proto::encoding::SegmentHeader& hdr,
    SegmentInMemory& res,
    const StreamDescriptor::Proto& desc);

template<class DS>
std::size_t decode(
    const TypeDescriptor &td,
    const arcticdb::proto::encoding::EncodedField &field,
    const uint8_t *input,
    DS &data_sink,
    std::optional<util::BitMagic>& bv);

std::optional<google::protobuf::Any> decode_metadata(
    const Segment& segment);

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
#include <arcticdb/codec/codec-inl.hpp>
