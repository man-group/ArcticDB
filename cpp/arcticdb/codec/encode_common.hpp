/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

/// @file encode_common.hpp Functions and classes used by both V1 and V2 encodings

#pragma once

#include <arcticdb/codec/segment.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/codec/default_codecs.hpp>

#include <cstddef>

namespace arcticdb {

/// @brief This should be the block data type descriptor when the shapes array is encoded as a block
using ShapesBlockTDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension::Dim0>>;

using ByteArrayTDT = TypeDescriptorTag<DataTypeTag<DataType::UINT8>, DimensionTag<Dimension::Dim1>>;

template<template<typename> class TypedBlock, class TD, EncodingVersion encoder_version>
struct TypedBlockEncoderImpl;

template<EncodingVersion v, typename VersionedColumnEncoder>
struct EncodingPolicyType {
    static constexpr EncodingVersion version = v;
    using ColumnEncoder = VersionedColumnEncoder;
};

template<typename EncodingPolicyType>
size_t calc_num_blocks(const ColumnData& column_data) {
    if constexpr (EncodingPolicyType::version == EncodingVersion::V1)
        return column_data.num_blocks() + (column_data.num_blocks() * !column_data.shapes()->empty());
    else
        return column_data.num_blocks() + !column_data.shapes()->empty();
}

template<typename EncodingPolicyType>
struct BytesEncoder {
    using Encoder = TypedBlockEncoderImpl<TypedBlockData, ByteArrayTDT, EncodingPolicyType::version>;
    using BytesBlock = TypedBlockData<ByteArrayTDT>;
    using ShapesEncoder = TypedBlockEncoderImpl<TypedBlockData, ShapesBlockTDT, EncodingPolicyType::version>;

    template<typename EncodedFieldType>
    static void encode(
        const ChunkedBuffer &data,
        const arcticdb::proto::encoding::VariantCodec &codec_opts,
        Buffer &out_buffer,
        std::ptrdiff_t &pos,
        EncodedFieldType& encoded_field
    ) {
        if constexpr (EncodingPolicyType::version == EncodingVersion::V1) {
            const auto bytes_count = static_cast<shape_t>(data.bytes());
            auto typed_block = BytesBlock(
                data.data(),
                &bytes_count,
                bytes_count,
                1u,
                data.block_and_offset(0).block_);
            Encoder::encode(codec_opts, typed_block, encoded_field, out_buffer, pos);
        } else if constexpr (EncodingPolicyType::version == EncodingVersion::V2) {
            const shape_t row_count = 1;  // BytesEncoder data is stored as an array with a single row
            const auto shapes_data = static_cast<ShapesBlockTDT::DataTypeTag::raw_type>(data.bytes());
            auto shapes_block = TypedBlockData<ShapesBlockTDT>(&shapes_data,
                                                               nullptr,
                                                               sizeof(shape_t),
                                                               row_count,
                                                               data.block_and_offset(0).block_);
            const auto bytes_count = static_cast<shape_t>(data.bytes());
            auto data_block = BytesBlock(data.data(),
                                         &bytes_count,
                                         static_cast<shape_t>(bytes_count),
                                         row_count,
                                         data.block_and_offset(0).block_);
            ShapesEncoder::encode_shapes(codec::default_shapes_codec(), shapes_block, encoded_field, out_buffer, pos);
            Encoder::encode_values(codec_opts, data_block, encoded_field, out_buffer, pos);
            auto *field_nd_array = encoded_field.mutable_ndarray();
            const auto total_items_count = field_nd_array->items_count() + row_count;
            field_nd_array->set_items_count(total_items_count);
        } else {
            static_assert(std::is_same_v<decltype(EncodingPolicyType::version), void>, "Unknown encoding version");
        }
    }

    static size_t max_compressed_size(const arcticdb::proto::encoding::VariantCodec &codec_opts, shape_t data_size) {
        const shape_t shapes_bytes = sizeof(shape_t);
        const auto values_block = BytesBlock(data_size, &data_size);
        if constexpr (EncodingPolicyType::version == EncodingVersion::V1) {
            const auto shapes_block = BytesBlock(shapes_bytes, &shapes_bytes);
            return Encoder::max_compressed_size(codec_opts, values_block) +
                Encoder::max_compressed_size(codec_opts, shapes_block);
        } else if constexpr (EncodingPolicyType::version == EncodingVersion::V2) {
            const auto shapes_block = TypedBlockData<ShapesBlockTDT>(shapes_bytes, &shapes_bytes);
            return Encoder::max_compressed_size(codec_opts, values_block) +
                ShapesEncoder::max_compressed_size(codec::default_shapes_codec(), shapes_block);
        } else {
            static_assert(std::is_same_v<decltype(EncodingPolicyType::version), void>, "Unknown encoding version");
        }
    }

    static size_t num_encoded_blocks(const ChunkedBuffer& buffer) {
        return buffer.num_blocks() + 1;
    }
};

struct SizeResult {
    size_t max_compressed_bytes_;
    size_t uncompressed_bytes_;
    shape_t encoded_blocks_bytes_;
};

template<typename EncodingPolicyType>
void calc_metadata_size(
        const SegmentInMemory &in_mem_seg,
        const arcticdb::proto::encoding::VariantCodec &codec_opts,
        SizeResult &result) {
    if (in_mem_seg.metadata()) {
        const auto metadata_bytes = static_cast<shape_t>(in_mem_seg.metadata()->ByteSizeLong());
        result.uncompressed_bytes_ += metadata_bytes + sizeof(shape_t);
        result.max_compressed_bytes_ +=
            BytesEncoder<EncodingPolicyType>::max_compressed_size(codec_opts, metadata_bytes);
        ARCTICDB_TRACE(log::codec(), "Metadata requires {} max_compressed_bytes", result.max_compressed_bytes_);
    }
}

template<typename EncodingPolicyType>
void calc_columns_size(
    const SegmentInMemory &in_mem_seg,
    const arcticdb::proto::encoding::VariantCodec &codec_opts,
    SizeResult &result
) {
    for (std::size_t c = 0; c < in_mem_seg.num_columns(); ++c) {
        auto column_data = in_mem_seg.column_data(c);
        const auto [uncompressed, required] = EncodingPolicyType::ColumnEncoder::max_compressed_size(codec_opts,
                                                                                                     column_data);
        result.uncompressed_bytes_ += uncompressed;
        result.max_compressed_bytes_ += required;
        ARCTICDB_TRACE(log::codec(),
                       "Column {} requires {} max_compressed_bytes, total {}",
                       c,
                       required,
                       result.max_compressed_bytes_);
    }
}

template<typename EncodingPolicyType>
void calc_string_pool_size(
    const SegmentInMemory &in_mem_seg,
    const arcticdb::proto::encoding::VariantCodec &codec_opts,
    SizeResult &result
) {
    if (in_mem_seg.has_string_pool()) {
        auto string_col = in_mem_seg.string_pool_data();
        const auto [uncompressed, required] = EncodingPolicyType::ColumnEncoder::max_compressed_size(codec_opts,
                                                                                                     string_col);
        result.uncompressed_bytes_ += uncompressed;
        result.max_compressed_bytes_ += required;
        ARCTICDB_TRACE(log::codec(),
                       "String pool requires {} max_compressed_bytes, total {}",
                       required,
                       result.max_compressed_bytes_);
    }
}

template<typename EncodingPolicyType>
void encode_metadata(
        const SegmentInMemory& in_mem_seg,
        SegmentHeader& segment_header,
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        Buffer &out_buffer,
        std::ptrdiff_t& pos) {
    if (in_mem_seg.metadata()) {
        const auto bytes_count = static_cast<shape_t>(in_mem_seg.metadata()->ByteSizeLong());
        ARCTICDB_TRACE(log::codec(), "Encoding {} bytes of metadata", bytes_count);
        constexpr int max_stack_alloc = 1 << 11;
        bool malloced{false};
        uint8_t* meta_ptr;
        if (bytes_count > max_stack_alloc) {
            meta_ptr = reinterpret_cast<uint8_t*>(malloc(bytes_count));
            malloced = true;
        } else {
            meta_ptr = reinterpret_cast<uint8_t*>(alloca(bytes_count));
        }
        ChunkedBuffer meta_buffer;
        meta_buffer.add_external_block(meta_ptr, bytes_count);
        const auto num_encoded_fields = BytesEncoder<EncodingPolicyType>::num_encoded_blocks(meta_buffer);
        auto& encoded_field = segment_header.mutable_metadata_field(num_encoded_fields);
        google::protobuf::io::ArrayOutputStream aos(&meta_buffer[0], static_cast<int>(bytes_count));
        in_mem_seg.metadata()->SerializeToZeroCopyStream(&aos);
        ARCTICDB_TRACE(log::codec(), "Encoding metadata to position {}", pos);
        BytesEncoder<EncodingPolicyType>::encode(meta_buffer, codec_opts, out_buffer, pos, encoded_field);
        ARCTICDB_TRACE(log::codec(), "Encoded metadata to position {}", pos);
        if (malloced)
            free(meta_ptr);
    } else {
        ARCTICDB_TRACE(log::codec(), "Not encoding any metadata");
    }
}

template<typename EncodingPolicyType>
void encode_string_pool(
    const SegmentInMemory &in_mem_seg,
    SegmentHeader &segment_header,
    const arcticdb::proto::encoding::VariantCodec &codec_opts,
    Buffer &out_buffer,
    std::ptrdiff_t &pos
) {
    if (in_mem_seg.has_string_pool()) {
        ARCTICDB_TRACE(log::codec(), "Encoding string pool to position {}", pos);
        auto col = in_mem_seg.string_pool_data();
        auto& encoded_field = segment_header.mutable_string_pool_field(calc_num_blocks<EncodingPolicyType>(col));
        EncodingPolicyType::ColumnEncoder::encode(codec_opts, col, encoded_field, out_buffer, pos);
        ARCTICDB_TRACE(log::codec(), "Encoded string pool to position {}", pos);
    }
}

[[nodiscard]] SizeResult max_compressed_size_v1(
    const SegmentInMemory &in_mem_seg,
    const arcticdb::proto::encoding::VariantCodec &codec_opts);

[[nodiscard]] SizeResult max_compressed_size_v2(
    const SegmentInMemory &in_mem_seg,
    const arcticdb::proto::encoding::VariantCodec &codec_opts);

} //namespace arcticdb