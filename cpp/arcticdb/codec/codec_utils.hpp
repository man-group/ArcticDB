/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/codec/segment.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

#include <cstddef>

namespace arcticdb {

    /// @brief This should be the block data type descriptor when the shapes array is encoded as a block
    using ShapesBlockTDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension::Dim0>>;
    using ByteArrayTDT = TypeDescriptorTag<DataTypeTag<DataType::UINT8>, DimensionTag<Dimension::Dim1>>;


    //TODO  Provide tuning parameters to choose the encoding through encoding tags (also acting as config options)
    template<template<typename> class TypedBlock, class TD, EncodingVersion encoder_version>
    struct TypedBlockEncoderImpl;

    /// TODO: Remove forward declaration
    arcticdb::proto::encoding::VariantCodec shapes_encoding_opts();

    template<EncodingVersion v>
    struct BytesEncoder {
        using Encoder = TypedBlockEncoderImpl<TypedBlockData, ByteArrayTDT, v>;
        using BytesBlock = TypedBlockData<ByteArrayTDT>;
        using ShapesEncoder = TypedBlockEncoderImpl<TypedBlockData, ShapesBlockTDT, v>;

        template<typename EncodedFieldType>
        static void encode(
            const ChunkedBuffer& data,
            const arcticdb::proto::encoding::VariantCodec& codec_opts,
            Buffer& out_buffer,
            std::ptrdiff_t& pos,
            EncodedFieldType* encoded_field
        ) {
            const shape_t bytes_count = static_cast<shape_t>(data.bytes());
            if constexpr(v == EncodingVersion::V1) {
                auto typed_block = BytesBlock(
                    data.data(),
                    &bytes_count,
                    bytes_count,
                    1u,
                    data.block_and_offset(0).block_);
                Encoder::encode(codec_opts, typed_block, *encoded_field, out_buffer, pos);
            } else if constexpr(v == EncodingVersion::V2) {
                const size_t row_count = 1;
                auto shapes_block = TypedBlockData<ShapesBlockTDT>(&bytes_count,
                    nullptr,
                    sizeof(shape_t),
                    row_count,
                    data.block_and_offset(0).block_);
                auto data_block = BytesBlock(data.data(),
                    &bytes_count,
                    bytes_count,
                    row_count,
                    data.block_and_offset(0).block_);
                ShapesEncoder::encode_shapes(shapes_encoding_opts(), shapes_block, *encoded_field, out_buffer, pos);
                Encoder::encode_values(codec_opts, data_block, *encoded_field, out_buffer, pos);
                auto* field_nd_array = encoded_field->mutable_ndarray();
                const auto total_items_count = field_nd_array->items_count() + row_count;
                field_nd_array->set_items_count(total_items_count);
            } else {
                static_assert(std::is_same_v<decltype(v), void>, "Unknown encoding version");
            }
        }

        static size_t
        max_compressed_size(const arcticdb::proto::encoding::VariantCodec& codec_opts, shape_t data_size) {
            const shape_t shapes_bytes = sizeof(shape_t);
            const auto values_block = BytesBlock(data_size, &data_size);
            if constexpr(v == EncodingVersion::V1) {
                const auto shapes_block = BytesBlock(shapes_bytes, &shapes_bytes);
                return Encoder::max_compressed_size(codec_opts, values_block) +
                       Encoder::max_compressed_size(codec_opts, shapes_block);
            } else if constexpr(v == EncodingVersion::V2) {
                const auto shapes_block = TypedBlockData<ShapesBlockTDT>(shapes_bytes, &shapes_bytes);
                return Encoder::max_compressed_size(codec_opts, values_block) +
                       ShapesEncoder::max_compressed_size(shapes_encoding_opts(), shapes_block);
            } else {
                static_assert(std::is_same_v<decltype(v), void>, "Unknown encoding version");
            }
        }
    };

    struct SizeResult {
        size_t max_compressed_bytes_;
        size_t uncompressed_bytes_;
        shape_t encoded_blocks_bytes_;
    };

    template<EncodingVersion v>
    void calc_metadata_size(
        const SegmentInMemory& in_mem_seg,
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        SizeResult& result
    ) {
        if(in_mem_seg.metadata()) {
            const auto metadata_bytes = static_cast<shape_t>(in_mem_seg.metadata()->ByteSizeLong());
            result.uncompressed_bytes_ += metadata_bytes + sizeof(shape_t);
            result.max_compressed_bytes_ += BytesEncoder<v>::max_compressed_size(codec_opts, metadata_bytes);
            ARCTICDB_TRACE(log::codec(), "Metadata requires {} max_compressed_bytes", result.max_compressed_bytes_);
        }
    }

    template<typename ColumnEncoder>
    void calc_columns_size(
        const SegmentInMemory& in_mem_seg,
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        SizeResult& result
    ) {
        ColumnEncoder encoder;
        for (std::size_t c = 0; c < in_mem_seg.num_columns(); ++c) {
            auto column_data = in_mem_seg.column_data(c);
            const auto [uncompressed, required] = encoder.max_compressed_size(codec_opts, column_data);
            result.uncompressed_bytes_ += uncompressed;
            result.max_compressed_bytes_ += required;
            ARCTICDB_TRACE(log::codec(),
                "Column {} requires {} max_compressed_bytes, total {}",
                c,
                required,
                result.max_compressed_bytes_);
        }
    }

    template<typename ColumnEncoder>
    void calc_string_pool_size(
        const SegmentInMemory& in_mem_seg,
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        SizeResult& result
    ) {
        if(in_mem_seg.has_string_pool()) {
            ColumnEncoder encoder;
            auto string_col = in_mem_seg.string_pool_data();
            const auto [uncompressed, required] = encoder.max_compressed_size(codec_opts, string_col);
            result.uncompressed_bytes_ += uncompressed;
            result.max_compressed_bytes_ += required;
            ARCTICDB_TRACE(log::codec(),
                "String pool requires {} max_compressed_bytes, total {}",
                required,
                result.max_compressed_bytes_);
        }
    }

    template<EncodingVersion v>
    void encode_metadata(
        const SegmentInMemory& in_mem_seg,
        arcticdb::proto::encoding::SegmentHeader& segment_header,
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        Buffer& out_buffer,
        std::ptrdiff_t& pos
    ) {
        if(in_mem_seg.metadata()) {
            const auto bytes_count = static_cast<shape_t>(in_mem_seg.metadata()->ByteSizeLong());
            ARCTICDB_TRACE(log::codec(), "Encoding {} bytes of metadata", bytes_count);
            auto encoded_field = segment_header.mutable_metadata_field();

            constexpr int max_stack_alloc = 1 << 11;
            bool malloced{false};
            uint8_t* meta_ptr{nullptr};
            if(bytes_count > max_stack_alloc) {
                meta_ptr = reinterpret_cast<uint8_t*>(malloc(bytes_count));
                malloced = true;
            } else {
                meta_ptr = reinterpret_cast<uint8_t*>(alloca(bytes_count));
            }
            ChunkedBuffer meta_buffer;
            meta_buffer.add_external_block(meta_ptr, bytes_count, 0u);
            google::protobuf::io::ArrayOutputStream aos(&meta_buffer[0], static_cast<int>(bytes_count));
            in_mem_seg.metadata()->SerializeToZeroCopyStream(&aos);
            BytesEncoder<v>::encode(meta_buffer, codec_opts, out_buffer, pos, encoded_field);
            ARCTICDB_DEBUG(log::codec(), "Encoded metadata to position {}", pos);
            if(malloced)
                free(meta_ptr);
        } else {
            ARCTICDB_DEBUG(log::codec(), "Not encoding any metadata");
        }
    }

    template<typename ColumnEncoder>
    void encode_string_pool(
        const SegmentInMemory& in_mem_seg,
        arcticdb::proto::encoding::SegmentHeader& segment_header,
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        Buffer& out_buffer,
        std::ptrdiff_t& pos
    ) {
        if(in_mem_seg.has_string_pool()) {
            ColumnEncoder encoder;
            ARCTICDB_TRACE(log::codec(), "Encoding string pool to position {}", pos);
            auto* encoded_field = segment_header.mutable_string_pool_field();
            auto col = in_mem_seg.string_pool_data();
            encoder.encode(codec_opts, col, encoded_field, out_buffer, pos);
            ARCTICDB_TRACE(log::codec(), "Encoded string pool to position {}", pos);
        }
    }
}
