/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#ifndef ARCTICDB_SEGMENT_ENCODER_H_
#error "This should only be included by codec.hpp"
#endif

#include <arcticdb/codec/core.hpp>
#include <arcticdb/codec/encoding_sizes.hpp>
#include <arcticdb/codec/passthrough.hpp>
#include <arcticdb/codec/zstd.hpp>
#include <arcticdb/codec/lz4.hpp>
#include <arcticdb/codec/encoded_field.hpp>
//#include <arcticdb/codec/tp4.hpp>
#include <arcticdb/codec/slice_data_sink.hpp>

#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/util/bitset.hpp>
#include <arcticdb/entity/performance_tracing.hpp>

#include <arcticdb/util/buffer.hpp>
#include <arcticdb/util/hash.hpp>
#include <arcticdb/util/sparse_utils.hpp>


#include <google/protobuf/text_format.h>

#include <type_traits>

namespace arcticdb {

template<template<typename> class TypedBlock, class TD>
struct TypedBlockEncoderImpl {

    static size_t max_compressed_size(
        const arcticdb::proto::encoding::VariantCodec &codec_opts,
            const TypedBlock<TD> &typed_block) {
        switch (codec_opts.codec_case()) {
            case arcticdb::proto::encoding::VariantCodec::kZstd:
                return get_zstd_compressed_size(typed_block);
            case arcticdb::proto::encoding::VariantCodec::kLz4:
                return get_lz4_compressed_size(typed_block);
            case arcticdb::proto::encoding::VariantCodec::kPassthrough :
                return get_passthrough_compressed_size(typed_block);
            default:
                return get_passthrough_compressed_size(typed_block);
        }
    }
    /**
     * Perform encoding of in memory field for storage
     * @param f  view on the field data to encode. Can mutate underlying buffer
     * @param field description of the encoding operation
     * @param out output buffer to write the encoded values to. Must be resized if pos becomes > size
     * @param pos position in bytes in the buffer where to start writing.
     *          Modified to reflect the position after the last byte written
     * @param opt Option used to dispatch to the appropriate encoder and configure it
     */
    template <typename EncodedFieldType>
    static void encode(
            const arcticdb::proto::encoding::VariantCodec &codec_opts,
            TypedBlock<TD> &typed_block,
            EncodedFieldType &field,
            Buffer &out,
            std::ptrdiff_t &pos) {
        switch (codec_opts.codec_case()) {
            case arcticdb::proto::encoding::VariantCodec::kZstd:
                encode_zstd(codec_opts.zstd(), typed_block, field, out, pos);
                break;
            case arcticdb::proto::encoding::VariantCodec::kLz4:
                encode_lz4(codec_opts.lz4(), typed_block, field, out, pos);
                break;
            case arcticdb::proto::encoding::VariantCodec::kPassthrough :
                encode_passthrough(typed_block, field, out, pos);
                break;
//            case arcticdb::proto::encoding::VariantCodec::kTp4:
//                encode_tp4(codec_opts.tp4(), field, field, out, pos);
//                break;
            default:
                encode_passthrough(typed_block, field, out, pos);
        }
    }

    template <typename EncodedFieldType>
    static void encode_passthrough(
        TypedBlock<TD> &typed_block,
        EncodedFieldType &field,
        Buffer &out,
        std::ptrdiff_t &pos) {
        arcticdb::detail::PassthroughEncoder<TypedBlock, TD>::encode(typed_block, field, out, pos);
    }

    template <typename EncodedFieldType>
    static void encode_zstd(
        const arcticdb::proto::encoding::VariantCodec::Zstd &opts,
        TypedBlock<TD> &typed_block,
        EncodedFieldType& field,
        Buffer &out,
        std::ptrdiff_t &pos) {
        arcticdb::detail::ZstdEncoder<TypedBlock, TD>::encode(opts, typed_block, field, out, pos);
    }

    template <typename EncodedFieldType>
    static void encode_lz4(
        const arcticdb::proto::encoding::VariantCodec::Lz4 &opts,
        TypedBlock<TD> &typed_block,
        EncodedFieldType &field,
        Buffer &out,
        std::ptrdiff_t &pos) {
        arcticdb::detail::Lz4Encoder<TypedBlock, TD>::encode(opts, typed_block, field, out, pos);
    }

    static size_t get_passthrough_compressed_size(const TypedBlock<TD> &typed_block ) {
        return arcticdb::detail::PassthroughEncoder<TypedBlock, TD>::max_compressed_size(typed_block);
    }

    static size_t get_zstd_compressed_size(const TypedBlock<TD> &typed_block) {
        return arcticdb::detail::ZstdEncoder<TypedBlock, TD>::max_compressed_size(typed_block);
    }

    static size_t get_lz4_compressed_size(const TypedBlock<TD> &typed_block) {
        return arcticdb::detail::Lz4Encoder<TypedBlock, TD>::max_compressed_size(typed_block);
    }
};

template<typename T, typename BlockType>
void decode_block(const BlockType &block, const std::uint8_t *input, T *output) {
    ARCTICDB_SUBSAMPLE_AGG(DecodeBlock)
    std::size_t size_to_decode = block.out_bytes();
    std::size_t decoded_size = block.in_bytes();

    if (!block.has_codec()) {
        arcticdb::detail::PassthroughDecoder::decode_block<T>(input, size_to_decode, output, decoded_size);
    } else {
        std::uint32_t encoder_version = block.encoder_version();
        switch (block.codec().codec_case()) {
            case arcticdb::proto::encoding::VariantCodec::kZstd:
                arcticdb::detail::ZstdDecoder::decode_block<T>(encoder_version,
                                                     input,
                                                     size_to_decode,
                                                     output,
                                                     decoded_size);
                break;
            case arcticdb::proto::encoding::VariantCodec::kLz4:
                arcticdb::detail::Lz4Decoder::decode_block<T>(encoder_version,
                                                    input,
                                                    size_to_decode,
                                                    output,
                                                    decoded_size);
                break;
            default:
                util::raise_error_msg("Unsupported block codec {}", block);
        }
    }
}

template<class DataSink, typename NDArrayEncodedFieldType>
std::size_t decode_ndarray(
    const TypeDescriptor &td,
    const NDArrayEncodedFieldType &field,
    const std::uint8_t *input,
    DataSink &data_sink,
    std::optional<util::BitMagic>& bv) {
    ARCTICDB_SUBSAMPLE_AGG(DecodeNdArray)
    std::size_t read_bytes = 0;
    td.visit_tag([&](auto type_desc_tag) {
        using TD = std::decay_t<decltype(type_desc_tag)>;
        using T = typename TD::DataTypeTag::raw_type;

        auto shape_size = encoding_sizes::shape_uncompressed_size(field);
        shape_t *shapes_out = nullptr;
        if(shape_size > 0) {
            shapes_out = data_sink.allocate_shapes(shape_size);
            util::check(td.dimension() == Dimension::Dim0 || field.shapes_size() == field.values_size(),
                        "Mismatched field and value sizes: {} != {}", field.shapes_size(), field.values_size());
        }

        auto data_size = encoding_sizes::data_uncompressed_size(field);
        auto data_begin = static_cast<uint8_t *>(data_sink.allocate_data(data_size));
        util::check(data_begin != nullptr, "Failed to allocate data of size {}", data_size);
        auto data_out = data_begin;
        auto data_in = input;
        auto num_blocks = field.values_size();

        ARCTICDB_TRACE(log::codec(), "Decoding ndarray with type {}, uncompressing {} ({}) bytes in {} blocks",
            td, data_size, encoding_sizes::ndarray_field_compressed_size(field), num_blocks);

        for (auto block_num = 0; block_num < num_blocks; ++block_num) {
            if (td.dimension() != Dimension::Dim0) {
                const auto& shape = field.shapes(block_num);
                decode_block<shape_t>(shape, data_in, shapes_out);
                data_in += shape.out_bytes();
                shapes_out += shape.in_bytes() / sizeof(shape_t);
                data_sink.advance_shapes(shape.in_bytes());
            }

            const auto& block_info = field.values(block_num);
            ARCTICDB_TRACE(log::codec(), "Decoding block {} at pos {}", block_num, data_in - input);
            size_t block_inflated_size;
            decode_block<T>(block_info, data_in, reinterpret_cast<T *>(data_out));
            block_inflated_size = block_info.in_bytes();
            data_out += block_inflated_size;
            data_sink.advance_data(block_inflated_size);
            data_in += block_info.out_bytes();
        }

        if(field.sparse_map_bytes()) {
            util::check_magic<util::BitMagicStart>(data_in);
            const auto bitmap_size =   field.sparse_map_bytes() - (sizeof(util::BitMagicStart) + sizeof(util::BitMagicEnd)); //TODO functions
            bv = util::deserialize_bytes_to_bitmap(data_in, bitmap_size);
            util::check_magic<util::BitMagicEnd>(data_in);
            data_sink.set_allow_sparse(true);
        }

        read_bytes = encoding_sizes::ndarray_field_compressed_size(field);
        util::check(data_in - input == intptr_t(read_bytes),
                    "Decoding compressed size mismatch, expected decode size {} to equal total size {}", data_in - input ,
                    read_bytes);

        util::check(data_out - data_begin == intptr_t(data_size),
                    "Decoding uncompressed size mismatch, expected position {} to be equal to data size {}",
                    data_out - data_begin, data_size);
    });
    return read_bytes;
}

template<class DataSink, typename EncodedFieldType>
std::size_t decode_field(
    const TypeDescriptor &td,
    const EncodedFieldType &field,
    const std::uint8_t *input,
    DataSink &data_sink,
    std::optional<util::BitMagic>& bv) {
    size_t magic_size = 0u;
    if constexpr(std::is_same_v<EncodedFieldType, arcticdb::EncodedField>) {
        magic_size += sizeof(ColumnMagic);
        check_magic<ColumnMagic>(input);
    }

    switch (field.encoding_case()) {
        case EncodedFieldType::kNdarray:
            return decode_ndarray(td, field.ndarray(), input, data_sink, bv) + magic_size;
        default:
            util::raise_error_msg("Unsupported encoding {}", field);
    }
}

} // namespace arcticdb