#pragma once

#include <cstddef>

#include <arcticdb/util/buffer.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/hash.hpp>
#include <arcticdb/codec/compression/encoding_scan_result.hpp>
#include <arcticdb/codec/compression/delta.hpp>
#include <arcticdb/codec/compression/ffor.hpp>
#include <arcticdb/codec/compression/frequency.hpp>
#include <arcticdb/codec/compression/bitpack.hpp>
#include <arcticdb/codec/compression/plain.hpp>
#include <arcticdb/codec/compression/alp.hpp>
#include <arcticdb/codec/compression/constant.hpp>
#include <arcticdb/codec/core.hpp>

namespace arcticdb {

inline EncodedBlock block_from_scan_result(const EncodingScanResult &scan_result, bool is_shape) {
    EncodedBlock block;

    block.in_bytes_ = static_cast<uint32_t>(scan_result.original_size_);
    block.out_bytes_ = static_cast<uint32_t>(scan_result.estimated_size_);
    block.encoder_version_ = 2;
    block.is_shape_ = is_shape;

    BlockCodecImpl codec;
    auto adaptive_codec = codec.mutable_adaptive();
    adaptive_codec->encoding_type_ = scan_result.type_;
    block.codec_ = codec;
    return block;
}

template<template<typename> class BlockType, class TD>
struct AdaptiveEncoder {
    static void encode_shapes(
        const BlockType<TD> &block,
        Buffer &out,
        std::ptrdiff_t &pos,
        EncodedBlock &output_block,
        const EncodingScanResult &result) {
        using namespace arcticdb::entity;
        using CodecHelperType = arcticdb::detail::CodecHelper<TD>;
        using T = typename CodecHelperType::T;
        CodecHelperType helper;
        const T *data = block.data();
        const size_t data_byte_size = block.nbytes();
        helper.ensure_buffer(out, pos, data_byte_size);
        if constexpr (std::is_unsigned_v<T>) {
            switch (result.type_) {
            case EncodingType::FFOR: {
                FForCompressor<T> ffor(std::get<FFORCompressData>(result.data_));
                pos += ffor.compress_shapes(
                    data,
                    reinterpret_cast<T *>(out.data() + pos),
                    result.estimated_size_);
            }
            break;
            case EncodingType::PLAIN: {
                pos += PlainCompressor<T>::compress_shapes(
                    data,
                    reinterpret_cast<T *>(out.data() + pos),
                    result.estimated_size_);
            }
            break;
            default:
                util::raise_rte("Unknown encoding type in adaptive integer encoding: {}", static_cast<uint16_t>(result.type_));
            }
        } else {
            util::raise_rte("Unexpected type in shapes encoding");
        }
        output_block.set_hash(helper.hasher().digest());
    }

    static void encode_data(
        ColumnData column_data,
        Buffer &out,
        std::ptrdiff_t &pos,
        EncodedFieldImpl &field,
        EncodingScanResult &result) {
        using T = TD::DataTypeTag::raw_type;
        auto output_block = field.mutable_ndarray()->add_values(EncodingVersion::V2);
        *output_block = block_from_scan_result(result, false);
        if constexpr (std::is_integral_v<T>) {
            switch (result.type_) {
            case EncodingType::CONSTANT: {
                pos += ConstantCompressor<T>::compress(
                   column_data,
                   reinterpret_cast<T *>(out.data() + pos),
                   result.estimated_size_);
            }
            break;
            case EncodingType::DELTA: {
                DeltaCompressor<T> delta(std::get<DeltaCompressData>(result.data_));
                pos += delta.compress(
                    column_data,
                    reinterpret_cast<T *>(out.data() + pos),
                    result.estimated_size_);
            }
            break;
            case EncodingType::FFOR: {
                FForCompressor<T> ffor(std::get<FFORCompressData>(result.data_));
                pos += ffor.compress(
                    column_data,
                    reinterpret_cast<T *>(out.data() + pos),
                    result.estimated_size_);
            }
            break;
            case EncodingType::BITPACK: {
                BitPackCompressor<T> bitpack(std::get<BitPackData>(result.data_));
                pos += bitpack.compress(
                    column_data,
                    reinterpret_cast<T *>(out.data() + pos),
                    result.estimated_size_);
            }
            break;
            case EncodingType::PLAIN: {
                pos += PlainCompressor<T>::compress(
                    column_data,
                    reinterpret_cast<T *>(out.data() + pos),
                    result.estimated_size_);
            }
            break;
            case EncodingType::FREQUENCY: {
                FrequencyCompressor<T> frequency(std::get<FrequencyEncodingData>(result.data_));
                const auto bytes = frequency.compress(
                    column_data,
                    result.original_size_ / sizeof(T),
                    out.data() + pos,
                    result.estimated_size_);

                output_block->set_out_bytes(bytes);
                pos += bytes;
            }
            break;
            default:
                util::raise_rte("Unknown encoding type in adaptive integer encoding: {}", static_cast<uint16_t>(result.type_));
            }
        } else if constexpr (std::is_floating_point_v<T>) {
            switch (result.type_) {
            case EncodingType::CONSTANT: {
                pos += ConstantCompressor<T>::compress(
                    column_data,
                    reinterpret_cast<T *>(out.data() + pos),
                    result.estimated_size_);
            }
            break;
            case EncodingType::ALP: {
                util::check(std::holds_alternative<ALPCompressData<T>>(result.data_), "Expected ALP compress data in ALP compression");
                ALPCompressor<T> compressor{std::move(std::get<ALPCompressData<T>>(result.data_))};
                const auto bytes = compressor.compress(
                    column_data,
                    reinterpret_cast<T *>(out.data() + pos),
                    result.estimated_size_);

                output_block->set_out_bytes(bytes);
                pos += bytes;
            }
            break;
            case EncodingType::PLAIN: {
                pos += PlainCompressor<T>::compress(
                    column_data,
                    reinterpret_cast<T *>(out.data() + pos),
                    result.estimated_size_);
            }
            break;
            case EncodingType::FREQUENCY: {
                FrequencyCompressor<T> frequency(std::get<FrequencyEncodingData>(result.data_));
                const auto bytes = frequency.compress(
                    column_data,
                    result.original_size_ / sizeof(T),
                    out.data() + pos,
                    result.estimated_size_);

                output_block->set_out_bytes(bytes);
                pos += bytes;
            }
            break;
            default:
                util::raise_rte("Unknown encoding type in adaptive floating-point encoding: {}", static_cast<uint16_t>(result.type_));
            }
        } else {
            util::raise_rte("Unhandled type in adaptive encoding");
        }
    }
};

inline void check_result(EncodingType type, const DecompressResult &result, size_t in_bytes, size_t out_bytes) {
    util::check(result.uncompressed_ == out_bytes,
                "Uncompressed size mismatch in {} decoder: {} != {}",
                static_cast<uint16_t>(type),
                result.uncompressed_,
                out_bytes);

    util::check(result.compressed_ == in_bytes,
                "Compressed size mismatch in {} decoder: {} != {}",
                static_cast<uint16_t>(type),
                result.compressed_,
                in_bytes);
}

struct AdaptiveDecoder {
    template<typename T>
    static void decode_block(
        const EncodedBlock &block,
        std::uint32_t,
        const std::uint8_t *in,
        std::size_t in_bytes,
        T *out,
        std::size_t out_bytes) {
        auto type = block.codec().adaptive().encoding_type_;
        const auto *t_in = reinterpret_cast<const T *>(in);
        if constexpr (std::is_integral_v<T>) {
                switch (type) {
                case EncodingType::FFOR: {
                    auto result = FForDecompressor<T>::decompress(t_in, out);
                    check_result(type, result, in_bytes, out_bytes);
                }
                break;
                case EncodingType::DELTA: {
                    DeltaDecompressor<T> decompressor;
                    decompressor.init(t_in);
                    auto result = decompressor.decompress(t_in, out);
                    check_result(type, result, in_bytes, out_bytes);
                }
                break;
                case EncodingType::BITPACK: {
                    auto result = BitPackDecompressor<T>::decompress(t_in, out);
                    check_result(type, result, in_bytes, out_bytes);
                }
                break;
                case EncodingType::FREQUENCY: {
                    auto result = FrequencyDecompressor<T>::decompress(in, out);
                    check_result(type, result, in_bytes, out_bytes);
                }
                break;
                case EncodingType::CONSTANT: {
                    auto result = ConstantDecompressor<T>::decompress(in, out);
                    check_result(type, result, in_bytes, out_bytes);
                }
                break;
                case EncodingType::PLAIN: {
                    auto result = PlainDecompressor<T>::decompress(in, out);
                    check_result(type, result, in_bytes, out_bytes);
                }
                break;
                default:util::raise_rte("Unknown encoding type: {}", static_cast<uint16_t>(type));
                }
        } else if constexpr (std::is_floating_point_v<T>) {
            switch (type) {
            case EncodingType::FREQUENCY: {
                auto result = FrequencyDecompressor<T>::decompress(in, out);
                check_result(type, result, in_bytes, out_bytes);
            }
            break;
            case EncodingType::CONSTANT: {
                auto result = ConstantDecompressor<T>::decompress(in, out);
                check_result(type, result, in_bytes, out_bytes);
            }
            break;
            case EncodingType::ALP: {
                ALPDecompressor<T> decompressor;
                decompressor.init(t_in);
                auto result = decompressor.decompress(t_in, out);
                check_result(type, result, in_bytes, out_bytes);
            }
            break;
            case EncodingType::PLAIN: {
                auto result = PlainDecompressor<T>::decompress(in, out);
                check_result(type, result, in_bytes, out_bytes);
            }
            break;
            default:util::raise_rte("Unknown encoding type: {}", static_cast<uint16_t>(type));
            }
        } else {
            util::raise_rte("Unknown type in decoding");
        }
    }
};

} //namespace arcticdb