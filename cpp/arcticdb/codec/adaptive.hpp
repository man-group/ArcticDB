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

namespace arcticdb {

inline EncodedBlock block_from_scan_result(const EncodingScanResult& scan_result, bool is_shape) {
    EncodedBlock block;

    block.in_bytes_  = static_cast<uint32_t>(scan_result.original_size_);
    block.out_bytes_ = static_cast<uint32_t>(scan_result.estimated_size_);
    block.encoder_version_ = 1;
    block.is_shape_ = is_shape;

    BlockCodecImpl codec;
    auto adaptive_codec = codec.mutable_adaptive();
    adaptive_codec->encoding_type_ = scan_result.type_;
    switch(scan_result.type_) {
    case EncodingType::PLAIN:
        break;

    case EncodingType::FFOR: {
        util::check(std::holds_alternative<FFORCompressData>(scan_result.data_), "Expected FFOR compress data for codec type");
        const auto &ffor = std::get<FFORCompressData>(scan_result.data_);
        memcpy(codec.data_.data(), &ffor, sizeof(ffor));
        break;
    }
    case EncodingType::DELTA: {
        util::check(std::holds_alternative<DeltaCompressData>(scan_result.data_),
                    "Expected delta compress data for codec type");
        const auto &delta = std::get<DeltaCompressData>(scan_result.data_);
        memcpy(codec.data_.data(), &delta, std::min(BlockCodec::DataSize, sizeof(delta)));
        break;
    }
    default:
        codec.type_ = Codec::UNKNOWN;
        break;
    }

    block.codec_ = codec;
    return block;
}
/*
template<template<typename> class BlockType, class TD>
struct AdaptiveEncoderV1 {
    using Opts = AdaptiveCodec;

    static size_t max_compressed_size(const BlockType<TD> & ) {
        util::raise_rte("Adaptive encoding not supported with V1 format");
    }

    template <typename EncodedFieldType>
    static void encode(
        const Opts&,
        const BlockType<TD>&,
        EncodedFieldType&,
        Buffer&,
        std::ptrdiff_t&) {
            util::raise_rte("Adaptive encoding not supported with V1 format");
    }
};
*/

template<template<typename> class BlockType, class TD>
struct AdaptiveEncoder {
    static void encode_shapes(
        const BlockType<TD> &block,
        Buffer &out,
        std::ptrdiff_t &pos,
        EncodedBlock& output_block,
        const EncodingScanResult& result) {
        using namespace arcticdb::entity;
        using CodecHelperType = arcticdb::detail::CodecHelper<TD>;
        using T = typename CodecHelperType::T;
        CodecHelperType helper;
        const T* data = block.data();
        const size_t data_byte_size = block.nbytes();
        helper.ensure_buffer(out, pos, data_byte_size);

        T *t_out = out.ptr_cast<T>(pos, data_byte_size);
        encode_block(data, data_byte_size, helper.hasher(), t_out, pos, output_block, result);
        output_block.set_hash(helper.hasher().digest());
    }

    static void encode_data(
            ColumnData column_data,
            Buffer &out,
            std::ptrdiff_t &pos,
            EncodedBlock& output_block,
            const EncodingScanResult& result) {
        using T = TD::DataTypeTag::raw_type;
        output_block = block_from_scan_result(result, false);
        if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
            if constexpr(std::is_unsigned_v<T>) {
                switch (result.type_) {
                case EncodingType::DELTA: {
                    DeltaCompressor<T> delta(std::get<DeltaCompressData>(result.data_));
                    pos += delta.compress(column_data, reinterpret_cast<T *>(out.data() + pos), result.estimated_size_);
                }
                case EncodingType::FFOR: {
                    FForCompressor<T> ffor(std::get<FFORCompressData>(result.data_));
                    pos += ffor.compress(column_data, reinterpret_cast<T *>(out.data() + pos), result.estimated_size_);
                }
                case EncodingType::BITPACK: {
                    BitPackCompressor<T> bitpack(std::get<BitPackData>(result.data_));
                    pos += bitpack.compress(column_data, reinterpret_cast<T *>(out.data() + pos), result.estimated_size_);
                }
                case EncodingType::PLAIN: {
                    pos += PlainCompressor<T>::compress(column_data, reinterpret_cast<T *>(out.data() + pos), result.estimated_size_);
                }
                case EncodingType::FREQUENCY: {
                    FrequencyCompressor<T> frequency(std::get<FrequencyEncodingData>(result.data_));
                    pos += frequency.compress(column_data, result.original_size_ / sizeof(T), out.data() + pos, result.estimated_size_);
                }
                default:
                    util::raise_rte("Unknown encoding type in adaptive integer encoding: {}", static_cast<uint16_t>(result.type_));
                }
            } else {
                util::raise_rte("Time to implement signed integer handling");
            }
        } else if constexpr(std::is_floating_point_v<T>) {
            switch (result.type_) {
                case EncodingType::ALP: {
                    pos += ALPCompressor<T>::compress(column_data, reinterpret_cast<T *>(out.data() + pos), result.estimated_size_);
                }
                case EncodingType::PLAIN: {
                    pos += PlainCompressor<T>::compress(column_data, reinterpret_cast<T *>(out.data() + pos), result.estimated_size_);
                }
                case EncodingType::FREQUENCY: {
                    FrequencyCompressor<T> frequency(std::get<FrequencyEncodingData>(result.data_));
                    pos += frequency.compress(column_data, result.original_size_ / sizeof(T), out.data() + pos, result.estimated_size_);
                }
                default:
                    util::raise_rte("Unknown encoding type in adaptive floating-point encoding: {}", static_cast<uint16_t>(result.type_));
            }
        } else {
            util::raise_rte("Unhandled type in adaptive encoding");
        }
    }
};

void check_result(EncodingType type, const DecompressResult& result, size_t in_bytes, size_t out_bytes) {
    util::check(result.uncompressed_ == out_bytes, "Uncompressed size mismatch in {} decoder: {} != {}", static_cast<uint16_t>(type), result.uncompressed_, out_bytes);
    util::check(result.compressed_ == in_bytes, "Uncompressed size mismatch in {} decoder: {} != {}", static_cast<uint16_t>(type), result.compressed_, in_bytes);
}

struct AdaptiveDecoder {
    template<typename T>
    static void decode_block(
        const EncodedBlock& block,
        [[maybe_unused]] std::uint32_t encoder_version,
        const std::uint8_t *in,
        std::size_t in_bytes,
        T *out,
        std::size_t out_bytes) {
        auto type = block.codec().adaptive().encoding_type_;
        switch(type) {
        case EncodingType::FFOR: {
            auto result = FForDecompressor<T>::decompress(in, out);
            check_result(type, result, in_bytes, out_bytes);
            break;
        }
        case EncodingType::DELTA: {
            DeltaDecompressor<T> decompressor;
            decompressor.init(in);
            auto result = decompressor.decompress(in, out);
            check_result(type, result, in_bytes, out_bytes);
            break;
        }
        case EncodingType::BITPACK: {
            auto result = BitPackDecompressor<T>::decompress(in, out);
            check_result(type, result, in_bytes, out_bytes);
            break;
        }
        case EncodingType::FREQUENCY: {
            auto result = FrequencyDecompressor<T>::decompress(in, out);
            check_result(type, result, in_bytes, out_bytes);
            break;
        }
        case EncodingType::CONSTANT: {
            auto result = ConstantDecompressor<T>::decompress(in, out);
            check_result(type, result, in_bytes, out_bytes);
            break;
        }
        case EncodingType::ALP: {
            ALPDecompressor<T> decompressor;
            decompressor.init(in);
            auto result = decompressor.decompress(in, out);
            check_result(type, result, in_bytes, out_bytes);
            break;
        }
        case EncodingType::PLAIN: {
            auto result = PlainDecompressor<T>::decompress(in, out);
            check_result(type, result, in_bytes, out_bytes);
            break;
        }
        default:
            util::raise_rte("Unknown encoding type: {}", type);
        }

    }
};

} //namespace arcticdb