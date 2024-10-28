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

private:
    template<class T, typename EncodedBlockType>
    static void encode_block(
        const T* in,
        size_t in_byte_size,
        HashAccum& hasher,
        T* out,
        std::ptrdiff_t& pos,
        EncodedBlockType& output_block,
        EncodingScanResult scan_result [[maybe_unused]]) {
        memcpy(out, in, in_byte_size);
        hasher(in, in_byte_size / sizeof(T));
        pos += static_cast<ssize_t>(in_byte_size);
        output_block.mutable_codec()->type_ = Codec::ADAPTIVE;
        output_block.set_in_bytes(in_byte_size);
        output_block.set_out_bytes(in_byte_size);
    }
};

struct AdaptiveDecoder {
    template<typename T>
    static void decode_block(
        [[maybe_unused]] std::uint32_t encoder_version,
        const std::uint8_t *in,
        std::size_t in_bytes,
        T *t_out,
        std::size_t out_bytes) {
        arcticdb::util::check_arg(in_bytes == out_bytes, "expected  in_bytes==out_bytes, actual {} != {}", in_bytes,out_bytes);
        memcpy(t_out, in, in_bytes);
    }
};

} //namespace arcticdb