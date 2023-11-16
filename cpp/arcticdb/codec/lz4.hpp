/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/codec/core.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <arcticdb/util/buffer.hpp>
#include <arcticdb/util/hash.hpp>
#include <arcticdb/util/dump_bytes.hpp>

#include <lz4.h>
#include <type_traits>

namespace arcticdb::detail {

struct Lz4BlockEncoder {

    using Opts = arcticdb::proto::encoding::VariantCodec::Lz4;
    static constexpr std::uint32_t VERSION = 1;

    static std::size_t max_compressed_size(std::size_t size) {
        return LZ4_compressBound(static_cast<int>(size));
    }

    static void set_shape_defaults(Opts &opts) {
        opts.set_acceleration(0);
    }

    template<class T, class CodecType>
    static std::size_t encode_block(
            const Opts& opts,
            const T *in,
            BlockProtobufHelper &block_utils,
            HashAccum &hasher,
            T *out,
            std::size_t out_capacity,
            std::ptrdiff_t &pos,
            CodecType& out_codec) {
        int compressed_bytes = LZ4_compress_default(
            reinterpret_cast<const char *>(in),
            reinterpret_cast<char *>(out),
            int(block_utils.bytes_), int(out_capacity));

        util::check_arg(compressed_bytes >= 0, "expected compressed bytes >= 0, actual {}", compressed_bytes);
        ARCTICDB_TRACE(log::storage(), "Block of size {} compressed to {} bytes", block_utils.bytes_, compressed_bytes);
        hasher(in, block_utils.count_);
        pos += ssize_t(compressed_bytes);
        out_codec.mutable_lz4()->MergeFrom(opts);
        return std::size_t(compressed_bytes);
    }
};

template<template<typename> class F, class TD>
using Lz4Encoder = GenericBlockEncoder<F<TD>, TD, Lz4BlockEncoder>;

struct Lz4Decoder {
/*
 * encoder_version is here to support multiple versions but won't be used before we have them
 */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"

    template<typename T>
    static void decode_block([[maybe_unused]] std::uint32_t encoder_version, const std::uint8_t *in, std::size_t in_bytes, T *t_out,
                             std::size_t out_bytes) {

        ARCTICDB_TRACE(log::codec(), "Lz4 decoder reading block: {} {}", in_bytes, out_bytes);
        int real_decomp = LZ4_decompress_safe(
            reinterpret_cast<const char *>(in),
            reinterpret_cast<char *>(t_out),
            int(in_bytes),
            int(out_bytes)
        );
        codec::check<ErrorCode::E_DECODE_ERROR>(real_decomp > 0, "Error while decoding with lz4 at address {:x} with size {}. Code {}", uintptr_t(in), in_bytes, real_decomp);
        codec::check<ErrorCode::E_DECODE_ERROR>(std::size_t(real_decomp) == out_bytes,
                        "expected out_bytes == lz4 decompressed bytes, actual {} != {}",
                        out_bytes,
                        real_decomp);
    }

#pragma GCC diagnostic pop
};

} // namespace arcticdb::detail