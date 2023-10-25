/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/codec/segment.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>
#include <arcticdb/storage/common.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/util/dump_bytes.hpp>

#include <zstd.h>

namespace arcticdb::detail {

struct ZstdBlockEncoder {

    using Opts = arcticdb::proto::encoding::VariantCodec::Zstd;
    static constexpr std::uint32_t VERSION = 1;

    static std::size_t max_compressed_size(std::size_t size) {
        return ZSTD_compressBound(size);
    }

    static void set_shape_defaults(Opts &opts) {
        opts.set_level(0);
    }

    template<class T, typename CodecType>
    static std::size_t encode_block(
            const Opts &opts,
            const T* in,
            BlockProtobufHelper &block_utils,
            HashAccum& hasher,
            T *out,
            std::size_t out_capacity,
            std::ptrdiff_t &pos,
            CodecType& out_codec) {
        std::size_t compressed_bytes = ZSTD_compress(out, out_capacity, in, block_utils.bytes_, opts.level());
        hasher(in, block_utils.count_);
        pos += compressed_bytes;
        out_codec.mutable_zstd()->MergeFrom(opts);
        return compressed_bytes;
    }
};

template<template<typename> class F, class TD>
using ZstdEncoder = GenericBlockEncoder<F<TD>, TD, ZstdBlockEncoder>;

struct ZstdDecoder {

/*
 * encoder_version is here to support multiple versions but won't be used before we have them
 */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"

    template<typename T>
    static void decode_block(
        std::uint32_t encoder_version,
        const std::uint8_t *in,
        std::size_t in_bytes,
        T *t_out,
        std::size_t out_bytes) {

        const std::size_t decomp_size = ZSTD_getFrameContentSize(in, in_bytes);
        util::check_arg(decomp_size == out_bytes, "expected out_bytes == zstd deduced bytes, actual {} != {}",
                        out_bytes, decomp_size);
        std::size_t real_decomp = ZSTD_decompress(t_out, out_bytes, in, in_bytes);
        util::check_arg(real_decomp == out_bytes, "expected out_bytes == zstd decompressed bytes, actual {} != {}",
                        out_bytes, real_decomp);
    }
#pragma GCC diagnostic pop
};

} // namespace arcticdb::detail