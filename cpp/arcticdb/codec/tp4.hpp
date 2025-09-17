/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/codec/core.hpp>

#include <arcticdb/util/buffer.hpp>
#include "hash.hpp>

#include <tp4/bitutil.h>
#include <tp4/fp.h>
#include <type_traits>

namespace arcticdb::detail {

struct TurboPForBase {
    using Opts = arcticdb::proto::encoding::VariantCodec::TurboPfor;
    static constexpr std::uint32_t VERSION = 1;

    static std::size_t max_compressed_size(std::size_t size) { return (size * 3) / 2; }
};

template<arcticdb::proto::encoding::VariantCodec::TurboPfor::SubCodecs S>
struct TurboPForBlockCodec {};

template<>
struct TurboPForBlockCodec<arcticdb::proto::encoding::VariantCodec::TurboPfor::FP_DELTA> : TurboPForBase {

    template<class T>
    inline static std::size_t encode_block(
            const T* in, Block& block, HashAccum& hasher, T* t_out, std::size_t out_capacity, std::ptrdiff_t& pos
    ) {
        hasher(in, block.count);
        auto* out = reinterpret_cast<std::uint8_t*>(t_out);
        std::size_t compressed_bytes = encode_block_(in, block, out);
        pos += compressed_bytes;
        return compressed_bytes;
    }

    template<class T>
    inline static std::size_t encode_block_(T* in, Block& block, std::uint8_t* out) {
        std::size_t compressed_bytes = 0;
        if constexpr (std::is_integral_v<T>) {
            if constexpr (std::is_unsigned_v<T>) {
                if constexpr (std::is_same_v<T, std::uint64_t>) {
                    compressed_bytes = fppenc64(in, block.count, out, 0);
                } else if constexpr (std::is_same_v<T, std::uint32_t>) {
                    compressed_bytes = fppenc32(in, block.count, out, 0);
                } else if constexpr (std::is_same_v<T, std::uint16_t>) {
                    compressed_bytes = fppenc16(in, block.count, out, 0);
                } else if constexpr (std::is_same_v<T, std::uint8_t>) {
                    compressed_bytes = fppenc8(in, block.count, out, 0);
                }
            } else if constexpr (std::is_signed_v<T>) {
                using Unsigned_T = std::make_unsigned_t<T>;
                auto u_in = reinterpret_cast<const Unsigned_T*>(in);
                compressed_bytes = encode_block_(u_in, block, out);
            }
        } else if constexpr (std::is_floating_point_v<T>) {
            if constexpr (std::is_same_v<T, double>) {
                auto* u_in = reinterpret_cast<const std::uint64_t*>(in);
                encode_block_(u_in, block, out);
            } else if constexpr (std::is_same_v<T, std::uint32_t>) {
                auto* u_in = reinterpret_cast<const std::uint32_t*>(in);
                encode_block_(u_in, block, out);
            }
        }
        return compressed_bytes;
    }
    template<class T>
    static void decode_block(const std::uint8_t* in, std::size_t in_count, T* t_out, std::size_t out_bytes) {
        if constexpr (std::is_integral_v<T>) {
            if constexpr (std::is_unsigned_v<T>) {
                std::uint8_t* in_nc = (std::uint8_t*)in;
                std::size_t decompressed_bytes = 0;
                if constexpr (std::is_same_v<T, std::uint64_t>) {
                    decompressed_bytes = fppdec64(in_nc, in_count, t_out, 0);
                } else if constexpr (std::is_same_v<T, std::uint32_t>) {
                    decompressed_bytes = fppdec32(in_nc, in_count, t_out, 0);
                } else if constexpr (std::is_same_v<T, std::uint16_t>) {
                    decompressed_bytes = fppdec16(in_nc, in_count, t_out, 0);
                } else if constexpr (std::is_same_v<T, std::uint8_t>) {
                    decompressed_bytes = fppdec8(in_nc, in_count, t_out, 0);
                }
                util::check_arg(
                        decompressed_bytes == out_bytes,
                        "expected out_bytes == decompressed bytes, actual {} != {}",
                        out_bytes,
                        decompressed_bytes
                );
            } else if constexpr (std::is_signed_v<T>) {
                using Unsigned_T = std::make_unsigned_t<T>;
                auto u_out = reinterpret_cast<Unsigned_T*>(t_out);
                decode_block(in, in_count, u_out, out_bytes);
            }
        } else if constexpr (std::is_floating_point_v<T>) {
            if constexpr (std::is_same_v<T, double>) {
                auto* u_out = reinterpret_cast<const std::uint64_t*>(t_out);
                decode_block(in, in_count, u_out, out_bytes);
            } else if constexpr (std::is_same_v<T, std::uint32_t>) {
                auto* u_out = reinterpret_cast<const std::uint32_t*>(t_out);
                decode_block(in, in_count, u_out, out_bytes);
            }
        }
    }
};

/**
 * Force encoded type for shapes
 */
struct ShapeEncoder : TurboPForBlockCodec<arcticdb::proto::encoding::VariantCodec::TurboPfor::FP_DELTA> {

    using Parent = TurboPForBlockCodec<arcticdb::proto::encoding::VariantCodec::TurboPfor::FP_DELTA>;

    template<class T>
    static std::size_t encode_block(
            const T* in, Block& block, HashAccum& hasher, T* out, std::size_t out_capacity, std::ptrdiff_t& pos,
            arcticdb::proto::encoding::VariantCodec& out_codec
    ) {
        std::size_t compressed_size = Parent::encode_block(in, block, hasher, out, out_capacity, pos);

        out_codec.mutable_tp4()->set_sub_codec(arcticdb::proto::encoding::VariantCodec::TurboPfor::FP_DELTA);
        return compressed_size;
    }
};

struct TurboPForBlockEncoder : TurboPForBase {

    template<class T>
    static std::size_t encode_block(
            const Opts& opts, const T* in, Block& block, HashAccum& hasher, T* out, std::size_t out_capacity,
            std::ptrdiff_t& pos, arcticdb::proto::encoding::VariantCodec& out_codec
    ) {
        std::size_t compressed_size = 0;
        switch (opts.sub_codec()) {
        case Opts::FP_DELTA:
            compressed_size =
                    TurboPForBlockCodec<Opts::FP_DELTA>::encode_block(in, block, hasher, out, out_capacity, pos);
            break;
        default:
            raise_unsupported_msg("Unsupported tp4 subcodec {}", opts);
        }
        out_codec.mutable_tp4()->MergeFrom(opts);
        return compressed_size;
    }
};

template<template<typename> class F, class TD>
using TurboPForEncoder = GenericBlockEncoder<F<TD>, TD, TurboPForBlockEncoder, ShapeEncoder>;

struct TurboPForDecoder {

    template<typename T>
    static void decode_block(
            const arcticdb::proto::encoding::Block& block, const std::uint8_t* in, std::size_t in_bytes, T* t_out,
            std::size_t out_bytes
    ) {
        switch (block.codec().tp4().sub_codec()) {
        case arcticdb::proto::encoding::VariantCodec::TurboPfor::FP_DELTA:
            TurboPForBlockCodec<arcticdb::proto::encoding::VariantCodec::TurboPfor::FP_DELTA>::decode_block(
                    in, in_bytes, t_out, out_bytes
            );
            break;
        default:
            raise_unsupported_msg("Unsupported tp4 block {}", block);
        }
    }
};

} // namespace arcticdb::detail