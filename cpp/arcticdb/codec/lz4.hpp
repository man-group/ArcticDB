/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/codec/core.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <arcticdb/util/hash.hpp>

#include <lz4.h>
#include <type_traits>

namespace arcticdb::detail {

struct Lz4BlockEncoder {

    using Opts = arcticdb::proto::encoding::VariantCodec::Lz4;
    static constexpr std::uint32_t VERSION = 1;

    static std::size_t max_compressed_size(std::size_t size) { return LZ4_compressBound(static_cast<int>(size)); }

    static void set_shape_defaults(Opts& opts) { opts.set_acceleration(0); }

    template<class T, class CodecType>
    static std::size_t encode_block(
            const Opts& opts, const T* in, BlockDataHelper& block_utils, HashAccum& hasher, T* out,
            std::size_t out_capacity, std::ptrdiff_t& pos, CodecType& out_codec
    ) {
        int compressed_bytes = LZ4_compress_default(
                reinterpret_cast<const char*>(in),
                reinterpret_cast<char*>(out),
                int(block_utils.bytes_),
                int(out_capacity)
        );

        // Compressed bytes equal to 0 means error unless there is nothing to compress.
        util::check_arg(
                compressed_bytes > 0 || (compressed_bytes == 0 && block_utils.bytes_ == 0),
                "expected compressed bytes >= 0, actual {}",
                compressed_bytes
        );
        ARCTICDB_TRACE(
                log::storage(),
                "Block of size {} compressed to {} bytes: {}",
                block_utils.bytes_,
                compressed_bytes,
                dump_bytes(out, compressed_bytes, 10U)
        );
        hasher(in, block_utils.count_);
        pos += ssize_t(compressed_bytes);
        copy_codec(*out_codec.mutable_lz4(), opts);
        return std::size_t(compressed_bytes);
    }
};

struct Lz4Decoder {
    template<typename T>
    static void decode_block(
            [[maybe_unused]] std::uint32_t
                    encoder_version, // support multiple versions but won't be used before we have them
            const std::uint8_t* in, std::size_t in_bytes, T* t_out, std::size_t out_bytes
    ) {
        ARCTICDB_TRACE(log::codec(), "Lz4 decoder reading block: {} {}", in_bytes, out_bytes);

        // Decompressed size < 0 means an error occurred in LZ4 during the decompression. In case it's  negative
        // the specific value is somewhat random and does not mean anything. Decompressed size of 0 is allowed and means
        // 0 bytes were passed for compression. In that case t_out is allowed to be null since it's not used at all.
        const int decompressed_size = LZ4_decompress_safe(
                reinterpret_cast<const char*>(in), reinterpret_cast<char*>(t_out), int(in_bytes), int(out_bytes)
        );
        util::check_arg(
                decompressed_size >= 0,
                "Error while decoding with lz4 at address {:x} with size {}. Code {}",
                uintptr_t(in),
                in_bytes,
                decompressed_size
        );

        util::check_arg(
                std::size_t(decompressed_size) == out_bytes,
                "expected out_bytes == lz4 decompressed bytes, actual {} != {}",
                out_bytes,
                decompressed_size
        );
    }
};

} // namespace arcticdb::detail
