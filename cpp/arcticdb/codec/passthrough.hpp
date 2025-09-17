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
#include <arcticdb/util/hash.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <type_traits>
#include <bitset>

namespace arcticdb::detail {

template<template<typename> class BlockType, class TD>
struct PassthroughEncoderV1 {
    using Opts = arcticdb::proto::encoding::VariantCodec::Passthrough;

    static size_t max_compressed_size(const BlockType<TD>& block) {
        using Helper = CodecHelper<TD>;
        if constexpr (Helper::dim == entity::Dimension::Dim0) {
            // Only store data, no shapes since dimension is 0
            auto v_block = Helper::scalar_block(block.row_count());
            return v_block.bytes_;
        } else {
            auto helper_array_block = Helper::nd_array_block(block.row_count(), block.shapes());
            return helper_array_block.shapes_.bytes_ + helper_array_block.values_.bytes_;
        }
    }

    template<typename EncodedFieldType>
    static void encode(
            const Opts&, const BlockType<TD>& block, EncodedFieldType& field, Buffer& out, std::ptrdiff_t& pos
    ) {
        using namespace arcticdb::entity;
        using Helper = CodecHelper<TD>;
        using T = typename Helper::T;
        Helper helper;
        helper.hasher_.reset(helper.seed);
        const T* d = block.data();
        std::size_t block_row_count = block.row_count();

        if constexpr (Helper::dim == entity::Dimension::Dim0) {
            // Only store data, no shapes since dimension is 0
            auto scalar_block = Helper::scalar_block(block_row_count);
            helper.ensure_buffer(out, pos, scalar_block.bytes_);

            // doing copy + hash in one pass, this might have a negative effect on perf
            // since the hashing is path dependent. This is a toy example though so not critical
            T* t_out = out.ptr_cast<T>(pos, scalar_block.bytes_);
            encode_block(d, scalar_block, helper.hasher_, t_out, pos);

            auto* nd_array = field.mutable_ndarray();
            auto total_row_count = nd_array->items_count() + block_row_count;
            nd_array->set_items_count(total_row_count);
            auto values = nd_array->add_values(EncodingVersion::V1);
            (void)values->mutable_codec()->mutable_passthrough();
            scalar_block.set_block_data(*values, helper.hasher_.digest(), scalar_block.bytes_);
        } else {
            auto helper_array_block = Helper::nd_array_block(block_row_count, block.shapes());
            helper.ensure_buffer(out, pos, helper_array_block.shapes_.bytes_ + helper_array_block.values_.bytes_);

            // write shapes
            auto s_out = out.ptr_cast<shape_t>(pos, helper_array_block.shapes_.bytes_);
            encode_block(block.shapes(), helper_array_block.shapes_, helper.hasher_, s_out, pos);
            HashedValue shape_hash = helper.get_digest_and_reset();

            // write values
            T* t_out = out.ptr_cast<T>(pos, helper_array_block.values_.bytes_);
            encode_block(d, helper_array_block.values_, helper.hasher_, t_out, pos);
            auto field_nd_array = field.mutable_ndarray();
            // Important: In case V2 EncodedField is used shapes must be added before values.
            auto shapes = field_nd_array->add_shapes();
            (void)shapes->mutable_codec()->mutable_passthrough();

            auto values = field_nd_array->add_values(EncodingVersion::V1);
            (void)values->mutable_codec()->mutable_passthrough();

            helper_array_block.update_field_size(*field_nd_array);
            helper_array_block.set_block_data(
                    shapes,
                    values,
                    shape_hash,
                    helper_array_block.shapes_.bytes_,
                    helper.hasher_.digest(),
                    helper_array_block.values_.bytes_
            );
        }
    }

  private:
    template<class T>
    static void encode_block(
            const T* in, BlockDataHelper& block_utils, HashAccum& hasher, T* out, std::ptrdiff_t& pos
    ) {
        memcpy(out, in, block_utils.bytes_);
        hasher(in, block_utils.bytes_ / sizeof(T));
        pos += static_cast<ssize_t>(block_utils.bytes_);
    }
};

/// @brief "Encoder" which stores the input in the output buffer without performing any transformation
/// @note The difference between arcticdb::detail::PassthroughEncoder and arcticdb::detail::PassthroughEncoder2 is that
/// the latter does not care about the shapes array.
/// @see arcticdb::ColumnEncoder2 arcticdb::detail::GenericBlockEncoder2
template<template<typename> class BlockType, class TD>
struct PassthroughEncoderV2 {
    using Opts = arcticdb::proto::encoding::VariantCodec::Passthrough;

    static size_t max_compressed_size(const BlockType<TD>& block) { return block.nbytes(); }

    template<typename EncodedBlockType>
    static void encode(
            const Opts&, const BlockType<TD>& block, Buffer& out, std::ptrdiff_t& pos, EncodedBlockType* encoded_block
    ) {
        using namespace arcticdb::entity;
        using Helper = CodecHelper<TD>;
        using T = typename Helper::T;
        Helper helper;
        helper.hasher_.reset(helper.seed);
        const T* d = block.data();
        const size_t data_byte_size = block.nbytes();
        helper.ensure_buffer(out, pos, data_byte_size);

        // doing copy + hash in one pass, this might have a negative effect on perf
        // since the hashing is path dependent. This is a toy example though so not critical
        T* t_out = out.ptr_cast<T>(pos, data_byte_size);
        encode_block(d, data_byte_size, helper.hasher_, t_out, pos);
        encoded_block->set_in_bytes(data_byte_size);
        encoded_block->set_out_bytes(data_byte_size);
        encoded_block->set_hash(helper.hasher_.digest());
        (void)encoded_block->mutable_codec()->mutable_passthrough();
    }

  private:
    template<class T>
    static void encode_block(const T* in, size_t in_byte_size, HashAccum& hasher, T* out, std::ptrdiff_t& pos) {
        memcpy(out, in, in_byte_size);
        hasher(in, in_byte_size / sizeof(T));
        pos += static_cast<ssize_t>(in_byte_size);
    }
};

struct PassthroughDecoder {
    template<typename T>
    static void decode_block(const std::uint8_t* in, std::size_t in_bytes, T* t_out, std::size_t out_bytes) {
        arcticdb::util::check_arg(
                in_bytes == out_bytes, "expected  in_bytes==out_bytes, actual {} != {}", in_bytes, out_bytes
        );
        memcpy(t_out, in, in_bytes);
    }
};

} // namespace arcticdb::detail
