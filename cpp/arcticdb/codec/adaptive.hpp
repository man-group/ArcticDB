#pragma once

#include <cstddef>
#include <arcticdb/util/buffer.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/hash.hpp>

namespace arcticdb::detail {

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
    using Opts = AdaptiveCodec;

    static size_t max_compressed_size(const BlockType<TD> &block) {
        return block.nbytes();
    }

    template <typename EncodedBlockType>
    static void encode(
        const Opts&,
        const BlockType<TD> &block,
        Buffer &out,
        std::ptrdiff_t &pos,
        EncodedBlockType* output_block) {
        using namespace arcticdb::entity;
        using CodecHelperType = CodecHelper<TD>;
        using T = typename CodecHelperType::T;
        CodecHelperType helper;
        const T* d = block.data();
        const size_t data_byte_size = block.nbytes();
        helper.ensure_buffer(out, pos, data_byte_size);

        T *t_out = out.ptr_cast<T>(pos, data_byte_size);
        encode_block(d, data_byte_size, helper.hasher(), t_out, pos, output_block);
        output_block->set_hash(helper.hasher().digest());
    }
private:
    template<class T, typename EncodedBlockType>
    static void encode_block(
        const T* in,
        size_t in_byte_size,
        HashAccum& hasher,
        T* out,
        std::ptrdiff_t& pos,
        EncodedBlockType* output_block) {
        memcpy(out, in, in_byte_size);
        hasher(in, in_byte_size / sizeof(T));
        pos += static_cast<ssize_t>(in_byte_size);
        output_block->mutable_codec()->type_ = Codec::ADAPTIVE;
        output_block->set_in_bytes(in_byte_size);
        output_block->set_out_bytes(in_byte_size);
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
} //namespace arcticdb::detail