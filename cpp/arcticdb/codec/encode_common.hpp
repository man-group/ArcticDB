/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

/// @file encode_common.hpp Functions and classes used by both V1 and V2 encodings

#pragma once

#include <arcticdb/codec/segment.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/configs_map.hpp>

#include <folly/ScopeGuard.h>

#include <algorithm>
#include <cstddef>
#include <vector>

namespace arcticdb {

/// @brief This should be the block data type descriptor when the shapes array is encoded as a block
using ShapesBlockTDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension::Dim0>>;

using ByteArrayTDT = TypeDescriptorTag<DataTypeTag<DataType::UINT8>, DimensionTag<Dimension::Dim1>>;

template<template<typename> class TypedBlock, class TD, EncodingVersion encoder_version>
struct TypedBlockEncoderImpl;

template<EncodingVersion v, typename VersionedColumnEncoder>
struct EncodingPolicyType {
    static constexpr EncodingVersion version = v;
    using ColumnEncoder = VersionedColumnEncoder;
};

/// Encoded blocks a field of num_value_blocks takes: V1 writes a shapes block per values block, V2 one shapes block
template<typename EncodingPolicyType>
size_t encoded_field_blocks(size_t num_value_blocks, bool has_shapes) {
    if constexpr (EncodingPolicyType::version == EncodingVersion::V1)
        return num_value_blocks + (num_value_blocks * has_shapes);
    else
        return num_value_blocks + has_shapes;
}

template<typename EncodingPolicyType>
size_t calc_num_blocks(const ColumnData& column_data) {
    return encoded_field_blocks<EncodingPolicyType>(column_data.num_blocks(), !column_data.shapes()->empty());
}

template<typename EncodingPolicyType>
struct BytesEncoder {
    using Encoder = TypedBlockEncoderImpl<TypedBlockData, ByteArrayTDT, EncodingPolicyType::version>;
    using BytesBlock = TypedBlockData<ByteArrayTDT>;
    using ShapesEncoder = TypedBlockEncoderImpl<TypedBlockData, ShapesBlockTDT, EncodingPolicyType::version>;

    template<typename EncodedFieldType>
    static void encode(
            const ChunkedBuffer& data, const arcticdb::proto::encoding::VariantCodec& codec_opts, Buffer& out_buffer,
            std::ptrdiff_t& pos, EncodedFieldType& encoded_field
    ) {
        if constexpr (EncodingPolicyType::version == EncodingVersion::V1) {
            const auto bytes_count = static_cast<shape_t>(data.bytes());
            auto typed_block = BytesBlock(data.data(), &bytes_count, bytes_count, 1u, data.block_and_offset(0).block_);
            Encoder::encode(codec_opts, typed_block, encoded_field, out_buffer, pos);
        } else if constexpr (EncodingPolicyType::version == EncodingVersion::V2) {
            const shape_t row_count = 1; // BytesEncoder data is stored as an array with a single row
            const auto shapes_data = static_cast<ShapesBlockTDT::DataTypeTag::raw_type>(data.bytes());
            auto shapes_block = TypedBlockData<ShapesBlockTDT>(
                    &shapes_data, nullptr, sizeof(shape_t), row_count, data.block_and_offset(0).block_
            );
            const auto bytes_count = static_cast<shape_t>(data.bytes());
            auto data_block = BytesBlock(
                    data.data(),
                    &bytes_count,
                    static_cast<shape_t>(bytes_count),
                    row_count,
                    data.block_and_offset(0).block_
            );
            ShapesEncoder::encode_shapes(codec::default_shapes_codec(), shapes_block, encoded_field, out_buffer, pos);
            Encoder::encode_values(codec_opts, data_block, encoded_field, out_buffer, pos);
            auto* field_nd_array = encoded_field.mutable_ndarray();
            const auto total_items_count = field_nd_array->items_count() + row_count;
            field_nd_array->set_items_count(total_items_count);
        } else {
            static_assert(std::is_same_v<decltype(EncodingPolicyType::version), void>, "Unknown encoding version");
        }
    }

    static size_t max_compressed_size(const arcticdb::proto::encoding::VariantCodec& codec_opts, shape_t data_size) {
        const shape_t shapes_bytes = sizeof(shape_t);
        const auto values_block = BytesBlock(data_size, &data_size);
        if constexpr (EncodingPolicyType::version == EncodingVersion::V1) {
            const auto shapes_block = BytesBlock(shapes_bytes, &shapes_bytes);
            return Encoder::max_compressed_size(codec_opts, values_block) +
                   Encoder::max_compressed_size(codec_opts, shapes_block);
        } else if constexpr (EncodingPolicyType::version == EncodingVersion::V2) {
            const auto shapes_block = TypedBlockData<ShapesBlockTDT>(shapes_bytes, &shapes_bytes);
            return Encoder::max_compressed_size(codec_opts, values_block) +
                   ShapesEncoder::max_compressed_size(codec::default_shapes_codec(), shapes_block);
        } else {
            static_assert(std::is_same_v<decltype(EncodingPolicyType::version), void>, "Unknown encoding version");
        }
    }

    static size_t num_encoded_blocks(const ChunkedBuffer& buffer) { return buffer.num_blocks() + 1; }
};

struct SizeResult {
    size_t max_compressed_bytes_;
    size_t uncompressed_bytes_;
    shape_t encoded_blocks_bytes_;
};

template<typename EncodingPolicyType>
void calc_metadata_size(
        const SegmentInMemory& in_mem_seg, const arcticdb::proto::encoding::VariantCodec& codec_opts, SizeResult& result
) {
    if (in_mem_seg.metadata()) {
        const auto metadata_bytes = static_cast<shape_t>(in_mem_seg.metadata()->ByteSizeLong());
        result.uncompressed_bytes_ += metadata_bytes + sizeof(shape_t);
        result.max_compressed_bytes_ +=
                BytesEncoder<EncodingPolicyType>::max_compressed_size(codec_opts, metadata_bytes);
        ARCTICDB_TRACE(log::codec(), "Metadata requires {} max_compressed_bytes", result.max_compressed_bytes_);
    }
}

template<typename EncodingPolicyType>
void calc_columns_size(
        const SegmentInMemory& in_mem_seg, const arcticdb::proto::encoding::VariantCodec& codec_opts, SizeResult& result
) {
    for (std::size_t c = 0; c < in_mem_seg.num_columns(); ++c) {
        auto column_data = in_mem_seg.column_data(c);
        const auto [uncompressed, required] =
                EncodingPolicyType::ColumnEncoder::max_compressed_size(codec_opts, column_data);
        result.uncompressed_bytes_ += uncompressed;
        result.max_compressed_bytes_ += required;
        ARCTICDB_TRACE(
                log::codec(),
                "Column {} requires {} max_compressed_bytes, total {}",
                c,
                required,
                result.max_compressed_bytes_
        );
    }
}

template<typename EncodingPolicyType>
void calc_string_pool_size(
        const SegmentInMemory& in_mem_seg, const arcticdb::proto::encoding::VariantCodec& codec_opts, SizeResult& result
) {
    if (in_mem_seg.has_string_pool()) {
        auto string_col = in_mem_seg.string_pool_data();
        const auto [uncompressed, required] =
                EncodingPolicyType::ColumnEncoder::max_compressed_size(codec_opts, string_col);
        result.uncompressed_bytes_ += uncompressed;
        result.max_compressed_bytes_ += required;
        ARCTICDB_TRACE(
                log::codec(),
                "String pool requires {} max_compressed_bytes, total {}",
                required,
                result.max_compressed_bytes_
        );
    }
}

template<typename EncodingPolicyType>
void encode_metadata(
        const SegmentInMemory& in_mem_seg, SegmentHeader& segment_header,
        const arcticdb::proto::encoding::VariantCodec& codec_opts, Buffer& out_buffer, std::ptrdiff_t& pos
) {
    if (in_mem_seg.metadata()) {
        const auto bytes_count = static_cast<shape_t>(in_mem_seg.metadata()->ByteSizeLong());
        ARCTICDB_TRACE(log::codec(), "Encoding {} bytes of metadata", bytes_count);
        constexpr int max_stack_alloc = 1 << 11;
        bool malloced{false};
        uint8_t* meta_ptr;
        if (bytes_count > max_stack_alloc) {
            meta_ptr = reinterpret_cast<uint8_t*>(malloc(bytes_count));
            malloced = true;
        } else {
            meta_ptr = reinterpret_cast<uint8_t*>(alloca(bytes_count));
        }
        ChunkedBuffer meta_buffer;
        meta_buffer.add_external_block(meta_ptr, bytes_count);
        const auto num_encoded_fields = BytesEncoder<EncodingPolicyType>::num_encoded_blocks(meta_buffer);
        auto& encoded_field = segment_header.mutable_metadata_field(num_encoded_fields);
        google::protobuf::io::ArrayOutputStream aos(&meta_buffer[0], static_cast<int>(bytes_count));
        in_mem_seg.metadata()->SerializeToZeroCopyStream(&aos);
        ARCTICDB_TRACE(log::codec(), "Encoding metadata to position {}", pos);
        BytesEncoder<EncodingPolicyType>::encode(meta_buffer, codec_opts, out_buffer, pos, encoded_field);
        ARCTICDB_TRACE(log::codec(), "Encoded metadata to position {}", pos);
        if (malloced)
            free(meta_ptr);
    } else {
        ARCTICDB_TRACE(log::codec(), "Not encoding any metadata");
    }
}

/// The string pool is built in small pages, one encoded block each. Readers decode it into one contiguous buffer
/// whatever the block count, so the writer re-cuts it into blocks of this many bytes (0 keeps the pages).
/// Read once per encode and passed down, since the config can change while an encode runs.
inline size_t string_pool_block_bytes() {
    constexpr int64_t min_block_bytes = int64_t{64} * 1024;
    constexpr int64_t max_block_bytes = int64_t{64} * 1024 * 1024;
    const auto configured = ConfigsMap::instance()->get_int("Codec.StringPoolBlockBytes", int64_t{256} * 1024);
    if (configured <= 0)
        return 0;
    return static_cast<size_t>(std::clamp(configured, min_block_bytes, max_block_bytes));
}

/// Pools whose pages average at least this many bytes (long strings) gain too little from re-cutting to pay for it
inline constexpr size_t string_pool_large_page_bytes = size_t{32} * 1024;

inline size_t string_pool_recut_blocks(const ChunkedBuffer& pool, size_t block_bytes) {
    return (pool.bytes() + block_bytes - 1) / block_bytes;
}

/// Whether the pool is encoded one block per page rather than re-cut into blocks of block_bytes
inline bool string_pool_keeps_pages(const ChunkedBuffer& pool, size_t block_bytes) {
    const auto pages = pool.num_blocks();
    if (block_bytes == 0 || pages <= 1 || pool.bytes() >= string_pool_large_page_bytes * pages)
        return true;
    // More blocks than pages would outgrow the output buffer, which is sized from the pages
    return string_pool_recut_blocks(pool, block_bytes) >= pages;
}

inline size_t string_pool_block_count(const ChunkedBuffer& pool, size_t block_bytes) {
    return string_pool_keeps_pages(pool, block_bytes) ? pool.num_blocks() : string_pool_recut_blocks(pool, block_bytes);
}

/// The pool's bytes [offset, offset + size): read in place when they lie inside one page, else gathered into scratch
inline const uint8_t* string_pool_bytes_at(
        const ChunkedBuffer& pool, size_t offset, size_t size, std::vector<uint8_t>& scratch
) {
    util::check(
            offset + size <= pool.bytes(),
            "String pool block [{}, {}) is past the pool's {} bytes",
            offset,
            offset + size,
            pool.bytes()
    );
    const auto spans = pool.byte_blocks_at(offset, size);
    if (spans.size() == 1)
        return spans[0].first;
    size_t gathered = 0;
    for (const auto& [data, bytes] : spans) {
        memcpy(scratch.data() + gathered, data, bytes);
        gathered += bytes;
    }
    return scratch.data();
}

/// Encodes the pool re-cut into num_blocks blocks of block_bytes, one block at a time, so that at most one block is
/// ever copied
template<typename EncodingPolicyType, typename EncodedFieldType>
void encode_string_pool_blocks(
        const ChunkedBuffer& pool, size_t block_bytes, size_t num_blocks,
        const arcticdb::proto::encoding::VariantCodec& codec_opts, EncodedFieldType& encoded_field, Buffer& out_buffer,
        std::ptrdiff_t& pos
) {
    using Encoder = TypedBlockEncoderImpl<TypedBlockData, ByteArrayTDT, EncodingPolicyType::version>;
    const auto total_bytes = pool.bytes();
    // Every block is block_bytes long except the last
    std::vector<shape_t> shapes(num_blocks);
    for (size_t i = 0; i < num_blocks; ++i)
        shapes[i] = static_cast<shape_t>(std::min(block_bytes, total_bytes - i * block_bytes));

    if constexpr (EncodingPolicyType::version == EncodingVersion::V2) {
        using ShapesEncoder = TypedBlockEncoderImpl<TypedBlockData, ShapesBlockTDT, EncodingPolicyType::version>;
        const TypedBlockData<ShapesBlockTDT> shapes_block{
                shapes.data(), nullptr, num_blocks * sizeof(shape_t), num_blocks, nullptr
        };
        ShapesEncoder::encode_shapes(codec::default_shapes_codec(), shapes_block, encoded_field, out_buffer, pos);
    }

    // Reused across encodes on this thread: allocating and freeing a block-sized buffer per segment changes glibc's
    // mmap and trim thresholds, so the next segment's string pool pages are faulted in afresh
    thread_local std::vector<uint8_t> scratch;
    constexpr size_t max_retained_scratch_bytes = size_t{1} << 20;
    scratch.resize(std::max(scratch.size(), std::min(block_bytes, total_bytes)));
    const auto release_large_scratch = folly::makeGuard([] {
        if (scratch.size() > max_retained_scratch_bytes)
            std::vector<uint8_t>().swap(scratch);
    });
    for (size_t i = 0; i < num_blocks; ++i) {
        const auto size = static_cast<size_t>(shapes[i]);
        const auto* data = string_pool_bytes_at(pool, i * block_bytes, size, scratch);
        const TypedBlockData<ByteArrayTDT> block{data, &shapes[i], size, 1u, nullptr};
        if constexpr (EncodingPolicyType::version == EncodingVersion::V1)
            Encoder::encode(codec_opts, block, encoded_field, out_buffer, pos);
        else
            Encoder::encode_values(codec_opts, block, encoded_field, out_buffer, pos);
    }
}

template<typename EncodingPolicyType>
void encode_string_pool(
        const SegmentInMemory& in_mem_seg, SegmentHeader& segment_header,
        const arcticdb::proto::encoding::VariantCodec& codec_opts, Buffer& out_buffer, std::ptrdiff_t& pos,
        size_t block_bytes
) {
    if (in_mem_seg.has_string_pool()) {
        ARCTICDB_TRACE(log::codec(), "Encoding string pool to position {}", pos);
        const auto& pool = in_mem_seg.const_string_pool().data();
        if (string_pool_keeps_pages(pool, block_bytes)) {
            auto col = in_mem_seg.string_pool_data();
            auto& encoded_field = segment_header.mutable_string_pool_field(calc_num_blocks<EncodingPolicyType>(col));
            EncodingPolicyType::ColumnEncoder::encode(codec_opts, col, encoded_field, out_buffer, pos);
        } else {
            const auto num_blocks = string_pool_block_count(pool, block_bytes);
            auto& encoded_field =
                    segment_header.mutable_string_pool_field(encoded_field_blocks<EncodingPolicyType>(num_blocks, true)
                    );
            encode_string_pool_blocks<EncodingPolicyType>(
                    pool, block_bytes, num_blocks, codec_opts, encoded_field, out_buffer, pos
            );
        }
        ARCTICDB_TRACE(log::codec(), "Encoded string pool to position {}", pos);
    }
}

[[nodiscard]] SizeResult max_compressed_size_v1(
        const SegmentInMemory& in_mem_seg, const arcticdb::proto::encoding::VariantCodec& codec_opts
);

[[nodiscard]] SizeResult max_compressed_size_v2(
        const SegmentInMemory& in_mem_seg, const arcticdb::proto::encoding::VariantCodec& codec_opts
);

} // namespace arcticdb