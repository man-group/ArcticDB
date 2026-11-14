/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <cstdint>
#include <numeric>

namespace arcticdb::encoding_sizes {

// A block's decoded size is stored, and callers size allocations from it before anything is decompressed. No codec can
// expand its stored bytes beyond a fixed ratio, so a larger decoded size is corrupt.
template<typename BlockType>
void check_block_decoded_size(const BlockType& block) {
    uint64_t max_expansion = 1; // passthrough copies its stored bytes
    if (block.has_codec()) {
        switch (block.codec().codec_type()) {
        case Codec::LZ4:
            max_expansion = 255; // a match's length grows by at most 255 per stored byte
            break;
        case Codec::ZSTD:
            max_expansion = 32768; // an RLE block expands 4 stored bytes to at most 128 KiB
            break;
        default:
            // No bound applies, and decode_block cannot decode it, so reject before the size is allocated
            codec::raise<ErrorCode::E_DECODE_ERROR>(
                    "Corrupt segment: unsupported block codec {}", codec_type_to_string(block.codec().codec_type())
            );
        }
    }
    constexpr uint64_t slack = 64; // fixed per-block overhead of a tiny compressed block
    const uint64_t decoded_bytes = block.in_bytes();
    const uint64_t stored_bytes = block.out_bytes();
    codec::check<ErrorCode::E_DECODE_ERROR>(
            decoded_bytes <= stored_bytes * max_expansion + slack,
            "Corrupt segment: block of {} stored bytes decodes to {} bytes",
            stored_bytes,
            decoded_bytes
    );
}

template<typename NDArrayEncodedFieldType>
std::size_t shape_compressed_size(const NDArrayEncodedFieldType& nda) {
    return std::accumulate(
            std::begin(nda.shapes()),
            std::end(nda.shapes()),
            size_t(0),
            [](size_t a, const auto& block) { return a + block.out_bytes(); }
    );
}

template<typename NDArrayEncodedFieldType>
std::size_t data_compressed_size(const NDArrayEncodedFieldType& nda) {
    return std::accumulate(
            std::begin(nda.values()),
            std::end(nda.values()),
            size_t(0),
            [](size_t a, const auto& block) { return a + block.out_bytes(); }
    );
}

template<typename NDArrayEncodedFieldType>
std::size_t shape_uncompressed_size(const NDArrayEncodedFieldType& nda) {
    return std::accumulate(
            std::begin(nda.shapes()),
            std::end(nda.shapes()),
            size_t(0),
            [](size_t a, const auto& block) {
                check_block_decoded_size(block);
                return a + block.in_bytes();
            }
    );
}

template<typename NDArrayEncodedFieldType>
std::size_t data_uncompressed_size(const NDArrayEncodedFieldType& nda) {
    return std::accumulate(
            std::begin(nda.values()),
            std::end(nda.values()),
            size_t(0),
            [](size_t a, const auto& block) {
                check_block_decoded_size(block);
                return a + block.in_bytes();
            }
    );
}

template<typename NDArrayEncodedFieldType>
std::size_t bitmap_serialized_size(const NDArrayEncodedFieldType& nda) {
    return nda.sparse_map_bytes();
}

template<typename NDArrayEncodedFieldType>
std::size_t ndarray_field_compressed_size(const NDArrayEncodedFieldType& nda) {
    return shape_compressed_size(nda) + data_compressed_size(nda) + bitmap_serialized_size(nda);
}

template<typename NDArrayEncodedFieldType>
std::size_t uncompressed_size(const NDArrayEncodedFieldType& nda) {
    return shape_uncompressed_size(nda) + data_uncompressed_size(nda) + bitmap_serialized_size(nda);
}

inline std::size_t field_compressed_size(const EncodedFieldImpl& field) {
    switch (field.encoding_case()) {
    case EncodedFieldType::NDARRAY:
        return ndarray_field_compressed_size(field.ndarray());
    default:
        util::raise_rte("Unsupported encoding {}", field.DebugString());
    }
}

inline std::size_t field_uncompressed_size(const EncodedFieldImpl& field) {
    switch (field.encoding_case()) {
    case EncodedFieldType::NDARRAY:
        return uncompressed_size(field.ndarray());
    default:
        util::raise_rte("Unsupported encoding {}", field.DebugString());
    }
}

template<typename FieldCollectionType>
std::size_t segment_compressed_size(const FieldCollectionType& fields) {
    std::size_t total = 0;
    for (auto& field : fields) {
        switch (field.encoding_case()) {
        case arcticdb::proto::encoding::EncodedField::kNdarray: {
            auto compressed_sz = ndarray_field_compressed_size(field.ndarray());
            ARCTICDB_TRACE(log::storage(), "From segment header: compressed: {}", compressed_sz);
            total += compressed_sz;
            break;
        }
            /*  case arcticdb::proto::encoding::EncodedField::kDictionary:
                  total += compressed_size(field.dictionary());
                  break;*/
        default:
            util::raise_rte("Unsupported encoding in {}", util::format(field));
        }
    }
    return total;
}

} // namespace arcticdb::encoding_sizes
