/* Copyright 2023 Man Group Operations Limited
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
#include <numeric>

namespace arcticdb::encoding_sizes {

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
            [](size_t a, const auto& block) { return a + block.in_bytes(); }
    );
}

template<typename NDArrayEncodedFieldType>
std::size_t data_uncompressed_size(const NDArrayEncodedFieldType& nda) {
    return std::accumulate(
            std::begin(nda.values()),
            std::end(nda.values()),
            size_t(0),
            [](size_t a, const auto& block) { return a + block.in_bytes(); }
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
