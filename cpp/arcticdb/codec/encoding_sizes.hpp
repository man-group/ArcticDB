/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <numeric>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb::encoding_size {
    inline std::size_t shape_compressed_size(const arcticdb::proto::encoding::NDArrayEncodedField &nda) {
        return std::accumulate(std::begin(nda.shapes()), std::end(nda.shapes()), size_t(0),
                               [] (size_t a, const arcticdb::proto::encoding::Block& block) { return a + block.out_bytes(); });
    }

    inline std::size_t data_compressed_size(const arcticdb::proto::encoding::NDArrayEncodedField &nda) {
        return std::accumulate(std::begin(nda.values()), std::end(nda.values()), size_t(0),
                               [] (size_t a, const arcticdb::proto::encoding::Block& block) { return a + block.out_bytes(); });
    }

    inline std::size_t shape_uncompressed_size(const arcticdb::proto::encoding::NDArrayEncodedField &nda) {
        return std::accumulate(std::begin(nda.shapes()), std::end(nda.shapes()), size_t(0),
                               [] (size_t a, const arcticdb::proto::encoding::Block& block) { return a + block.in_bytes(); });
    }

    inline std::size_t data_uncompressed_size(const arcticdb::proto::encoding::NDArrayEncodedField &nda) {
        return  std::accumulate(std::begin(nda.values()), std::end(nda.values()), size_t(0),
                               [] (size_t a, const arcticdb::proto::encoding::Block& block) { return a + block.in_bytes(); });
    }

    inline std::size_t bitmap_serialized_size(const arcticdb::proto::encoding::NDArrayEncodedField &nda) {
        return  nda.sparse_map_bytes();
    }

    inline std::size_t compressed_size(const arcticdb::proto::encoding::NDArrayEncodedField &nda) {
        return shape_compressed_size(nda) + data_compressed_size(nda) + bitmap_serialized_size(nda);
    }

    inline std::size_t compressed_size(const arcticdb::proto::encoding::DictionaryEncodedField &d) {
        return (d.has_values() ? compressed_size(d.values()) : 0) +
            (d.has_positions() ? compressed_size(d.positions()) : 0);
    }

inline std::size_t uncompressed_size(const arcticdb::proto::encoding::NDArrayEncodedField &nda) {
    return shape_uncompressed_size(nda) + data_uncompressed_size(nda) + bitmap_serialized_size(nda);
}

inline std::size_t uncompressed_size(const arcticdb::proto::encoding::DictionaryEncodedField &d) {
    return (d.has_values() ? uncompressed_size(d.values()) : 0) +
        (d.has_positions() ? uncompressed_size(d.positions()) : 0);
}

inline std::size_t compressed_size(const arcticdb::proto::encoding::EncodedField &field) {
    switch (field.encoding_case()) {
        case arcticdb::proto::encoding::EncodedField::kNdarray:
            return compressed_size(field.ndarray());
            default:
                util::raise_error_msg("Unsupported encoding {}", field);
    }
}

std::size_t compressed_size(const arcticdb::proto::encoding::SegmentHeader &sh);

std::size_t uncompressed_size(const arcticdb::proto::encoding::SegmentHeader &sh);

std::size_t represented_size(const arcticdb::proto::encoding::SegmentHeader& sh, size_t total_rows);
} // namespace encoding_size
