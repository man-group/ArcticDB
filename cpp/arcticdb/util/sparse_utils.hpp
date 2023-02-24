/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/util/preprocess.hpp>

#include <bitmagic/bm.h>
#include <bitmagic/bmserial.h>

#include <algorithm>
#include <cstdint>

namespace arcticdb::util {

#define NaT 0x8000000000000000

template <typename RawType>
inline void densify_buffer_using_bitmap(BitMagic &block_bitset, arcticdb::ChunkedBuffer &dense_buffer, const uint8_t* sparse_ptr) {
    bm::bvector<>::enumerator en = block_bitset.first();
    bm::bvector<>::enumerator en_end = block_bitset.end();
    auto element_size = sizeof(RawType);
    auto dense_ptr = dense_buffer.data();
    util::check(block_bitset.count() * element_size <= dense_buffer.bytes(),
                "Dense buffer of size {} cannot store {} * {} bytes",
                dense_buffer.bytes(), block_bitset.count(), element_size);

    size_t pos_in_dense_buffer = 0;
    while (en < en_end) {
        auto dense_index_in_bitset = *en;
        // TODO: add asserts
        auto copy_to = dense_ptr + pos_in_dense_buffer * element_size;
        auto copy_from = sparse_ptr + dense_index_in_bitset * element_size;
        ARCTICDB_TRACE(log::version(), "densify: copying from value: {}, copying to {}, element at pos: {}",
                             copy_from - sparse_ptr, copy_to - dense_ptr, *(reinterpret_cast<const RawType *>(copy_from)));
        memcpy(copy_to, copy_from, element_size);
        ++pos_in_dense_buffer;
        ++en;
    }
}

template <typename RawType>
inline void expand_dense_buffer_using_bitmap(util::BitMagic &bv, const uint8_t *dense_ptr,
                                             uint8_t *sparse_ptr) {
    bm::bvector<>::enumerator en = bv.first();
    bm::bvector<>::enumerator en_end = bv.end();
    auto element_sz = sizeof(RawType);

    size_t pos_in_dense_buffer = 0;
    while (en < en_end) {
        auto dense_index_in_bitset = *en;
        auto copy_to = sparse_ptr + dense_index_in_bitset * element_sz;
        auto copy_from = dense_ptr + pos_in_dense_buffer * element_sz;
        auto bytes_to_copy = element_sz;
        ARCTICDB_TRACE(log::version(), "expand: copying from value: {}, copying to {}, element at pos: {}",
                             copy_from - dense_ptr, copy_to - sparse_ptr, *(reinterpret_cast<const RawType *>(copy_from)));
        memcpy(copy_to, copy_from, bytes_to_copy);
        ++pos_in_dense_buffer;
        ++en;
    }
}

template <typename TagType>
void default_initialize(uint8_t* data, size_t bytes) {
    using RawType = typename TagType::DataTypeTag::raw_type;
    const auto num_rows ARCTICDB_UNUSED = bytes / sizeof(RawType);

    constexpr auto data_type =TagType::DataTypeTag::data_type;
    auto type_ptr ARCTICDB_UNUSED = reinterpret_cast<RawType*>(data);
    if constexpr (is_sequence_type(data_type)) {
        std::fill(type_ptr, type_ptr + num_rows, not_a_string());
    }
    else if constexpr (is_floating_point_type(data_type)) {
        std::fill(type_ptr, type_ptr + num_rows, std::numeric_limits<double>::quiet_NaN());
    }
    else if constexpr (is_time_type(data_type)) {
        std::fill(type_ptr, type_ptr + num_rows, NaT);
    } else {
        std::fill(data, data + bytes, 0);
    }
}

template <typename RawType>
ChunkedBuffer scan_floating_point_to_sparse(
    RawType* ptr,
    size_t rows_to_write,
    util::BitMagic& block_bitset) {
    auto scan_ptr = ptr;
    for (size_t idx = 0; idx < rows_to_write; ++idx, ++scan_ptr) {
        block_bitset[idx] = isnan(*scan_ptr) <= 0;
    }

    const auto bytes = block_bitset.count() * sizeof(RawType);
    auto dense_buffer = ChunkedBuffer::presized(bytes);
    auto start = reinterpret_cast<const uint8_t *>(ptr);
    densify_buffer_using_bitmap<RawType>(block_bitset, dense_buffer, start);
    return dense_buffer;
}

inline util::BitMagic deserialize_bytes_to_bitmap(const std::uint8_t*& input, size_t bytes_in_sparse_bitmap) {
    util::BitMagic bv;
    bm::deserialize(bv, input);
    ARCTICDB_DEBUG(log::version(), "count in bitvector while decoding: {}", bv.count());
    input += bytes_in_sparse_bitmap;
    return bv;
}

inline void dump_bitvector(const util::BitMagic& bv) {
    bm::bvector<>::enumerator en = bv.first();
    bm::bvector<>::enumerator en_end = bv.end();

    std::vector<uint32_t> vals;
    while (en < en_end) {
        auto idx = *en;
        vals.push_back(idx);
        ++en;
    }
    log::version().info("Bit vector values {}", vals);
}


}