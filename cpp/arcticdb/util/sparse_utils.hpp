/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/util/preprocess.hpp>
#include <arcticdb/util/type_handler.hpp>
#include <arcticdb/util/bitset.hpp>
#include <arcticdb/util/constants.hpp>
#include <arcticdb/util/decode_path_data.hpp>
#include <arcticdb/util/type_traits.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <fmt/ranges.h>

#include <bitmagic/bmserial.h>

namespace arcticdb::util {

template<typename RawType>
void densify_buffer_using_bitmap(
        const util::BitSet& block_bitset, arcticdb::ChunkedBuffer& dense_buffer, const uint8_t* sparse_ptr
) {
    auto en = block_bitset.first();
    auto en_end = block_bitset.end();
    auto element_size = sizeof(RawType);
    auto dense_ptr = dense_buffer.data();
    util::check(
            block_bitset.count() * element_size <= dense_buffer.bytes(),
            "Dense buffer of size {} cannot store {} * {} bytes",
            dense_buffer.bytes(),
            block_bitset.count(),
            element_size
    );

    size_t pos_in_dense_buffer = 0;
    while (en < en_end) {
        auto dense_index_in_bitset = *en;
        // TODO: add asserts
        auto copy_to = dense_ptr + pos_in_dense_buffer * element_size;
        auto copy_from = sparse_ptr + dense_index_in_bitset * element_size;
        ARCTICDB_TRACE(
                log::version(),
                "densify: copying from value: {}, copying to {}, element at pos: {}",
                copy_from - sparse_ptr,
                copy_to - dense_ptr,
                *(reinterpret_cast<const RawType*>(copy_from))
        );
        memcpy(copy_to, copy_from, element_size);
        ++pos_in_dense_buffer;
        ++en;
    }
}

template<typename RawType>
void expand_dense_buffer_using_bitmap(const BitMagic& bv, const uint8_t* dense_ptr, uint8_t* sparse_ptr) {
    auto en = bv.first();
    auto en_end = bv.end();
    auto element_sz = sizeof(RawType);

    size_t pos_in_dense_buffer = 0;
    while (en < en_end) {
        auto dense_index_in_bitset = *en;
        auto copy_to = sparse_ptr + dense_index_in_bitset * element_sz;
        auto copy_from = dense_ptr + pos_in_dense_buffer * element_sz;
        auto bytes_to_copy = element_sz;
        ARCTICDB_TRACE(
                log::version(),
                "expand: copying from value: {}, copying to {}, element at pos: {}",
                copy_from - dense_ptr,
                copy_to - sparse_ptr,
                *(reinterpret_cast<const RawType*>(copy_from))
        );
        memcpy(copy_to, copy_from, bytes_to_copy);
        ++pos_in_dense_buffer;
        ++en;
    }
}

template<typename TagType>
requires util::instantiation_of<TagType, TypeDescriptorTag>
void default_initialize(uint8_t* data, const size_t bytes) {
    using RawType = typename TagType::DataTypeTag::raw_type;
    const auto num_rows ARCTICDB_UNUSED = bytes / sizeof(RawType);
    constexpr auto data_type = TagType::DataTypeTag::data_type;
    auto type_ptr ARCTICDB_UNUSED = reinterpret_cast<RawType*>(data);
    if constexpr (is_sequence_type(data_type)) {
        std::fill_n(type_ptr, num_rows, not_a_string());
    } else if constexpr (is_floating_point_type(data_type)) {
        std::fill_n(type_ptr, num_rows, std::numeric_limits<RawType>::quiet_NaN());
    } else if constexpr (is_time_type(data_type)) {
        std::fill_n(type_ptr, num_rows, NaT);
    } else if constexpr (is_integer_type(data_type) || is_bool_type(data_type)) {
        std::memset(data, 0, bytes);
    }
}

/// Initialize a buffer either using a custom default value or using a predefined default value for the type
/// @param[in] default_value Variant holding either a value of the raw type for the type tag or std::monostate
template<typename TagType>
requires util::instantiation_of<TagType, TypeDescriptorTag>
void initialize(uint8_t* data, const size_t bytes, const std::optional<Value>& default_value) {
    using RawType = typename TagType::DataTypeTag::raw_type;
    if (default_value) {
        DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                default_value->descriptor() == TagType::type_descriptor(),
                "Mismatched default value type"
        );
        const auto num_rows = bytes / sizeof(RawType);
        std::fill_n(reinterpret_cast<RawType*>(data), num_rows, default_value->get<RawType>());
    } else {
        default_initialize<TagType>(data, bytes);
    }
}

template<typename TagType>
requires util::instantiation_of<TagType, TypeDescriptorTag>
void initialize(ChunkedBuffer& buffer, size_t offset, size_t bytes, const std::optional<Value>& default_value) {
    auto blocks = buffer.byte_blocks_at(offset, bytes);
    for (auto [data, size] : blocks) {
        initialize<TagType>(data, size, default_value);
    }
}

[[nodiscard]] util::BitSet scan_object_type_to_sparse(const PyObject* const* ptr, size_t rows_to_write);

template<typename RawType>
ChunkedBuffer scan_floating_point_to_sparse(RawType* ptr, size_t rows_to_write, util::BitMagic& block_bitset) {
    auto scan_ptr = ptr;
    for (size_t idx = 0; idx < rows_to_write; ++idx, ++scan_ptr) {
        block_bitset[bv_size(idx)] = !isnan(*scan_ptr);
    }

    const auto bytes = block_bitset.count() * sizeof(RawType);
    auto dense_buffer = ChunkedBuffer::presized(bytes);
    auto start = reinterpret_cast<const uint8_t*>(ptr);
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

util::BitMagic truncate_sparse_map(const util::BitMagic& input_sparse_map, size_t start_row, size_t end_row);

inline void dump_bitvector(const util::BitMagic& bv) {
    auto en = bv.first();
    auto en_end = bv.end();

    std::vector<uint32_t> vals;
    while (en < en_end) {
        auto idx = *en;
        vals.push_back(idx);
        ++en;
    }
    ARCTICDB_DEBUG(log::version(), "Bit vector values {}", vals);
}

} // namespace arcticdb::util
