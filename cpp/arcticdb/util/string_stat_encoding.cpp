/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/string_stat_encoding.hpp>

#include <arcticdb/column_store/column.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/util/string_utils.hpp>

#include <algorithm>
#include <cstring>

#ifdef _MSC_VER
#include <cstdlib>
#endif

namespace arcticdb {

namespace {
uint64_t byteswap_to_most_significant_first(uint64_t value) {
#ifdef _MSC_VER
    return _byteswap_uint64(value);
#else
    return __builtin_bswap64(value);
#endif
}

constexpr uint64_t shift_to_byte_in_packed(char byte, size_t i) {
    const auto unsigned_byte = static_cast<uint8_t>(byte);
    const auto shift_left_bits = (truncated_prefix_bytes - i) * 8;
    return (static_cast<uint64_t>(unsigned_byte) << shift_left_bits);
}
} // namespace

uint64_t pack_string_stat(std::string_view utf8_str) {
    uint64_t packed{0};

    if (utf8_str.size() > truncated_prefix_bytes) {
        std::memcpy(&packed, utf8_str.data(), sizeof(uint64_t));
        packed = byteswap_to_most_significant_first(packed) & ~least_significant_byte_only_mask;
        return packed | truncated_string_length_marker;
    }

    for (size_t i = 0; i < utf8_str.size(); ++i) {
        const uint64_t byte_in_packed = shift_to_byte_in_packed(utf8_str[i], i);
        packed |= byte_in_packed;
    }

    return packed | utf8_str.size();
}

uint64_t pack_string(std::string_view raw_pool_string, entity::DataType raw_pool_string_data_type) {
    switch (raw_pool_string_data_type) {
    case entity::DataType::UTF_FIXED64:
        return pack_string_stat(util::utf32_to_u8(raw_pool_string));
    case entity::DataType::ASCII_FIXED64:
        return pack_string_stat(util::strip_ascii_padding(raw_pool_string));
    case entity::DataType::UTF_DYNAMIC64:
    case entity::DataType::ASCII_DYNAMIC64:
        return pack_string_stat(raw_pool_string);
    default:
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                "Unexpected data type {} in pack_string", raw_pool_string_data_type
        );
    }
}

} // namespace arcticdb
