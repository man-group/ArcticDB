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
} // namespace

uint64_t pack_string_stat(std::string_view utf8_str) {
    uint64_t packed{0};

    std::memcpy(&packed, utf8_str.data(), std::min(utf8_str.size(), truncated_prefix_bytes));
    packed = byteswap_to_most_significant_first(packed) & ~least_significant_byte_only_mask;

    const uint64_t length_byte =
            utf8_str.size() > truncated_prefix_bytes ? truncated_string_length_marker : utf8_str.size();

    return packed | length_byte;
}

uint64_t pack_string(std::string_view raw_pool_string, entity::DataType raw_pool_string_data_type) {
    // UTF_FIXED64 is the only type whose pool holds null-padded UTF-32. Not is_fixed_string_type:
    // ASCII_FIXED64 pads at one byte per character, and reinterpreting that as UTF-32 packs garbage.
    if (raw_pool_string_data_type == entity::DataType::UTF_FIXED64) {
        return pack_string_stat(util::utf32_to_u8(raw_pool_string));
    }

    if (raw_pool_string_data_type == entity::DataType::ASCII_FIXED64) {
        return pack_string_stat(util::strip_ascii_padding(raw_pool_string));
    }

    return pack_string_stat(raw_pool_string);
}

uint64_t pack_string_at_offset(const ColumnWithStrings& column, entity::position_t offset_in_pool) {
    const auto raw_pool_string = column.string_at_offset(offset_in_pool);
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            raw_pool_string.has_value(),
            "Missing string pool entry at offset {} generating column stats",
            offset_in_pool
    );
    return pack_string(*raw_pool_string, column.column_->type().data_type());
}

} // namespace arcticdb
