/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/string_stat_encoding.hpp>

#include <boost/locale.hpp>

#include <algorithm>

namespace arcticdb {

namespace {
constexpr size_t calculate_shift_bits(size_t i) { return (truncated_prefix_bytes - i) * 8; }

// Fixed-width pools pad with null codepoints, so trailing nulls are dropped. Interior ones are data
// and must survive, which is why this cannot use util::utf32_to_u8: that stops at the *first* null,
// so "a\0b" would pack as "a", and since the engine's equality path does match an embedded null in a
// fixed-width column, a query for the whole value would sort above the stored max and prune a row
// slice that contains it.
std::string utf32_to_u8_stripping_padding(std::string_view raw_str) {
    std::u32string_view utf32{reinterpret_cast<const char32_t*>(raw_str.data()), raw_str.size() / 4};
    const auto last_value = utf32.find_last_not_of(char32_t{0});
    utf32 = utf32.substr(0, last_value == std::u32string_view::npos ? 0 : last_value + 1);
    return boost::locale::conv::utf_to_utf<char>(utf32.data(), utf32.data() + utf32.size());
}

constexpr uint64_t shift_to_byte_in_packed(char byte, size_t i) {
    const auto unsigned_byte = static_cast<uint8_t>(byte);
    const auto shift_left_bits = calculate_shift_bits(i);
    return (static_cast<uint64_t>(unsigned_byte) << shift_left_bits);
}
} // namespace

uint64_t pack_string_stat(std::string_view utf8_str) {
    const auto bytes_kept = std::min(utf8_str.size(), truncated_prefix_bytes);
    uint64_t packed{0};

    for (size_t i = 0; i < bytes_kept; ++i) {
        const uint64_t byte_in_packed = shift_to_byte_in_packed(utf8_str[i], i);
        packed |= byte_in_packed;
    }

    const uint64_t length_byte =
            utf8_str.size() > truncated_prefix_bytes ? truncated_string_length_marker : utf8_str.size();

    return packed | length_byte;
}

uint64_t pack_string_stat(std::string_view raw_str, entity::DataType data_type) {
    // UTF_FIXED64 is the only type whose pool holds null-padded UTF-32. Not is_fixed_string_type:
    // ASCII_FIXED64 pads at one byte per character, and reinterpreting that as UTF-32 packs garbage.
    if (data_type == entity::DataType::UTF_FIXED64) {
        return pack_string_stat(utf32_to_u8_stripping_padding(raw_str));
    }
    return pack_string_stat(raw_str);
}

UnpackedStringStat unpack_string_stat(uint64_t packed) {
    const auto length_byte = static_cast<size_t>(static_cast<uint8_t>(packed & rightmost_byte_only_mask));
    const bool was_truncated = (length_byte == truncated_string_length_marker);
    const auto length = was_truncated ? truncated_prefix_bytes : std::min(length_byte, truncated_prefix_bytes);

    std::string prefix(length, '\0');

    for (size_t i = 0; i < length; ++i) {
        const auto shift_right_bits = calculate_shift_bits(i);
        const auto packed_shifted_right = (packed >> shift_right_bits);
        prefix[i] = static_cast<char>(packed_shifted_right & rightmost_byte_only_mask);
    }
    return {std::move(prefix), was_truncated};
}

} // namespace arcticdb
