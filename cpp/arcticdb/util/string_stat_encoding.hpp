/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>

#include <cstdint>
#include <string>
#include <string_view>

namespace arcticdb {

struct ColumnWithStrings;

// A column stats segment holds one fixed-width cell per statistic, so a string min/max cannot store
// the whole value. Instead we keep the first truncated_prefix_bytes UTF-8 bytes in the high bytes
// of a uint64_t and the byte length in the low byte.
//
// The layout is chosen so that comparing two packed stats as unsigned integers is exactly bytewise
// comparison of the strings they were built from. The prefix bytes dominate, and where two values
// share a full-width prefix the length byte breaks the tie as shorter < longer < truncated, which is
// what bytewise string comparison does. Packing is therefore monotonic, so for a query q:
//     pack(q) < stored_min  =>  q sorts below every value in the row slice
//     pack(q) > stored_max  =>  q sorts above every value in the row slice
// Either lets a reader prune the row slice without ever giving a wrong answer.
constexpr size_t truncated_prefix_bytes = 7;

// Length byte meaning "the source string was longer than truncated_prefix_bytes, so the prefix
// stored here is incomplete". Deliberately the largest byte value so that a truncated stat sorts
// above any exact-length value sharing its prefix.
constexpr uint8_t truncated_string_length_marker = 255;

// Stencil for pulling the length byte out of a packed stat: ones in the low byte, zeros in the other
// seven, so ANDing keeps that byte and erases everything above it.
constexpr uint64_t rightmost_byte_only_mask = 0xFFULL;

// Packs the first truncated_prefix_bytes bytes of utf8_str into the high bytes, zero padded if
// shorter, and the byte length into the low byte. Truncation can split a multi-byte codepoint, which
// is harmless because every comparison of packed stats is bytewise.
uint64_t pack_string_stat(std::string_view utf8_str);

// Takes a string exactly as the column's string pool holds it. Fixed-width pools null pad to the
// column width, which is stripped here, and a fixed-width UTF pool holds UTF-32, which is transcoded
// to UTF-8 here. All other string types already hold UTF-8 (or ASCII, which is a subset), so every
// packed stat is comparable regardless of the source column's type.
uint64_t pack_string(std::string_view raw_pool_string, entity::DataType raw_pool_string_data_type);

// Resolves offset_in_pool against the column's string pool and packs what it finds. Raises if the
// offset has no pool entry, so callers must filter out the None and NaN placeholder offsets first.
uint64_t pack_string_at_offset(const ColumnWithStrings& column, entity::position_t offset_in_pool);

// The prefix bytes are not necessarily valid UTF-8, since truncation can split a codepoint. Callers
// that need to display them must decode permissively.
struct UnpackedStringStat {
    std::string text;
    bool was_truncated;
};

UnpackedStringStat unpack_string(uint64_t packed);

} // namespace arcticdb
