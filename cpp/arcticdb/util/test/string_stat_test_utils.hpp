/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/string_stat_encoding.hpp>

#include <algorithm>
#include <cstdint>
#include <string>

namespace arcticdb {

// The prefix bytes are not necessarily valid UTF-8, since truncation can split a codepoint.
struct UnpackedStringStat {
    std::string text;
    bool was_truncated;
};

// The inverse of pack_string. Nothing in the engine reads a packed stat back as text - pruning
// compares packed values directly - so this exists only for tests to assert on what a packed stat
// holds.
inline UnpackedStringStat unpack_string(uint64_t packed) {
    const auto length_byte = static_cast<size_t>(packed & least_significant_byte_only_mask);
    const bool was_truncated = (length_byte == truncated_string_length_marker);
    const auto length = was_truncated ? truncated_prefix_bytes : std::min(length_byte, truncated_prefix_bytes);

    std::string text(length, '\0');

    for (size_t i = 0; i < length; ++i) {
        const auto shift_right_bits = (truncated_prefix_bytes - i) * 8;
        const auto packed_shifted_right = (packed >> shift_right_bits);
        text[i] = static_cast<char>(packed_shifted_right & least_significant_byte_only_mask);
    }
    return {std::move(text), was_truncated};
}

} // namespace arcticdb
