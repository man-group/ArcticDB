/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/magic_num.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace arcticdb {

// Sizes and offsets read from a stored segment are untrusted. Before reading `bytes` from `data`, check that they lie
// within [data, end) and raise a decode error otherwise, rather than reading past the buffer.
inline void check_input_available(const uint8_t* data, const uint8_t* end, size_t bytes, std::string_view what) {
    const size_t remaining = (data != nullptr && end != nullptr && data <= end) ? static_cast<size_t>(end - data) : 0;
    codec::check<ErrorCode::E_DECODE_ERROR>(
            data != nullptr && bytes <= remaining,
            "Corrupt segment: {} needs {} bytes but only {} remain",
            what,
            bytes,
            remaining
    );
}

// Checks that `bytes` lie within [data, end), then advances data past them.
inline void skip_input(const uint8_t*& data, const uint8_t* end, size_t bytes, std::string_view what) {
    check_input_available(data, end, bytes, what);
    data += bytes;
}

template<typename MagicType>
void check_magic_bounded(const uint8_t*& data, const uint8_t* end) {
    check_input_available(data, end, sizeof(MagicType), "magic number");
    util::check_magic<MagicType>(data);
}

} // namespace arcticdb
