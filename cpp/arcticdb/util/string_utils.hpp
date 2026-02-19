/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <string_view>
#include <array>
#include <algorithm>
#include <vector>
#include <string>
#include <cstdint>

namespace arcticdb::util {

inline int64_t num_from_strv(std::string_view strv) {
    uint64_t val = 0;
    for (auto c : strv)
        val = val * 10 + (c - '0');

    return *reinterpret_cast<int64_t*>(&val);
};

inline std::string to_lower(std::string&& str) {
    std::ranges::transform(str, str.begin(), ::tolower);
    return str;
}

template<uint32_t expected_size>
std::array<std::string_view, expected_size> split_to_array(std::string_view strv, char delim) {
    std::array<std::string_view, expected_size> output;
    auto first = strv.begin();
    auto last = strv.cend();
    auto count = 0u;
    while (first != strv.end() && count < expected_size) {
        const auto second = std::find(first, last, delim);
        if (first != second)
            output[count++] = strv.substr(std::distance(strv.begin(), first), std::distance(first, second));

        if (second == strv.end())
            break;

        first = std::next(second);
    }

    return output;
}

inline std::vector<std::string_view> split_to_vector(std::string_view strv, char delim) {
    std::vector<std::string_view> output;
    auto first = strv.begin();
    auto last = strv.cend();
    while (first != strv.end()) {
        const auto second = std::find(first, last, delim);
        if (first != second)
            output.emplace_back(strv.substr(std::distance(strv.begin(), first), std::distance(first, second)));

        if (second == strv.end())
            break;

        first = std::next(second);
    }

    return output;
}

constexpr char escape_char = '~';

inline std::string_view strv_from_pos(const std::string& str, size_t start, size_t length) {
    return std::string_view{str.data() + start, length};
}

std::string safe_encode(const std::string& value);

std::string safe_decode(const std::string& value);

// We store fixed-width (i.e. UTF-32 strings) in the string pool alongside UTF-8 strings, and the string pool does not
// know which strings are which. Therefore methods like get_const_view return a std::string_view regardless. If the
// string is UTF-32, this converts that view into a UTF-8 string
std::string utf32_to_u8(std::string_view strv);

struct TransparentStringHash {
    using is_transparent = void; // enable heterogeneous overloads
    using is_avalanching = void; // mark class as high quality avalanching hash
    [[nodiscard]] uint64_t operator()(std::string_view str) const noexcept;
};

} // namespace arcticdb::util