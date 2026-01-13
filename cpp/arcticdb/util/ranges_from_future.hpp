/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <vector>

/*
 * utils behaving similarly to C++20 range features for easy replacement in the future
 */

namespace arcticdb::utils {

/**
 * Pre-C++20 emulation of ranges::views::elements, with the ability to alter the output type.
 * @tparam OutType Override the output type. Must have a c'tor that can accept the element's original type.
 */
template<
        size_t n, typename RangeOfPairs,
        typename OutType = std::remove_const_t<typename std::tuple_element<n, typename RangeOfPairs::value_type>::type>>
std::vector<OutType> copy_of_elements(const RangeOfPairs& rop) {
    std::vector<OutType> as_vector;
    as_vector.reserve(rop.size());
    for (const auto& pair : rop) {
        as_vector.emplace_back(std::get<n>(pair));
    }
    return as_vector;
}

/**
 * Upgradeable to std::views::keys(). This implementation has to copy unfortunately.
 */
template<typename Map>
inline auto keys(const Map& map) {
    return copy_of_elements<0>(map);
}

/**
 * Upgradeable to std::views::values(). This implementation has to copy unfortunately.
 */
template<typename Map>
inline auto values(const Map& map) {
    return copy_of_elements<1>(map);
}

/**
 * Like std::views::values(), but alters the element type. See copy_of_elements' documentation.
 */
// No direct upgrade path to ranges, but trivial given how copy_of_elements is implemented.
template<typename OutType, typename Map>
inline auto copy_of_values_as(const Map& map) {
    return copy_of_elements<1, Map, OutType>(map);
}

} // namespace arcticdb::utils