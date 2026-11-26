/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <concepts>
#include <limits>
#include <type_traits>

namespace arcticdb::util {

namespace detail {
template<typename T>
using integral_of_t =
        typename std::conditional_t<std::is_enum_v<T>, std::underlying_type<T>, std::type_identity<T>>::type;

template<typename From, typename To>
concept losslessly_convertible_integral_to =
        std::integral<From> && std::integral<To> && (std::is_unsigned_v<From> || std::is_signed_v<To>) &&
        std::numeric_limits<From>::digits <= std::numeric_limits<To>::digits;
} // namespace detail

template<typename From, typename To>
concept losslessly_convertible_to = detail::losslessly_convertible_integral_to<detail::integral_of_t<From>, To>;

template<typename To, typename From>
requires losslessly_convertible_to<From, To>
constexpr To lossless_cast(From value) {
    return static_cast<To>(value);
}

} // namespace arcticdb::util
