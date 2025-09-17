/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstddef> // for std::size_t
#include <variant>
#include <tuple>

namespace arcticdb::util {

template<class... Ts>
struct overload : Ts... {
    using Ts::operator()...;
};
template<class... Ts>
overload(Ts...) -> overload<Ts...>;

template<typename>
struct is_tuple : std::false_type {};

template<typename... T>
struct is_tuple<std::tuple<T...>> : std::true_type {};

template<typename T>
constexpr bool is_tuple_v = is_tuple<T>::value;

template<std::size_t... I, typename Tuple, class... Ts>
requires is_tuple_v<std::decay_t<Tuple>>
auto variant_match(std::index_sequence<I...>, Tuple&& tuple, Ts&&... ts) {
    return std::visit(overload{std::forward<Ts>(ts)...}, std::get<I>(std::forward<Tuple>(tuple))...);
}

template<class Variant, class... Ts>
auto variant_match(Variant&& v, Ts&&... ts) {
    if constexpr (is_tuple_v<std::decay_t<Variant>>) {
        static constexpr auto tuple_size = std::tuple_size_v<std::decay_t<Variant>>;
        // For supporting tuple of variants, e.g. variant_match(std::make_tuple(std::variant<...>(...),
        // std::variant<...>(...)), [](auto &&a, auto &&b){...})
        return variant_match(std::make_index_sequence<tuple_size>{}, std::forward<Variant>(v), std::forward<Ts>(ts)...);
    } else
        return std::visit(overload{std::forward<Ts>(ts)...}, std::forward<Variant>(v));
}

} // namespace arcticdb::util
