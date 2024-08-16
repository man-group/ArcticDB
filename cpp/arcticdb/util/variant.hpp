/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the
 * file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source
 * License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstddef> // for std::size_t
#include <variant>

namespace arcticdb::util {

template <class... Ts> struct overload : Ts... {
  using Ts::operator()...;
};
template <class... Ts> overload(Ts...) -> overload<Ts...>;

template <typename> struct is_tuple : std::false_type {};

template <typename... T> struct is_tuple<std::tuple<T...>> : std::true_type {};

template <std::size_t... I, class... TupleTs, class... Ts>
auto variant_match(std::index_sequence<I...>, const std::tuple<TupleTs...>& v,
                   Ts... ts) {
  return std::visit(overload{ts...}, std::get<I>(v)...);
}

template <std::size_t... I, class... TupleTs, class... Ts>
auto variant_match(std::index_sequence<I...>, std::tuple<TupleTs...>&& v, Ts... ts) {
  return std::visit(overload{ts...}, std::get<I>(v)...);
}

template <class Variant, class... Ts> auto variant_match(Variant&& v, Ts... ts) {
  if constexpr (is_tuple<std::remove_cv_t<std::remove_reference_t<Variant>>>::value) {
    static constexpr auto tuple_size =
        std::tuple_size<std::remove_cv_t<std::remove_reference_t<decltype(v)>>>::value;
    return variant_match(
        std::make_index_sequence<tuple_size>{}, std::forward<Variant>(v),
        ts...); // For supporting tuple of variants, e.g.
                // variant_match(std::make_tuple(std::variant<...>(...),
                // std::variant<...>(...)), [](auto &&a, auto &&b){...})
  } else
    return std::visit(overload{ts...}, v);
}

template <class Variant, class... Ts> auto variant_match(const Variant&& v, Ts... ts) {
  if constexpr (is_tuple<std::remove_cv_t<std::remove_reference_t<Variant>>>::value) {
    static constexpr auto tuple_size =
        std::tuple_size<std::remove_cv_t<std::remove_reference_t<decltype(v)>>>::value;
    return variant_match(std::make_index_sequence<tuple_size>{},
                         std::forward<Variant>(v), ts...);
  } else
    return std::visit(overload{ts...}, v);
}

} // namespace arcticdb::util
