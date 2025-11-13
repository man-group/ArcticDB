/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <type_traits>
#include <arcticdb/entity/types.hpp>

namespace arcticdb::util {

template<template<typename...> class TT, typename T>
struct is_instantiation_of : std::false_type {};

template<template<typename...> class TT, typename... Ts>
struct is_instantiation_of<TT, TT<Ts...>> : std::true_type {};

template<typename T, template<typename...> class TT>
inline constexpr bool is_instantiation_of_v = is_instantiation_of<TT, T>::value;

template<typename T, template<typename...> class TT>
concept instantiation_of = is_instantiation_of_v<T, TT>;

template<typename T, typename... U>
concept any_of = std::disjunction_v<std::is_same<T, U>...>;

template<typename T>
concept contiguous_type_tagged_data = requires(T t) {
    instantiation_of<T, std::pair>;
    std::ranges::contiguous_range<typename T::first_type>;
    std::same_as<typename T::second_type, entity::TypeDescriptor>;
};

template<typename Given, typename Base>
concept forwarding_ref_to = std::same_as<std::decay_t<Given>, Base>;

} // namespace arcticdb::util
