/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <type_traits>
#include <concepts>
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

template<class T>
concept tensor = requires(T a, ssize_t i) {
    typename T::value_type;
    { a.ndim() } -> std::convertible_to<ssize_t>;
    { a.size() } -> std::convertible_to<ssize_t>;
    { a.itemsize() } -> std::convertible_to<ssize_t>;
    { a.strides(i) } -> std::convertible_to<ssize_t>;
    { a.shape(i) } -> std::convertible_to<ssize_t>;
};

template<class T>
concept arithmetic = std::integral<T> || std::floating_point<T>;

template<class T>
concept arithmetic_tensor = tensor<T> && arithmetic<typename T::value_type>;
} // namespace arcticdb::util
