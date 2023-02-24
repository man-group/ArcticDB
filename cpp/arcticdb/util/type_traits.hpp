/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <type_traits>

namespace arcticdb::util {

template<template<typename...> class TT, typename T>
struct is_instantiation_of : std::false_type {};

template<template<typename...> class TT, typename... Ts>
struct is_instantiation_of<TT, TT<Ts...>> : std::true_type {};

template<typename T, template<typename...> class TT>
inline constexpr bool is_instantiation_of_v = is_instantiation_of<TT, T>::value;

} // namespace arcticdb::util
