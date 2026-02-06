/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <concepts>

namespace arcticdb::util {
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