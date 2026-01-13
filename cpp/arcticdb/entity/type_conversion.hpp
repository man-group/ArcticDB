/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstdint>

namespace arcticdb {

template<size_t size>
struct UnsignedPromoteImpl;

template<>
struct UnsignedPromoteImpl<8> {
    using type = int64_t;
};

template<>
struct UnsignedPromoteImpl<4> {
    using type = int64_t;
};

template<>
struct UnsignedPromoteImpl<2> {
    using type = int32_t;
};

template<>
struct UnsignedPromoteImpl<1> {
    using type = int16_t;
};

template<typename T>
struct UnsignedPromote {
    using type = typename UnsignedPromoteImpl<sizeof(T)>::type;
};

template<typename T, typename U>
struct Comparable {
    using left_type = typename std::conditional<
            std::is_signed_v<T> == std::is_signed_v<U>, std::common_type_t<T, U>,
            typename std::conditional<std::is_signed_v<T>, T, typename UnsignedPromote<T>::type>::type>::type;

    using right_type = typename std::conditional<
            std::is_signed_v<T> == std::is_signed_v<U>, std::common_type_t<T, U>,
            typename std::conditional<std::is_signed_v<U>, U, typename UnsignedPromote<U>::type>::type>::type;
};

template<>
struct Comparable<double, double> {
    using left_type = double;
    using right_type = double;
};

template<>
struct Comparable<double, float> {
    using left_type = double;
    using right_type = double;
};

template<>
struct Comparable<float, double> {
    using left_type = double;
    using right_type = double;
};

template<>
struct Comparable<float, float> {
    using left_type = float;
    using right_type = float;
};

template<>
struct Comparable<double, uint64_t> {
    using left_type = double;
    using right_type = double;
};

template<>
struct Comparable<uint64_t, double> {
    using left_type = double;
    using right_type = double;
};

template<>
struct Comparable<float, uint64_t> {
    using left_type = float;
    using right_type = float;
};

template<>
struct Comparable<uint64_t, float> {
    using left_type = float;
    using right_type = float;
};

template<>
struct Comparable<uint64_t, uint64_t> {
    using left_type = uint64_t;
    using right_type = uint64_t;
};

template<typename T>
struct Comparable<T, double> {
    using left_type = double;
    using right_type = double;
};

template<typename T>
struct Comparable<double, T> {
    using left_type = double;
    using right_type = double;
};

template<typename T>
struct Comparable<T, float> {
    using left_type = float;
    using right_type = float;
};

template<typename T>
struct Comparable<float, T> {
    using left_type = float;
    using right_type = float;
};

template<typename T>
struct Comparable<uint64_t, T> {
    using left_type = uint64_t;
    using right_type = typename std::conditional<std::is_signed_v<T>, int64_t, T>::type;
};

template<typename T>
struct Comparable<T, uint64_t> {
    using left_type = typename std::conditional<std::is_signed_v<T>, int64_t, T>::type;
    using right_type = uint64_t;
};
} // namespace arcticdb