#pragma once

#include <cstdint>
#include <cstddef>
#include <limits>
#include <type_traits>

namespace arcticdb {

template <typename T, typename U=std::make_unsigned<T>::type>
constexpr U zigzag_encode(T n) {
    constexpr size_t shift = sizeof(T) * std::numeric_limits<uint8_t>::digits - 1;
    return (n << 1) ^ (n >> shift);
}

template <typename T, typename U=std::make_signed<T>::type>
constexpr U zigzag_decode(T n) {
    return (n >> 1) ^ -(n & 1);
}

static_assert(zigzag_decode(zigzag_encode(int64_t{23})) == 23);


} //namespace arcticdb