#pragma once

#include <cstddef>
#include <cstdint>
#include <numeric>
#include <algorithm>

#include <arcticdb/codec/fastlanes_common.hpp>
#include <arcticdb/log/log.hpp>


namespace arcticdb {

#ifndef WIN23
template <typename T>
uint64_t leftmost_bit(T t) {
    if (t == 0)
        return 0;

    if constexpr(sizeof(T) == 8) {
        return (Helper<T>::num_bits - 1) - __builtin_clzll(t);
    } else {
        return (Helper<T>::num_bits - 1) - __builtin_clz(static_cast<uint32_t>(t));
    }
}

#else
#include <iostream>
#include <intrin.h>

template <typename T>
size_t leftmost_bit(size_t x) {
    unsigned long index;
    if (_BitScanReverse64(&index, x)) {
        return index;
    }
    return 0; // No bits are set
}
#endif

template <typename T>
uint8_t msb(T* data) {
    std::array<uint64_t, 8> bits;
    for (auto i = 0UL; i < 1024UL; i += 8) {
        bits[0] = std::max(bits[0], leftmost_bit(data[i]));
        bits[1] = std::max(bits[1], leftmost_bit(data[i + 1]));
        bits[2] = std::max(bits[2], leftmost_bit(data[i + 2]));
        bits[3] = std::max(bits[3], leftmost_bit(data[i + 3]));
        bits[4] = std::max(bits[4], leftmost_bit(data[i + 4]));
        bits[5] = std::max(bits[5], leftmost_bit(data[i + 5]));
        bits[6] = std::max(bits[6], leftmost_bit(data[i + 6]));
        bits[7] = std::max(bits[7], leftmost_bit(data[i + 7]));
    }

    auto it = std::max_element(bits.begin(), bits.end());

    return *it;
}


template <typename T>
uint8_t msb_single(T* data) {
    uint8_t bit = 0;
    for (auto i = 0UL; i < 1024UL; ++i) {
       bit = std::max(bit, leftmost_bit(data[i]));
    }

    return bit;
}

template <typename T>
uint8_t msb_max(T* data) {
    T max = 0;
    for (auto i = 0UL; i < 1024; ++i) {
        max = std::max(max, data[i]);
    }

    return leftmost_bit(max);
}

template <typename T>
std::enable_if<std::is_unsigned<T>::value, std::pair<T, T>>::type min_max(T* data) {
    T max = 0;

    for (auto i = 0UL; i < 1024; ++i) {
        max = std::max(max, data[i]);

    }

    T min = 0;
    for (auto i = 0UL; i < 1024; ++i) {
        min = std::min(min, data[i]);

    }


    return {min, max};
}

template <typename T>
std::pair<T, T> min_max_pair(std::pair<T, T> left, std::pair<T, T> right) {
    return {std::min(left.first, right.first), std::max(left.second, right.second)};
}

} //namespace arcticdb