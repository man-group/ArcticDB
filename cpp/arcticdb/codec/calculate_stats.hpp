/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <cstddef>

#include <cstddef>
#include <cstdint>
#include <numeric>
#include <algorithm>

#include <arcticdb/codec/fastlanes_common.hpp>
#include <arcticdb/log/log.hpp>

#include <cstddef>
#include <cstdint>
#include <numeric>
#include <algorithm>

#include <arcticdb/codec/fastlanes_common.hpp>
#include <arcticdb/log/log.hpp>
#include <immintrin.h>
#include <ankerl/unordered_dense.h>

namespace arcticdb {


template <typename T>
bool is_constant(const T* data, size_t num_rows) {
    if(num_rows == 0)
        return true;

    const T value = data[0];
    constexpr size_t CHUNK_SIZE = 64;

    auto loops = num_rows / CHUNK_SIZE;
    for(auto i = 0UL; i < loops; ++i) {
        size_t chunk_mismatch = 0;
        #pragma clang loop vectorize(enable) interleave(enable)
        for(size_t j = 0; j < CHUNK_SIZE; ++j) {
            chunk_mismatch |= (data[i + j] != value);
        }

        if(chunk_mismatch)
            return false;
    }

    auto remainder = num_rows - (loops * CHUNK_SIZE);
    for(auto i = num_rows - remainder; i < num_rows; ++i) {
        if(data[i] != value)
            return false;
    }

    return true;
}

#ifdef ARCTICDB_USE_SIMD
template <typename T>
bool is_constant_simd(const T* data, size_t num_rows) {
    if(num_rows == 0)
        return true;

    const T value = *data;
    // Broadcast value to vector register
    __m256i val_vec = _mm256_set1_epi64x(value);

    // Process 4 elements at once using AVX2
    size_t vec_size = num_rows / 4;
    for(size_t i = 0; i < vec_size; i += 4) {
        __m256i data_vec = _mm256_loadu_si256((__m256i*)(data + i));
        __m256i cmp = _mm256_cmpeq_epi64(data_vec, val_vec);
        if(_mm256_movemask_pd((__m256d)cmp) != 0xF)
            return false;
    }

    // Handle remainder
    for(size_t i = vec_size * 4; i < num_rows; ++i) {
        if(data[i] != value)
            return false;
    }
    return true;
}
#endif
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

template <typename T>
size_t count_unique(T* data, size_t num_rows) {
    ankerl::unordered_dense::set<T> unique(data, data + num_rows);
    return unique.size();
}

} //namespace arcticdb
