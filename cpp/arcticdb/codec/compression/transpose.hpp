#pragma once

#include <array>
#include <cstdint>
#include <cstddef>

#include <arcticdb/codec/compression/fastlanes_common.hpp>
#include <arcticdb/log/log.hpp>

namespace arcticdb {


template<typename T>
void transpose(const T* __restrict input, T* __restrict output) {
    using h = Helper<T>;
    constexpr size_t num_bits = h::num_bits;
    constexpr size_t num_lanes = h::num_lanes;
    for (size_t lane = 0; lane < num_lanes; ++lane) {
        const T* src = input + lane * num_bits;

        for (size_t row = 0; row < num_bits; row += 8) {
            output[fastlanes_index(row, lane)]     = src[row];
            output[fastlanes_index(row+1, lane)]   = src[row+1];
            output[fastlanes_index(row+2, lane)]   = src[row+2];
            output[fastlanes_index(row+3, lane)]   = src[row+3];
            output[fastlanes_index(row+4, lane)]   = src[row+4];
            output[fastlanes_index(row+5, lane)]   = src[row+5];
            output[fastlanes_index(row+6, lane)]   = src[row+6];
            output[fastlanes_index(row+7, lane)]   = src[row+7];
        }
    }
}

template<typename T>
void untranspose(const T* input, T* output) {
    using h = Helper<T>;
    constexpr size_t num_bits = h::num_bits;
    constexpr size_t num_lanes = h::num_lanes;
#ifdef __clang__
#pragma clang loop vectorize(enable)
#endif
    for (size_t lane = 0; lane < num_lanes; ++lane) {
        T* target = output + lane * num_bits;
        for(size_t row = 0; row < num_bits; row += 8) {
            target[row] = input[fastlanes_index(row, lane)];
            target[row+1] = input[fastlanes_index(row+1, lane)];
            target[row+2] = input[fastlanes_index(row+2, lane)];
            target[row+3] = input[fastlanes_index(row+3, lane)];
            target[row+4] = input[fastlanes_index(row+4, lane)];
            target[row+5] = input[fastlanes_index(row+5, lane)];
            target[row+6] = input[fastlanes_index(row+6, lane)];
            target[row+7] = input[fastlanes_index(row+7, lane)];
        }
    }
}

} //namespace arcticdb