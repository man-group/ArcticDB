#pragma once

#include <array>
#include <cstdint>
#include <cstddef>

#include <arcticdb/codec/fastlanes_common.hpp>
#include <arcticdb/log/log.hpp>

namespace arcticdb {

template <typename T>
void transpose(const std::array<T, 1024>& input, std::array<T, 1024>& output) {
    for (std::size_t i = 0; i < 1024; ++i) {
        output[i] = input[transposed_index(i)];
    }
}

template <typename T>
void untranspose(const T*  input, T* output) {
    for (std::size_t i = 0; i < 1024; ++i) {
        output[transposed_index(i)] = input[i];
    }
}

/*template <typename T>
void untranspose(const T* __restrict in, T* __restrict out) {
    constexpr size_t nesting_depth = 256;
    loop<T, nesting_depth>([&in, &out](auto j) {
        out[transposed_index(j)] = in[j];
    });
    loop<T, nesting_depth>([&in, &out](auto j) {
        out[transposed_index(j + nesting_depth)] = in[j + nesting_depth];
    });
    loop<T, nesting_depth>([&in, &out](auto j) {
        out[transposed_index(j + (nesting_depth * 2))] = in[j + (nesting_depth * 2)];
    });
    loop<T, nesting_depth>([&in, &out](auto j) {
        out[transposed_index(j + (nesting_depth * 3))] = in[j + (nesting_depth * 3)];
    });
} */

}  //namespace arcticdb