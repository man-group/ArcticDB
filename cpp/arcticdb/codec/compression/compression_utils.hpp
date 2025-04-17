#pragma once

#include <cstddef>
#include <climits>

namespace arcticdb {

constexpr size_t round_up_bits(size_t bits) noexcept {
    return (bits + (CHAR_BIT - 1)) / CHAR_BIT;
}

template <typename T>
constexpr size_t round_up_bits_in_t(size_t bits) noexcept {
    constexpr size_t t_bits = sizeof(T) * CHAR_BIT;
    return (bits + (t_bits - 1)) / t_bits;
}

template <typename T>
constexpr std::size_t round_up_bytes_in_t(size_t bytes) {
    constexpr auto t_size = sizeof(T);
    return ((bytes + t_size - 1) / t_size) * t_size;
}
} // namespace arcticdb