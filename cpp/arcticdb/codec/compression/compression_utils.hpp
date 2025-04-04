#pragma once

#include <cstddef>
#include <climits>

namespace arcticdb {

constexpr size_t round_up_bits(size_t bits) noexcept {
    return (bits + (CHAR_BIT - 1)) / CHAR_BIT;
}

template <typename T>
constexpr size_t round_up_in_t(size_t bits) noexcept {
    constexpr size_t t_bits = sizeof(T) * CHAR_BIT;
    return (bits + (t_bits - 1)) / t_bits;
}
} // namespace arcticdb