#pragma once

#include <cstddef>
#include <climits>

namespace arcticdb {

constexpr size_t round_up_bits(size_t bits) noexcept {
    return (bits + (CHAR_BIT - 1)) / CHAR_BIT;
}
} // namespace arcticdb