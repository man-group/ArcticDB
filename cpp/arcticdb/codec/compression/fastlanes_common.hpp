/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <utility>
#include <limits>
#include <array>

#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {
namespace fastlanes {

constexpr std::size_t FastLanesWidth = 1024;

template<class T, T... inds, class F>
constexpr void loop(std::integer_sequence<T, inds...>, F &&f) {
    (f(std::integral_constant<T, inds>{}), ...);
}
} // namespace fastlanes

static constexpr size_t BLOCK_SIZE = 1024;

template<class T, T count, class F>
constexpr void loop(F &&f) {
    fastlanes::loop(std::make_integer_sequence<T, count>{}, std::forward<F>(f));
}

template<typename T>
constexpr size_t type_bits() {
    return sizeof(T) * std::numeric_limits<uint8_t>::digits;
}

template<typename T>
struct Helper {
    static constexpr size_t num_bits = type_bits<T>();
    static constexpr size_t register_width = fastlanes::FastLanesWidth;
    static constexpr size_t num_lanes = register_width / num_bits;
};

static_assert(Helper<uint64_t>::num_lanes == 16);
static_assert(Helper<uint8_t>::num_lanes == 128);
static_assert(Helper<uint16_t>::num_bits == 16);

constexpr std::array<size_t, 8> FL_ORDER = { 0, 4, 2, 6, 1, 5, 3, 7 };

constexpr size_t transposed_index(size_t index) {
    auto lane = index % 16;
    auto order = (index / 16) % 8;
    auto row = index / 128;

    return (lane * 64) + (FL_ORDER[order] * 8) + row;
}

constexpr size_t fastlanes_index(size_t row, size_t lane) {
    const auto o = row / 8;
    const auto s = row % 8;
    return (FL_ORDER[o] * 16) + (s * 128) + lane;
}

static_assert(fastlanes_index(1, 0) == 128);
static_assert(fastlanes_index(0, 1) == 1);

template<typename HeaderType, typename T>
static constexpr size_t header_size_in_t() {
    return (sizeof(HeaderType) + sizeof(T) - 1) / sizeof(T);
}

template<typename T, size_t bit_width>
constexpr T construct_mask() {
    if constexpr (bit_width == type_bits<T>())
        return T(-1);
    else
        return (T(1) << bit_width) - 1;
}

template<typename T, size_t width>
struct BitPackHelper {
    static constexpr size_t bit_width = width;
    static constexpr size_t num_bits = Helper<T>::num_bits;
    static constexpr size_t num_lanes = Helper<T>::num_lanes;
    static_assert(bit_width <= num_bits);

    static constexpr T mask = construct_mask<T, bit_width>();

    static constexpr size_t remaining_bits(size_t row) {
        return ((row + 1) * bit_width) % num_bits;
    };

    static constexpr size_t current_bits(size_t row) {
        return bit_width - remaining_bits(row);
    }

    static constexpr size_t current_word (size_t row) {
        return (row * bit_width) / num_bits;
    }

    static constexpr size_t next_word (size_t row) {
        return ((row + 1) * bit_width) / num_bits;
    }

    static constexpr bool at_end(size_t row) {
        return next_word(row) > current_word(row);
    }

    static constexpr size_t shift(size_t row) {
        return (row * bit_width) % num_bits;
    }
};

static_assert(BitPackHelper<uint8_t, 3>::mask == 7);
static_assert(BitPackHelper<uint8_t, 3>::at_end(2));
static_assert(!BitPackHelper<uint8_t, 3>::at_end(3));

template<typename T, template<typename, size_t> class FusedType, size_t... Is>
size_t dispatch_bitwidth_impl(
    const T* __restrict in,
    T* __restrict out,
    size_t bit_width,
    const T* __restrict base,
    std::index_sequence<Is...>) {

    size_t result = 0;

    (void)((bit_width == Is + 1
            ? (result = FusedType<T, Is + 1>::go(
            in,
            out,
            base), true)
            : false) || ...);

    return result;
}

template<typename T, template<typename, size_t> class FusedType>
size_t dispatch_bitwidth(
    const T* __restrict in,
    T* __restrict out,
    const T* __restrict base,
    size_t bit_width) {

    constexpr size_t max_bits_allowed = sizeof(T) * 8;

    if (ARCTICDB_LIKELY(bit_width <= max_bits_allowed)) {
        return dispatch_bitwidth_impl<T, FusedType>(
            in,
            out,
            bit_width,
            base,
            std::make_index_sequence<max_bits_allowed - 1>{}
        );
    } else {
        util::raise_rte("Bit width exceeds type size");
    }
}

template <typename T>
struct MakeUnsignedWithBool {
    using type = std::make_unsigned_t<T>;
};

template <>
struct MakeUnsignedWithBool<bool> {
    using type = uint8_t;
};

template <typename T>
using MakeUnsignedType = typename MakeUnsignedWithBool<T>::type;

} // namespace arcticdb