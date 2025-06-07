#pragma once

#include <cstdint>
#include <cstddef>
#include <climits>

#include <arcticdb/codec/compression/fastlanes_common.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {

#define BITPACK_ROW(n) {                                                     \
    constexpr std::size_t row = (n);                                         \
    std::size_t idx = fastlanes_index(row, lane);                                      \
    T src = kernel(in, idx, lane);                                           \
    src &= Parent::mask;                                                     \
    if constexpr (row == 0) {                                                \
        tmp = src;                                                           \
    } else {                                                                 \
        tmp |= src << ((row * bit_width) & (Parent::num_bits - 1));          \
    }                                                                        \
    if constexpr (p::at_end(row)) {                                          \
        constexpr auto current_word = p::current_word(row);                  \
        constexpr auto remaining_bits = p::remaining_bits(row);              \
        out[Parent::num_lanes * current_word + lane] = tmp;                  \
        tmp = src >> (bit_width - remaining_bits);                           \
    }                                                                        \
}

template<typename T, std::size_t bit_width>
struct BitPackFused : public BitPackHelper<MakeUnsignedType<T>, bit_width> {
    using Parent = BitPackHelper<MakeUnsignedType<T>, bit_width>;
    using p = Parent;

    static constexpr std::size_t BLOCK_SIZE = 1024;

    static constexpr std::size_t compressed_size() {
        constexpr std::size_t total_bits = BLOCK_SIZE * bit_width;
        return (total_bits + Parent::num_bits - 1) / Parent::num_bits;
    }

    template<typename Kernel>
    static size_t go(const T *__restrict in, T *__restrict out, Kernel& kernel) {
        for (std::size_t lane = 0; lane < Parent::num_lanes; ++lane) {
            T tmp = 0;

            if constexpr (Helper<T>::num_bits == 64) {
                BITPACK_ROW(0)
                BITPACK_ROW(1)
                BITPACK_ROW(2)
                BITPACK_ROW(3)
                BITPACK_ROW(4)
                BITPACK_ROW(5)
                BITPACK_ROW(6)
                BITPACK_ROW(7)
                BITPACK_ROW(8)
                BITPACK_ROW(9)
                BITPACK_ROW(10)
                BITPACK_ROW(11)
                BITPACK_ROW(12)
                BITPACK_ROW(13)
                BITPACK_ROW(14)
                BITPACK_ROW(15)
                BITPACK_ROW(16)
                BITPACK_ROW(17)
                BITPACK_ROW(18)
                BITPACK_ROW(19)
                BITPACK_ROW(20)
                BITPACK_ROW(21)
                BITPACK_ROW(22)
                BITPACK_ROW(23)
                BITPACK_ROW(24)
                BITPACK_ROW(25)
                BITPACK_ROW(26)
                BITPACK_ROW(27)
                BITPACK_ROW(28)
                BITPACK_ROW(29)
                BITPACK_ROW(30)
                BITPACK_ROW(31)
                BITPACK_ROW(32)
                BITPACK_ROW(33)
                BITPACK_ROW(34)
                BITPACK_ROW(35)
                BITPACK_ROW(36)
                BITPACK_ROW(37)
                BITPACK_ROW(38)
                BITPACK_ROW(39)
                BITPACK_ROW(40)
                BITPACK_ROW(41)
                BITPACK_ROW(42)
                BITPACK_ROW(43)
                BITPACK_ROW(44)
                BITPACK_ROW(45)
                BITPACK_ROW(46)
                BITPACK_ROW(47)
                BITPACK_ROW(48)
                BITPACK_ROW(49)
                BITPACK_ROW(50)
                BITPACK_ROW(51)
                BITPACK_ROW(52)
                BITPACK_ROW(53)
                BITPACK_ROW(54)
                BITPACK_ROW(55)
                BITPACK_ROW(56)
                BITPACK_ROW(57)
                BITPACK_ROW(58)
                BITPACK_ROW(59)
                BITPACK_ROW(60)
                BITPACK_ROW(61)
                BITPACK_ROW(62)
                BITPACK_ROW(63)
            } else if constexpr (Helper<T>::num_bits == 32) {
                BITPACK_ROW(0)
                BITPACK_ROW(1)
                BITPACK_ROW(2)
                BITPACK_ROW(3)
                BITPACK_ROW(4)
                BITPACK_ROW(5)
                BITPACK_ROW(6)
                BITPACK_ROW(7)
                BITPACK_ROW(8)
                BITPACK_ROW(9)
                BITPACK_ROW(10)
                BITPACK_ROW(11)
                BITPACK_ROW(12)
                BITPACK_ROW(13)
                BITPACK_ROW(14)
                BITPACK_ROW(15)
                BITPACK_ROW(16)
                BITPACK_ROW(17)
                BITPACK_ROW(18)
                BITPACK_ROW(19)
                BITPACK_ROW(20)
                BITPACK_ROW(21)
                BITPACK_ROW(22)
                BITPACK_ROW(23)
                BITPACK_ROW(24)
                BITPACK_ROW(25)
                BITPACK_ROW(26)
                BITPACK_ROW(27)
                BITPACK_ROW(28)
                BITPACK_ROW(29)
                BITPACK_ROW(30)
                BITPACK_ROW(31)
            } else if constexpr (Helper<T>::num_bits == 16) {
                BITPACK_ROW(0)
                BITPACK_ROW(1)
                BITPACK_ROW(2)
                BITPACK_ROW(3)
                BITPACK_ROW(4)
                BITPACK_ROW(5)
                BITPACK_ROW(6)
                BITPACK_ROW(7)
                BITPACK_ROW(8)
                BITPACK_ROW(9)
                BITPACK_ROW(10)
                BITPACK_ROW(11)
                BITPACK_ROW(12)
                BITPACK_ROW(13)
                BITPACK_ROW(14)
                BITPACK_ROW(15)
            } else if constexpr (Helper<T>::num_bits == 8) {
                BITPACK_ROW(0)
                BITPACK_ROW(1)
                BITPACK_ROW(2)
                BITPACK_ROW(3)
                BITPACK_ROW(4)
                BITPACK_ROW(5)
                BITPACK_ROW(6)
                BITPACK_ROW(7)
            } else {
                util::raise_rte("Unsupported underlying type bit size in BitPackFused::go");
            }
        }

        return compressed_size();
    }
};

#define BITUNPACK_ROW(n) {                  \
    using U = MakeUnsignedType<T>;                                                           \
    constexpr std::size_t row = (n);                                                            \
    constexpr auto shift = p::shift(row);                                                       \
    if constexpr (p::at_end(row)) {                                                             \
        constexpr auto current_bits = p::current_bits(row);                                     \
        constexpr auto current_bits_mask = construct_mask<U, current_bits>();                   \
        tmp = (src >> shift) & current_bits_mask;                                               \
        if constexpr (p::next_word(row) < bit_width) {                                          \
            constexpr auto next_word = p::next_word(row);                                       \
            constexpr auto remaining_bits_mask = construct_mask<U, p::remaining_bits(row)>();   \
            src = in[num_lanes * next_word + lane];                                             \
            tmp |= (src & remaining_bits_mask) << current_bits;                                 \
        }                                                                                       \
    } else {                                                                                    \
        tmp = (src >> shift) & mask;                                                            \
    }                                                                                           \
    size_t idx = fastlanes_index(row, lane);                                                              \
    kernel(out, idx, tmp, lane);                                                                \
}

template<typename T, std::size_t bit_width>
struct BitUnpackFused : public BitPackHelper<MakeUnsignedType<T>, bit_width> {
    using Parent = BitPackHelper<MakeUnsignedType<T>, bit_width>;
    static constexpr auto num_bits = Parent::num_bits;
    static constexpr auto num_lanes = Parent::num_lanes;
    static constexpr auto mask = Parent::mask;
    using p = Parent;
    static constexpr std::size_t BLOCK_SIZE = 1024;

    static constexpr size_t compressed_size() {
        constexpr size_t total_bits = BLOCK_SIZE * bit_width;
        return (total_bits + sizeof(T) * 8 - 1) / (sizeof(T) * 8);
    }

    template<typename Kernel>
    static size_t go(const T* __restrict in, T* __restrict out, Kernel& kernel) {
        for (std::size_t lane = 0; lane < num_lanes; ++lane) {
            T src = in[lane];
            T tmp;
            if constexpr (Helper<T>::num_bits == 64) {
                BITUNPACK_ROW(0)
                BITUNPACK_ROW(1)
                BITUNPACK_ROW(2)
                BITUNPACK_ROW(3)
                BITUNPACK_ROW(4)
                BITUNPACK_ROW(5)
                BITUNPACK_ROW(6)
                BITUNPACK_ROW(7)
                BITUNPACK_ROW(8)
                BITUNPACK_ROW(9)
                BITUNPACK_ROW(10)
                BITUNPACK_ROW(11)
                BITUNPACK_ROW(12)
                BITUNPACK_ROW(13)
                BITUNPACK_ROW(14)
                BITUNPACK_ROW(15)
                BITUNPACK_ROW(16)
                BITUNPACK_ROW(17)
                BITUNPACK_ROW(18)
                BITUNPACK_ROW(19)
                BITUNPACK_ROW(20)
                BITUNPACK_ROW(21)
                BITUNPACK_ROW(22)
                BITUNPACK_ROW(23)
                BITUNPACK_ROW(24)
                BITUNPACK_ROW(25)
                BITUNPACK_ROW(26)
                BITUNPACK_ROW(27)
                BITUNPACK_ROW(28)
                BITUNPACK_ROW(29)
                BITUNPACK_ROW(30)
                BITUNPACK_ROW(31)
                BITUNPACK_ROW(32)
                BITUNPACK_ROW(33)
                BITUNPACK_ROW(34)
                BITUNPACK_ROW(35)
                BITUNPACK_ROW(36)
                BITUNPACK_ROW(37)
                BITUNPACK_ROW(38)
                BITUNPACK_ROW(39)
                BITUNPACK_ROW(40)
                BITUNPACK_ROW(41)
                BITUNPACK_ROW(42)
                BITUNPACK_ROW(43)
                BITUNPACK_ROW(44)
                BITUNPACK_ROW(45)
                BITUNPACK_ROW(46)
                BITUNPACK_ROW(47)
                BITUNPACK_ROW(48)
                BITUNPACK_ROW(49)
                BITUNPACK_ROW(50)
                BITUNPACK_ROW(51)
                BITUNPACK_ROW(52)
                BITUNPACK_ROW(53)
                BITUNPACK_ROW(54)
                BITUNPACK_ROW(55)
                BITUNPACK_ROW(56)
                BITUNPACK_ROW(57)
                BITUNPACK_ROW(58)
                BITUNPACK_ROW(59)
                BITUNPACK_ROW(60)
                BITUNPACK_ROW(61)
                BITUNPACK_ROW(62)
                BITUNPACK_ROW(63)
            } else if constexpr (Helper<T>::num_bits == 32) {
                BITUNPACK_ROW(0)
                BITUNPACK_ROW(1)
                BITUNPACK_ROW(2)
                BITUNPACK_ROW(3)
                BITUNPACK_ROW(4)
                BITUNPACK_ROW(5)
                BITUNPACK_ROW(6)
                BITUNPACK_ROW(7)
                BITUNPACK_ROW(8)
                BITUNPACK_ROW(9)
                BITUNPACK_ROW(10)
                BITUNPACK_ROW(11)
                BITUNPACK_ROW(12)
                BITUNPACK_ROW(13)
                BITUNPACK_ROW(14)
                BITUNPACK_ROW(15)
                BITUNPACK_ROW(16)
                BITUNPACK_ROW(17)
                BITUNPACK_ROW(18)
                BITUNPACK_ROW(19)
                BITUNPACK_ROW(20)
                BITUNPACK_ROW(21)
                BITUNPACK_ROW(22)
                BITUNPACK_ROW(23)
                BITUNPACK_ROW(24)
                BITUNPACK_ROW(25)
                BITUNPACK_ROW(26)
                BITUNPACK_ROW(27)
                BITUNPACK_ROW(28)
                BITUNPACK_ROW(29)
                BITUNPACK_ROW(30)
                BITUNPACK_ROW(31)
            } else if constexpr (Helper<T>::num_bits == 16) {
                BITUNPACK_ROW(0)
                BITUNPACK_ROW(1)
                BITUNPACK_ROW(2)
                BITUNPACK_ROW(3)
                BITUNPACK_ROW(4)
                BITUNPACK_ROW(5)
                BITUNPACK_ROW(6)
                BITUNPACK_ROW(7)
                BITUNPACK_ROW(8)
                BITUNPACK_ROW(9)
                BITUNPACK_ROW(10)
                BITUNPACK_ROW(11)
                BITUNPACK_ROW(12)
                BITUNPACK_ROW(13)
                BITUNPACK_ROW(14)
                BITUNPACK_ROW(15)
            } else if constexpr (Helper<T>::num_bits == 8) {
                BITUNPACK_ROW(0)
                BITUNPACK_ROW(1)
                BITUNPACK_ROW(2)
                BITUNPACK_ROW(3)
                BITUNPACK_ROW(4)
                BITUNPACK_ROW(5)
                BITUNPACK_ROW(6)
                BITUNPACK_ROW(7)
            } else {
                util::raise_rte("Unsupported underlying type bit size in BitUnpackFused::go");
            }
        }
        return compressed_size();
    }
};

template<typename T, template<typename, size_t> class FusedType, typename Kernel, size_t... Is>
size_t dispatch_bitwidth_fused_impl(
    const T* __restrict in,
    T* __restrict out,
    size_t bit_width,
    Kernel& kernel,
    std::index_sequence<Is...>) {

    size_t result = 0;

    (void)((bit_width == Is + 1
      ? (result = FusedType<T, Is + 1>::go(
            in, out,
            kernel), true)
      : false) || ...);

    return result;
}

template<typename T, template<typename, size_t> class FusedType, typename Kernel>
size_t dispatch_bitwidth_fused(
    const T* __restrict in,
    T* __restrict out,
    size_t bit_width,
    Kernel& kernel) {

    constexpr size_t max_bits_allowed = sizeof(T) * 8;

    if (ARCTICDB_LIKELY(bit_width <= max_bits_allowed)) {
        return dispatch_bitwidth_fused_impl<T, FusedType>(
            in,
            out,
            bit_width,
            kernel,
            std::make_index_sequence<max_bits_allowed - 1>{}
        );
    } else {
        util::raise_rte("Bit width exceeds type size");
    }
}


template<typename T>
void scalar_pack(T value, size_t bit_width, size_t& bit_offset, T& current_word, T*& out) {  // out by reference
    static constexpr size_t bits_per_word = sizeof(T) * CHAR_BIT;

    if (bit_offset + bit_width > bits_per_word) {
        size_t bits_remaining = bits_per_word - bit_offset;
        current_word |= (value & ((T(1) << bits_remaining) - 1)) << bit_offset;
        *out = current_word;
        ++out;
        current_word = value >> bits_remaining;
        bit_offset = bit_width - bits_remaining;
    } else {
        current_word |= (value & ((T(1) << bit_width) - 1)) << bit_offset;
        bit_offset += bit_width;

        if (bit_offset == bits_per_word) {
            *out = current_word;
            ++out;
            current_word = 0;
            bit_offset = 0;
        }
    }
}

template<typename T>
T scalar_unpack(size_t bit_width, size_t& bit_offset, T& current_word, const T*& in) {  // in by reference
    static constexpr size_t bits_per_word = sizeof(T) * 8;
    T value;

    if (bit_offset + bit_width > bits_per_word) {
        size_t bits_remaining = bits_per_word - bit_offset;
        value = (current_word >> bit_offset) & ((T(1) << bits_remaining) - 1);
        ++in;
        current_word = *in;
        value |= (current_word & ((T(1) << (bit_width - bits_remaining)) - 1)) << bits_remaining;
        bit_offset = bit_width - bits_remaining;
    } else {
        value = (current_word >> bit_offset) & ((T(1) << bit_width) - 1);
        bit_offset += bit_width;

        if (bit_offset == bits_per_word) {
            ++in;
            current_word = *in;
            bit_offset = 0;
        }
    }
    return value;
}

} //namespace arcticdb