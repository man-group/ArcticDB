#pragma once

#include <cstdint>
#include <cstddef>
#include <climits>

#include <arcticdb/codec/compression/fastlanes_common.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {

template<typename T, size_t bit_width>
struct BitPack : public BitPackHelper<T, bit_width> {
    using Parent = BitPackHelper<T, bit_width>;
    static constexpr auto num_bits = Parent::num_bits;
    static constexpr auto num_lanes = Parent::num_lanes;
    static constexpr auto mask = Parent::mask;
    using p = Parent;
    static constexpr size_t BLOCK_SIZE = 1024;


    static constexpr size_t compressed_size() {
        constexpr size_t total_bits = BLOCK_SIZE * bit_width;
        return (total_bits + sizeof(T) * 8 - 1) / (sizeof(T) * 8);
    }


    static size_t go(const T *__restrict in, T *__restrict out);
};

extern template struct BitPack<uint32_t, 16>;

template<typename T, size_t bit_width>
struct BitUnpack : public BitPackHelper<T, bit_width> {
    using Parent = BitPackHelper<T, bit_width>;
    static constexpr auto num_bits = Parent::num_bits;
    static constexpr auto num_lanes = Parent::num_lanes;
    static constexpr auto mask = Parent::mask;
    using p = Parent;
    static constexpr size_t BLOCK_SIZE = 1024;


    static constexpr size_t compressed_size() {
        constexpr size_t total_bits = BLOCK_SIZE * bit_width;
        return (total_bits + sizeof(T) * 8 - 1) / (sizeof(T) * 8);
    }

    static size_t go(const T *__restrict in, T *__restrict out) ;
};

extern template struct BitUnpack<uint32_t, 16>;

template<typename T, template<typename, size_t> class FusedType, typename Kernel, size_t... Is>
size_t dispatch_bitwidth_fused_impl(
    const T* RESTRICT in,
    T* RESTRICT out,
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
    const T* RESTRICT in,
    T* RESTRICT out,
    size_t bit_width,
    Kernel& kernel) {

    constexpr size_t max_bits_allowed = sizeof(T) * 8;

    if (EXPECT(bit_width <= max_bits_allowed, 1)) {
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
    ARCTICDB_TRACE(log::codec(), "Packing value {} at word {}", value, current_word);

    if (bit_offset + bit_width > bits_per_word) {
        size_t bits_remaining = bits_per_word - bit_offset;
        // Write lower bits to current word
        current_word |= (value & ((T(1) << bits_remaining) - 1)) << bit_offset;
        *out = current_word;
        ++out;  // This increment will now be reflected in caller
        // Start new word with upper bits
        current_word = value >> bits_remaining;
        bit_offset = bit_width - bits_remaining;
    } else {
        // Pack value into current word
        current_word |= (value & ((T(1) << bit_width) - 1)) << bit_offset;
        bit_offset += bit_width;

        // Write word if full
        if (bit_offset == bits_per_word) {
            *out = current_word;
            ++out;  // This increment will now be reflected in caller
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
        // Get lower bits from current word
        value = (current_word >> bit_offset) & ((T(1) << bits_remaining) - 1);
        // Get upper bits from next word
        ++in;
        current_word = *in;
        value |= (current_word & ((T(1) << (bit_width - bits_remaining)) - 1)) << bits_remaining;
        bit_offset = bit_width - bits_remaining;
    } else {
        // Extract value from current word
        value = (current_word >> bit_offset) & ((T(1) << bit_width) - 1);
        bit_offset += bit_width;

        // Load next word if needed
        if (bit_offset == bits_per_word) {
            ++in;
            current_word = *in;
            bit_offset = 0;
        }
    }
    ARCTICDB_DEBUG(log::codec(), "Returning {} at word {}", value, current_word);
    return value;
}

} //namespace arcticdb