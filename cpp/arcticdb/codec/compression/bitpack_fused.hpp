#pragma once

#include <cstdint>
#include <cstddef>
#include <climits>

#include <arcticdb/codec/compression/fastlanes_common.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {


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

template<typename T, typename p, size_t bit_width, typename Kernel>
HOT_FUNCTION
VECTOR_HINT
void bitpack_lane(
    const size_t lane,
    const T* RESTRICT in,
    T* RESTRICT out,
    Kernel& kernel) {
    static constexpr auto num_bits = p::num_bits;
    static constexpr auto num_lanes = p::num_lanes;
    static constexpr auto mask = p::mask;

    ALIGN_HINT(32) T tmp;

    loop<T, num_bits>([&](auto r) {
        constexpr size_t row = r;
        size_t idx = row * p::num_lanes + lane;
        ALIGN_HINT(32) T src = kernel(in[idx], lane);
        src &= mask;

        if constexpr(row == 0) {
            tmp = src;
        } else {
            tmp |= src << ((row * bit_width) & (num_bits - 1));
        }

        if constexpr(p::at_end(row)) {
            constexpr auto current_word = p::current_word(row);
            constexpr auto remaining_bits = p::remaining_bits(row);
            out[num_lanes * current_word + lane] = tmp;
            tmp = src >> (bit_width - remaining_bits);
        }
    });
}

template<typename T, size_t bit_width>
struct ALIGN_HINT(64) BitPackFused : public BitPackHelper<T, bit_width> {
    using Parent = BitPackHelper<T, bit_width>;
    static constexpr auto num_lanes = Parent::num_lanes;
    static constexpr size_t BLOCK_SIZE = 1024;

    static constexpr size_t compressed_size() {
        constexpr size_t total_bits = BLOCK_SIZE * bit_width;  // 1024 * bit_width
        return (total_bits + sizeof(T) * 8 - 1) / (sizeof(T) * 8);
    }

    template<typename Kernel>
    HOT_FUNCTION
    static size_t go(
        const T* RESTRICT in,
        T* RESTRICT out,
        Kernel&& kernel) {

        // Prefetch cache lines
        __builtin_prefetch(in);
        __builtin_prefetch(out);

        VECTORIZE_LOOP
        ALIGNED_ACCESS
        for(auto lane = 0UL; lane < num_lanes; ++lane) {
            bitpack_lane<T, Parent, bit_width>(lane, in, out, kernel);
        }

        return compressed_size();
    }
};

template<typename T, typename p, size_t bit_width, typename Kernel>
HOT_FUNCTION
VECTOR_HINT
void bitunpack_lane(
    const size_t lane,
    const T* RESTRICT in,
    T* RESTRICT out,
    Kernel& kernel) {
    static constexpr auto num_bits = p::num_bits;
    static constexpr auto num_lanes = p::num_lanes;
    static constexpr auto mask = p::mask;

    ALIGN_HINT(32) T src = in[lane];
    ALIGN_HINT(32) T tmp;

    loop<T, num_bits>([&](auto r) {
        constexpr size_t row = r;
        constexpr auto shift = p::shift(row);

        if constexpr (p::at_end(row)) {
            constexpr auto current_bits = p::current_bits(row);
            constexpr auto current_bits_mask = construct_mask<T, current_bits>();
            tmp = (src >> shift) & current_bits_mask;

            if constexpr (p::next_word(row) < bit_width) {
                constexpr auto next_word = p::next_word(row);
                constexpr auto remaining_bits_mask = construct_mask<T, p::remaining_bits(row)>();
                src = in[num_lanes * next_word + lane];
                tmp |= (src & remaining_bits_mask) << current_bits;
            }
        } else {
            tmp = (src >> shift) & mask;
        }

        size_t idx = row * p::num_lanes + lane;
        out[idx] = kernel(tmp, lane);
    });
}

template<typename T, size_t bit_width>
struct ALIGN_HINT(64) BitUnpackFused : public BitPackHelper<T, bit_width> {
    using Parent = BitPackHelper<T, bit_width>;
    static constexpr auto num_lanes = Parent::num_lanes;
    static constexpr size_t BLOCK_SIZE = 1024;

    static constexpr size_t compressed_size() {
        constexpr size_t total_bits = BLOCK_SIZE * bit_width;
        return (total_bits + sizeof(T) * 8 - 1) / (sizeof(T) * 8);
    }


    template<typename Kernel>
    HOT_FUNCTION
    static size_t go(
        const T* RESTRICT in,
        T* RESTRICT out,
        Kernel&& kernel) {

        // Prefetch cache lines
        __builtin_prefetch(in);
        __builtin_prefetch(out);

        VECTORIZE_LOOP
        ALIGNED_ACCESS
        for(auto lane = 0UL; lane < num_lanes; ++lane) {
            bitunpack_lane<T, Parent, bit_width>(
                lane, in, out, kernel);
        }

        return compressed_size();
    }
};

template<typename T, template<typename, size_t> class FusedType, typename Kernel, size_t... Is>
ALWAYS_INLINE
size_t dispatch_bitwidth_impl(
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
ALWAYS_INLINE
size_t dispatch_bitwidth(
    const T* RESTRICT in,
    T* RESTRICT out,
    size_t bit_width,
    Kernel& kernel) {

    constexpr size_t max_bits_allowed = sizeof(T) * 8;

    if (EXPECT(bit_width <= max_bits_allowed, 1)) {
        return dispatch_bitwidth_impl<T, FusedType>(
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
    ARCTICDB_DEBUG(log::codec(), "Packing value {} at word {}", value, current_word);

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

template<typename T, typename Kernel>
HOT_FUNCTION
VECTOR_HINT
void scan_lane(
    const size_t lane,
    const T* RESTRICT in,
    Kernel& kernel) {
    using h = Helper<T>;
    static constexpr auto num_bits = h::num_bits;
    static constexpr auto num_lanes = h::num_lanes;

    loop<T, num_bits>([&](auto r) {
        constexpr size_t row = r;
        size_t idx = row * num_lanes + lane;
        kernel(in[idx], lane);
    });
}

template<typename T, size_t bit_width = CHAR_BIT * sizeof(T)>
struct ALIGN_HINT(64) FastLanesScan : public BitPackHelper<T, bit_width> {
    using h = Helper<T>;
    static constexpr auto num_lanes = h::num_lanes;

    template<typename Kernel>
    HOT_FUNCTION
    static void go(
        const T* RESTRICT in,
        Kernel&& kernel) {

        __builtin_prefetch(in);

        VECTORIZE_LOOP
        ALIGNED_ACCESS
        for(auto lane = 0UL; lane < num_lanes; ++lane) {
           scan_lane<T>(lane, in, kernel);
        }
    }
};

} //namespace arcticdb