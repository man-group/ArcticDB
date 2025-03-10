#pragma once

#include <cstdint>
#include <cstddef>

#include <arcticdb/codec/compression/fastlanes_common.hpp>

namespace arcticdb {



template<typename T, size_t bit_width>
struct FFORBitPack : public BitPackHelper<T, bit_width> {
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

    static size_t go(const T *__restrict in, T *__restrict out, const T* __restrict base_ptr) {
        for(auto lane = 0UL; lane < num_lanes; ++lane) {
            T tmp = 0;
            const T base = *base_ptr;
            loop<T, num_bits>([lane, in, out, &tmp, base](auto r) {
                constexpr size_t row =   r;
                T src = *(in + index(row, lane)) - base;
                src &= mask;

                if constexpr(row == 0) {
                    tmp = src;
                } else {
                    tmp |= src << ((row * bit_width) & (num_bits - 1));
                }

                if constexpr(p::at_end(row)) {
                    constexpr auto current_word =  p::current_word(row);
                    constexpr auto remaining_bits = p::remaining_bits(row);
                    out[num_lanes * current_word + lane] = tmp;
                    tmp = src >> (bit_width - remaining_bits);
                }
            });
        };
        return compressed_size();
    }
};

template<typename T, size_t bit_width>
struct FFORBitUnpack : public BitPackHelper<T, bit_width> {
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

    static size_t go(const T *__restrict in, T *__restrict out, const T* __restrict base_ptr) {
        for(auto lane = 0UL; lane < num_lanes; ++lane) {
            T src = in[lane];
            T tmp;
            const T base = *base_ptr;
            loop<T, num_bits>([lane, in, out, &tmp, &src, base](auto row) {
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

                tmp += base;
                *(out + index(row, lane)) = tmp;
            });
        }
        return compressed_size();
    }
};

} // namespace arcticdb