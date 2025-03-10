#include <arcticdb/codec/compression/bitpack.hpp>

namespace arcticdb {

#define BITPACK_ROW(r)                                                                          \
    {                                                                                           \
        constexpr size_t row = (r);                                                             \
        size_t idx = index(row, lane);                                                          \
        T src = in[idx];                                                                        \
        src &= mask;                                                                            \
        if constexpr ((r) == 0) {                                                                 \
            tmp = src;                                                                          \
        } else {                                                                                \
            tmp |= src << (((r) * bit_width) & (num_bits - 1));                                 \
        }                                                                                       \
        if constexpr (p::at_end(r)) {                                                           \
            constexpr auto current_word = p::current_word(r);                                   \
            constexpr auto remaining_bits = p::remaining_bits(r);                               \
            out[num_lanes * current_word + lane] = tmp;                                         \
            tmp = src >> (bit_width - remaining_bits);                                          \
        }                                                                                       \
    }

template<typename T, size_t bit_width>
    size_t BitPack<T, bit_width>::go(const T *__restrict in, T *__restrict out) {
        for(auto lane = 0UL; lane < num_lanes; ++lane) {
            T tmp = 0;
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
        }
        return compressed_size();
    }

template struct BitPack<uint32_t, 16>;


template<typename T, size_t bit_width>
size_t BitUnpack<T, bit_width>::go(const T *__restrict in, T *__restrict out) {
    for(auto lane = 0UL; lane < num_lanes; ++lane) {
        T src = in[lane];
        T tmp;
        loop<T, num_bits>([lane, in, out, &tmp, &src](auto row) {
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

            size_t idx = index(row, lane);
            *(out + idx) =  tmp;
        });
    }
    return compressed_size();
}


template struct BitUnpack<uint32_t, 16>;

} //namespace arcticdb