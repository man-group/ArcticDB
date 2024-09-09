#include <cstdint>
#include <cstddef>


#include <arcticdb/codec/fastlanes_common.hpp>
#include <arcticdb/log/log.hpp>

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
void bitpack_lane(
        const size_t lane,
        const T* __restrict in,
        T* __restrict out,
        Kernel& kernel) {
    static constexpr auto num_bits = p::num_bits;
    static constexpr auto num_lanes = p::num_lanes;
    static constexpr auto mask = p::mask;

    T tmp = 0;
    loop<T, num_bits>([lane, in, out, &tmp, &kernel](auto r) {
        constexpr size_t row =   r;
        size_t idx = index(row, lane);
        T src = kernel(in[idx]);
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
            //log::version().info("Writing to index {}", num_bits * current_word + lane);
            tmp = src >> (bit_width - remaining_bits);
        }
    });
}

template<typename T, size_t bit_width>
struct BitPackFused : public BitPackHelper<T, bit_width> {
    using Parent = BitPackHelper<T, bit_width>;
    static constexpr auto num_lanes = Parent::num_lanes;

    template <typename Kernel>
    static void go(const T *__restrict in, T *__restrict out, Kernel &&kernel) {
        for(auto lane = 0UL; lane < num_lanes; ++lane) {
            bitpack_lane<T, Parent, bit_width, Kernel>(lane, in, out, kernel);
        };
    }
};

template <typename T, typename Parent, size_t bit_width, typename Kernel>
void bitunpack_lane(
        size_t lane,
        const T *__restrict in,
        T *__restrict out,
        Kernel &kernel) {
    static constexpr auto num_bits = Parent::num_bits;
    static constexpr auto num_lanes = Parent::num_lanes;
    static constexpr auto mask = Parent::mask;
    using p = Parent;

    T src = in[lane];
    T tmp;
    loop<T, num_bits>([lane, in, out, &tmp, &kernel, &src](auto row) {
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
        out[idx] = kernel(tmp);
    });
}

template<typename T, size_t bit_width>
struct BitUnpackFused : public BitPackHelper<T, bit_width> {
    using Parent = BitPackHelper<T, bit_width>;
    static constexpr auto num_lanes = Parent::num_lanes;

    template<typename Kernel>
    static void go(const T *__restrict in, T *__restrict out, Kernel &&kernel) {
        for(auto lane = 0UL; lane < num_lanes; ++lane) {
            bitunpack_lane<T, Parent, bit_width, Kernel>(lane, in, out, kernel);
        }
    }
};

} // namespace arcticdb