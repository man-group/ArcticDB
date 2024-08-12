#include <cstdint>
#include <cstddef>
#include <limits>
#include <utility>

namespace arcticdb {

namespace detail {
template<class T, T... inds, class F>
constexpr void loop(std::integer_sequence<T, inds...>, F &&f) {
    (f(std::integral_constant<T, inds>{}), ...);
}
}

template<class T, T count, class F>
constexpr void loop(F &&f) {
    detail::loop(std::make_integer_sequence<T, count>{}, std::forward<F>(f));
}

template<typename T>
constexpr size_t t_bits() {
    return sizeof(T) * std::numeric_limits<uint8_t>::digits;
}

template<typename T, size_t bit_width>
constexpr T make_mask() {
    if constexpr (bit_width == t_bits<T>())
        return T(-1);
    else
        return (T(1) << bit_width) - 1;
}

template<typename T, size_t width>
struct BitPack {
    static constexpr size_t bit_width = width;
    static constexpr size_t register_width = 1024;
    static constexpr size_t num_bits = t_bits<T>();
    static_assert(bit_width <= num_bits);

    static constexpr T mask = make_mask<T, bit_width>();
    static constexpr size_t num_lanes = register_width / num_bits;

    static constexpr size_t bit_pos(size_t row) {
        return (row * bit_width) % num_bits;
    }

    static constexpr size_t row_pos(size_t row) {
        return (row * bit_width) / num_bits;
    }

    static constexpr bool at_end(size_t row) {
        return bit_pos(row) + bit_width >= num_bits;
    }

    static constexpr size_t unused_bits(size_t row) {
        return (num_bits - bit_pos(row)) % num_bits;
    }

    static constexpr size_t used_bits(size_t row) {
        return num_bits - unused_bits(row);
    }

    static constexpr size_t remaining_bits(size_t row) {
        return at_end(row) ? bit_width - unused_bits(row) : 0;
    }
};

static_assert(BitPack<uint64_t, 5>::num_lanes == 16);
static_assert(BitPack<uint8_t, 6>::num_lanes == 128);
static_assert(BitPack<uint16_t, 7>::num_bits == 16);
static_assert(BitPack<uint8_t, 3>::mask == 7);static_assert(BitPack<uint8_t, 3>::bit_pos(5) == 7);

static_assert(BitPack<uint8_t, 3>::unused_bits(2) == 2);

static_assert(BitPack<uint8_t, 3>::bit_pos(0) == 0);
static_assert(BitPack<uint8_t, 3>::bit_pos(1) == 3);
static_assert(BitPack<uint8_t, 3>::bit_pos(4) == 4);
static_assert(BitPack<uint8_t, 3>::bit_pos(3) == 1);
static_assert(BitPack<uint8_t, 3>::bit_pos(5) == 7);
static_assert(BitPack<uint8_t, 3>::bit_pos(2) == 6);
static_assert(BitPack<uint8_t, 3>::at_end(2));
static_assert(!BitPack<uint8_t, 3>::at_end(3));
static_assert(BitPack<uint8_t, 3>::unused_bits(2) == 2);
static_assert(BitPack<uint8_t, 3>::used_bits(2) == 6);

template<typename T, size_t bit_width>
void bitpack(const T *__restrict in, T *__restrict out) {
    using BitPacker = BitPack<T, bit_width>;
    T tmp;
    T src;
    for (auto i = 0U; i < BitPacker::num_lanes; ++i) {
        loop<T, BitPacker::num_bits>([&i, &src, in, &tmp, &out](auto j) {
            src = *(in + BitPacker::num_lanes * j.value + i);
            src = src & BitPacker::mask;
            if constexpr (BitPacker::bit_pos(j.value) == 0)
                tmp = src;
            else
                tmp |= src <<  BitPacker::bit_pos(j.value);

            if constexpr (BitPacker::at_end(j.value)) {
                constexpr auto bits_left = BitPacker::unused_bits(j.value);
                *(out + i) = tmp;
                if constexpr (bits_left > 0) {
                    src = *(in + BitPacker::num_lanes * j.value + i);
                    tmp |= (src & make_mask<T, bits_left>()) << BitPacker::used_bits(bits_left);
                }

                if constexpr (j.value != BitPacker::num_bits - 1) {
                    out += BitPacker::num_lanes;
                }

                if constexpr (bits_left > 0)
                    tmp = src >> bits_left;
            }
        });
        out -= BitPacker::num_lanes * (BitPacker::bit_width - 1);
    }
}

template<typename T, size_t bit_width>
void bitunpack(const T *__restrict in, T *__restrict out) {
    using BitPacker = BitPack<T, bit_width>;
    T tmp_0;
    T register_0;

    for (auto i = 0U; i < BitPacker::num_lanes; ++i) {
        loop<T, BitPacker::num_bits>([&i, in, &tmp_0, &out, &register_0](auto j) {
            if constexpr(j.value == 0) {
                register_0 = in[i];
                tmp_0 = register_0 & BitPacker::mask;
            } else {
                constexpr auto leftover_bits = BitPacker::remaining_bits(j.value - 1);
                if constexpr (leftover_bits != 0)
                    tmp_0 = (register_0  >> leftover_bits) & BitPacker::mask;
                else
                    tmp_0 = (register_0 >> BitPacker::bit_pos(j.value)) & BitPacker::mask;
            }

            if constexpr (BitPacker::at_end(j.value)) {
                constexpr auto bits_left_in_lane = BitPacker::unused_bits(j.value);
                if constexpr (bits_left_in_lane > 0) {
                    register_0 = in[i + (BitPacker::row_pos(j.value + 1) * BitPacker::num_lanes)];
                    tmp_0 |= (register_0  & make_mask<T, BitPacker::remaining_bits(j.value)>()) << bits_left_in_lane;
                }
            }

            out[i + (j.value * BitPacker::num_lanes)] = tmp_0;
        });
    }
}

} // namespace arcticdb