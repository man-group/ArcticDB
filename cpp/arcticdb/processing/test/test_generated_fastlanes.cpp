//#include <gtest/gtest.h>
//#include <boost/hana.hpp>
//#include <boost/circular_buffer.hpp>
//#include <array>
//#include <iostream>
//
//namespace hana = boost::hana;
//using namespace hana::literals;
//
//constexpr std::array<int, 8> FL_ORDER = {0, 4, 2, 6, 1, 5, 3, 7};
//
//constexpr int index(int row, int lane) {
//    int o = row / 8;
//    int s = row % 8;
//    return (FL_ORDER[o] * 16) + (s * 128) + lane;
//}
//
//template<int T, typename Func>
//void iterate(int lane, Func&& func) {
//    auto range = hana::to_tuple(hana::range_c<int, 0, T>);
//    hana::for_each(range, [&](auto row) {
//        int idx = index(row, lane);
//        func(idx);
//    });
//}
//
//template<typename T, int W, int lane, typename Func>
//void pack(std::vector<T>& packed, Func&& func) {
//    constexpr int T_bits = sizeof(T) * 8;
//
//    if (W == 0) {
//        // Nothing to do if W is 0, since the packed array is zero bytes.
//        return;
//    } else if (W == T_bits) {
//        // Special case for W=T, we can just copy the input value directly to the packed value.
//        hana::for_each(hana::range_c<int, 0, T_bits>, [&](auto row) {
//            int idx = index(row, lane);
//            packed[T_bits * row + lane] = func(idx);
//        });
//    } else {
//        // A mask of W bits.
//        T mask = (1 << W) - 1;
//        T tmp = 0;
//
//        // Loop over each of the rows of the lane.
//        hana::for_each(hana::range_c<int, 0, T_bits>, [&](auto row) {
//            int idx = index(row, lane);
//            T src = func(idx) & mask;
//
//            // Shift the src bits into their position in the tmp output variable.
//            if (row == 0) {
//                tmp = src;
//            } else {
//                tmp |= src << (row * W) % T_bits;
//            }
//
//            // If the next packed position is after our current one, then we have filled
//            // the current output and we can write the packed value.
//            int curr_word = (row * W) / T_bits;
//            int next_word = ((row + 1) * W) / T_bits;
//
//            if (next_word > curr_word) {
//                packed[T_bits * curr_word + lane] = tmp;
//                int remaining_bits = ((row + 1) * W) % T_bits;
//                // Keep the remaining bits for the next packed value.
//                tmp = src >> (W - remaining_bits);
//            }
//        });
//    }
//}
//
//template<typename T>
//constexpr T mask(size_t width) {
//    return (width == sizeof(T) * 8) ? static_cast<T>(-1) : (static_cast<T>(1) << (width % (sizeof(T) * 8))) - 1;
//}
//
//template<typename T, int W, int lane, typename Func>
//void unpack(const std::vector<T>& packed, Func&& func) {
//    constexpr int T_bits = sizeof(T) * 8;
//
//    if (W == 0) {
//        // Special case for W=0, we just need to zero the output.
//        hana::for_each(hana::range_c<int, 0, T_bits>, [&](auto row) {
//            int idx = index(row, lane);
//            T zero = 0;
//            func(idx, zero);
//        });
//    } else if (W == T_bits) {
//        // Special case for W=T, we can just copy the packed value directly to the output.
//        hana::for_each(hana::range_c<int, 0, T_bits>, [&](auto row) {
//            int idx = index(row, lane);
//            T src = packed[T_bits * row + lane];
//            func(idx, src);
//        });
//    } else {
//        T src = packed[lane];
//        T tmp;
//
//        hana::for_each(hana::range_c<int, 0, T_bits>, [&](auto row) {
//            int curr_word = (row * W) / T_bits;
//            int next_word = ((row + 1) * W) / T_bits;
//            int shift = (row * W) % T_bits;
//
//            if (next_word > curr_word) {
//                int remaining_bits = ((row + 1) * W) % T_bits;
//                int current_bits = W - remaining_bits;
//                tmp = (src >> shift) & mask<T>(current_bits);
//
//                if (next_word < W) {
//                    src = packed[T_bits * next_word + lane];
//                    tmp |= (src & mask<T>(remaining_bits)) << current_bits;
//                }
//            } else {
//                tmp = (src >> shift) & mask<T>(W);
//            }
//
//            int idx = index(row, lane);
//            func(idx, tmp);
//        });
//    }
//}
//
//TEST(PackUnpackTest, BasicTest) {
//    // Initialize values array
//    std::vector<uint16_t> values(1024);
//    for (int i = 0; i < 1024; ++i) {
//        values[i] = static_cast<uint16_t>(i % (1 << 15));
//    }
//
//    // Initialize packed array
//    std::vector<uint16_t> packed(960, 0);
//    constexpr int lanes = 1;  // Assuming single lane for simplicity; adjust as necessary
//
//    for (int lane = 0; lane < lanes; ++lane) {
//        pack<uint16_t, 15, lane>(packed, [&](int pos) -> uint16_t {
//            return values[pos];
//        });
//    }
//
//    // Initialize packed_orig array
//    std::vector<uint16_t> packed_orig(960, 0);
//    // Assuming BitPacking::pack is already defined somewhere in your codebase
//    BitPacking::pack<15>(values.data(), packed_orig.data());
//
//    // Initialize unpacked array
//    std::vector<uint16_t> unpacked(1024, 0);
//
//    for (int lane = 0; lane < lanes; ++lane) {
//        unpack<uint16_t, 15, lane>(packed, [&](int idx, uint16_t elem) {
//        unpacked[idx] = elem;
//    });
//    }
//
//    // Check that values are correctly unpacked
//    ASSERT_EQ(values, unpacked);
//}