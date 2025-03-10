
#include <gtest/gtest.h>

#include <arcticdb/codec/compression/ffor_bitpack.hpp>

namespace arcticdb {

TEST(FFORBitPackStressTest, TestStressRoundtripFFORBitPack) {
    // Total number of input elements: 1024 * 100.
    const size_t total_elements = 1024 * 100; // 102400 elements.
    // Each block is 1024 elements. Thus, number of blocks:
    const size_t block_size = 1024;
    const size_t num_blocks = total_elements / block_size; // 100 blocks.

    // Generate input values that, after subtracting 12, fit in 11 bits.
    // We generate values in the range [12, 2059], so that after compressing (value - 12) the range is [0, 2047].
    std::vector<uint32_t> input(total_elements);
    for (size_t i = 0; i < total_elements; ++i) {
        input[i] = static_cast<uint32_t>((i % 2048) + 12);
    }

    // For T = uint32_t with FastLanesWidth = 1024:
    //   - Helper<uint32_t>::num_lanes = 1024 / (sizeof(uint32_t)*8) = 1024 / 32 = 32.
    // Each call to FFORBitPack processes one block of 1024 values.
    // Per block, each lane produces 11 words (as computed from the bit packing logic).
    constexpr size_t num_lanes = 32;
    constexpr size_t words_per_lane = 11; // derived from the bit packing logic
    const size_t block_packed_size = num_lanes * words_per_lane; // 32 * 11 = 352.
    // Total packed storage for the whole vector:
    const size_t total_packed_size = num_blocks * block_packed_size; // 100 * 352 = 35200 elements.

    // Allocate buffers for packed data and output.
    std::vector<uint32_t> packed(total_packed_size, 0);
    std::vector<uint32_t> output(total_elements, 0);

    // Create kernel objects with reference value 12.
    const uint32_t base = 12;

    // Preliminary verification: pack/unpack the entire vector block-by-block and check equality.
    for (size_t block = 0; block < num_blocks; ++block) {
        const uint32_t* in_ptr = input.data() + block * block_size;
        uint32_t* packed_ptr = packed.data() + block * block_packed_size;
        FFORBitPack<uint32_t, 11>::go(in_ptr, packed_ptr, &base);
    }
    for (size_t block = 0; block < num_blocks; ++block) {
        const uint32_t* packed_ptr = packed.data() + block * block_packed_size;
        uint32_t* out_ptr = output.data() + block * block_size;
        FFORBitUnpack<uint32_t, 11>::go(packed_ptr, out_ptr, &base);
    }
    ASSERT_EQ(input, output) << "Preliminary pack/unpack did not restore the original input.";

    // Number of iterations for stress
    const size_t num_iterations = 10000;

    // Stress test: Measure total packing time (each iteration packs all blocks).
    auto start_pack = std::chrono::steady_clock::now();
    for (size_t iter = 0; iter < num_iterations; ++iter) {
        for (size_t block = 0; block < num_blocks; ++block) {
            const uint32_t* in_ptr = input.data() + block * block_size;
            uint32_t* packed_ptr = packed.data() + block * block_packed_size;
            FFORBitPack<uint32_t, 11>::go(in_ptr, packed_ptr, &base);
        }
    }
    auto end_pack = std::chrono::steady_clock::now();
    auto pack_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end_pack - start_pack).count();
    double avg_pack_per_iter = static_cast<double>(pack_duration_us) / num_iterations;

    // Stress test: Measure total unpacking time (each iteration unpacks all blocks).
    auto start_unpack = std::chrono::steady_clock::now();
    for (size_t iter = 0; iter < num_iterations; ++iter) {
        for (size_t block = 0; block < num_blocks; ++block) {
            const uint32_t* packed_ptr = packed.data() + block * block_packed_size;
            uint32_t* out_ptr = output.data() + block * block_size;
            FFORBitUnpack<uint32_t, 11>::go(packed_ptr, out_ptr, &base);
        }
    }
    auto end_unpack = std::chrono::steady_clock::now();
    auto unpack_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end_unpack - start_unpack).count();
    double avg_unpack_per_iter = static_cast<double>(unpack_duration_us) / num_iterations;

    // Final verification.
    for (size_t block = 0; block < num_blocks; ++block) {
        const uint32_t* packed_ptr = packed.data() + block * block_packed_size;
        uint32_t* out_ptr = output.data() + block * block_size;
        FFORBitUnpack<uint32_t, 11>::go(packed_ptr, out_ptr, &base);
    }
    ASSERT_EQ(input, output) << "Final unpack did not yield the original input.";

    std::cout << "Total pack time (all blocks, " << num_iterations << " iterations): "
              << pack_duration_us << " us\n";
    std::cout << "Average pack time per iteration: "
              << avg_pack_per_iter << " us\n";
    std::cout << "Total unpack time (all blocks, " << num_iterations << " iterations): "
              << unpack_duration_us << " us\n";
    std::cout << "Average unpack time per iteration: "
              << avg_unpack_per_iter << " us\n";
}


} // namespace arcticdb