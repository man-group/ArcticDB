//
// Created by root on 3/9/25.
//
#include <gtest/gtest.h>

#include <arcticdb/codec/compression/bitpack.hpp>

namespace arcticdb {
TEST(BitPackStress, Roundtrip) {
    using T = uint32_t;
    constexpr size_t BLOCK_SIZE = 1024;   // Each block has 1024 values
    constexpr size_t NUM_BLOCKS = 100;      // Total of 100 blocks per run
    constexpr size_t NUM_ITERATIONS = 1000000; // Run a million iterations (each processes 100 blocks)
    constexpr size_t BIT_WIDTH = 16;        // Example bit width (< sizeof(T)*8)

    // Prepare input data (ensure values fit in BIT_WIDTH bits)
    std::vector<T> input(NUM_BLOCKS * BLOCK_SIZE);
    for (size_t i = 0; i < input.size(); i++) {
        input[i] = static_cast<T>(i & 0xFFFF);  // Keep values in range of BIT_WIDTH
    }

    // Compute the compressed block size from the helper (for T = uint32_t, BIT_WIDTH = 16,
    // the compressed size per block is computed as:
    //    ceil( BLOCK_SIZE * BIT_WIDTH / (sizeof(T)*8) )
    constexpr size_t COMPRESSED_BLOCK_SIZE = BitPack<T, BIT_WIDTH>::compressed_size();

    // Allocate space for packed (compressed) and unpacked data.
    std::vector<T> packed(NUM_BLOCKS * COMPRESSED_BLOCK_SIZE, 0);
    std::vector<T> unpacked(NUM_BLOCKS * BLOCK_SIZE, 0);



    // Use a volatile checksum to prevent unwanted optimization.
    volatile T checksum = 0;

    // --- Performance test for packing ---
    auto start_pack = std::chrono::high_resolution_clock::now();
    for (size_t iter = 0; iter < NUM_ITERATIONS; iter++) {
        // Process each of the 100 blocks
        for (size_t b = 0; b < NUM_BLOCKS; b++) {
            const T* in_ptr = input.data() + b * BLOCK_SIZE;
            T* out_ptr = packed.data() + b * COMPRESSED_BLOCK_SIZE;
            // The go() function compresses one block and returns the compressed size.
            size_t comp_size = BitPack<T, BIT_WIDTH>::go(in_ptr, out_ptr);
            checksum += comp_size; // Accumulate to prevent optimization
        }
    }
    auto end_pack = std::chrono::high_resolution_clock::now();
    auto pack_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_pack - start_pack).count();
    double pack_avg_time = static_cast<double>(pack_duration) / (NUM_ITERATIONS );

    std::cout << "Packing average time per block: "
              << pack_avg_time << " microseconds" << std::endl;

    volatile size_t compressed = 0;
    auto start_unpack = std::chrono::high_resolution_clock::now();
    for (size_t iter = 0; iter < NUM_ITERATIONS; iter++) {
        // Process each of the 100 blocks
        for (size_t b = 0; b < NUM_BLOCKS; b++) {
            const T* in_ptr = packed.data() + b * COMPRESSED_BLOCK_SIZE;
            T* out_ptr = unpacked.data() + b * BLOCK_SIZE;
            // The go() function decompresses one block
            compressed += BitUnpack<T, BIT_WIDTH>::go(in_ptr, out_ptr);
            checksum += out_ptr[0]; // Use first unpacked value to prevent optimization
        }
    }
    auto end_unpack = std::chrono::high_resolution_clock::now();
    auto unpack_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_unpack - start_unpack).count();
    double unpack_avg_time = static_cast<double>(unpack_duration) / (NUM_ITERATIONS);
    std::cout << compressed << std::endl;
    std::cout << "Unpacking average time per block: "
              << unpack_avg_time << " microseconds" << std::endl;

    // Output checksum so the compiler does not remove the calls.
    std::cout << "Checksum: " << checksum << std::endl;
    ASSERT_EQ(input, unpacked);
}
} //namespace arcticdb