#include <gtest/gtest.h>
#include <chrono>
#include <iostream>
#include <vector>
#include <cstdint>
#include <climits>
#include <algorithm>
#include <lz4.h>
#include <random>

TEST(LZ4StressTest, CompressDecompressSeparate) {
    using T = uint32_t;
    const size_t numRows = 100 * 1024 + 32;
    const size_t iterations = 100;
    std::vector<T> input(numRows);
    std::random_device rd;
    std::mt19937 gen(rd());
    
    std::uniform_int_distribution<> dis(0, 1000);
    for (size_t i = 0; i < numRows; i++) {
        input[i] = dis(gen);
    }
    const int source_size = static_cast<int>(numRows * sizeof(T));
    const int max_dest_size = LZ4_compressBound(source_size);
    std::vector<char> compressed(max_dest_size, 0);
    std::vector<T> decompressed(numRows, 0);
    for (size_t i = 0; i < 10; i++) {
        int compressed_size = LZ4_compress_default(reinterpret_cast<const char*>(input.data()), compressed.data(), source_size, max_dest_size);
        LZ4_decompress_safe(compressed.data(), reinterpret_cast<char*>(decompressed.data()), compressed_size, source_size);
    }
    auto start_compress = std::chrono::high_resolution_clock::now();
    int total_comp_size = 0;
    int sample_comp_size [[maybe_unused]];
    for (size_t iter = 0; iter < iterations; iter++) {
        int compressed_size = LZ4_compress_default(reinterpret_cast<const char*>(input.data()), compressed.data(), source_size, max_dest_size);
        total_comp_size += compressed_size;
        if (iter == 0) sample_comp_size = compressed_size;
    }

    auto end_compress = std::chrono::high_resolution_clock::now();
    auto compress_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_compress - start_compress).count();
    double avg_compress_time = static_cast<double>(compress_duration) / iterations;
    int valid_comp_size = LZ4_compress_default(reinterpret_cast<const char*>(input.data()), compressed.data(), source_size, max_dest_size);
    auto start_decompress = std::chrono::high_resolution_clock::now();
    for (size_t iter = 0; iter < iterations; iter++) {
        (void)LZ4_decompress_safe(compressed.data(), reinterpret_cast<char*>(decompressed.data()), valid_comp_size, source_size);
    }

    auto end_decompress = std::chrono::high_resolution_clock::now();
    auto decompress_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_decompress - start_decompress).count();
    double avg_decompress_time = static_cast<double>(decompress_duration) / iterations;
    std::cout << "Average compression time per column: " << avg_compress_time << " microseconds" << std::endl;
    std::cout << "Average decompression time per column: " << avg_decompress_time << " microseconds" << std::endl;
    int final_decomp_size [[maybe_unused]] = LZ4_decompress_safe(compressed.data(), reinterpret_cast<char*>(decompressed.data()), valid_comp_size, source_size);
    const char* input_bytes = reinterpret_cast<const char*>(input.data());
    const char* decompressed_bytes = reinterpret_cast<const char*>(decompressed.data());
    for (size_t i = 0; i < static_cast<size_t>(source_size); i++) {
        ASSERT_EQ(input_bytes[i], decompressed_bytes[i]) << "Mismatch at byte index " << i;
    }
    ASSERT_GT(total_comp_size, 0);
}
