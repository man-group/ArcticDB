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
    // Define the range of random numbers (0 to 100)
    std::uniform_int_distribution<> dis(0, 1000);
    for (size_t i = 0; i < numRows; i++) {
        input[i] = dis(gen);
    }
    const int srcSize = static_cast<int>(numRows * sizeof(T));
    const int maxDstSize = LZ4_compressBound(srcSize);
    std::vector<char> compressed(maxDstSize, 0);
    std::vector<T> decompressed(numRows, 0);
    for (size_t i = 0; i < 10; i++) {
        int compSize = LZ4_compress_default(reinterpret_cast<const char*>(input.data()), compressed.data(), srcSize, maxDstSize);
        LZ4_decompress_safe(compressed.data(), reinterpret_cast<char*>(decompressed.data()), compSize, srcSize);
    }
    auto startCompress = std::chrono::high_resolution_clock::now();
    volatile int totalCompSize = 0;
    int sampleCompSize [[maybe_unused]];
    for (size_t iter = 0; iter < iterations; iter++) {
        int compSize = LZ4_compress_default(reinterpret_cast<const char*>(input.data()), compressed.data(), srcSize, maxDstSize);
        totalCompSize += compSize;
        if (iter == 0) sampleCompSize = compSize;
    }
    auto endCompress = std::chrono::high_resolution_clock::now();
    auto compressDuration = std::chrono::duration_cast<std::chrono::microseconds>(endCompress - startCompress).count();
    double avgCompressTime = static_cast<double>(compressDuration) / iterations;
    int validCompSize = LZ4_compress_default(reinterpret_cast<const char*>(input.data()), compressed.data(), srcSize, maxDstSize);
    auto startDecompress = std::chrono::high_resolution_clock::now();
    volatile int totalDecompSize = 0;
    for (size_t iter = 0; iter < iterations; iter++) {
        int decompSize = LZ4_decompress_safe(compressed.data(), reinterpret_cast<char*>(decompressed.data()), validCompSize, srcSize);
        totalDecompSize += decompSize;
    }
    auto endDecompress = std::chrono::high_resolution_clock::now();
    auto decompressDuration = std::chrono::duration_cast<std::chrono::microseconds>(endDecompress - startDecompress).count();
    double avgDecompressTime = static_cast<double>(decompressDuration) / iterations;
    std::cout << "Average compression time per column: " << avgCompressTime << " microseconds" << std::endl;
    std::cout << "Average decompression time per column: " << avgDecompressTime << " microseconds" << std::endl;
    int finalDecompSize [[maybe_unused]] = LZ4_decompress_safe(compressed.data(), reinterpret_cast<char*>(decompressed.data()), validCompSize, srcSize);
    const char* inputBytes = reinterpret_cast<const char*>(input.data());
    const char* decompBytes = reinterpret_cast<const char*>(decompressed.data());
    for (size_t i = 0; i < static_cast<size_t>(srcSize); i++) {
        ASSERT_EQ(inputBytes[i], decompBytes[i]) << "Mismatch at byte index " << i;
    }
    ASSERT_GT(totalCompSize, 0);
}
