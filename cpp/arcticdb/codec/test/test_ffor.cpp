#include <gtest/gtest.h>

#include <arcticdb/util/timer.hpp>
#include <arcticdb/codec/calculate_stats.hpp>
#include <arcticdb/codec/compression/ffor.hpp>

#include <random>
#include <algorithm>

namespace arcticdb {

class FForCodecTest : public ::testing::Test {
protected:
    static constexpr size_t values_per_block = 1024;

    template<typename T>
    void verify_roundtrip(const std::vector<T>& original) {
        ASSERT_FALSE(original.empty());

        // Allocate space for compressed data with header
        std::vector<T> compressed(original.size() + header_size_in_t<FForHeader<T>, T>());
        std::vector<T> decompressed(original.size());

        // Compress
        size_t compressed_size = encode_ffor_with_header(
            original.data(),
            compressed.data(),
            original.size()
        );

        const auto header_size = header_size_in_t<FForHeader<T>, T>();
        ASSERT_GE(compressed_size, header_size);

        // Verify header
        const auto* header = reinterpret_cast<const FForHeader<T>*>(compressed.data());
        EXPECT_EQ(header->num_rows, original.size());
        EXPECT_LE(header->bits_needed, sizeof(T) * 8);

        // Verify reference value is minimum value
        T min_value = *std::min_element(original.begin(), original.end());
        EXPECT_EQ(header->reference, min_value);

        // Decompress
        size_t decompressed_size = decode_ffor_with_header(
            compressed.data(),
            decompressed.data()
        );

        EXPECT_EQ(decompressed_size, original.size());

        // Verify values
        for (size_t i = 0; i < original.size(); ++i) {
            if(original[i] != decompressed[i])
                log::version().debug("poops");

            EXPECT_EQ(original[i], decompressed[i])
                        << "Mismatch at index " << i;
        }
    }
};

TEST_F(FForCodecTest, SingleBlock) {
    std::vector<uint32_t> data(values_per_block);
    std::iota(data.begin(), data.end(), 1000);
    verify_roundtrip(data);
}

TEST_F(FForCodecTest, MultipleCompleteBlocks) {
    std::vector<uint32_t> data(values_per_block * 4);
    std::iota(data.begin(), data.end(), 1000);
    verify_roundtrip(data);
}

TEST_F(FForCodecTest, BlocksWithRemainder) {
    std::vector<uint32_t> data(values_per_block * 3 + values_per_block/2);
    std::iota(data.begin(), data.end(), 1000);
    verify_roundtrip(data);
}

TEST_F(FForCodecTest, SmallRange) {
    std::vector<uint32_t> data(values_per_block * 2);
    std::mt19937 rng(42);
    std::uniform_int_distribution<uint32_t> dist(1000, 1015);  // 4-bit range

    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = dist(rng);
    }
    std::sort(data.begin(), data.end());
    verify_roundtrip(data);
}

TEST_F(FForCodecTest, LargeRange) {
    std::vector<uint32_t> data(values_per_block * 2);
    std::mt19937 rng(42);
    std::uniform_int_distribution<uint32_t> dist(0, 1000000);

    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = dist(rng);
    }
    std::sort(data.begin(), data.end());
    verify_roundtrip(data);
}

TEST_F(FForCodecTest, DifferentTypes) {
    // Test uint32_t
    {
        std::vector<uint32_t> data(values_per_block * 2);
        std::iota(data.begin(), data.end(), 1000U);
        verify_roundtrip(data);
    }

    // Test uint64_t
    {
        std::vector<uint64_t> data(1024/64 * 2);  // Adjust for larger type
        std::iota(data.begin(), data.end(), 1000ULL);
        verify_roundtrip(data);
    }
}

TEST_F(FForCodecTest, EdgeCases) {
    // Test minimum size (one block)
   /* {
        std::vector<uint32_t> data(values_per_block);
        std::iota(data.begin(), data.end(), 0);
        verify_roundtrip(data);
    }

    // Test with just over one block
    {
        std::vector<uint32_t> data(values_per_block + 1);
        std::iota(data.begin(), data.end(), 0);
        verify_roundtrip(data);
    }*/

    // Test constant values
    {
        std::vector<uint32_t> data(values_per_block * 2, 42);
        verify_roundtrip(data);
    }

    // Test minimum range (all same value except one)
    {
        std::vector<uint32_t> data(values_per_block * 2, 42);
        data.back() = 43;  // One different value
        verify_roundtrip(data);
    }
}

TEST_F(FForCodecTest, RangePatterns) {
    // Test exponential range
    {
        std::vector<uint32_t> data(values_per_block * 2);
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = 1 << (i % 10);  // Use modulo to prevent overflow
        }
        std::sort(data.begin(), data.end());
        verify_roundtrip(data);
    }

    // Test logarithmic range
    {
        std::vector<uint32_t> data(values_per_block * 2);
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = static_cast<uint32_t>(std::log2(i + 2) * 1000);
        }
        verify_roundtrip(data);
    }

    // Test clustered values
    {
        std::vector<uint32_t> data(values_per_block * 2);
        std::mt19937 rng(42);
        std::normal_distribution<double> dist(1000, 10);

        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = static_cast<uint32_t>(dist(rng));
        }
        std::sort(data.begin(), data.end());
        verify_roundtrip(data);
    }
}

TEST_F(FForCodecTest, StressTest) {
    std::mt19937 rng(42);

    // Test different data patterns
    for (int test = 0; test < 10; ++test) {
        std::vector<uint32_t> data(values_per_block * 3 + values_per_block/2);

        switch (test % 3) {
        case 0: {
            // Small range with noise
            std::normal_distribution<double> noise(0, 5);
            for (size_t i = 0; i < data.size(); ++i) {
                data[i] = static_cast<uint32_t>(1000 + noise(rng));
            }
            break;
        }
        case 1: {
            // Multiple distinct ranges
            for (size_t i = 0; i < data.size(); ++i) {
                if (i < data.size()/3) {
                    data[i] = i % 100;
                } else if (i < 2*data.size()/3) {
                    data[i] = 1000 + (i % 100);
                } else {
                    data[i] = 10000 + (i % 100);
                }
            }
            break;
        }
        case 2: {
            // Random but bounded range
            std::uniform_int_distribution<uint32_t> dist(0, 1000);
            for (size_t i = 0; i < data.size(); ++i) {
                data[i] = dist(rng);
            }
            break;
        }
        }

        std::sort(data.begin(), data.end());
        SCOPED_TRACE("Test iteration " + std::to_string(test));
        verify_roundtrip(data);
    }
}

TEST(FFORStressTest, CompressDecompressSeparate) {
    using T = uint32_t;
    const size_t numRows = 100 * 1024;
    const size_t iterations = 1000000;
    std::vector<T> input(numRows);
    for (size_t i = 0; i < numRows; i++) {
        input[i] = static_cast<T>(i % 1000);
    }
    std::vector<T> compressed(numRows * 2, 0);
    std::vector<T> decompressed(numRows, 0);
    auto startCompress = std::chrono::high_resolution_clock::now();
    size_t totalCompSize = 0;
    for (size_t iter = 0; iter < iterations; iter++) {
        size_t compSize = encode_ffor_with_header(input.data(), compressed.data(), numRows);
        totalCompSize += compSize;
    }
    auto endCompress = std::chrono::high_resolution_clock::now();
    auto compressDuration = std::chrono::duration_cast<std::chrono::microseconds>(endCompress - startCompress).count();
    double avgCompressTime = static_cast<double>(compressDuration) / iterations;
    auto startDecompress = std::chrono::high_resolution_clock::now();
    for (size_t iter = 0; iter < iterations; iter++) {
        decode_ffor_with_header(compressed.data(), decompressed.data());
    }
    auto endDecompress = std::chrono::high_resolution_clock::now();
    auto decompressDuration = std::chrono::duration_cast<std::chrono::microseconds>(endDecompress - startDecompress).count();
    double avgDecompressTime = static_cast<double>(decompressDuration) / iterations;
    std::cout << "Average compression time per column: " << avgCompressTime << " microseconds" << std::endl;
    std::cout << "Average decompression time per column: " << avgDecompressTime << " microseconds" << std::endl;
    for (size_t i = 0; i < numRows; i++) {
        ASSERT_EQ(input[i], decompressed[i]) << "Mismatch at index " << i;
    }
    ASSERT_GT(totalCompSize, 0u);
}


TEST(BitPackStressTest, BlockBasedKernelPackUnpackStressTest) {
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
    // Each call to BitPackFused processes one block of 1024 values.
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
    FForCompressKernel<uint32_t> compress_kernel(12);
    FForUncompressKernel<uint32_t> uncompress_kernel(12);

    // Preliminary verification: pack/unpack the entire vector block-by-block and check equality.
    for (size_t block = 0; block < num_blocks; ++block) {
        const uint32_t* in_ptr = input.data() + block * block_size;
        uint32_t* packed_ptr = packed.data() + block * block_packed_size;
        BitPackFused<uint32_t, 11>::go(in_ptr, packed_ptr, compress_kernel);
    }
    for (size_t block = 0; block < num_blocks; ++block) {
        const uint32_t* packed_ptr = packed.data() + block * block_packed_size;
        uint32_t* out_ptr = output.data() + block * block_size;
        BitUnpackFused<uint32_t, 11>::go(packed_ptr, out_ptr, uncompress_kernel);
    }
    ASSERT_EQ(input, output) << "Preliminary pack/unpack did not restore the original input.";

    // Number of iterations for stress
    const size_t num_iterations = 1000000;

    // Stress test: Measure total packing time (each iteration packs all blocks).
    auto start_pack = std::chrono::steady_clock::now();
    for (size_t iter = 0; iter < num_iterations; ++iter) {
        for (size_t block = 0; block < num_blocks; ++block) {
            const uint32_t* in_ptr = input.data() + block * block_size;
            uint32_t* packed_ptr = packed.data() + block * block_packed_size;
            BitPackFused<uint32_t, 11>::go(in_ptr, packed_ptr, compress_kernel);
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
            BitUnpackFused<uint32_t, 11>::go(packed_ptr, out_ptr, uncompress_kernel);
        }
    }
    auto end_unpack = std::chrono::steady_clock::now();
    auto unpack_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end_unpack - start_unpack).count();
    double avg_unpack_per_iter = static_cast<double>(unpack_duration_us) / num_iterations;

    // Final verification.
    for (size_t block = 0; block < num_blocks; ++block) {
        const uint32_t* packed_ptr = packed.data() + block * block_packed_size;
        uint32_t* out_ptr = output.data() + block * block_size;
        BitUnpackFused<uint32_t, 11>::go(packed_ptr, out_ptr, uncompress_kernel);
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