#include <gtest/gtest.h>

#include <arcticdb/codec/compression/ffor.hpp>
#include <arcticdb/codec/test/encoding_test_common.hpp>

#include <random>

namespace arcticdb {
class FForCodecTest : public ::testing::Test {
protected:
    static constexpr size_t values_per_block = 1024;

    template<typename T>
    void verify_roundtrip(const std::vector<T> &original, TypeDescriptor type) {
        ASSERT_FALSE(original.empty());
        auto wrapper = from_vector(original, type);

        auto ffor_data = FForCompressor<T>::reference_and_bitwidth(wrapper.data_);
        size_t expected_size_bytes = FForCompressor<T>::compressed_size(ffor_data, original.size());
        size_t expected_size_elems = expected_size_bytes / sizeof(T);

        std::vector<T> compressed(expected_size_elems + 1, 0);
        T guard_val = static_cast<T>(0xDEADBEEF);
        compressed[expected_size_elems] = guard_val;

        std::vector<T> decompressed(original.size());

        FForCompressor<T> ffor{ffor_data};
        size_t actual_compressed_bytes = ffor.compress(
            wrapper.data_,
            compressed.data(),
            expected_size_bytes
        );
        ASSERT_EQ(actual_compressed_bytes, expected_size_bytes);

        EXPECT_EQ(compressed[expected_size_elems], guard_val) << "Compression wrote beyond the reported boundaries";

        const auto *header = reinterpret_cast<const FForHeader<T> *>(compressed.data());
        EXPECT_EQ(header->num_rows, original.size());
        EXPECT_LE(header->bits_needed, sizeof(T) * 8);

        T min_value = *std::min_element(original.begin(), original.end());
        EXPECT_EQ(header->reference, min_value);

        auto result = FForDecompressor<T>::decompress(compressed.data(), decompressed.data());
        EXPECT_EQ(result.uncompressed_, original.size() * sizeof(T));

        for (size_t i = 0; i < original.size(); ++i) {
            if (original[i] != decompressed[i])
                log::version().debug("poops");
            EXPECT_EQ(original[i], decompressed[i]) << "Mismatch at index " << i;
        }
    }
};

TEST_F(FForCodecTest, SingleBlock) {
    std::vector<uint32_t> data(values_per_block);
    std::iota(data.begin(), data.end(), 1000);
    verify_roundtrip(data, make_scalar_type(DataType::UINT32));
}

TEST_F(FForCodecTest, VerySmallArray) {
    std::vector<uint32_t> data(12);
    std::iota(data.begin(), data.end(), 1000);
    verify_roundtrip(data, make_scalar_type(DataType::UINT32));
}

TEST_F(FForCodecTest, MultipleCompleteBlocks) {
    std::vector<uint32_t> data(values_per_block * 4);
    std::iota(data.begin(), data.end(), 1000);
    verify_roundtrip(data, make_scalar_type(DataType::UINT32));
}

TEST_F(FForCodecTest, BlocksWithRemainder) {
    std::vector<uint32_t> data(values_per_block * 3 + values_per_block/2);
    std::iota(data.begin(), data.end(), 1000);
    verify_roundtrip(data, make_scalar_type(DataType::UINT32));
}

TEST_F(FForCodecTest, SmallRange) {
    std::vector<uint32_t> data(values_per_block * 2);
    std::mt19937 rng(42);
    std::uniform_int_distribution<uint32_t> dist(1000, 1015);

    for (auto& i : data)
        i = dist(rng);

    std::sort(data.begin(), data.end());
    verify_roundtrip(data, make_scalar_type(DataType::UINT32));
}

TEST_F(FForCodecTest, LargeRange) {
    std::vector<uint32_t> data(values_per_block * 2);
    std::mt19937 rng(42);
    std::uniform_int_distribution<uint32_t> dist(0, 1000000);

    for (auto& i : data)
        i = dist(rng);

    std::sort(data.begin(), data.end());
    verify_roundtrip(data, make_scalar_type(DataType::UINT32));
}

TEST_F(FForCodecTest, DifferentTypes) {
    {
        std::vector<uint32_t> data(values_per_block * 2);
        std::iota(data.begin(), data.end(), 1000U);
        verify_roundtrip(data, make_scalar_type(DataType::UINT32));
    }

    {
        std::vector<uint64_t> data(1024/64 * 2);  // Adjust for larger type
        std::iota(data.begin(), data.end(), 1000ULL);
        verify_roundtrip(data, make_scalar_type(DataType::UINT64));
    }
}

TEST_F(FForCodecTest, EdgeCases) {
    // Test minimum size (one block)
    {
        std::vector<uint32_t> data(values_per_block);
        std::iota(data.begin(), data.end(), 0);
        verify_roundtrip(data, make_scalar_type(DataType::UINT32));
    }

    // Test with just over one block
    {
        std::vector<uint32_t> data(values_per_block + 1);
        std::iota(data.begin(), data.end(), 0);
        verify_roundtrip(data, make_scalar_type(DataType::UINT32));
    }

    // Test constant values
    {
        std::vector<uint32_t> data(values_per_block * 2, 42);
        verify_roundtrip(data, make_scalar_type(DataType::UINT32));
    }

    // Test minimum range (all same value except one)
    {
        std::vector<uint32_t> data(values_per_block * 2, 42);
        data.back() = 43;  // One different value
        verify_roundtrip(data, make_scalar_type(DataType::UINT32));
    }
}

TEST_F(FForCodecTest, RangePatterns) {
    // Exponential
    {
        std::vector<uint32_t> data(values_per_block * 2);
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = 1 << (i % 10);  // Use modulo to prevent overflow
        }
        std::sort(data.begin(), data.end());
        verify_roundtrip(data, make_scalar_type(DataType::UINT32));
    }

    // Logarithmic
    {
        std::vector<uint32_t> data(values_per_block * 2);
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = static_cast<uint32_t>(std::log2(i + 2) * 1000);
        }
        verify_roundtrip(data, make_scalar_type(DataType::UINT32));
    }

    // Normal distribution
    {
        std::vector<uint32_t> data(values_per_block * 2);
        std::mt19937 rng(42);
        std::normal_distribution<double> dist(1000, 10);

        for (auto& i : data)
            i = static_cast<uint32_t>(dist(rng));

        std::sort(data.begin(), data.end());
        verify_roundtrip(data, make_scalar_type(DataType::UINT32));
    }
}
TEST_F(FForCodecTest, SingleBlockSignedInt32) {
    std::vector<int32_t> data(values_per_block);
    std::iota(data.begin(), data.end(), -1000);
    verify_roundtrip(data, make_scalar_type(DataType::INT32));
}

TEST_F(FForCodecTest, MultipleCompleteBlocksSignedInt32) {
    std::vector<int32_t> data(values_per_block * 4);
    std::iota(data.begin(), data.end(), -5000);
    verify_roundtrip(data, make_scalar_type(DataType::INT32));
}

TEST_F(FForCodecTest, BlocksWithRemainderSignedInt32) {
    std::vector<int32_t> data(values_per_block * 3 + values_per_block / 2);
    std::iota(data.begin(), data.end(), -1000);
    verify_roundtrip(data, make_scalar_type(DataType::INT32));
}

TEST_F(FForCodecTest, SmallRangeSignedInt32) {
    std::vector<int32_t> data(values_per_block * 2);
    std::mt19937 rng(42);
    std::uniform_int_distribution<int32_t> dist(-100, -85);
    for (auto& i : data)
        i = dist(rng);
    std::sort(data.begin(), data.end());
    verify_roundtrip(data, make_scalar_type(DataType::INT32));
}

TEST_F(FForCodecTest, LargeRangeSignedInt32) {
    std::vector<int32_t> data(values_per_block * 2);
    std::mt19937 rng(42);
    std::uniform_int_distribution<int32_t> dist(-1000000, 1000000);
    for (auto& i : data)
        i = dist(rng);
    std::sort(data.begin(), data.end());
    verify_roundtrip(data, make_scalar_type(DataType::INT32));
}

TEST_F(FForCodecTest, DifferentTypesSigned) {
    {
        std::vector<int32_t> data(values_per_block * 2);
        std::iota(data.begin(), data.end(), -500);
        verify_roundtrip(data, make_scalar_type(DataType::INT32));
    }
    {
        std::vector<int64_t> data(values_per_block * 2);
        std::iota(data.begin(), data.end(), -10000LL);
        verify_roundtrip(data, make_scalar_type(DataType::INT64));
    }
    {
        std::vector<int8_t> data(256);
        for (size_t i = 0; i < data.size(); ++i)
            data[i] = static_cast<int8_t>(i % 128 - 64);
        verify_roundtrip(data, make_scalar_type(DataType::INT8));
    }
}

TEST_F(FForCodecTest, EdgeCasesSigned) {
    {
        std::vector<int32_t> data(values_per_block);
        std::iota(data.begin(), data.end(), -1000);
        verify_roundtrip(data, make_scalar_type(DataType::INT32));
    }
    {
        std::vector<int32_t> data(values_per_block + 1);
        std::iota(data.begin(), data.end(), -1000);
        verify_roundtrip(data, make_scalar_type(DataType::INT32));
    }
    {
        std::vector<int32_t> data(values_per_block * 2, -42);
        verify_roundtrip(data, make_scalar_type(DataType::INT32));
    }
    {
        std::vector<int32_t> data(values_per_block * 2, -42);
        data.back() = -41;
        verify_roundtrip(data, make_scalar_type(DataType::INT32));
    }
}

TEST_F(FForCodecTest, RangePatternsSigned) {
    {
        std::vector<int32_t> data(values_per_block * 2);
        for (size_t i = 0; i < data.size(); ++i)
            data[i] = -(1 << (i % 10));
        std::sort(data.begin(), data.end());
        verify_roundtrip(data, make_scalar_type(DataType::INT32));
    }
    {
        std::vector<int32_t> data(values_per_block * 2);
        for (size_t i = 0; i < data.size(); ++i)
            data[i] = static_cast<int32_t>(std::log2(i + 2) * 1000);
        verify_roundtrip(data, make_scalar_type(DataType::INT32));
    }
    {
        std::vector<int32_t> data(values_per_block * 2);
        std::mt19937 rng(42);
        std::normal_distribution<double> dist(0, 10);
        for (auto& i : data)
            i = static_cast<int32_t>(dist(rng));
        std::sort(data.begin(), data.end());
        verify_roundtrip(data, make_scalar_type(DataType::INT32));
    }
}

TEST_F(FForCodecTest, StressTest) {
    std::mt19937 rng(42);

    for (int test = 0; test < 10; ++test) {
        std::vector<uint32_t> data(values_per_block * 3 + values_per_block/2);

        switch (test % 3) {
        case 0: {
            // Small range with noisedd
            std::normal_distribution<double> noise(0, 5);
            for (auto& i : data) {
                i = static_cast<uint32_t>(1000 + noise(rng));
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
            for (auto& i : data) {
                i = dist(rng);
            }
            break;
        }
        }

        std::sort(data.begin(), data.end());
        SCOPED_TRACE("Test iteration " + std::to_string(test));
        verify_roundtrip(data, make_scalar_type(DataType::UINT32));
    }
}

TEST(FFORStressTest, CompressDecompress) {
    using T = uint32_t;
    constexpr size_t num_rows = 100 * 1024;
    constexpr size_t iterations = 10000;
    std::vector<T> input(num_rows);
    for (size_t i = 0; i < num_rows; i++) {
        input[i] = static_cast<T>(i % 1000);
    }
    auto wrapper = from_vector(input, make_scalar_type(DataType::UINT32));
    auto ffor_data = FForCompressor<T>::reference_and_bitwidth(wrapper.data_);
    FForCompressor<T> ffor{ffor_data};
    auto estimated_size = ffor.compressed_size(ffor_data, num_rows);
    std::vector<T> compressed(estimated_size, 0);
    std::vector<T> decompressed(num_rows, 0);
    auto start_compress = std::chrono::high_resolution_clock::now();
    size_t total_comp_size = 0;
    for (size_t iter = 0; iter < iterations; iter++) {

        size_t compressed_size = ffor.compress(wrapper.data_, compressed.data(), estimated_size);
        total_comp_size += compressed_size;
    }
    auto end_compress = std::chrono::high_resolution_clock::now();
    auto compress_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_compress - start_compress).count();
    double avg_compress_time = static_cast<double>(compress_duration) / iterations;
    auto start_decompress = std::chrono::high_resolution_clock::now();
    for (size_t iter = 0; iter < iterations; iter++) {
        FForDecompressor<T>::decompress(compressed.data(), decompressed.data());
    }
    auto end_decompress = std::chrono::high_resolution_clock::now();
    auto decompress_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_decompress - start_decompress).count();
    double avg_decompress_time = static_cast<double>(decompress_duration) / iterations;
    std::cout << "Average compression time per column: " << avg_compress_time << " microseconds" << std::endl;
    std::cout << "Average decompression time per column: " << avg_decompress_time << " microseconds" << std::endl;
    for (size_t i = 0; i < num_rows; i++) {
        ASSERT_EQ(input[i], decompressed[i]) << "Mismatch at index " << i;
    }
    ASSERT_GT(total_comp_size, 0u);
}

TEST(BitPackStressTest, BlockBasedKernelPackUnpackStressTest) {
    const size_t total_elements = 1024 * 100;
    const size_t block_size = 1024;
    const size_t num_blocks = total_elements / block_size; // 100 blocks.

    std::vector<uint32_t> input(total_elements);
    for (size_t i = 0; i < total_elements; ++i) {
        input[i] = static_cast<uint32_t>((i % 2048) + 12);
    }

    constexpr size_t num_lanes = 32;
    constexpr size_t words_per_lane = 11; 
    const size_t block_packed_size = num_lanes * words_per_lane; 
    const size_t total_packed_size = num_blocks * block_packed_size;

    std::vector<uint32_t> packed(total_packed_size, 0);
    std::vector<uint32_t> output(total_elements, 0);

    FForCompressKernel<uint32_t> compress_kernel(12);
    FForUncompressKernel<uint32_t> uncompress_kernel(12);

    // Verify
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

    // Stress
    const size_t num_iterations = 10000;
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

    for (size_t block = 0; block < num_blocks; ++block) {
        const uint32_t* packed_ptr = packed.data() + block * block_packed_size;
        uint32_t* out_ptr = output.data() + block * block_size;
        BitUnpackFused<uint32_t, 11>::go(packed_ptr, out_ptr, uncompress_kernel);
    }
    ASSERT_EQ(input, output) << "Final unpack did not yield the original input.";

    std::cout << "Total pack time (all blocks, " << num_iterations << " iterations): " << pack_duration_us << " us\n";
    std::cout << "Average pack time per iteration: " << avg_pack_per_iter << " us\n";
    std::cout << "Total unpack time (all blocks, " << num_iterations << " iterations): " << unpack_duration_us << " us\n";
    std::cout << "Average unpack time per iteration: " << avg_unpack_per_iter << " us\n";
}

} // namespace arcticdb