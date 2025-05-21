#include <gtest/gtest.h>
#include <arcticdb/codec/compression/delta.hpp>
#include <arcticdb/codec/test/encoding_test_common.hpp>

#include <vector>
#include <random>
#include <chrono>
#include <iostream>
#include <numeric>

namespace arcticdb {

class DeltaCompressionTest : public ::testing::Test {
protected:
    std::mt19937 rng_{42};

    template<typename T>
    std::vector<T> generate_data(size_t size, T start = T{0}, T base_step = T{1}) {
        static_assert(std::is_integral_v<T>, "Type must be integral");
        std::vector<T> data(size);
        std::mt19937 gen(rng_);

        T min_step = base_step * T{8} / T{10};
        T max_step = base_step * T{12} / T{10};

        min_step = std::max(min_step, T{1});
        max_step = std::max(max_step, static_cast<T>(min_step + 1));

        std::uniform_int_distribution<T> step_var(min_step, max_step);

        T current [[maybe_unused]] = start;
        for (size_t i = 0; i < size; ++i) {
           data[i] = current;
           current += step_var(gen);
        }

        return data;
    }

    template<typename T>
    void verify_roundtrip(const std::vector<T>& input, TypeDescriptor type) {
        DeltaCompressor<T> compressor;
        DeltaDecompressor<T> decompressor{};
        auto wrapper = from_vector(input, type);

        size_t expected_bytes = compressor.scan(wrapper.data_, input.size());
        ASSERT_GT(expected_bytes, 0) << "Scan returned zero size";

        ASSERT_EQ(expected_bytes % sizeof(T), 0) << "Scan returned size (" << expected_bytes << " bytes) is not a multiple of element size (" << sizeof(T) << ")";
        size_t expected_elements = expected_bytes / sizeof(T);

        // Choose a red zone value that fits in T.
        T red_zone;
        if constexpr (sizeof(T) == 1) {
            red_zone = static_cast<T>(0xAD);
        } else if constexpr (sizeof(T) == 2) {
            red_zone = static_cast<T>(0xDEAD);
        } else if constexpr (sizeof(T) == 4) {
            red_zone = static_cast<T>(0xDEADBEEF);
        } else {
            red_zone = static_cast<T>(0xDEADBEEFDEADBEEFULL);
        }
        std::vector<T> compressed(expected_elements + 1, T{0});
        compressed[expected_elements] = red_zone; // Write the red zone value

        size_t compressed_bytes = compressor.compress(wrapper.data_, compressed.data(), expected_bytes);
        ASSERT_EQ(compressed_bytes, expected_bytes)
                        << "Actual compressed size (" << compressed_bytes * sizeof(T)
                        << " bytes) does not match expected size (" << expected_bytes << " bytes)";

        ASSERT_EQ(compressed[expected_elements], red_zone) << "Red zone overwritten";

        decompressor.init(compressed.data());
        ASSERT_EQ(decompressor.num_rows(), input.size());
        std::vector<T> output(input.size());
        decompressor.decompress(compressed.data(), output.data());
        ASSERT_EQ(output, input);
    }
};

TEST_F(DeltaCompressionTest, SingleFullBlock) {
    auto input = generate_data<uint16_t>(1024, 0, 1);
    verify_roundtrip(input, make_scalar_type(DataType::UINT16));
}

TEST_F(DeltaCompressionTest, SinglePartialBlock) {
    auto input = generate_data<uint16_t>(500, 0, 1);
    verify_roundtrip(input, make_scalar_type(DataType::UINT16));
}

TEST_F(DeltaCompressionTest, SmallPartialBlock) {
    auto input = generate_data<uint16_t>(10, 0, 1);
    verify_roundtrip(input, make_scalar_type(DataType::UINT16));
}

TEST_F(DeltaCompressionTest, ThreeFullBlocks) {
    auto input = generate_data<uint16_t>(1024 * 3, 0, 1);
    verify_roundtrip(input, make_scalar_type(DataType::UINT16));
}

TEST_F(DeltaCompressionTest, BlocksPlusRemainder) {
    auto input = generate_data<uint16_t>(1024 * 2 + 500, 0, 1);
    verify_roundtrip(input, make_scalar_type(DataType::UINT16));
}

TEST_F(DeltaCompressionTest, SimpleSmallBLock) {
    std::vector<uint16_t> input{1, 2, 3, 63, 64, 65};
    verify_roundtrip(input, make_scalar_type(DataType::UINT16));
}

TEST_F(DeltaCompressionTest, SmallSizes) {
    for (size_t size : {1, 2, 3, 63, 64, 65}) {
        SCOPED_TRACE("Testing size: " + std::to_string(size));
        auto input = generate_data<uint16_t>(size, 0, 1);
        verify_roundtrip(input, make_scalar_type(DataType::UINT16));
    }
}

TEST_F(DeltaCompressionTest, DifferentTypes) {
    {
        auto input = generate_data<uint8_t>(4000, 0, 1);
        verify_roundtrip(input, make_scalar_type(DataType::UINT16));
    }

    {
        auto input = generate_data<uint16_t>(4000, 0, 1);
        verify_roundtrip(input, make_scalar_type(DataType::UINT16));
    }

    {
        auto input = generate_data<uint32_t>(4000, 0, 1);
        verify_roundtrip(input, make_scalar_type(DataType::UINT16));
    }

    {
        auto input = generate_data<uint64_t>(4000, 0, 1);
        verify_roundtrip(input, make_scalar_type(DataType::UINT16));
    }
}

TEST_F(DeltaCompressionTest, SmallRange) {
    auto input = generate_data<uint64_t>(2000, 0, 1);
    verify_roundtrip(input, make_scalar_type(DataType::UINT16));
}

TEST_F(DeltaCompressionTest, DifferentRanges) {
    // Small deltas
    {
        auto input = generate_data<uint64_t>(2000, 0, 1);
        verify_roundtrip(input, make_scalar_type(DataType::UINT16));
    }

    // Medium deltas
    {
        auto input = generate_data<uint64_t>(2000, 0, 100);
        verify_roundtrip(input, make_scalar_type(DataType::UINT16));
    }

    // Large deltas
    {
        auto input = generate_data<uint64_t>(2000, 0, 4000);
        verify_roundtrip(input, make_scalar_type(DataType::UINT16));
    }
}

TEST_F(DeltaCompressionTest, MonotonicSequences) {
    std::vector<uint16_t> input(2000);

    std::iota(input.begin(), input.end(), 0);
    verify_roundtrip(input, make_scalar_type(DataType::UINT16));
}

TEST_F(DeltaCompressionTest, SingleFullBlockSignedInt16) {
    auto input = generate_data<int16_t>(1024, -500, 1);
    verify_roundtrip(input, make_scalar_type(DataType::INT16));
}

TEST_F(DeltaCompressionTest, SinglePartialBlockSignedInt16) {
    auto input = generate_data<int16_t>(500, -500, 1);
    verify_roundtrip(input, make_scalar_type(DataType::INT16));
}

TEST_F(DeltaCompressionTest, ThreeFullBlocksSignedInt32) {
    auto input = generate_data<int32_t>(1024 * 3, -100000, 5);
    verify_roundtrip(input, make_scalar_type(DataType::INT32));
}

TEST_F(DeltaCompressionTest, BlocksPlusRemainderSignedInt32) {
    auto input = generate_data<int32_t>(1024 * 2 + 500, -100000, 5);
    verify_roundtrip(input, make_scalar_type(DataType::INT32));
}

TEST_F(DeltaCompressionTest, SimpleSmallBlockSignedInt8) {
    std::vector<int8_t> input{-120, -119, -118, -60, -59, -58};
    verify_roundtrip(input, make_scalar_type(DataType::INT8));
}

TEST_F(DeltaCompressionTest, NegativeStartSignedInt32) {
    auto input = generate_data<int32_t>(2000, -10000, 10);
    verify_roundtrip(input, make_scalar_type(DataType::INT32));
}

TEST_F(DeltaCompressionTest, MonotonicSequencesSignedInt16) {
    std::vector<int16_t> input(2000);
    std::iota(input.begin(), input.end(), -500);
    verify_roundtrip(input, make_scalar_type(DataType::INT16));
}

TEST_F(DeltaCompressionTest, SmallSizesSignedInt32) {
    for (size_t size : {1, 2, 3, 63, 64, 65}) {
        auto input = generate_data<int32_t>(size, -500, 1);
        verify_roundtrip(input, make_scalar_type(DataType::INT32));
    }
}

TEST_F(DeltaCompressionTest, DifferentRangesSignedInt32) {
    {
        auto input = generate_data<int32_t>(2000, -100000, 1);
        verify_roundtrip(input, make_scalar_type(DataType::INT32));
    }
    {
        auto input = generate_data<int32_t>(2000, -100000, 100);
        verify_roundtrip(input, make_scalar_type(DataType::INT32));
    }
    {
        auto input = generate_data<int32_t>(2000, -100000, 4000);
        verify_roundtrip(input, make_scalar_type(DataType::INT32));
    }
}
TEST_F(DeltaCompressionTest, SizeEstimation) {
    auto input = generate_data<uint16_t>(2000, 0, 1);

    DeltaCompressor<uint16_t> compressor;
    auto wrapper = from_vector(input, make_scalar_type(DataType::UINT16));
    size_t estimated_size = compressor.scan(wrapper.data_, input.size());

    std::vector<uint16_t> compressed(estimated_size);
    auto compressed_size = compressor.compress(wrapper.data_, compressed.data(), estimated_size);
    ASSERT_EQ(compressed_size, estimated_size);

    DeltaDecompressor<uint16_t> decompressor{};
    decompressor.init(compressed.data());

    ASSERT_EQ(estimated_size, decompressor.compressed_size(compressed.data()));
}

TEST_F(DeltaCompressionTest, SizeEstimationPartial) {
    auto input = generate_data<uint16_t>(500, 0, 1);

    DeltaCompressor<uint16_t> compressor;
    auto wrapper = from_vector(input, make_scalar_type(DataType::UINT16));
    size_t estimated_size = compressor.scan(wrapper.data_, input.size());

    std::vector<uint16_t> compressed(estimated_size);
    auto compressed_size = compressor.compress(wrapper.data_, compressed.data(), estimated_size);
    ASSERT_EQ(compressed_size, estimated_size);

    DeltaDecompressor<uint16_t> decompressor{};
    decompressor.init(compressed.data());

    ASSERT_EQ(estimated_size, decompressor.compressed_size(compressed.data()));
}

template<typename T>
std::vector<T> generate_compressible_data(size_t size, T start = T{0}) {
    std::vector<T> data(size);
    std::mt19937 gen(42);
    std::uniform_int_distribution<T> step_dist(1, 5);
    T current = start;
    for (size_t i = 0; i < size; ++i) {
        data[i] = current;
        current += step_dist(gen);
    }
    return data;
}

TEST(DeltaCompressionStressTest, CompressDecompressSeparate) {
    using T = uint32_t;
    const size_t numRows = 100 * 1024;
    const size_t iterations = 1000;

    auto input = generate_compressible_data<T>(numRows);
    auto wrapper = from_vector(input, make_scalar_type(DataType::UINT32));
    DeltaCompressor<T> scanner;
    size_t reqSize = scanner.scan(wrapper.data_, numRows);
    std::vector<T> compressed(reqSize + 128, 0);
    std::vector<T> decompressed(numRows, 0);

    auto start_compress = std::chrono::high_resolution_clock::now();
    size_t total_comp_size = 0;
    for (size_t i = 0; i < iterations; i++) {
        DeltaCompressor<T> compressor;
        auto estimated_size = compressor.scan(wrapper.data_, input.size());
        size_t comp_size = compressor.compress(wrapper.data_, compressed.data(), estimated_size);
        total_comp_size += comp_size;
    }
    auto end_compress = std::chrono::high_resolution_clock::now();
    auto compress_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_compress - start_compress).count();
    double avg_compress_time = static_cast<double>(compress_duration) / iterations;
    auto start_decompress = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < iterations; i++) {
        DeltaDecompressor<T> decompressor{};
        decompressor.init(compressed.data());
        (void)decompressor.decompress(compressed.data(), decompressed.data());
    }
    auto end_decompress = std::chrono::high_resolution_clock::now();
    auto decompress_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_decompress - start_decompress).count();
    double avg_decompress_time = static_cast<double>(decompress_duration) / iterations;
    std::cout << "Average compression time per column: " << avg_compress_time << " microseconds" << std::endl;
    std::cout << "Average decompression time per column: " << avg_decompress_time << " microseconds" << std::endl;
    auto count = 10;
    for(auto i = 0UL; i < input.size(); ++i)
        if(input[i] != decompressed[i]) {
            std::cout << i << ": " << input[i] << " != " << decompressed[i] << std::endl;
            --count;
            if(count == 0)
                break;
        }
    ASSERT_EQ(input, decompressed);
    ASSERT_GT(total_comp_size, 0u);
}
} // namespace arcticdb