#include <gtest/gtest.h>
#include <arcticdb/codec/compression/delta.hpp>

#include <vector>
#include <random>
#include <chrono>
#include <iostream>
#include <numeric>

namespace arcticdb {

class CompressionTest : public ::testing::Test {
protected:
    std::mt19937 rng{42};

    template<typename T>
    std::vector<T> generate_data(size_t size, T start = T{0}, T base_step = T{1}) {
        static_assert(std::is_integral_v<T>, "Type must be integral");
        std::vector<T> data(size);
        std::random_device rd;
        std::mt19937 gen(rd());

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
    void verify_roundtrip(const std::vector<T>& input) {
        ColumnCompressor<T> compressor;
        ColumnDecompressor<T> decompressor{};

        size_t compressed_size = compressor.scan(input.data(), input.size());
        std::vector<T> compressed(compressed_size);

        std::vector<T> output(input.size());
        compressor.compress(input.data(), compressed.data(), compressed_size);

        decompressor.init(compressed.data());
        ASSERT_EQ(decompressor.num_rows(), input.size());
        decompressor.decompress(compressed.data(), output.data());
        ASSERT_EQ(output, input);
    }
};

TEST_F(CompressionTest, SingleFullBlock) {
    auto input = generate_data<uint16_t>(1024, 0, 1);
    verify_roundtrip(input);
}

TEST_F(CompressionTest, SinglePartialBlock) {
    auto input = generate_data<uint16_t>(500, 0, 1);
    verify_roundtrip(input);
}

TEST_F(CompressionTest, SmallPartialBlock) {
    auto input = generate_data<uint16_t>(10, 0, 1);
    verify_roundtrip(input);
}

TEST_F(CompressionTest, ThreeFullBlocks) {
    auto input = generate_data<uint16_t>(1024 * 3, 0, 1);
    verify_roundtrip(input);
}

TEST_F(CompressionTest, BlocksPlusRemainder) {
    auto input = generate_data<uint16_t>(1024 * 2 + 500, 0, 1);
    verify_roundtrip(input);
}

TEST_F(CompressionTest, SmallSizes) {
    for (size_t size : {1, 2, 3, 63, 64, 65}) {
        SCOPED_TRACE("Testing size: " + std::to_string(size));
        auto input = generate_data<uint16_t>(size, 0, 1);
        verify_roundtrip(input);
    }
}

TEST_F(CompressionTest, DifferentTypes) {
    {
        auto input = generate_data<uint8_t>(4000, 0, 1);
        verify_roundtrip(input);
    }

    {
        auto input = generate_data<uint16_t>(4000, 0, 1);
        verify_roundtrip(input);
    }

    {
        auto input = generate_data<uint32_t>(4000, 0, 1);
        verify_roundtrip(input);
    }

    {
        auto input = generate_data<uint64_t>(4000, 0, 1);
        verify_roundtrip(input);
    }
}

TEST_F(CompressionTest, SmallRange) {
    auto input = generate_data<uint64_t>(2000, 0, 1);
    verify_roundtrip(input);
}

TEST_F(CompressionTest, DifferentRanges) {
    // Small deltas
    {
        auto input = generate_data<uint64_t>(2000, 0, 1);
        verify_roundtrip(input);
    }

    // Medium deltas
    {
        auto input = generate_data<uint64_t>(2000, 0, 100);
        verify_roundtrip(input);
    }

    // Large deltas
    {
        auto input = generate_data<uint64_t>(2000, 0, 4000);
        verify_roundtrip(input);
    }
}

TEST_F(CompressionTest, MonotonicSequences) {
    std::vector<uint16_t> input(2000);

    std::iota(input.begin(), input.end(), 0);
    verify_roundtrip(input);
}

TEST_F(CompressionTest, SizeEstimation) {
    auto input = generate_data<uint16_t>(2000, 0, 1);

    ColumnCompressor<uint16_t> compressor;
    size_t estimated_size = compressor.scan(input.data(), input.size());

    std::vector<uint16_t> compressed(estimated_size);
    auto compressed_size = compressor.compress(input.data(), compressed.data(), estimated_size);
    ASSERT_EQ(compressed_size, estimated_size);

    ColumnDecompressor<uint16_t> decompressor{};
    decompressor.init(compressed.data());

    ASSERT_EQ(estimated_size, decompressor.compressed_size(compressed.data()));
}

TEST_F(CompressionTest, SizeEstimationPartial) {
    auto input = generate_data<uint16_t>(500, 0, 1);

    ColumnCompressor<uint16_t> compressor;
    size_t estimated_size = compressor.scan(input.data(), input.size());

    std::vector<uint16_t> compressed(estimated_size);
    auto compressed_size = compressor.compress(input.data(), compressed.data(), estimated_size);
    ASSERT_EQ(compressed_size, estimated_size);

    ColumnDecompressor<uint16_t> decompressor{};
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

    ColumnCompressor<T> scanner;
    size_t reqSize = scanner.scan(input.data(), numRows);
    std::vector<T> compressed(reqSize + 128, 0);
    std::vector<T> decompressed(numRows, 0);

    auto start_compress = std::chrono::high_resolution_clock::now();
    volatile size_t total_comp_size = 0;
    for (size_t i = 0; i < iterations; i++) {
        ColumnCompressor<T> compressor;
        auto estimated_size = compressor.scan(input.data(), input.size());
        size_t comp_size = compressor.compress(input.data(), compressed.data(), estimated_size);
        total_comp_size += comp_size;
    }
    auto end_compress = std::chrono::high_resolution_clock::now();
    auto compress_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_compress - start_compress).count();
    double avg_compress_time = static_cast<double>(compress_duration) / iterations;
    auto start_decompress = std::chrono::high_resolution_clock::now();
    volatile size_t total_decomp_rows = 0;
    for (size_t i = 0; i < iterations; i++) {
        ColumnDecompressor<T> decompressor{};
        decompressor.init(compressed.data());
        size_t rows_decomp = decompressor.decompress(compressed.data(), decompressed.data());
        total_decomp_rows += rows_decomp;
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