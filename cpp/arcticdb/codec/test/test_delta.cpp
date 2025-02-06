#include <gtest/gtest.h>
#include <arcticdb/codec/compression/delta.hpp>

#include <vector>
#include <random>
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <vector>
#include <numeric>

namespace arcticdb {


class CompressionTest : public ::testing::Test {
protected:
    std::mt19937 rng{42};  // Fixed seed for reproducibility

    template<typename T>
    std::vector<T> generate_data(size_t size, T start = T{0}, T base_step = T{1}) {
        static_assert(std::is_integral_v<T>, "Type must be integral");
        std::vector<T> data(size);
        std::random_device rd;
        std::mt19937 gen(rd());

        // Vary the step by up to ±20% of base_step
        T min_step = base_step * T{8} / T{10};  // 80% of base
        T max_step = base_step * T{12} / T{10}; // 120% of base

        // Ensure we have at least 1 as minimum step for integral types
        min_step = std::max(min_step, T{1});
        max_step = std::max(max_step, static_cast<T>(min_step + 1));

        std::uniform_int_distribution<T> step_var(min_step, max_step);

        T current = start;
        for (size_t i = 0; i < size; ++i) {
            data[i] = current;
            current += step_var(gen);
        }

        return data;
    }

    // Helper to verify roundtrip
    template<typename T>
    void verify_roundtrip(const std::vector<T>& input) {
        ColumnCompressor<T> compressor;
        ColumnDecompressor<T> decompressor;

        // First get required size
        size_t compressed_size = compressor.scan(input.data(), input.size());
        log::version().info("Generated {} rows", input.size());
        // Allocate and compress
        std::vector<T> compressed(compressed_size);
        compressor.compress(input.data(), compressed.data());

        // Decompress
        decompressor.init(compressed.data());
        ASSERT_EQ(decompressor.num_rows(), input.size());

        std::vector<T> output(input.size());
        decompressor.decompress(compressed.data(), output.data());

        for(auto i = 0UL; i < input.size(); ++i)  {
            if(input[i] != output[i])
                log::codec().error("Value mismatch at index {}, {} != {}",
                                   i, input[i], output[i]);

            ASSERT_EQ(input[i], output[i]);
        }
        ASSERT_EQ(output, input);
    }
};

// Test exactly one block (1024 values)
TEST_F(CompressionTest, SingleBlock) {
    auto input = generate_data<uint16_t>(1024 + Helper<uint16_t>::num_lanes, 0, 1);
    verify_roundtrip(input);
}

// Test less than one block
TEST_F(CompressionTest, PartialBlock) {
    auto input = generate_data<uint16_t>(500, 0, 1);
    verify_roundtrip(input);
}

// Test less than one block
TEST_F(CompressionTest, SmallPartialBlock) {
    auto input = generate_data<uint16_t>(10, 0, 1);
    verify_roundtrip(input);
}

// Test multiple complete blocks
TEST_F(CompressionTest, MultipleBlocks) {
    auto input = generate_data<uint16_t>(1024 * 3 + Helper<uint16_t>::num_lanes, 0, 1);
    verify_roundtrip(input);
}

// Test multiple blocks plus remainder
TEST_F(CompressionTest, BlocksPlusRemainder) {
    auto input = generate_data<uint16_t>(1024 * 2 + 500, 0, 1);
    verify_roundtrip(input);
}

// Test edge cases for small sizes
TEST_F(CompressionTest, SmallSizes) {
    for (size_t size : {1, 2, 3, 63, 64, 65}) {
        SCOPED_TRACE("Testing size: " + std::to_string(size));
        auto input = generate_data<uint16_t>(size, 0, 1);
        verify_roundtrip(input);
    }
}

// Test different integer types
TEST_F(CompressionTest, DifferentTypes) {
    // uint16_t
    {
        auto input = generate_data<uint16_t>(2000, 0, 1);
        verify_roundtrip(input);
    }

    // uint32_t
    {
        auto input = generate_data<uint32_t>(2000, 0, 1);
        verify_roundtrip(input);
    }

    // uint64_t
    {
        auto input = generate_data<uint64_t>(2000, 0, 1);
        verify_roundtrip(input);
    }
}

TEST_F(CompressionTest, SmallRange) {
    // Small deltas
    {
        auto input = generate_data<uint64_t>(2000, 0, 1);
        verify_roundtrip(input);
    }
}


// Test different value ranges (affecting bit widths)
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

// Test monotonic sequences
TEST_F(CompressionTest, MonotonicSequences) {
    std::vector<uint16_t> input(2000);

    std::iota(input.begin(), input.end(), 0);
    verify_roundtrip(input);
}

// Test constant values
TEST_F(CompressionTest, ConstantValues) {
    std::vector<uint16_t> input(2000, 42);
    verify_roundtrip(input);
}

// Test compression size estimation
TEST_F(CompressionTest, SizeEstimation) {
    auto input = generate_data<uint16_t>(2000, 0, 1);

    ColumnCompressor<uint16_t> compressor;
    size_t estimated_size = compressor.scan(input.data(), input.size());

    // Compress the data
    std::vector<uint16_t> compressed(estimated_size);
    auto compressed_size = compressor.compress(input.data(), compressed.data());
    ASSERT_EQ(compressed_size, estimated_size);

    // Initialize decompressor to get actual size
    ColumnDecompressor<uint16_t> decompressor;
    decompressor.init(compressed.data());

    // Verify estimated size matches actual size
    ASSERT_EQ(estimated_size, decompressor.compressed_size(compressed.data()));
}

TEST_F(CompressionTest, SizeEstimationPartial) {
    auto input = generate_data<uint16_t>(500, 0, 1);

    ColumnCompressor<uint16_t> compressor;
    size_t estimated_size = compressor.scan(input.data(), input.size());

    // Compress the data
    std::vector<uint16_t> compressed(estimated_size);
    auto compressed_size = compressor.compress(input.data(), compressed.data());
    ASSERT_EQ(compressed_size, estimated_size);

    // Initialize decompressor to get actual size
    ColumnDecompressor<uint16_t> decompressor;
    decompressor.init(compressed.data());

    // Verify estimated size matches actual size
    ASSERT_EQ(estimated_size, decompressor.compressed_size(compressed.data()));
}
} // namespace arcticdb