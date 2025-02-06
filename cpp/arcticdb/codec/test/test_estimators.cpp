#include <gtest/gtest.h>
#include <arcticdb/codec/compression/estimators.hpp>

#include <random>
#include <numeric>

namespace arcticdb {
class CompressionEstimationTest : public ::testing::Test {
protected:
    // Each block must be 1024 bits
    static constexpr size_t bits_per_block = 1024;

    template<typename T>
    static constexpr size_t values_per_block() {
        return bits_per_block / (sizeof(T) * 8);
    }

    // Generate test data with proper block alignment
    template<typename T>
    std::vector<T> generate_test_data(size_t num_blocks) {
        return std::vector<T>(values_per_block<T>() * num_blocks);
    }

    // Verify block alignment
    template<typename T>
    void verify_block_alignment(const std::vector<T>& data) {
        ASSERT_EQ(data.size() % values_per_block<T>(), 0)
                        << "Data size must be multiple of " << values_per_block<T>();
    }
};

/*
TEST_F(CompressionEstimationTest, RLEConstantValue) {
    using T = int32_t;
    constexpr size_t num_blocks = 4;  // 4 * 1024 bits = 4096 bits total
    auto data = generate_test_data<T>(num_blocks);
    verify_block_alignment(data);

    // Fill with constant value
    std::fill(data.begin(), data.end(), 42);

    auto estimate = estimate_compression(data.data(), data.size(), RunLengthEstimator<T>{});

    EXPECT_EQ(estimate.estimated_bits_needed, 1)
                << "Constant value should need only 1 bit for run length";
    EXPECT_NEAR(estimate.estimated_ratio, 1.0/32.0, 0.001)
                << "32-bit values compressed to 1 bit should have ~3.125% ratio";
}

TEST_F(CompressionEstimationTest, RLEVariableRuns) {
    using T = int32_t;
    constexpr size_t num_blocks = 4;
    auto data = generate_test_data<T>(num_blocks);
    verify_block_alignment(data);

    // Create runs of length 4
    for (size_t i = 0; i < data.size(); i += 4) {
        std::fill_n(data.begin() + i, 4, i/4);
    }

    auto estimate = estimate_compression(data.data(), data.size(), RunLengthEstimator<T>{});

    EXPECT_EQ(estimate.estimated_bits_needed, 3)
                << "Run length of 4 should need 3 bits";
}

TEST_F(CompressionEstimationTest, DeltaLinearSequence) {
    using T = int32_t;
    constexpr size_t num_blocks = 4;
    auto data = generate_test_data<T>(num_blocks);
    verify_block_alignment(data);

    // Generate linear sequence
    std::iota(data.begin(), data.end(), 0);

    auto estimate = estimate_compression(data.data(), data.size(), DeltaEstimator<T>{});

    EXPECT_EQ(estimate.estimated_bits_needed, 2)
                << "Delta of 1 should need 2 bits (1 for value, 1 for sign)";
    EXPECT_NEAR(estimate.estimated_ratio, 2.0/32.0, 0.001)
                << "32-bit values compressed to 2 bits should have ~6.25% ratio";
}

TEST_F(CompressionEstimationTest, DeltaExponentialSequence) {
    using T = int32_t;
    constexpr size_t num_blocks = 4;
    auto data = generate_test_data<T>(num_blocks);
    verify_block_alignment(data);

    // Generate exponentially increasing deltas
    T value = 0;
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = value;
        value += (1 << (i % 10));  // Increase delta exponentially, mod 10 to avoid overflow
    }

    auto estimate = estimate_compression(data.data(), data.size(), DeltaEstimator<T>{});
    EXPECT_GT(estimate.estimated_bits_needed, 10)
                << "Exponential deltas should need more bits";
}

TEST_F(CompressionEstimationTest, FFORSmallRange) {
    using T = int32_t;
    constexpr size_t num_blocks = 4;
    auto data = generate_test_data<T>(num_blocks);
    verify_block_alignment(data);

    // Fill with values in small range (1000-1015)
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 1000 + (i % 16);
    }
    std::sort(data.begin(), data.end());  // FFOR expects sorted data

    auto estimate = estimate_compression(data.data(), data.size(), FForEstimator<T>{});

    EXPECT_EQ(estimate.estimated_bits_needed, 4)
                << "Range of 16 values should need 4 bits";
}

TEST_F(CompressionEstimationTest, FFORLargeRange) {
    using T = int32_t;
    constexpr size_t num_blocks = 4;
    auto data = generate_test_data<T>(num_blocks);
    verify_block_alignment(data);

    // Fill with values spanning large range
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = i * 1000;
    }

    auto estimate = estimate_compression(data.data(), data.size(), FForEstimator<T>{});

    size_t expected_bits = std::bit_width(
        static_cast<std::make_unsigned_t<T>>(data.back() - data.front()));
    EXPECT_EQ(estimate.estimated_bits_needed, expected_bits);
}

TEST_F(CompressionEstimationTest, CompareAllMethods) {
    using T = int32_t;
    constexpr size_t num_blocks = 4;
    auto data = generate_test_data<T>(num_blocks);
    verify_block_alignment(data);

    // Generate data that should favor different methods
    size_t i = 0;
    while (i < data.size()) {
        // Create runs for RLE
        size_t run_length = std::min<size_t>(4, data.size() - i);
        std::fill_n(data.begin() + i, run_length, i/4);
        i += run_length;
    }

    std::sort(data.begin(), data.end());  // Sort for delta/FFOR

    auto rle_estimate = estimate_compression(data.data(), data.size(), RunLengthEstimator<T>{});
    auto delta_estimate = estimate_compression(data.data(), data.size(), DeltaEstimator<T>{});
    auto ffor_estimate = estimate_compression(data.data(), data.size(), FForEstimator<T>{});

    // All estimates should be valid
    EXPECT_GT(rle_estimate.estimated_bits_needed, 0);
    EXPECT_LE(rle_estimate.estimated_bits_needed, sizeof(T) * 8);

    EXPECT_GT(delta_estimate.estimated_bits_needed, 0);
    EXPECT_LE(delta_estimate.estimated_bits_needed, sizeof(T) * 8);

    EXPECT_GT(ffor_estimate.estimated_bits_needed, 0);
    EXPECT_LE(ffor_estimate.estimated_bits_needed, sizeof(T) * 8);
}

TEST_F(CompressionEstimationTest, DifferentTypes) {
    // Test uint32_t
    {
        auto data = generate_test_data<uint32_t>(4);
        verify_block_alignment(data);
        std::iota(data.begin(), data.end(), 0);

        auto estimate = estimate_compression(
            data.data(), data.size(), DeltaEstimator<uint32_t>{});
        EXPECT_GT(estimate.estimated_bits_needed, 0);
    }

    // Test uint64_t
    {
        auto data = generate_test_data<uint64_t>(4);
        verify_block_alignment(data);
        std::iota(data.begin(), data.end(), 0);

        auto estimate = estimate_compression(
            data.data(), data.size(), DeltaEstimator<uint64_t>{});
        EXPECT_GT(estimate.estimated_bits_needed, 0);
    }
}

class FrequencyEstimationTest : public ::testing::Test {
protected:
    static constexpr size_t bits_per_block = 1024;

    template<typename T>
    static constexpr size_t values_per_block() {
        return bits_per_block / (sizeof(T) * 8);
    }

    template<typename T>
    std::vector<T> generate_test_data(size_t num_blocks) {
        return std::vector<T>(values_per_block<T>() * num_blocks);
    }
};

TEST_F(FrequencyEstimationTest, PerfectDominance) {
    using T = int32_t;
    constexpr size_t num_blocks = 4;
    auto data = generate_test_data<T>(num_blocks);

    // Fill with single value
    std::fill(data.begin(), data.end(), 42);

    auto estimate = estimate_compression(
        data.data(),
        data.size(),
        FrequencyBitsCalculator<T>{90.0});

    // Should be very good compression
    EXPECT_LT(estimate.estimated_ratio, 0.1)
                << "Perfect dominance should achieve high compression";
}

TEST_F(FrequencyEstimationTest, JustAboveThreshold) {
    using T = int32_t;
    constexpr size_t num_blocks = 4;
    auto data = generate_test_data<T>(num_blocks);

    // Fill with 91% dominant value
    std::fill(data.begin(), data.end(), 42);
    size_t num_exceptions = data.size() * 0.09;  // 9% exceptions

    std::mt19937 rng(42);
    for (size_t i = 0; i < num_exceptions; ++i) {
        size_t idx = rng() % data.size();
        data[idx] = 43;  // Different value
    }

    auto estimate = estimate_compression(
        data.data(),
        data.size(),
        FrequencyBitsCalculator<T>{90.0});

    EXPECT_LT(estimate.estimated_ratio, 1.0)
                << "Should achieve some compression";
}

TEST_F(FrequencyEstimationTest, BelowThreshold) {
    using T = int32_t;
    constexpr size_t num_blocks = 4;
    auto data = generate_test_data<T>(num_blocks);

    // Fill with 85% dominant value
    std::fill(data.begin(), data.end(), 42);
    size_t num_exceptions = data.size() * 0.15;  // 15% exceptions

    std::mt19937 rng(42);
    for (size_t i = 0; i < num_exceptions; ++i) {
        size_t idx = rng() % data.size();
        data[idx] = 43;
    }

    auto estimate = estimate_compression(
        data.data(),
        data.size(),
        FrequencyBitsCalculator<T>{90.0});

    EXPECT_NEAR(estimate.estimated_ratio, 1.0, 0.001)
                << "Below threshold should not compress";
}

TEST_F(FrequencyEstimationTest, DifferentThresholds) {
    using T = int32_t;
    constexpr size_t num_blocks = 4;
    auto data = generate_test_data<T>(num_blocks);

    // Fill with 85% dominant value
    std::fill(data.begin(), data.end(), 42);
    size_t num_exceptions = data.size() * 0.15;

    std::mt19937 rng(42);
    for (size_t i = 0; i < num_exceptions; ++i) {
        size_t idx = rng() % data.size();
        data[idx] = 43;
    }

    // Should compress with 80% threshold
    auto estimate80 = estimate_compression(
        data.data(),
        data.size(),
        FrequencyBitsCalculator<T>{80.0});

    // Should not compress with 90% threshold
    auto estimate90 = estimate_compression(
        data.data(),
        data.size(),
        FrequencyBitsCalculator<T>{90.0});

    EXPECT_LT(estimate80.estimated_ratio, 1.0);
    EXPECT_NEAR(estimate90.estimated_ratio, 1.0, 0.001);
}

*/

}