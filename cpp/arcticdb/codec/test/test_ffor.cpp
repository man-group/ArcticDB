#include <gtest/gtest.h>

#include <arcticdb/util/timer.hpp>
#include <arcticdb/codec/calculate_stats.hpp>
#include <arcticdb/codec/compression/ffor.hpp>

#include <random>
#include <algorithm>

namespace arcticdb {

class FForCodecTest : public ::testing::Test {
protected:
    static constexpr size_t values_per_block = 1024 / (sizeof(uint32_t) * 8);

    template<typename T>
    void verify_roundtrip(const std::vector<T>& original) {
        ASSERT_FALSE(original.empty());

        // Allocate space for compressed data with header
        std::vector<T> compressed(original.size() + sizeof(FForHeader<T>)/sizeof(T));
        std::vector<T> decompressed(original.size());

        // Compress
        size_t compressed_size = encode_ffor_with_header(
            original.data(),
            compressed.data(),
            original.size()
        );

        ASSERT_GT(compressed_size, sizeof(FForHeader<T>)/sizeof(T));

        // Verify header
        const auto* header = reinterpret_cast<const FForHeader<T>*>(compressed.data());
        EXPECT_EQ(header->num_rows, original.size());
        EXPECT_GT(header->bits_needed, 0);
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
    {
        std::vector<uint32_t> data(values_per_block);
        std::iota(data.begin(), data.end(), 0);
        verify_roundtrip(data);
    }

    // Test with just over one block
    {
        std::vector<uint32_t> data(values_per_block + 1);
        std::iota(data.begin(), data.end(), 0);
        verify_roundtrip(data);
    }

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


} // namespace arcticdb