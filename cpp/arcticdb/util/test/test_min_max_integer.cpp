#include <gtest/gtest.h>
#include <random>
#include <limits>
#include "arcticdb/util/min_max_integer.hpp"

namespace arcticdb {

TEST(MinMaxFinder, Int8Basic) {
    int8_t data[] = {0, 1, 2, 3, 4};
    auto result = find_min_max(data, 5);
    EXPECT_EQ(result.min, 0);
    EXPECT_EQ(result.max, 4);
}

TEST(MinMaxFinder, Int8Extremes) {
    int8_t data[] = {INT8_MIN, -1, 0, 1, INT8_MAX};
    auto result = find_min_max(data, 5);
    EXPECT_EQ(result.min, INT8_MIN);
    EXPECT_EQ(result.max, INT8_MAX);
}

TEST(MinMaxFinder, UInt8Extremes) {
    uint8_t data[] = {0, 1, UINT8_MAX - 1, UINT8_MAX};
    auto result = find_min_max(data, 4);
    EXPECT_EQ(result.min, 0);
    EXPECT_EQ(result.max, UINT8_MAX);
}

TEST(MinMaxFinder, Int64Basic) {
    int64_t data[] = {0, 1, 2, 3, 4};
    auto result = find_min_max(data, 5);
    EXPECT_EQ(result.min, 0);
    EXPECT_EQ(result.max, 4);
}

TEST(MinMaxFinder, Int64Extremes) {
    int64_t data[] = {INT64_MIN, -1, 0, 1, INT64_MAX};
    auto result = find_min_max(data, 5);
    EXPECT_EQ(result.min, INT64_MIN);
    EXPECT_EQ(result.max, INT64_MAX);
}
TEST(MinMaxFinder, EmptyArray) {
    std::vector<int> data;
    auto result = find_min_max(data.data(), 0);
    EXPECT_EQ(result.min, std::numeric_limits<int>::max());
    EXPECT_EQ(result.max, std::numeric_limits<int>::min());
}

TEST(MinMaxFinder, SingleElement) {
    int data[] = {42};
    auto result = find_min_max(data, 1);
    EXPECT_EQ(result.min, 42);
    EXPECT_EQ(result.max, 42);
}

// Alignment Tests
TEST(MinMaxFinder, UnalignedSize) {
    std::vector<int> data(67);  // Non-multiple of SIMD width
    std::iota(data.begin(), data.end(), 0);  // Fill with 0..66
    auto result = find_min_max(data.data(), data.size());
    EXPECT_EQ(result.min, 0);
    EXPECT_EQ(result.max, 66);
}

TEST(MinMaxFinder, RandomInt32) {
    std::mt19937 rng{std::random_device{}()};
    std::vector<int32_t> data(1000);
    std::uniform_int_distribution<int32_t> dist(INT32_MIN, INT32_MAX);

    for (auto &x : data) {
        x = dist(rng);
    }

    auto result = find_min_max(data.data(), data.size());
    auto std_result = std::minmax_element(data.begin(), data.end());

    EXPECT_EQ(result.min, *std_result.first);
    EXPECT_EQ(result.max, *std_result.second);
}

TEST(MinMaxFinder, MinSignedBasic) {
    int32_t data[] = {1, -2, 3, -4, 5};
    EXPECT_EQ(find_min(data, 5), -4);
}

TEST(MinMaxFinder, MinUnsignedBasic) {
    uint32_t data[] = {1, 2, 3, 4, 5};
    EXPECT_EQ(find_min(data, 5), 1);
}

TEST(MinMaxFinder, MinSignedExtremes) {
    int32_t data[] = {
        std::numeric_limits<int32_t>::max(),
        std::numeric_limits<int32_t>::min(),
        0, -1, 1
    };
    EXPECT_EQ(find_min(data, 5), std::numeric_limits<int32_t>::min());
}

TEST(MinMaxFinder, MinUnsignedExtremes) {
    uint32_t data[] = {
        std::numeric_limits<uint32_t>::max(),
        0, 1,
        std::numeric_limits<uint32_t>::max() - 1
    };
    EXPECT_EQ(find_min(data, 4), 0);
}

TEST(MinMaxFinder, MinEmptyArray) {
    std::vector<int32_t> data;
    EXPECT_EQ(find_min(data.data(), 0), std::numeric_limits<int32_t>::max());
}

TEST(MinMaxFinder, MinSingleElement) {
    int32_t data[] = {42};
    EXPECT_EQ(find_min(data, 1), 42);
}

// Max Finder Tests
TEST(MinMaxFinder, MaxSignedBasic) {
    int32_t data[] = {1, -2, 3, -4, 5};
    EXPECT_EQ(find_max(data, 5), 5);
}

TEST(MinMaxFinder, MaxUnsignedBasic) {
    uint32_t data[] = {1, 2, 3, 4, 5};
    EXPECT_EQ(find_max(data, 5), 5);
}

TEST(MinMaxFinder, MaxSignedExtremes) {
    int32_t data[] = {
        std::numeric_limits<int32_t>::max(),
        std::numeric_limits<int32_t>::min(),
        0, -1, 1
    };
    EXPECT_EQ(find_max(data, 5), std::numeric_limits<int32_t>::max());
}

TEST(MinMaxFinder, MaxUnsignedExtremes) {
    uint32_t data[] = {
        std::numeric_limits<uint32_t>::max(),
        0, 1,
        std::numeric_limits<uint32_t>::max() - 1
    };
    EXPECT_EQ(find_max(data, 4), std::numeric_limits<uint32_t>::max());
}

TEST(MinMaxFinder, MaxEmptyArray) {
    std::vector<int32_t> data;
    EXPECT_EQ(find_max(data.data(), 0), std::numeric_limits<int32_t>::min());
}

TEST(MinMaxFinder, MaxSingleElement) {
    int32_t data[] = {42};
    EXPECT_EQ(find_max(data, 1), 42);
}

// Different Integer Types Tests
TEST(MinMaxFinder, Int8Types) {
    int8_t data[] = {
        std::numeric_limits<int8_t>::min(),
        0,
        std::numeric_limits<int8_t>::max()
    };
    EXPECT_EQ(find_min(data, 3), std::numeric_limits<int8_t>::min());
    EXPECT_EQ(find_max(data, 3), std::numeric_limits<int8_t>::max());
}

TEST(MinMaxFinder, UInt8Types) {
    uint8_t data[] = {
        0,
        std::numeric_limits<uint8_t>::max() / 2,
        std::numeric_limits<uint8_t>::max()
    };
    EXPECT_EQ(find_min(data, 3), 0);
    EXPECT_EQ(find_max(data, 3), std::numeric_limits<uint8_t>::max());
}

TEST(MinMaxFinder, Int64Types) {
    int64_t data[] = {
        std::numeric_limits<int64_t>::min(),
        0,
        std::numeric_limits<int64_t>::max()
    };
    EXPECT_EQ(find_min(data, 3), std::numeric_limits<int64_t>::min());
    EXPECT_EQ(find_max(data, 3), std::numeric_limits<int64_t>::max());
}

TEST(MinMaxFinder, RandomInt32Separate) {
    std::mt19937 rng{std::random_device{}()};
    std::vector<int32_t> data(1000);
    std::uniform_int_distribution<int32_t> dist(
        std::numeric_limits<int32_t>::min(),
        std::numeric_limits<int32_t>::max()
    );

    for(auto& x : data) {
        x = dist(rng);
    }

    auto min_result = find_min(data.data(), data.size());
    auto max_result = find_max(data.data(), data.size());

    auto std_min = *std::min_element(data.begin(), data.end());
    auto std_max = *std::max_element(data.begin(), data.end());

    EXPECT_EQ(min_result, std_min);
    EXPECT_EQ(max_result, std_max);
}

TEST(MinMaxFinder, RandomUInt32) {
    std::mt19937 rng{std::random_device{}()};
    std::vector<uint32_t> data(1000);
    std::uniform_int_distribution<uint32_t> dist(
        std::numeric_limits<uint32_t>::min(),
        std::numeric_limits<uint32_t>::max()
    );

    for(auto& x : data) {
        x = dist(rng);
    }

    auto min_result = find_min(data.data(), data.size());
    auto max_result = find_max(data.data(), data.size());

    auto std_min = *std::min_element(data.begin(), data.end());
    auto std_max = *std::max_element(data.begin(), data.end());

    EXPECT_EQ(min_result, std_min);
    EXPECT_EQ(max_result, std_max);
}

TEST(MinMaxFinder, Stress) {
    std::mt19937 rng{std::random_device{}()};
    constexpr size_t size = 100'000'000;
    std::vector<int> data(size);
    std::uniform_int_distribution<int> dist(INT32_MIN, INT32_MAX);

    for (auto &x : data) {
        x = dist(rng);
    }

    auto start = std::chrono::high_resolution_clock::now();
    auto result = find_min_max(data.data(), data.size());
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Time taken: " << duration.count() << "ms\n";

    // Compare with std::minmax_element
    start = std::chrono::high_resolution_clock::now();
    auto std_result = std::minmax_element(data.begin(), data.end());
    end = std::chrono::high_resolution_clock::now();

    auto std_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "std::minmax_element time: " << std_duration.count() << "ms\n";

    EXPECT_EQ(result.min, *std_result.first);
    EXPECT_EQ(result.max, *std_result.second);
}

} //namespace arcticdb