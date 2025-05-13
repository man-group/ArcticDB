#include <gtest/gtest.h>
#include <vector>
#include <random>
#include <algorithm>

#include <arcticdb/util/min_max_float.hpp>

namespace arcticdb {

class FloatFinderTest : public ::testing::Test {
protected:
    std::mt19937 rng{std::random_device{}()};

    // Helper to create aligned data
    template<typename T>
    std::vector<T> create_aligned_data(size_t n) {
        std::vector<T> data(n + 16);  // Extra space for alignment
        size_t offset = (64 - (reinterpret_cast<uintptr_t>(data.data()) % 64)) / sizeof(T);
        return std::vector<T>(data.data() + offset, data.data() + offset + n);
    }
};

TEST_F(FloatFinderTest, VectorAlignedFloat) {
    auto data = create_aligned_data<float>(64);  // One full vector
    std::iota(data.begin(), data.end(), 0.0f);

    EXPECT_FLOAT_EQ(find_float_min(data.data(), data.size()), 0.0f);
    EXPECT_FLOAT_EQ(find_float_max(data.data(), data.size()), 63.0f);
}

TEST_F(FloatFinderTest, VectorAlignedDouble) {
    auto data = create_aligned_data<double>(32);  // One full vector
    std::iota(data.begin(), data.end(), 0.0);

    EXPECT_DOUBLE_EQ(find_float_min(data.data(), data.size()), 0.0);
    EXPECT_DOUBLE_EQ(find_float_max(data.data(), data.size()), 31.0);
}

TEST_F(FloatFinderTest, VectorUnalignedSize) {
    std::vector<float> data(67);  // Non-multiple of vector size
    std::iota(data.begin(), data.end(), 0.0f);

    EXPECT_FLOAT_EQ(find_float_min(data.data(), data.size()), 0.0f);
    EXPECT_FLOAT_EQ(find_float_max(data.data(), data.size()), 66.0f);
}

TEST_F(FloatFinderTest, VectorWithNaNs) {
    auto data = create_aligned_data<float>(64);
    for (size_t i = 0; i < data.size(); i++) {
        data[i] = (i % 2 == 0) ? static_cast<float>(i) :
                  std::numeric_limits<float>::quiet_NaN();
    }

    EXPECT_FLOAT_EQ(find_float_min(data.data(), data.size()), 0.0f);
    EXPECT_FLOAT_EQ(find_float_max(data.data(), data.size()), 62.0f);
}

#ifdef HAS_VECTOR_EXTENSIONS

TEST_F(FloatFinderTest, LargeArrayPerformance) {
    constexpr size_t size = 100'000'000;
    auto data = create_aligned_data<float>(size);
    std::uniform_real_distribution<float> dist(-1000.0f, 1000.0f);

    for (auto &x : data)
        x = dist(rng);

    auto start = std::chrono::high_resolution_clock::now();
    auto min_result = find_float_min(data.data(), data.size());
    auto max_result = find_float_max(data.data(), data.size());
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "SIMD time: " << duration.count() << "ms\n";

    // Compare with std::minmax_element
    start = std::chrono::high_resolution_clock::now();
    auto std_result_max = std::max_element(data.begin(), data.end());
    auto std_result_min = std::min_element(data.begin(), data.end());
    end = std::chrono::high_resolution_clock::now();

    auto std_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "std::minmax_element time: " << std_duration.count() << "ms\n";

    EXPECT_FLOAT_EQ(min_result, *std_result_min);
    EXPECT_FLOAT_EQ(max_result, *std_result_max);
}

#endif

TEST_F(FloatFinderTest, EmptyInputFloat) {
    std::vector<float> data;
    EXPECT_FLOAT_EQ(find_float_min(data.data(), 0), std::numeric_limits<float>::infinity());
    EXPECT_FLOAT_EQ(find_float_max(data.data(), 0), -std::numeric_limits<float>::infinity());
}

TEST_F(FloatFinderTest, EmptyInputDouble) {
    std::vector<double> data;
    EXPECT_DOUBLE_EQ(find_float_min(data.data(), 0), std::numeric_limits<double>::infinity());
    EXPECT_DOUBLE_EQ(find_float_max(data.data(), 0), -std::numeric_limits<double>::infinity());
}

TEST_F(FloatFinderTest, UniformDataFloat) {
    auto data = create_aligned_data<float>(64);
    std::fill(data.begin(), data.end(), 5.0f);
    EXPECT_FLOAT_EQ(find_float_min(data.data(), data.size()), 5.0f);
    EXPECT_FLOAT_EQ(find_float_max(data.data(), data.size()), 5.0f);
}

TEST_F(FloatFinderTest, UniformDataDouble) {
    auto data = create_aligned_data<double>(32);
    std::fill(data.begin(), data.end(), 7.0);
    EXPECT_DOUBLE_EQ(find_float_min(data.data(), data.size()), 7.0);
    EXPECT_DOUBLE_EQ(find_float_max(data.data(), data.size()), 7.0);
}

TEST_F(FloatFinderTest, AllNaNFloat) {
    auto data = create_aligned_data<float>(64);
    std::fill(data.begin(), data.end(), std::numeric_limits<float>::quiet_NaN());
    EXPECT_FLOAT_EQ(find_float_min(data.data(), data.size()), std::numeric_limits<float>::infinity());
    EXPECT_FLOAT_EQ(find_float_max(data.data(), data.size()), -std::numeric_limits<float>::infinity());
}

TEST_F(FloatFinderTest, AllNaNDouble) {
    auto data = create_aligned_data<double>(32);
    std::fill(data.begin(), data.end(), std::numeric_limits<double>::quiet_NaN());
    EXPECT_DOUBLE_EQ(find_float_min(data.data(), data.size()), std::numeric_limits<double>::infinity());
    EXPECT_DOUBLE_EQ(find_float_max(data.data(), data.size()), -std::numeric_limits<double>::infinity());
}

TEST_F(FloatFinderTest, SingleElementFloat) {
    std::vector<float> data = { 42.0f };
    EXPECT_FLOAT_EQ(find_float_min(data.data(), data.size()), 42.0f);
    EXPECT_FLOAT_EQ(find_float_max(data.data(), data.size()), 42.0f);
}

TEST_F(FloatFinderTest, SingleElementDouble) {
    std::vector<double> data = { 3.14159 };
    EXPECT_DOUBLE_EQ(find_float_min(data.data(), data.size()), 3.14159);
    EXPECT_DOUBLE_EQ(find_float_max(data.data(), data.size()), 3.14159);
}

TEST_F(FloatFinderTest, DoubleRemainderCase) {
    // For doubles the vector is 64 bytes so lane_count = 8.
    // Create an array of 11 doubles (1 full vector + 3 remainder).
    std::vector<double> data(11);
    // Fill with values such that min is 2.0 and max is 10.0
    // First 8 elements: 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 3.0
    // Remainder: 2.0, 8.0, 4.0
    data[0] = 4.0; data[1] = 5.0; data[2] = 6.0; data[3] = 7.0;
    data[4] = 8.0; data[5] = 9.0; data[6] = 10.0; data[7] = 3.0;
    data[8] = 2.0; data[9] = 8.0; data[10] = 4.0;
    EXPECT_DOUBLE_EQ(find_float_min(data.data(), data.size()), 2.0);
    EXPECT_DOUBLE_EQ(find_float_max(data.data(), data.size()), 10.0);
}

} // namespace arcticdb