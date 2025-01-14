#include <gtest/gtest.h>
#include <vector>
#include <random>

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


} // namespace arcticdb