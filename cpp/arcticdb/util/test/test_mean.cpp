#include <gtest/gtest.h>
#include <vector>
#include <random>
#include <numeric>

#include <arcticdb/util/mean.hpp>

namespace arcticdb {

class MeanFinderTest : public ::testing::Test {
protected:
    std::mt19937 rng{std::random_device{}()};

    template<typename T>
    std::vector<T> create_aligned_data(size_t n) {
        std::vector<T> data(n + 16);
        size_t offset = (64 - (reinterpret_cast<uintptr_t>(data.data()) % 64)) / sizeof(T);
        return std::vector<T>(data.data() + offset, data.data() + offset + n);
    }
};

TEST_F(MeanFinderTest, BasicInt32) {
    std::vector<int32_t> data = {1, 2, 3, 4, 5};
    EXPECT_DOUBLE_EQ(find_mean(data.data(), data.size()), 3.0);
}

TEST_F(MeanFinderTest, BasicUInt32) {
    std::vector<uint32_t> data = {1, 2, 3, 4, 5};
    EXPECT_DOUBLE_EQ(find_mean(data.data(), data.size()), 3.0);
}

TEST_F(MeanFinderTest, SingleElement) {
    std::vector<int32_t> data = {42};
    EXPECT_DOUBLE_EQ(find_mean(data.data(), data.size()), 42.0);
}

// Vector Size Tests
TEST_F(MeanFinderTest, VectorSizedArray) {
    auto data = create_aligned_data<int32_t>(64);
    std::iota(data.begin(), data.end(), 0);
    EXPECT_DOUBLE_EQ(find_mean(data.data(), data.size()), 31.5);
}

TEST_F(MeanFinderTest, NonVectorSizedArray) {
    std::vector<int32_t> data(67);
    std::iota(data.begin(), data.end(), 0);
    EXPECT_DOUBLE_EQ(find_mean(data.data(), data.size()), 33.0);
}

TEST_F(MeanFinderTest, Int8Type) {
    std::vector<int8_t> data = {-128, 0, 127};
    EXPECT_DOUBLE_EQ(find_mean(data.data(), data.size()), -0.3333333333333333);
}

TEST_F(MeanFinderTest, UInt8Type) {
    std::vector<uint8_t> data = {0, 128, 255};
    EXPECT_DOUBLE_EQ(find_mean(data.data(), data.size()), 127.66666666666667);
}

TEST_F(MeanFinderTest, Int64Type) {
    std::vector<int64_t> data = {
        std::numeric_limits<int64_t>::min(),
        0,
        std::numeric_limits<int64_t>::max()
    };
    EXPECT_FALSE(std::isnan(find_mean(data.data(), data.size())));
}

TEST_F(MeanFinderTest, LargeIntegers) {
    std::vector<int32_t> data = {1000000, 2000000, 3000000};
    EXPECT_DOUBLE_EQ(find_mean(data.data(), data.size()), 2000000.0);
}

TEST_F(MeanFinderTest, MaxIntegers) {
    std::vector<int32_t> data = {
        std::numeric_limits<int32_t>::max(),
        std::numeric_limits<int32_t>::max()
    };
    EXPECT_DOUBLE_EQ(find_mean(data.data(), data.size()),
                     static_cast<double>(std::numeric_limits<int32_t>::max()));
}

TEST_F(MeanFinderTest, RandomInt32) {
    auto data = create_aligned_data<int32_t>(1000);
    std::uniform_int_distribution<int32_t> dist(-1000, 1000);

    for (auto &x : data) {
        x = dist(rng);
    }

    double expected = std::accumulate(data.begin(), data.end(), 0.0) / static_cast<double>(data.size());
    EXPECT_NEAR(find_mean(data.data(), data.size()), expected, 1e-10);
}

TEST_F(MeanFinderTest, RandomUInt32) {
    auto data = create_aligned_data<uint32_t>(1000);
    std::uniform_int_distribution<uint32_t> dist(0, 1000);

    for (auto &x : data)
        x = dist(rng);

    double expected = std::accumulate(data.begin(), data.end(), 0.0) / static_cast<double>(data.size());
    EXPECT_NEAR(find_mean(data.data(), data.size()), expected, 1e-10);
}

TEST_F(MeanFinderTest, AllZeros) {
    std::vector<int32_t> data(1000, 0);
    EXPECT_DOUBLE_EQ(find_mean(data.data(), data.size()), 0.0);
}

TEST_F(MeanFinderTest, AlternatingValues) {
    std::vector<int32_t> data(1000);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = (i % 2 == 0) ? 1000 : -1000;
    }
    EXPECT_DOUBLE_EQ(find_mean(data.data(), data.size()), 0.0);
}

TEST_F(MeanFinderTest, BasicFloat) {
    std::vector<float> data = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
    EXPECT_DOUBLE_EQ(find_mean(data.data(), data.size()), 3.0);
}

TEST_F(MeanFinderTest, BasicDouble) {
    std::vector<double> data = {1.0, 2.0, 3.0, 4.0, 5.0};
    EXPECT_DOUBLE_EQ(find_mean(data.data(), data.size()), 3.0);
}

TEST_F(MeanFinderTest, FloatWithNaNs) {
    std::vector<float> data = {1.0f, std::numeric_limits<float>::quiet_NaN(), 3.0f, std::numeric_limits<float>::quiet_NaN(), 5.0f};
    double expected = (1.0 + 3.0 + 5.0) / 3.0;
    EXPECT_DOUBLE_EQ(find_mean(data.data(), data.size()), expected);
}

TEST_F(MeanFinderTest, DoubleWithNaNs) {
    std::vector<double> data = {1.0, std::numeric_limits<double>::quiet_NaN(), 3.0, std::numeric_limits<double>::quiet_NaN(), 5.0};
    double expected = (1.0 + 3.0 + 5.0) / 3.0;
    EXPECT_DOUBLE_EQ(find_mean(data.data(), data.size()), expected);
}

TEST_F(MeanFinderTest, EmptyArrayFloat) {
    std::vector<float> data;
    EXPECT_THROW(find_mean(data.data(), data.size()), std::runtime_error);
}

TEST_F(MeanFinderTest, EmptyArrayDouble) {
    std::vector<double> data;
    EXPECT_THROW(find_mean(data.data(), data.size()), std::runtime_error);
}

TEST_F(MeanFinderTest, AlternatingFloatValues) {
    std::vector<float> data(1000);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = (i % 2 == 0) ? 1000.0f : -1000.0f;
    }
    EXPECT_DOUBLE_EQ(find_mean(data.data(), data.size()), 0.0);
}

TEST_F(MeanFinderTest, ExtremeFloatAverage) {
    std::vector<float> data = {std::numeric_limits<float>::lowest(), std::numeric_limits<float>::max()};
    EXPECT_DOUBLE_EQ(find_mean(data.data(), data.size()), 0.0);
}

TEST_F(MeanFinderTest, ExtremeDoubleAverage) {
    std::vector<double> data = {std::numeric_limits<double>::lowest(), std::numeric_limits<double>::max()};
    EXPECT_DOUBLE_EQ(find_mean(data.data(), data.size()), 0.0);
}

#ifdef HAS_VECTOR_EXTENSIONS

TEST_F(MeanFinderTest, LargeArrayPerformance) {
    constexpr size_t size = 100'000'000;
    auto data = create_aligned_data<int32_t>(size);
    std::uniform_int_distribution<int32_t> dist(-1000, 1000);

    for (auto &x : data)
        x = dist(rng);

    auto start = std::chrono::high_resolution_clock::now();
    double simd_mean = find_mean(data.data(), data.size());
    auto end = std::chrono::high_resolution_clock::now();

    auto simd_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    start = std::chrono::high_resolution_clock::now();
    double std_mean = std::accumulate(data.begin(), data.end(), 0.0) / static_cast<double>(data.size());
    end = std::chrono::high_resolution_clock::now();

    auto std_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "SIMD mean time: " << simd_duration.count() << "ms\n";
    std::cout << "Standard mean time: " << std_duration.count() << "ms\n";
    std::cout << "Speedup: " << static_cast<double>(std_duration.count()) /
        static_cast<double>(simd_duration.count()) << "x\n";

    EXPECT_NEAR(simd_mean, std_mean, 1e-10);
}

#endif

} // namespace arcticdb