#include <gtest/gtest.h>
#include <random>
#include <limits>
#include <algorithm>
#include <arcticdb/util/min_max_integer.hpp>

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
TEST(MinMaxFinder, SingleElement) {
    int data[] = {42};
    auto result = find_min_max(data, 1);
    EXPECT_EQ(result.min, 42);
    EXPECT_EQ(result.max, 42);
}

TEST(MinMaxFinder, UnalignedSize) {
    std::vector<int> data(67);
    std::iota(data.begin(), data.end(), 0);
    auto result = find_min_max(data.data(), data.size());
    EXPECT_EQ(result.min, 0);
    EXPECT_EQ(result.max, 66);
}

TEST(MinMaxFinder, RandomInt32) {
    std::mt19937 rng{std::random_device{}()};
    std::vector<int32_t> data(1000);
    std::uniform_int_distribution<int32_t> dist(INT32_MIN, INT32_MAX);

    for (auto &x : data)
        x = dist(rng);

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

TEST(MinMaxFinder, MinSingleElement) {
    int32_t data[] = {42};
    EXPECT_EQ(find_min(data, 1), 42);
}

TEST(MaxFinder, MaxSignedBasic) {
    int32_t data[] = {1, -2, 3, -4, 5};
    EXPECT_EQ(find_max(data, 5), 5);
}

TEST(MaxFinder, MaxUnsignedBasic) {
    uint32_t data[] = {1, 2, 3, 4, 5};
    EXPECT_EQ(find_max(data, 5), 5);
}

TEST(MaxFinder, MaxSignedExtremes) {
    int32_t data[] = {
        std::numeric_limits<int32_t>::max(),
        std::numeric_limits<int32_t>::min(),
        0, -1, 1
    };
    EXPECT_EQ(find_max(data, 5), std::numeric_limits<int32_t>::max());
}

TEST(MaxFinder, MaxUnsignedExtremes) {
    uint32_t data[] = {
        std::numeric_limits<uint32_t>::max(),
        0, 1,
        std::numeric_limits<uint32_t>::max() - 1
    };
    EXPECT_EQ(find_max(data, 4), std::numeric_limits<uint32_t>::max());
}

TEST(MaxFinder, MaxSingleElement) {
    int32_t data[] = {42};
    EXPECT_EQ(find_max(data, 1), 42);
}

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

TEST(MinMaxFinder, EmptyArrayMinMax) {
    std::vector<int32_t> data;
    EXPECT_THROW(find_min_max(data.data(), data.size()), std::runtime_error);
}

TEST(MinMaxFinder, EmptyArrayMin) {
    std::vector<int32_t> data;
    EXPECT_THROW(find_min(data.data(), data.size()), std::runtime_error);
}

TEST(MinMaxFinder, EmptyArrayMax) {
    std::vector<int32_t> data;
    EXPECT_THROW(find_max(data.data(), data.size()), std::runtime_error);
}

TEST(MinMaxFinder, ExactVectorMultipleInt32) {
    constexpr size_t lane_count = 64 / sizeof(int32_t);
    const size_t n = lane_count * 3;
    std::vector<int32_t> data(n);
    for (size_t i = 0; i < n; i++)
        data[i] = static_cast<int32_t>(i);
    auto result = find_min_max(data.data(), data.size());
    EXPECT_EQ(result.min, 0);
    EXPECT_EQ(result.max, static_cast<int32_t>(n - 1));
}

TEST(MinMaxFinder, AllExtremeMaxInt32) {
    std::vector<int32_t> data(100, std::numeric_limits<int32_t>::max());
    auto min_val = find_min(data.data(), data.size());
    auto max_val = find_max(data.data(), data.size());
    EXPECT_EQ(min_val, std::numeric_limits<int32_t>::max());
    EXPECT_EQ(max_val, std::numeric_limits<int32_t>::max());
}

TEST(MinMaxFinder, AllExtremeMinInt32) {
    std::vector<int32_t> data(100, std::numeric_limits<int32_t>::min());
    auto min_val = find_min(data.data(), data.size());
    auto max_val = find_max(data.data(), data.size());
    EXPECT_EQ(min_val, std::numeric_limits<int32_t>::min());
    EXPECT_EQ(max_val, std::numeric_limits<int32_t>::min());
}

TEST(MinMaxFinder, Stress) {
    std::mt19937 rng{std::random_device{}()};
    constexpr size_t size = 100'000'000;
    std::vector<int> data(size);
    std::uniform_int_distribution<int> dist(INT32_MIN, INT32_MAX);

    for (auto &x : data)
        x = dist(rng);

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

#ifdef HAS_VECTOR_EXTENSIONS

class MinMaxStressTest : public ::testing::Test {
protected:
    std::mt19937_64 rng{std::random_device{}()};

    template<typename T>
    std::vector<T> create_aligned_data(size_t n) {
        std::vector<T> data(n + 16);
        size_t offset = (64 - (reinterpret_cast<uintptr_t>(data.data()) % 64)) / sizeof(T);
        return std::vector<T>(data.data() + offset, data.data() + offset + n);
    }

    template<typename T>
    void run_benchmark(const std::vector<T>& data, const std::string& test_name) {
        constexpr int num_runs = 10;

        auto simd_min_time = std::chrono::microseconds(0);
        auto simd_max_time = std::chrono::microseconds(0);
        auto std_min_time = std::chrono::microseconds(0);
        auto std_max_time = std::chrono::microseconds(0);

        T simd_min;
        T simd_max;
        T std_min;
        T std_max;

        // Run multiple times to get average performance
        for(int i = 0; i < num_runs; i++) {
            {
                auto start = std::chrono::high_resolution_clock::now();
                simd_min = find_min(data.data(), data.size());
                auto end = std::chrono::high_resolution_clock::now();
                simd_min_time += std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            }

            {
                auto start = std::chrono::high_resolution_clock::now();
                simd_max = find_max(data.data(), data.size());
                auto end = std::chrono::high_resolution_clock::now();
                simd_max_time += std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            }

            {
                auto start = std::chrono::high_resolution_clock::now();
                std_min = *std::min_element(data.begin(), data.end());
                auto end = std::chrono::high_resolution_clock::now();
                std_min_time += std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            }

            {
                auto start = std::chrono::high_resolution_clock::now();
                std_max = *std::max_element(data.begin(), data.end());
                auto end = std::chrono::high_resolution_clock::now();
                std_max_time += std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            }

            EXPECT_EQ(simd_min, std_min);
            EXPECT_EQ(simd_max, std_max);
        }

        // Calculate average times
        double simd_min_avg = simd_min_time.count() / static_cast<double>(num_runs);
        double simd_max_avg = simd_max_time.count() / static_cast<double>(num_runs);
        double std_min_avg = std_min_time.count() / static_cast<double>(num_runs);
        double std_max_avg = std_max_time.count() / static_cast<double>(num_runs);

        std::cout << "\n" << test_name << " Results:\n"
                  << "SIMD min time:     " << std::fixed << std::setprecision(2)
                  << simd_min_avg << " µs\n"
                  << "std::min time:     " << std_min_avg << " µs\n"
                  << "SIMD min speedup:  " << std_min_avg/simd_min_avg << "x\n"
                  << "SIMD max time:     " << simd_max_avg << " µs\n"
                  << "std::max time:     " << std_max_avg << " µs\n"
                  << "SIMD max speedup:  " << std_max_avg/simd_max_avg << "x\n";
    }
};

TEST_F(MinMaxStressTest, LargeRandomInt32) {
    constexpr size_t size = 10'000'000;
    auto data = create_aligned_data<int32_t>(size);
    std::uniform_int_distribution<int32_t> dist(
        std::numeric_limits<int32_t>::min(),
        std::numeric_limits<int32_t>::max()
    );

    for(auto& x : data) {
        x = dist(rng);
    }

    run_benchmark(data, "Large Random Int32");
}

TEST_F(MinMaxStressTest, LargeRandomUInt32) {
    constexpr size_t size = 10'000'000;
    auto data = create_aligned_data<uint32_t>(size);
    std::uniform_int_distribution<uint32_t> dist(
        std::numeric_limits<uint32_t>::min(),
        std::numeric_limits<uint32_t>::max()
    );

    for(auto& x : data) {
        x = dist(rng);
    }

    run_benchmark(data, "Large Random UInt32");
}

TEST_F(MinMaxStressTest, SmallIntegers) {
    constexpr size_t size = 10'000'000;
    auto data = create_aligned_data<int8_t>(size);
    std::uniform_int_distribution<int16_t> dist(
        std::numeric_limits<int8_t>::min(),
        std::numeric_limits<int8_t>::max()
    );

    for(auto& x : data) {
        x = static_cast<int8_t>(dist(rng));
    }

    run_benchmark(data, "Small Integers (int8_t)");
}

TEST_F(MinMaxStressTest, LargeIntegers) {
    constexpr size_t size = 10'000'000;
    auto data = create_aligned_data<int64_t>(size);
    std::uniform_int_distribution<int64_t> dist(
        std::numeric_limits<int64_t>::min(),
        std::numeric_limits<int64_t>::max()
    );

    for(auto& x : data) {
        x = dist(rng);
    }

    run_benchmark(data, "Large Integers (int64_t)");
}

TEST_F(MinMaxStressTest, MonotonicIncreasing) {
    constexpr size_t size = 10'000'000;
    auto data = create_aligned_data<int32_t>(size);
    std::iota(data.begin(), data.end(), 0);

    run_benchmark(data, "Monotonic Increasing");
}

TEST_F(MinMaxStressTest, MonotonicDecreasing) {
    constexpr size_t size = 10'000'000;
    auto data = create_aligned_data<int32_t>(size);
    std::iota(data.rbegin(), data.rend(), 0);

    run_benchmark(data, "Monotonic Decreasing");
}

TEST_F(MinMaxStressTest, AllSameValue) {
    constexpr size_t size = 10'000'000;
    auto data = create_aligned_data<int32_t>(size);
    std::fill(data.begin(), data.end(), 42);

    run_benchmark(data, "All Same Value");
}

TEST_F(MinMaxStressTest, Alternating) {
    constexpr size_t size = 10'000'000;
    auto data = create_aligned_data<int32_t>(size);
    for(size_t i = 0; i < data.size(); ++i) {
        data[i] = (i % 2 == 0) ? 1000 : -1000;
    }

    run_benchmark(data, "Alternating Values");
}

TEST_F(MinMaxStressTest, MinMaxAtEnds) {
    constexpr size_t size = 10'000'000;
    auto data = create_aligned_data<int32_t>(size);
    std::uniform_int_distribution<int32_t> dist(-1000, 1000);

    for(auto& x : data) {
        x = dist(rng);
    }

    data.front() = std::numeric_limits<int32_t>::min();
    data.back() = std::numeric_limits<int32_t>::max();

    run_benchmark(data, "Min/Max at Ends");
}

TEST_F(MinMaxStressTest, UnalignedSize) {
    constexpr size_t size = 10'000'001;  // Prime number size
    auto data = create_aligned_data<int32_t>(size);
    std::uniform_int_distribution<int32_t> dist(
        std::numeric_limits<int32_t>::min(),
        std::numeric_limits<int32_t>::max()
    );

    for(auto& x : data) {
        x = dist(rng);
    }

    run_benchmark(data, "Unaligned Size");
}

#endif

} //namespace arcticdb