#include <gtest/gtest.h>
#include <vector>
#include <random>
#include <numeric>

#include <arcticdb/util/sum.hpp>

namespace arcticdb {

class SumFinderTest : public ::testing::Test {
protected:
    std::mt19937 rng{std::random_device{}()};

    template<typename T>
    std::vector<T> create_aligned_data(size_t n) {
        std::vector<T> data(n + 16);
        size_t offset = (64 - (reinterpret_cast<uintptr_t>(data.data()) % 64)) / sizeof(T);
        return std::vector<T>(data.data() + offset, data.data() + offset + n);
    }
};

TEST_F(SumFinderTest, LargeInt32Sum) {
    std::vector<int32_t> data(1000, 1000000);  // Would overflow int32
    double expected = 1000.0 * 1000000.0;
    EXPECT_DOUBLE_EQ(find_sum(data.data(), data.size()), expected);
}

TEST_F(SumFinderTest, SmallValuesLargeCount) {
    std::vector<int16_t> data(10000, 1);  // Many small values
    double expected = 10000.0;
    EXPECT_DOUBLE_EQ(find_sum(data.data(), data.size()), expected);
}

TEST_F(SumFinderTest, PrecisionTestSmallAndLarge) {
    std::vector<double> data = {1e15, 1.0, -1e15, 1.0};
    EXPECT_DOUBLE_EQ(find_sum(data.data(), data.size()), 2.0);
}

TEST_F(SumFinderTest, CompareWithStdAccumulate) {
    auto data = create_aligned_data<int32_t>(1000);
    std::uniform_int_distribution<int32_t> dist(-1000, 1000);

    for (auto &x : data)
        x = dist(rng);

    double simd_sum = find_sum(data.data(), data.size());
    double std_sum = std::accumulate(data.begin(), data.end(), 0.0);

    EXPECT_DOUBLE_EQ(simd_sum, std_sum);
}

TEST_F(SumFinderTest, SingleElementInt32) {
    std::vector<int32_t> data = {42};
    EXPECT_DOUBLE_EQ(find_sum(data.data(), data.size()), 42.0);
}

TEST_F(SumFinderTest, AllZerosInt32) {
    std::vector<int32_t> data(100, 0);
    EXPECT_DOUBLE_EQ(find_sum(data.data(), data.size()), 0.0);
}

TEST_F(SumFinderTest, MixedValuesInt32) {
    std::vector<int32_t> data = {-50, 100, -25, 75};
    double expected = -50.0 + 100.0 - 25.0 + 75.0;
    EXPECT_DOUBLE_EQ(find_sum(data.data(), data.size()), expected);
}

TEST_F(SumFinderTest, UnsignedIntSum) {
    std::vector<uint32_t> data = {1, 2, 3, 4, 5};
    double expected = 15.0;
    EXPECT_DOUBLE_EQ(find_sum(data.data(), data.size()), expected);
}

TEST_F(SumFinderTest, FloatSumTest) {
    std::vector<float> data = {1.5f, 2.5f, 3.0f};
    double expected = 1.5 + 2.5 + 3.0;
    EXPECT_DOUBLE_EQ(find_sum(data.data(), data.size()), expected);
}

TEST_F(SumFinderTest, SingleElementFloat) {
    std::vector<float> data = {3.37f};
    EXPECT_NEAR(find_sum(data.data(), data.size()), 3.37, 1e-6);
}

#ifdef HAS_VECTOR_EXTENSIONS

class SumStressTest : public ::testing::Test {
protected:
    std::mt19937_64 rng{std::random_device{}()};

    template<typename T>
    std::vector<T> create_aligned_data(size_t n) {
        std::vector<T> data(n + 16);
        size_t offset = (64 - (reinterpret_cast<uintptr_t>(data.data()) % 64)) / sizeof(T);
        return std::vector<T>(data.data() + offset, data.data() + offset + n);
    }

    double naive_sum(const double* data, size_t n) {
        double sum = 0.0;
        for(size_t i = 0; i < n; i++) {
            sum += data[i];
        }
        return sum;
    }

    double kahan_sum(const double* data, size_t n) {
        double sum = 0.0;
        double c = 0.0;

        for(size_t i = 0; i < n; i++) {
            double y = data[i] - c;
            double t = sum + y;
            c = (t - sum) - y;
            sum = t;
        }
        return sum;
    }

    void run_benchmark(const std::vector<double>& data, const std::string& test_name) {
        constexpr int num_runs = 10;

        auto simd_time = std::chrono::microseconds(0);
        auto naive_time = std::chrono::microseconds(0);
        auto kahan_time = std::chrono::microseconds(0);

        double simd_sum = 0.0;
        double naive_sum_result = 0.0;
        double kahan_sum_result = 0.0;

        for(int i = 0; i < num_runs; i++) {
            {
                auto start = std::chrono::high_resolution_clock::now();
                simd_sum = find_sum(data.data(), data.size());
                auto end = std::chrono::high_resolution_clock::now();
                simd_time += std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            }

            {
                auto start = std::chrono::high_resolution_clock::now();
                naive_sum_result = naive_sum(data.data(), data.size());
                auto end = std::chrono::high_resolution_clock::now();
                naive_time += std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            }

            {
                auto start = std::chrono::high_resolution_clock::now();
                kahan_sum_result = kahan_sum(data.data(), data.size());
                auto end = std::chrono::high_resolution_clock::now();
                kahan_time += std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            }
        }

        double simd_avg = simd_time.count() / static_cast<double>(num_runs);
        double naive_avg = naive_time.count() / static_cast<double>(num_runs);
        double kahan_avg = kahan_time.count() / static_cast<double>(num_runs);

        double simd_error = std::abs((simd_sum - kahan_sum_result) / kahan_sum_result);
        double naive_error = std::abs((naive_sum_result - kahan_sum_result) / kahan_sum_result);

        std::cout << "\n" << test_name << " Results:\n"
                  << std::scientific << std::setprecision(6)
                  << "SIMD Sum:    " << simd_sum << "\n"
                  << "Naive Sum:   " << naive_sum_result << "\n"
                  << "Kahan Sum:   " << kahan_sum_result << "\n"
                  << "SIMD Error:  " << simd_error << "\n"
                  << "Naive Error: " << naive_error << "\n"
                  << std::fixed << std::setprecision(2)
                  << "SIMD Time:   " << simd_avg << " µs\n"
                  << "Naive Time:  " << naive_avg << " µs\n"
                  << "Kahan Time:  " << kahan_avg << " µs\n"
                  << "SIMD Speedup vs Naive: " << naive_avg/simd_avg << "x\n";
    }
};

TEST_F(SumStressTest, LargeUniformValues) {
    size_t n = 10'000'000;
    auto data = create_aligned_data<double>(n);
    std::uniform_real_distribution<double> dist(1.0, 2.0);

    for(auto& x : data) {
        x = dist(rng);
    }

    run_benchmark(data, "Large Uniform Values");
}

TEST_F(SumStressTest, WideRangeValues) {
    size_t n = 10'000'000;
    auto data = create_aligned_data<double>(n);
    std::uniform_real_distribution<double> dist(-1e200, 1e200);

    for(auto& x : data) {
        x = dist(rng);
    }

    run_benchmark(data, "Wide Range Values");
}

TEST_F(SumStressTest, AlternatingLargeSmall) {
    size_t n = 10'000'000;
    auto data = create_aligned_data<double>(n);

    for(size_t i = 0; i < n; i++) {
        if(i % 2 == 0) {
            data[i] = 1e100;
        } else {
            data[i] = 1e-100;
        }
    }

    run_benchmark(data, "Alternating Large/Small Values");
}

TEST_F(SumStressTest, KahanChallenging) {
    size_t n = 10'000'000;
    auto data = create_aligned_data<double>(n);

    data[0] = 1.0;
    for(size_t i = 1; i < n; i++) {
        data[i] = 1.0e-16;
    }

    run_benchmark(data, "Kahan Challenging Sequence");
}

TEST_F(SumStressTest, RandomWalk) {
    size_t n = 10'000'000;
    auto data = create_aligned_data<double>(n);
    std::normal_distribution<double> dist(0.0, 1.0);

    data[0] = 0.0;
    for(size_t i = 1; i < n; i++) {
        data[i] = data[i-1] + dist(rng);
    }

    run_benchmark(data, "Random Walk");
}

TEST_F(SumStressTest, ExtremeValues) {
    size_t n = 10'000'000;
    auto data = create_aligned_data<double>(n);

    // Mix of extreme values
    for(size_t i = 0; i < n; i++) {
        switch(i % 4) {
        case 0: data[i] = std::numeric_limits<double>::max() / (n * 2.0); break;
        case 1: data[i] = std::numeric_limits<double>::min(); break;
        case 2: data[i] = -std::numeric_limits<double>::max() / (n * 2.0); break;
        case 3: data[i] = -std::numeric_limits<double>::min(); break;
        }
    }

    run_benchmark(data, "Extreme Values");
}

#endif

} // namespace arcticdb