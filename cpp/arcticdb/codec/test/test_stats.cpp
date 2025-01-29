/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <gtest/gtest.h>
#include <random>

#include <arcticdb/util/timer.hpp>
#include <arcticdb/codec/calculate_stats.hpp>

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/timer.hpp>

template<typename T>
std::vector<T> random_vector(
    size_t size,
    T min = std::numeric_limits<T>::min(),
    T max = std::numeric_limits<T>::max()) {
    const unsigned int seed = 12345;
    std::mt19937 generator(seed);
    std::uniform_int_distribution<T> distribution(min, max);

    std::vector<T> output(size);
    std::generate(output.begin(), output.end(), [&]() {
        return distribution(generator);
    });

    return output;
}

TEST(ConstantStats, Stress) {
    using namespace arcticdb;
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<size_t> size_dist(0, 100);
    std::vector<uint64_t> vec(100'000 + size_dist(gen), 42);
    interval_timer timer;
    timer.start_timer("ConstantScan");
    const auto num_runs = 1000000UL;
    bool constant;
    for(auto i = 0UL; i < num_runs; ++i) {
        constant = is_constant<uint64_t>(vec.data(), vec.size());
    }
    timer.stop_timer("Compress");
    ASSERT_EQ(constant, true);
    log::version().info("{}", timer.display_all());
}

TEST(LeftmostBit, Simple) {
    using namespace arcticdb;
    std::vector<uint64_t> vec(1024);
    std::iota(std::begin(vec), std::end(vec), 0);
    auto result = msb(vec.data());
    ASSERT_EQ(result, 9);
}

TEST(LeftmostBit, Stress) {
    using namespace arcticdb;
    auto data = random_vector<uint64_t>(1024 * 100, 21UL, 1UL << 10);
    auto compressed = std::vector<uint64_t>(1024 * 100);

    size_t num_runs = 1000000;

    interval_timer timer;
    timer.start_timer("Scan");
    uint8_t result = 0;
    auto count = 0;
    for(auto k = 0UL; k < num_runs; ++k) {
        for (auto i = 0; i < 100; ++i) {
            ++count;
            result = std::max(result, msb_max(data.data() + i * 1024));
        }
    }
    ASSERT_EQ(count, 100000000);
    timer.stop_timer("Scan");
    log::version().info("{}\n{}", result, timer.display_all());
}

TEST(MaxMsb, Stress) {
    using namespace arcticdb;
    auto data = random_vector<uint64_t>(1024 * 100, 21UL, 1UL << 10);
    auto compressed = std::vector<uint64_t>(1024 * 100);

    size_t num_runs = 1000000;

    interval_timer timer;
    timer.start_timer("Scan");
    uint8_t result = 0;
    auto count = 0;
    for(auto k = 0UL; k < num_runs; ++k) {
        for (auto i = 0; i < 100; ++i) {
            ++count;
            result = std::max(result, msb_max(data.data() + i * 1024));
        }
    }
    ASSERT_EQ(count, 100000000);
    timer.stop_timer("Scan");
    log::version().info("{}\n{}", result, timer.display_all());
}

TEST(MinMax, Stress) {
    using namespace arcticdb;
    auto data = random_vector<uint64_t>(1024 * 100, 21UL, 1UL << 10);
    auto compressed = std::vector<uint64_t>(1024 * 100);

    size_t num_runs = 1000000;

    interval_timer timer;
    timer.start_timer("Scan");
    auto result = std::pair<uint64_t, uint64_t>(0, 0);
    auto count = 0;
    for(auto k = 0UL; k < num_runs; ++k) {
        for (auto i = 0; i < 100; ++i) {
            ++count;
            result = min_max_pair(result, min_max(data.data() + i * 1024));
        }
    }
    ASSERT_EQ(count, 100000000);
    timer.stop_timer("Scan");
    log::version().info("{} - {}\n{}", result.first, result.second, timer.display_all());
}