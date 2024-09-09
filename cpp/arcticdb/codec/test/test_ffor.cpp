#include <gtest/gtest.h>
#include <arcticdb/codec/fastlanes_common.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/codec/bitpack_fused.hpp>
#include <arcticdb/util/timer.hpp>
#include <arcticdb/codec/scan.hpp>

#include <random>
#include <algorithm>

TEST(FFor, PrintIndex) {
    using namespace arcticdb;
    for(auto i = 0UL; i < Helper<uint64_t>::num_lanes; ++i) {
        for(auto j = 0UL; j < Helper<uint64_t>::register_width; ++j) {
            log::version().info("{}", index(j, i));
        }
    }
}

namespace arcticdb {

template<typename T>
struct FForCompress {
    const T reference_;
    
    explicit FForCompress(T reference) :
        reference_(reference) {
    }
    
    ARCTICDB_ALWAYS_INLINE T operator()(const T t) {
        return t - reference_;
    }
};

template<typename T>
struct FForUncompress {
    const T reference_;

    FForUncompress(T reference) :
        reference_(reference) {
    }

    ARCTICDB_ALWAYS_INLINE T operator()(T value) {
        return value + reference_;
    }
};

}

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

TEST(FFor, SimpleRoundtrip) {
    using namespace arcticdb;
    auto data = random_vector<uint64_t>(1024, 21UL, 1UL << 10);
    auto compressed = std::vector<uint64_t>(1024);
    BitPackFused<uint64_t, 11>::go(data.data(), compressed.data(), [] (auto t) { return t + 20; });

    std::vector<uint64_t> uncompressed(1024);
    
    BitUnpackFused<uint64_t, 11>::go(compressed.data(), uncompressed.data(), FForUncompress<uint64_t>{20UL});
    for(auto i = 0U; i < 1024; ++i) {
        ASSERT_EQ(data[i], uncompressed[i]);
    }
}

TEST(FForStress, fused) {
    using namespace arcticdb;
    auto data = random_vector<uint64_t>(1024 * 100, 21UL, 1UL << 10);
    auto compressed = std::vector<uint64_t>(1024 * 100);

    size_t num_runs = 1000000;
    interval_timer timer;
    timer.start_timer("pack");
    for(auto k = 0UL; k < num_runs; ++k) {
        for (auto i = 0; i < 100; ++i)
            //BitPackFused<uint64_t, 11>::go(data.data() + 1024 * i, compressed.data() + 176 * i, FForCompress<uint64_t>{20UL});
            BitPackFused<uint64_t, 11>::go(data.data() + 1024 * i, compressed.data() + 176 * i, [] (auto t) { return t + 20; });
    }
    timer.stop_timer("pack");
    std::vector<uint64_t> uncompressed(1024 * 100);
    timer.start_timer("unpack");
    for(auto k = 0UL; k < num_runs; ++k) {
        for (auto i = 0; i < 100; ++i)
            BitUnpackFused<uint64_t, 11>::go(compressed.data() + 176 * i, uncompressed.data() + 1024 * i, [] (auto t) { return t - 20; });
    }

    timer.stop_timer("unpack");
    log::version().info("\n{}", timer.display_all());
    for(auto i = 0; i < 100 * 1024; ++i) {
        ASSERT_EQ(data[i], uncompressed[i]);
    }
}

TEST(FForStress, FusedWithScan) {
    using namespace arcticdb;
    auto data = random_vector<uint64_t>(1024 * 100, 21UL, 1UL << 10);
    auto compressed = std::vector<uint64_t>(1024 * 100);

    auto result = std::pair<uint64_t, uint64_t>(0, 0);
    for (auto i = 0; i < 100; ++i) {
        result = min_max_pair(result, min_max(data.data() + i * 1024));
    }
    const auto min = result.first;
    size_t num_runs = 1000000;
    interval_timer timer;
    timer.start_timer("pack");
    for(auto k = 0UL; k < num_runs; ++k) {
        for (auto i = 0; i < 100; ++i)
            //BitPackFused<uint64_t, 11>::go(data.data() + 1024 * i, compressed.data() + 176 * i, FForCompress<uint64_t>{20UL});
            BitPackFused<uint64_t, 11>::go(data.data() + 1024 * i, compressed.data() + 176 * i, [min] (auto t) { return t + min; });
    }
    timer.stop_timer("pack");
    std::vector<uint64_t> uncompressed(1024 * 100);
    timer.start_timer("unpack");
    for(auto k = 0UL; k < num_runs; ++k) {
        for (auto i = 0; i < 100; ++i)
            BitUnpackFused<uint64_t, 11>::go(compressed.data() + 176 * i, uncompressed.data() + 1024 * i, [min] (auto t) { return t - min; });
    }

    timer.stop_timer("unpack");
    log::version().info("\n{}", timer.display_all());
    for(auto i = 0; i < 100 * 1024; ++i) {
        ASSERT_EQ(data[i], uncompressed[i]);
    }
}