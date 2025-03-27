#include <gtest/gtest.h>

#include <random>

#include <arcticdb/codec/compression/alp/alp.hpp>
#include <arcticdb/log/log.hpp>

namespace arcticdb {

TEST(ALP, DetermineScheme) {
    auto rd = std::random_device();
    std::default_random_engine generator(rd());
    auto dist = std::uniform_real_distribution<double>(0.0, 1000.0);
    constexpr size_t num_rows = 1024;
    std::vector<double> data;
    data.reserve(num_rows);
    for(auto i = 0UL; i < num_rows; ++i) {
        data.push_back(dist(generator));
    }
    alp::state<double> state;
    std::array<double, 1024> sample_buf;

    alp::encoder<double>::init(data.data(), 0, 1024, sample_buf.data(), state);
    log::codec().info("state: ", state.exceptions_count);

}


} //namespace arcticdb