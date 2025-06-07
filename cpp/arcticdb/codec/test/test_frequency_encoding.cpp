/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include "arcticdb/codec/compression/frequency.hpp"
#include <arcticdb/codec/test/encoding_test_common.hpp>
#include <arcticdb/util/timer.hpp>
#include <arcticdb/codec/test/encoding_test_common.hpp>

#include <random>

TEST(FrequencyCompressor, Basic) {
    using namespace arcticdb;
    using InputType = uint32_t;
    std::vector<InputType> data {1, 1, 1, 2, 1, 1, 1, 1, 2, 2, 5};
    FrequencyCompressor<InputType, 60> encoding;
    auto wrapper = from_vector(data, make_scalar_type(DataType::UINT32));
    encoding.scan(wrapper.data_);
    auto estimated_size = encoding.max_required_bytes(wrapper.data_, data.size());
    ASSERT_EQ(estimated_size.has_value(), true);
    std::vector<uint8_t> output(*estimated_size);

    auto bytes = encoding.compress(wrapper.data_, data.size(), output.data(), *estimated_size);
    ASSERT_EQ(bytes, 49);
    std::vector<uint32_t> decompressed(data.size());
    auto result = FrequencyDecompressor<InputType>::decompress(output.data(), decompressed.data());
    ASSERT_EQ(result.uncompressed_, data.size() * sizeof(InputType));
    ASSERT_EQ(decompressed, data);
}

TEST(FrequencyCompressor, Scan) {
    using namespace arcticdb;
    using InputType = uint32_t;
    std::vector<uint32_t> data {1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 5, 1, 1, 1};

    FrequencyCompressor<InputType, 60> encoding;
    auto wrapper = from_vector(data, make_scalar_type(DataType::UINT32));
    encoding.scan(wrapper.data_);
    auto estimated_size = encoding.max_required_bytes(wrapper.data_, data.size());
    ASSERT_EQ(estimated_size.has_value(), true);
    ASSERT_EQ(*estimated_size, 9066); // TODO this value is too large
}

TEST(FrequencyCompressor, Stress) {
    using namespace arcticdb;
    using InputType = uint32_t;
    auto data = random_numbers_with_leader<InputType>(100000, 23, 0.9);
    FrequencyCompressor<InputType> encoding;
    interval_timer timer;
    auto wrapper = from_vector(data, make_scalar_type(DataType::UINT32));
    encoding.scan(wrapper.data_);
    auto estimated_size = encoding.max_required_bytes(wrapper.data_, data.size());
    ASSERT_EQ(estimated_size.has_value(), true);
    std::vector<uint8_t> output(*estimated_size);
    timer.start_timer("Compress");
    size_t bytes;
    const auto num_runs = 100UL;
    for(auto i = 0UL; i < num_runs; ++i) {
        bytes = encoding.compress(wrapper.data_, data.size(), output.data(), *estimated_size);
    }
    timer.stop_timer("Compress");
    ASSERT_LT(bytes, estimated_size.value());
    std::vector<InputType> decompressed(data.size());
    timer.start_timer("Decompress");
    for(auto i = 0UL; i < num_runs; ++i) {
        (void) FrequencyDecompressor<InputType>::decompress(output.data(), decompressed.data());
    }
    timer.stop_timer("Decompress");
    log::version().info("{}", timer.display_all());
    ASSERT_EQ(decompressed, data);
}


TEST(FrequencyCompressor, StressScan) {
    using namespace arcticdb;
    using InputType = uint64_t;
    auto data = random_numbers_with_leader<InputType>(100000, 23, 0.9);
    interval_timer timer;
    FrequencyCompressor<InputType> encoding;
    timer.start_timer("Scan");
    const auto num_runs = 100UL;

    auto wrapper = from_vector(data, make_scalar_type(DataType::UINT32));
    for(auto i = 0UL; i < num_runs; ++i) {
        encoding.scan(wrapper.data_);
    }
    timer.stop_timer("Scan");
    log::version().info("{}", timer.display_all());
}

TEST(FrequencyCompressor, StressMaxRequired) {
    using namespace arcticdb;
    using InputType = uint64_t;
    auto data = random_numbers_with_leader<InputType>(100000, 23, 0.9);
    interval_timer timer;
    FrequencyCompressor<InputType> encoding;
    timer.start_timer("MaxRequired");
    auto wrapper = from_vector(data, make_scalar_type(DataType::UINT32));
    const auto num_runs = 100UL;
    std::optional<size_t> max_bytes;
    encoding.scan(wrapper.data_);
    for(auto i = 0UL; i < num_runs; ++i) {
        max_bytes = encoding.max_required_bytes(wrapper.data_, 100'000);
    }
    timer.stop_timer("MaxRequired");
    log::version().info("{}", timer.display_all());
}