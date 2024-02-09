/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/codec/frequency_encoding.hpp>
#include <arcticdb/codec/test/encoding_test_common.hpp>
#include <arcticdb/util/timer.hpp>

#include <random>

TEST(FrequencyEncoding, Basic) {
    using namespace arcticdb;
    using InputType = uint32_t;
    std::vector<InputType> data {1, 1, 1, 2, 1, 1, 1, 1, 2, 2, 5};
    FrequencyEncoding<InputType, 60> encoding;
    auto estimated_size = encoding.max_required_bytes(data.data(), data.size());
    ASSERT_EQ(estimated_size.has_value(), true);
    std::vector<uint8_t> output(*estimated_size);

    auto bytes = encoding.encode(data.data(), data.size(), output.data());
    ASSERT_EQ(bytes, 49);
    std::vector<uint32_t> decompressed(data.size());
    //(void)run_length_decode(output.data(), bytes, decompressed.data());
    auto num_rows = encoding.decode(output.data(), bytes, decompressed.data());
    ASSERT_EQ(num_rows, data.size());
    ASSERT_EQ(decompressed, data);
}

TEST(FrequencyEncoding, Scan) {
    using namespace arcticdb;
    using InputType = uint32_t;
    std::vector<uint32_t> data {1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 5, 1, 1, 1};

    FrequencyEncoding<InputType, 60> encoding;
    auto estimated_size = encoding.max_required_bytes(data.data(), data.size());
    ASSERT_EQ(estimated_size.has_value(), true);
    ASSERT_EQ(*estimated_size, 85);
}

TEST(FrequencyEncoding, Stress) {
    using namespace arcticdb;
    using InputType = uint32_t;
    auto data = random_numbers_with_leader<InputType>(1000000000, 23, 0.9);
    FrequencyEncoding<InputType> encoding;
    interval_timer timer;
    timer.start_timer("Compress");
    auto estimated_size = encoding.max_required_bytes(data.data(), data.size());
    ASSERT_EQ(estimated_size.has_value(), true);
    std::vector<uint8_t> output(*estimated_size);

    auto bytes = encoding.encode(data.data(), data.size(), output.data());
    timer.stop_timer("Compress");
    std::vector<InputType> decompressed(data.size());
    timer.start_timer("Decompress");
    (void)encoding.decode(output.data(), bytes, decompressed.data());
    timer.stop_timer("Decompress");
    log::version().info("{}", timer.display_all());
    ASSERT_EQ(decompressed, data);
}