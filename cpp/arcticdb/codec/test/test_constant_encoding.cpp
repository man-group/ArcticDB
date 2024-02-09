/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <gtest/gtest.h>
#include <arcticdb/codec/constant_encoding.hpp>

#include <random>

TEST(ConstantEncoding, Basic) {
    using namespace arcticdb;
    using InputType = uint32_t;
    std::vector<InputType> data(30);
    std::fill(std::begin(data), std::end(data), 23);
    ConstantEncoding<InputType> encoding;
    auto estimated_size = encoding.max_required_bytes(data.data(), data.size());
    ASSERT_EQ(estimated_size.has_value(), true);
    std::vector<uint8_t> output(*estimated_size);

    auto bytes = encoding.encode(data.data(), data.size(), output.data());
    ASSERT_EQ(bytes, 12);
    std::vector<uint32_t> decompressed(data.size());
    //(void)run_length_decode(output.data(), bytes, decompressed.data());
    auto num_rows = encoding.decode(output.data(), bytes, decompressed.data());
    ASSERT_EQ(num_rows, data.size());
    ASSERT_EQ(decompressed, data);
}

TEST(ConstantEncoding, Scan) {
    using namespace arcticdb;
    using InputType = uint32_t;
    std::vector<InputType> data {1, 1, 1, 2, 3, 1, 2, 2, 2, 2, 5};

    ConstantEncoding<InputType> encoding;
    auto estimated_size = encoding.max_required_bytes(data.data(), data.size());
    ASSERT_EQ(estimated_size.has_value(), false);

    data.clear();
    data.resize(42);
    std::fill(std::begin(data), std::end(data), 23);
    estimated_size = encoding.max_required_bytes(data.data(), data.size());
    ASSERT_EQ(estimated_size.has_value(), true);
}