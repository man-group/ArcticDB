/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <gtest/gtest.h>
#include <arcticdb/codec/compression/constant.hpp>
#include <arcticdb/codec/test/encoding_test_common.hpp>

#include <random>

TEST(ConstantEncoding, Basic) {
    using namespace arcticdb;
    using InputType = uint32_t;
    std::vector<InputType> data(30);
    std::fill(std::begin(data), std::end(data), 23);
    ConstantCompressor<InputType> encoder;
    auto estimated_size = encoder.max_required_bytes(data.data(), data.size());
    ASSERT_EQ(estimated_size.has_value(), true);
    std::vector<uint8_t> output(*estimated_size);
    auto column_data = from_vector(data, make_scalar_type(DataType::UINT32));
    auto bytes = encoder.compress(column_data.data_, reinterpret_cast<uint32_t*>(output.data()), *estimated_size);
    ASSERT_EQ(bytes, 12);
    std::vector<uint32_t> decompressed(data.size());
    auto result = ConstantDecompressor<InputType>::decompress(output.data(), decompressed.data());
    ASSERT_EQ(result.uncompressed_, data.size() * sizeof(InputType));
    ASSERT_EQ(decompressed, data);
}

TEST(ConstantEncoding, Scan) {
    using namespace arcticdb;
    using InputType = uint32_t;
    std::vector<InputType> data {1, 1, 1, 2, 3, 1, 2, 2, 2, 2, 5};

    ConstantCompressor<InputType> encoder;
    auto estimated_size = encoder.max_required_bytes(data.data(), data.size());
    ASSERT_EQ(estimated_size.has_value(), false);

    data.clear();
    data.resize(42);
    std::fill(std::begin(data), std::end(data), 23);
    estimated_size = encoder.max_required_bytes(data.data(), data.size());
    ASSERT_EQ(estimated_size.has_value(), true);
}