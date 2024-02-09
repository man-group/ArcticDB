/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <arcticdb/codec/test/encoding_test_common.hpp>
#include <arcticdb/codec/run_length_encoding.hpp>

// The first test checks that data will actually be encoded and
// decoded correctly, and the second test checks that we
// generally fail gracefully with unencodable data

template <typename T>
void test_encoder() {
    using namespace arcticdb;
    std::size_t count = 1000000;
    uint64_t seed = 42;
    std::vector<T> input_data = random_numbers_with_runs<T>(count, seed);

    RunLengthEncoding<T> encoding;
    auto encoded_size = encoding.max_required_bytes(input_data.data(), input_data.size());
    std::vector<uint8_t> encoded_data(*encoded_size);

    size_t actual_encoded_size = encoding.encode(input_data.data(), input_data.size(), encoded_data.data());

    std::vector<T> decoded_data(input_data.size());
    size_t actual_decoded_size = encoding.decode(encoded_data.data(), actual_encoded_size, decoded_data.data());

    RC_ASSERT(input_data.size() == actual_decoded_size);
    RC_ASSERT(input_data == decoded_data);
}

RC_GTEST_PROP(RunLengthEncoding, ReversibleEncodingDecoding, ()) {
    test_encoder<int8_t>();
    test_encoder<uint8_t>();
    test_encoder<int16_t>();
    test_encoder<uint16_t>();
    test_encoder<int32_t>();
    test_encoder<uint32_t>();
    test_encoder<int64_t>();
    test_encoder<uint64_t>();
}

template <typename T>
void test_encoder_random_data() {
    using namespace arcticdb;
    rc::check("run_length_encode random data",
              [](const std::vector<T> &input) {
                  RunLengthEncoding<T> encoding;
                  auto required_bytes = encoding.max_required_bytes(input.data(), input.size());
                  if (!required_bytes.has_value())
                      RC_SUCCEED("No single value comprises more than 90% of the array");

                  std::vector<uint8_t> encoded(*required_bytes);
                  size_t encoded_size = encoding.encode(input.data(), input.size(), encoded.data());

                  std::vector<T> decoded(input.size());
                  size_t decoded_size = encoding.decode(encoded.data(), encoded_size, decoded.data());

                  RC_ASSERT(decoded_size == input.size());
                  RC_ASSERT(decoded == input);
              });
}

RC_GTEST_PROP(RunLengthEncoding, GeneratedData, ()) {
    test_encoder_random_data<int8_t>();
    test_encoder_random_data<uint8_t>();
    test_encoder_random_data<int16_t>();
    test_encoder_random_data<uint16_t>();
    test_encoder_random_data<int32_t>();
    test_encoder_random_data<uint32_t>();
    test_encoder_random_data<int64_t>();
    test_encoder_random_data<uint64_t>();
    test_encoder_random_data<float>();
    test_encoder_random_data<double>();
}