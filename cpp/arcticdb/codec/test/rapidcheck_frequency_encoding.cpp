/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <arcticdb/codec/frequency_encoding.hpp>
#include <arcticdb/codec/test/encoding_test_common.hpp>

#include <vector>

// The first test checks that data will actually be encoded and
// decoded correctly, and the second test checks that we
// generally fail gracefully with unencodable data

template <typename T>
void test_encoder() {
    using namespace arcticdb;
    FrequencyEncoding<T> encoder;

    rc::check("Encode and decode should be reversible", [&] {
        int length = *rc::gen::inRange(1, 1000);
        T leader = *rc::gen::arbitrary<T>();
        uint64_t percentage = *rc::gen::inRange(91, 100);
        double ratio = static_cast<double>(percentage) / 100;
        unsigned int seed = *rc::gen::arbitrary<unsigned int>();

        std::vector<T> input = random_numbers_with_leader(length, leader, ratio, seed);
        size_t num_rows = input.size();

        auto max_bytes_opt = encoder.max_required_bytes(input.data(), num_rows);

        RC_ASSERT(max_bytes_opt.has_value());

        size_t max_bytes = max_bytes_opt.value();
        std::vector<uint8_t> encoded_data(max_bytes);

        size_t encoded_size = encoder.encode(input.data(), num_rows, encoded_data.data());

        std::vector<T> decoded_data(num_rows);
        encoder.decode(encoded_data.data(), encoded_size, decoded_data.data());

        RC_ASSERT(input == decoded_data);
    });
}

RC_GTEST_PROP(FrequencyEncoding, ReversibleEncodingDecoding, ()) {
    test_encoder<uint8_t>();
    test_encoder<int8_t>();
    test_encoder<uint16_t>();
    test_encoder<int16_t>();
    test_encoder<uint32_t>();
    test_encoder<int32_t>();
    test_encoder<uint64_t>();
    test_encoder<int64_t>();
    test_encoder<float>();
    test_encoder<double>();
}

template <typename T>
void test_encoder_random_data() {
    using namespace arcticdb;
    rc::check("frequency_encode random data",
              [](const std::vector<T> &input) {
                  FrequencyEncoding<T> encoding;
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

RC_GTEST_PROP(FrequencyEncoding, GeneratedData, ()) {
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