/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <arcticdb/codec/compression/frequency.hpp>
#include <arcticdb/codec/test/encoding_test_common.hpp>

#include <vector>

// The first test checks that data will actually be encoded and
// decoded correctly, and the second test checks that we
// generally fail gracefully with unencodable data

template <typename T, arcticdb::DataType data_type>
void test_encoder() {
    using namespace arcticdb;
    FrequencyCompressor<T> encoder;

    rc::check("Encode and decode should be reversible", [&] {
        int length = *rc::gen::inRange(1, 1000);
        T leader = *rc::gen::arbitrary<T>();
        uint64_t percentage = *rc::gen::inRange(91, 100);
        double ratio = static_cast<double>(percentage) / 100;
        unsigned int seed = *rc::gen::arbitrary<unsigned int>();

        std::vector<T> input = random_numbers_with_leader(length, leader, ratio, seed);
        size_t num_rows = input.size();
        auto wrapper = from_vector(input, make_scalar_type(data_type));
        auto max_bytes_opt = encoder.max_required_bytes(wrapper.data_, num_rows);

        RC_ASSERT(max_bytes_opt.has_value());

        size_t max_bytes = max_bytes_opt.value();
        std::vector<uint8_t> encoded_data(max_bytes);

        (void)encoder.compress(wrapper.data_, num_rows, encoded_data.data(), *max_bytes_opt);

        std::vector<T> decoded_data(num_rows);
        FrequencyDecompressor<T>::decompress(encoded_data.data(), decoded_data.data());

        RC_ASSERT(input == decoded_data);
    });
}

RC_GTEST_PROP(FrequencyCompressor, ReversibleEncodingDecoding, ()) {
    using namespace arcticdb;
    test_encoder<uint8_t, DataType::UINT8>();
    test_encoder<int8_t, DataType::INT8>();
    test_encoder<uint16_t, DataType::UINT16>();
    test_encoder<int16_t, DataType::INT16>();
    test_encoder<uint32_t, DataType::UINT32>();
    test_encoder<int32_t, DataType::INT32>();
    test_encoder<uint64_t, DataType::UINT64>();
    test_encoder<int64_t, DataType::INT64>();
    test_encoder<float, DataType::FLOAT32>();
    test_encoder<double, DataType::FLOAT64>();
}

template <typename T, arcticdb::DataType data_type>
void test_encoder_random_data() {
    using namespace arcticdb;
    rc::check("frequency_encode random data",
              [](const std::vector<T> &input) {
                  FrequencyCompressor<T> encoding;
                  auto wrapper = from_vector(input, make_scalar_type(data_type));
                  encoding.scan(wrapper.data_);
                  auto required_bytes = encoding.max_required_bytes(wrapper.data_, input.size());
                  if (!required_bytes.has_value() || required_bytes == 0)
                      RC_SUCCEED("No single value comprises more than 90% of the array");

                  std::vector<uint8_t> encoded(*required_bytes);
                  (void)encoding.compress(wrapper.data_, input.size(), encoded.data(), *required_bytes);

                  std::vector<T> decoded(input.size());
                  auto decoded_size =  FrequencyDecompressor<T>::decompress(encoded.data(), decoded.data());

                  RC_ASSERT(decoded_size.uncompressed_ == input.size() * sizeof(T));
                  RC_ASSERT(decoded == input);
              });
}

RC_GTEST_PROP(FrequencyCompressor, GeneratedData, ()) {
    using namespace arcticdb;
    test_encoder_random_data<int8_t, DataType::INT8>();
    test_encoder_random_data<uint8_t, DataType::UINT8>();
    test_encoder_random_data<int16_t, DataType::INT16>();
    test_encoder_random_data<uint16_t, DataType::UINT16>();
    test_encoder_random_data<int32_t, DataType::INT32>();
    test_encoder_random_data<uint32_t, DataType::UINT32>();
    test_encoder_random_data<int64_t, DataType::INT64>();
    test_encoder_random_data<uint64_t, DataType::UINT64>();
    test_encoder_random_data<float, DataType::FLOAT32>();
    test_encoder_random_data<double, DataType::FLOAT64>();
}