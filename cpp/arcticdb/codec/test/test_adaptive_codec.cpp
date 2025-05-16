#include <gtest/gtest.h>

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/codec/scanner.hpp>

#include <arcticdb/codec/test/encoding_test_common.hpp>

#include <random>

namespace arcticdb {

TEST(AdaptiveCodec, FFOR) {
    constexpr auto num_rows =  100'000;
    auto input = values_within_bitwidth<uint32_t>(20U, 7, 100'000);
    constexpr auto data_type = DataType::UINT32;
    auto column = std::make_shared<Column>(column_from_vector(input, make_scalar_type(data_type)));
    SegmentInMemory seg;
    FieldWrapper field_wrapper{make_scalar_type(data_type), "uints"};
    seg.add_column(field_wrapper.field(), column);
    seg.set_row_data(num_rows - 1);
    seg.calculate_statistics();
    auto scan_results = get_encodings(seg);
    auto codec_opts = codec::default_adaptive_codec();
    [[maybe_unused]] auto [max_compressed_size, uncompressed_size, encoded_buffer_size] = max_compressed_size_v2(seg, codec_opts, scan_results);
    ARCTICDB_DEBUG(log::codec(), "Max compressed: {} Uncompressed: {} Encoded buffer: {}", max_compressed_size, uncompressed_size, encoded_buffer_size);
    ASSERT_EQ(scan_results.value(0).first().type_, EncodingType::FFOR);
    auto copy_seg = seg.clone();
    auto encoded = encode_v2(std::move(seg), codec_opts);
    auto decoded = decode_segment(encoded);
    ASSERT_EQ(decoded, copy_seg);
}

TEST(AdaptiveCodec, BitPack) {
    constexpr auto num_rows   =  100'000;
    auto input = values_within_bitwidth<uint32_t>(0U, 7, 100'000);
    constexpr auto data_type = DataType::UINT32;
    auto column = std::make_shared<Column>(column_from_vector(input, make_scalar_type(data_type)));
    SegmentInMemory seg;
    FieldWrapper field_wrapper{make_scalar_type(data_type), "uints"};
    seg.add_column(field_wrapper.field(), column);
    seg.set_row_data(num_rows - 1);
    seg.calculate_statistics();
    auto scan_results = get_encodings(seg);
    auto codec_opts = codec::default_adaptive_codec();
    [[maybe_unused]] auto [max_compressed_size, uncompressed_size, encoded_buffer_size] = max_compressed_size_v2(seg, codec_opts, scan_results);
    ARCTICDB_DEBUG(log::codec(), "Max compressed: {} Uncompressed: {} Encoded buffer: {}", max_compressed_size, uncompressed_size, encoded_buffer_size);
    ASSERT_EQ(scan_results.value(0).first().type_, EncodingType::BITPACK);
    auto copy_seg = seg.clone();
    auto encoded = encode_v2(std::move(seg), codec_opts);
    auto decoded = decode_segment(encoded);
    ASSERT_EQ(decoded, copy_seg);
}

TEST(AdaptiveCodec, Delta) {
    constexpr auto num_rows =  100'000;
    auto input = delta_values<uint32_t>(1000U, 7, 100'000);
    constexpr auto data_type = DataType::UINT32;
    auto column = std::make_shared<Column>(column_from_vector(input, make_scalar_type(data_type)));
    SegmentInMemory seg;
    FieldWrapper field_wrapper{make_scalar_type(data_type), "uints"};
    seg.add_column(field_wrapper.field(), column);
    seg.set_row_data(num_rows - 1);
    seg.calculate_statistics();
    auto scan_results = get_encodings(seg);
    auto codec_opts = codec::default_adaptive_codec();
    [[maybe_unused]] auto [max_compressed_size, uncompressed_size, encoded_buffer_size] = max_compressed_size_v2(seg, codec_opts, scan_results);
    ARCTICDB_DEBUG(log::codec(), "Max compressed: {} Uncompressed: {} Encoded buffer: {}", max_compressed_size, uncompressed_size, encoded_buffer_size);
    ASSERT_EQ(scan_results.value(0).first().type_, EncodingType::DELTA);
    auto copy_seg = seg.clone();
    auto encoded = encode_v2(std::move(seg), codec_opts);
    auto decoded = decode_segment(encoded);
    ASSERT_EQ(decoded, copy_seg);
}

TEST(AdaptiveCodec, Constant) {
    constexpr auto num_rows =  100'000;

    std::vector<uint32_t> input;
    input.reserve(num_rows);
    for (std::size_t i = 0; i < num_rows; ++i) {
        input.push_back(42);
    }

    constexpr auto data_type = DataType::UINT32;
    auto column = std::make_shared<Column>(column_from_vector(input, make_scalar_type(data_type)));
    SegmentInMemory seg;
    FieldWrapper field_wrapper{make_scalar_type(data_type), "uints"};
    seg.add_column(field_wrapper.field(), column);
    seg.set_row_data(num_rows - 1);
    seg.calculate_statistics();
    auto scan_results = get_encodings(seg);
    auto codec_opts = codec::default_adaptive_codec();
    [[maybe_unused]] auto [max_compressed_size, uncompressed_size, encoded_buffer_size] = max_compressed_size_v2(seg, codec_opts, scan_results);
    ARCTICDB_DEBUG(log::codec(), "Max compressed: {} Uncompressed: {} Encoded buffer: {}", max_compressed_size, uncompressed_size, encoded_buffer_size);
    ASSERT_EQ(scan_results.value(0).first().type_, EncodingType::CONSTANT );
    auto copy_seg = seg.clone();
    auto encoded = encode_v2(std::move(seg), codec_opts);
    auto decoded = decode_segment(encoded);
    ASSERT_EQ(decoded, copy_seg);
}


TEST(AdaptiveCodec, Frequency) {
    constexpr auto num_rows =  100'000;

    auto input = values_with_duplicates<uint32_t>(num_rows, 0.97, 42);

    constexpr auto data_type = DataType::UINT32;
    auto column = std::make_shared<Column>(column_from_vector(input, make_scalar_type(data_type)));
    SegmentInMemory seg;
    FieldWrapper field_wrapper{make_scalar_type(data_type), "uints"};
    seg.add_column(field_wrapper.field(), column);
    seg.set_row_data(num_rows - 1);
    seg.calculate_statistics();
    auto scan_results = get_encodings(seg);
    auto codec_opts = codec::default_adaptive_codec();
    [[maybe_unused]] auto [max_compressed_size, uncompressed_size, encoded_buffer_size] = max_compressed_size_v2(seg, codec_opts, scan_results);
    ARCTICDB_DEBUG(log::codec(), "Max compressed: {} Uncompressed: {} Encoded buffer: {}", max_compressed_size, uncompressed_size, encoded_buffer_size);
    ASSERT_EQ(scan_results.value(0).first().type_, EncodingType::FREQUENCY);
    auto copy_seg = seg.clone();
    auto encoded = encode_v2(std::move(seg), codec_opts);
    auto decoded = decode_segment(encoded);
    ASSERT_EQ(decoded, copy_seg);
}

TEST(AdaptiveCodec, ALPRD) {
    constexpr auto num_rows =  100'000;
    auto input = random_doubles(100'000, -1000.0, 1000.0);
    constexpr auto data_type = DataType::FLOAT64;
    auto column = std::make_shared<Column>(column_from_vector(input, make_scalar_type(data_type)));
    SegmentInMemory seg;
    FieldWrapper field_wrapper{make_scalar_type(data_type), "uints"};
    seg.add_column(field_wrapper.field(), column);
    seg.set_row_data(num_rows - 1);
    seg.calculate_statistics();
    auto scan_results = get_encodings(seg);
    auto codec_opts = codec::default_adaptive_codec();
    [[maybe_unused]] auto [max_compressed_size, uncompressed_size, encoded_buffer_size] = max_compressed_size_v2(seg, codec_opts, scan_results);
    ARCTICDB_DEBUG(log::codec(), "Max compressed: {} Uncompressed: {} Encoded buffer: {}", max_compressed_size, uncompressed_size, encoded_buffer_size);
    ASSERT_EQ(scan_results.value(0).first().type_, EncodingType::ALP);
    auto copy_seg = seg.clone();
    auto encoded = encode_v2(std::move(seg), codec_opts);
    auto decoded = decode_segment(encoded);
    ASSERT_EQ(decoded, copy_seg);
}

TEST(AdaptiveCodec, ALP) {
    constexpr auto num_rows =  100'000;
    auto input = random_decimal_floats(0, 1000, 2, num_rows);
    constexpr auto data_type = DataType::FLOAT64;
    auto column = std::make_shared<Column>(column_from_vector(input, make_scalar_type(data_type)));
    SegmentInMemory seg;
    FieldWrapper field_wrapper{make_scalar_type(data_type), "uints"};
    seg.add_column(field_wrapper.field(), column);
    seg.set_row_data(num_rows - 1);
    seg.calculate_statistics();
    auto scan_results = get_encodings(seg);
    auto codec_opts = codec::default_adaptive_codec();
    [[maybe_unused]] auto [max_compressed_size, uncompressed_size, encoded_buffer_size] = max_compressed_size_v2(seg, codec_opts, scan_results);
    ARCTICDB_DEBUG(log::codec(), "Max compressed: {} Uncompressed: {} Encoded buffer: {}", max_compressed_size, uncompressed_size, encoded_buffer_size);
    ASSERT_EQ(scan_results.value(0).first().type_, EncodingType::ALP);
    auto copy_seg = seg.clone();
    auto encoded = encode_v2(std::move(seg), codec_opts);
    auto decoded = decode_segment(encoded);
    ASSERT_EQ(decoded, copy_seg);
}

} // namespace arcticdb