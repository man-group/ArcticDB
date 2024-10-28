#include <gtest/gtest.h>

#include <random>

#include <arcticdb/codec/compression/alp/alp.hpp>
#include <arcticdb/codec/compression/alp.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/codec/compression/alp_header.hpp>
#include <arcticdb/codec/test/encoding_test_common.hpp>
#include "codec/compression/ffor.hpp"

namespace arcticdb {

TEST(ALP, DetermineSchemeRealDouble) {
    auto data = random_doubles();
    alp::state<double> state;
    std::array<double, 1024> sample_buf;

    alp::encoder<double>::init(data.data(), 0, 1024, sample_buf.data(), state);
    ARCTICDB_DEBUG(log::codec(), "state: {}", state.exceptions_count);
    ASSERT_NE(state.scheme, alp::Scheme::INVALID);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP_RD);
}

TEST(ALP, RoundtripRD) {
    auto data = random_doubles();
    alp::state<double> state;
    std::array<double, 1024> sample_buf;

    alp::encoder<double>::init(data.data(), 0, 1024, sample_buf.data(), state);
    ARCTICDB_DEBUG(log::codec(), "state: {}", state.exceptions_count);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP_RD);

    std::array<uint16_t, BLOCK_SIZE> exceptions;
    std::array<uint16_t, BLOCK_SIZE> positions;
    std::array<uint16_t, BLOCK_SIZE> excp_count;
    std::array<uint64_t, BLOCK_SIZE> right;
    std::array<uint16_t, BLOCK_SIZE> left;

    alp::rd_encoder<double>::init(data.data(), 0, 1024, sample_buf.data(), state);
    alp::rd_encoder<double>::encode(data.data(), exceptions.data(), positions.data(), excp_count.data(), right.data(), left.data(), state);

    std::vector<double> output(1024);
    alp::rd_encoder<double>::decode(output.data(), right.data(), left.data(), exceptions.data(), positions.data(), excp_count.data(), state);
    ASSERT_EQ(data, output);
}

TEST(ALP, RoundtripRDWideRange) {
    auto data = random_doubles(1024, std::numeric_limits<double>::lowest(), std::numeric_limits<double>::max());
    alp::state<double> state;
    std::array<double, 1024> sample_buf;

    alp::encoder<double>::init(data.data(), 0, 1024, sample_buf.data(), state);
    ARCTICDB_DEBUG(log::codec(), "state: {}", state.exceptions_count);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP_RD);

    std::array<uint16_t, BLOCK_SIZE> exceptions;
    std::array<uint16_t, BLOCK_SIZE> positions;
    std::array<uint16_t, BLOCK_SIZE> excp_count;
    std::array<uint64_t, BLOCK_SIZE> right;
    std::array<uint16_t, BLOCK_SIZE> left;

    alp::rd_encoder<double>::init(data.data(), 0, 1024, sample_buf.data(), state);
    alp::rd_encoder<double>::encode(data.data(), exceptions.data(), positions.data(), excp_count.data(), right.data(), left.data(), state);

    std::vector<double> output(1024);
    alp::rd_encoder<double>::decode(output.data(), right.data(), left.data(), exceptions.data(), positions.data(), excp_count.data(), state);
    ASSERT_EQ(data, output);
}

TEST(ALP, RoundtripLong) {
    constexpr size_t NUM_VALUES = BLOCK_SIZE * 100;
    auto data = random_doubles(NUM_VALUES);
    alp::state<double> state;
    std::array<double, 1024> sample_buf;

    alp::encoder<double>::init(data.data(), 0, NUM_VALUES, sample_buf.data(), state);
    ARCTICDB_DEBUG(log::codec(), "state: {}", state.exceptions_count);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP_RD);

    std::array<uint16_t, NUM_VALUES> exceptions;
    std::array<uint16_t, NUM_VALUES> positions;
    std::array<uint16_t, NUM_VALUES> excp_count;
    std::array<uint64_t, NUM_VALUES> right;
    std::array<uint16_t, NUM_VALUES> left;

    auto num_blocks = NUM_VALUES / alp::config::VECTOR_SIZE;
    std::vector<alp::state<double>> states(num_blocks);
    for(auto i = 0UL; i < num_blocks; ++i) {
        const auto pos = i * alp::config::VECTOR_SIZE;
        alp::rd_encoder<double>::init(data.data(), pos, NUM_VALUES, sample_buf.data(), states[i]);
        alp::rd_encoder<double>::encode(
            data.data() + pos,
            exceptions.data() + pos,
            positions.data() + pos,
            excp_count.data() + pos,
            right.data() + pos,
            left.data() + pos,
            states[i]);
    }
    std::vector<double> output(NUM_VALUES);
    for(auto i = 0UL; i < num_blocks; ++i) {
        const auto pos = i * alp::config::VECTOR_SIZE;
        alp::rd_encoder<double>::decode(
            output.data() + pos,
            right.data() + pos,
            left.data() + pos,
            exceptions.data() + pos,
            positions.data() + pos,
            excp_count.data() + pos,
            states[i]);
    }
    for(auto i = 0UL; i < data.size(); ++i) {
        if(data[i] != output[i])
            ARCTICDB_DEBUG(log::codec(), "pants");
    }
    ASSERT_EQ(data, output);
}

TEST(ALP, DetermineSchemeALP) {
    auto data = random_decimal_floats(0, 1000, 2, 100 * 1024);
    alp::state<double> state;
    std::array<double, 1024> sample_buf;

    alp::encoder<double>::init(data.data(), 0, 1024, sample_buf.data(), state);
    ARCTICDB_DEBUG(log::codec(), "state: {}", state.exceptions_count);
    ASSERT_NE(state.scheme, alp::Scheme::INVALID);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP);
}

TEST(ALP, RoundtripALP) {
    auto data = random_decimal_floats(0, 1000, 2, 1024);
    alp::state<double> state;
    std::array<double, 1024> sample_buf;

    alp::encoder<double>::init(data.data(), 0, 1024, sample_buf.data(), state);
    ARCTICDB_DEBUG(log::codec(), "state: {}", state.exceptions_count);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP);

    std::array<double, BLOCK_SIZE> exceptions;
    std::array<uint16_t, BLOCK_SIZE> exception_positions;
    uint16_t exception_count = 0;
    std::array<int64_t, BLOCK_SIZE> encoded;

    alp::encoder<double>::init(data.data(), 0, 1024, sample_buf.data(), state);
    alp::encoder<double>::encode(data.data(), exceptions.data(), exception_positions.data(), &exception_count, encoded.data(), state);

    std::vector<double> output(1024);
    alp::decoder<double>::decode(encoded.data(), state.fac, state.exp, output.data());
    alp::decoder<double>::patch_exceptions(output.data(), exceptions.data(), exception_positions.data(), &exception_count);
    ASSERT_EQ(data, output);
}

TEST(ALP, SingleFullBlockDecimalRoundtrip) {
    ScopedDoubleConfig config{"Alp.MaxCompressRatio", 100.0};
    using T = double;
    constexpr size_t numRows = alp::config::VECTOR_SIZE; // exactly one block

    std::vector<T> data(numRows, 123.456);
    auto wrapper = from_vector(data, make_scalar_type(DataType::FLOAT64));

    alp::state<T> state{};
    std::array<T, alp::config::VECTOR_SIZE> sample_buf{};
    alp::encoder<T>::init(data.data(), 0, numRows, sample_buf.data(), state);
    // For this test we expect decimal mode (ALP).
    ASSERT_EQ(state.scheme, alp::Scheme::ALP);

    ALPCompressData<T> compress_data{std::move(state)};
    arcticdb::ALPCompressor<T> compressor(std::move(compress_data));

    size_t expected_bytes = sizeof(arcticdb::ALPHeader<T>) +
        sizeof(ALPDecimalColumnHeader<T>) +
        worst_case_required_alp_size<T>();
    size_t num_elements = expected_bytes / sizeof(T);
    std::vector<T> compressed(num_elements + 1, T{0});
    compressed[num_elements] = 0xDEADBEEF; // red-zone marker

    size_t compressed_bytes = compressor.compress(wrapper.data_, compressed.data(), expected_bytes);
    EXPECT_LE(compressed_bytes, expected_bytes);
    EXPECT_EQ(compressed[num_elements], 0xDEADBEEF);

    arcticdb::ALPDecompressor<T> decompressor;
    decompressor.init(compressed.data());
    std::vector<T> output(numRows, T{0});
    decompressor.decompress(compressed.data(), output.data());

    EXPECT_EQ(data, output);
}

TEST(ALP, RemainderOnlyDecimalRoundtrip) {
    using T = double;
    constexpr size_t numRows = alp::config::VECTOR_SIZE / 2;

    std::vector<T> data(numRows, 987.654);
    auto wrapper = from_vector(data, make_scalar_type(DataType::FLOAT64));

    alp::state<T> state{};
    std::array<T, alp::config::VECTOR_SIZE> sample_buf{};
    alp::encoder<T>::init(data.data(), 0, numRows, sample_buf.data(), state);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP);

    ALPCompressData<T> compress_data{std::move(state)};
    arcticdb::ALPCompressor<T> compressor(std::move(compress_data));

    size_t expected_bytes = sizeof(arcticdb::ALPHeader<T>) +
        sizeof(ALPDecimalColumnHeader<T>) +
        worst_case_required_alp_size<T>();
    std::vector<uint8_t> compressed(expected_bytes + 1, T{0});

    (void)compressor.compress(wrapper.data_, reinterpret_cast<T*>(compressed.data()), expected_bytes);
    arcticdb::ALPDecompressor<T> decompressor;
    decompressor.init(reinterpret_cast<T*>(compressed.data()));
    std::vector<T> output(numRows + 1, T{0});
    output[numRows] = 0xDEADBEEF;
    decompressor.decompress(reinterpret_cast<T*>(compressed.data()), output.data());
    EXPECT_EQ(output[numRows], 0xDEADBEEF);
    output.resize(numRows);
    EXPECT_EQ(data, output);
}

TEST(ALP, EndToEndSingleFullBlockRD) {
    using T = double;
    constexpr size_t numRows = alp::config::VECTOR_SIZE; // exactly one block

    std::vector<T> data(numRows, 3.14159);
    auto wrapper = from_vector(data, make_scalar_type(DataType::FLOAT64));

    alp::state<T> state{};
    std::array<T, alp::config::VECTOR_SIZE> sample_buf{};
    alp::encoder<T>::init(data.data(), 0, numRows, sample_buf.data(), state);
    alp::rd_encoder<T>::init(data.data(), 0, numRows, sample_buf.data(), state);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP_RD);

    ALPCompressData<T> compress_data{std::move(state)};
    arcticdb::ALPCompressor<T> compressor(std::move(compress_data));

    size_t expected_bytes = sizeof(arcticdb::ALPHeader<T>) +
        RealDoubleColumnHeader<T>(compressor.data_.state_).total_size() +
        worst_case_required_rd_size<T>();

    size_t num_elements = expected_bytes / sizeof(T);
    std::vector<T> compressed(num_elements + 1, T{0});
    compressed[num_elements] = 0xDEADBEEF; // red-zone

    size_t compressed_bytes = compressor.compress(wrapper.data_, compressed.data(), expected_bytes);
    EXPECT_LE(compressed_bytes, expected_bytes);

    arcticdb::ALPDecompressor<T> decompressor;
    decompressor.init(compressed.data());
    std::vector<T> output(numRows, T{0});
    decompressor.decompress(compressed.data(), output.data());

    EXPECT_EQ(data, output);
}

TEST(ALP, EndToEndRemainderOnlyDecimal) {
    using T = double;
    constexpr size_t numRows = alp::config::VECTOR_SIZE / 2;  // remainder only

    // Create data.
    std::vector<T> data(numRows, 987.654);
    auto wrapper = from_vector(data, make_scalar_type(DataType::FLOAT64));

    alp::state<T> state{};
    std::array<T, alp::config::VECTOR_SIZE> sample_buf{};
    alp::encoder<T>::init(data.data(), 0, numRows, sample_buf.data(), state);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP);

    ALPCompressData<T> compress_data{std::move(state)};
    arcticdb::ALPCompressor<T> compressor(std::move(compress_data));

    size_t expected_bytes = sizeof(arcticdb::ALPHeader<T>) +
        sizeof(ALPDecimalColumnHeader<T>) +
        worst_case_required_alp_size<T>();
    size_t num_elements = expected_bytes / sizeof(T);
    std::vector<T> compressed(num_elements + 1, T{0});
    compressed[num_elements] = 0xDEADBEEF;

    size_t compressed_bytes = compressor.compress(wrapper.data_, compressed.data(), expected_bytes);
    EXPECT_LE(compressed_bytes, expected_bytes);

    arcticdb::ALPDecompressor<T> decompressor;
    decompressor.init(compressed.data());
    std::vector<T> output(numRows, T{0});
    decompressor.decompress(compressed.data(), output.data());

    EXPECT_EQ(data, output);
}

template<typename T>
T red_zone_value() {
    if constexpr (sizeof(T) == 1)
        return static_cast<T>(0xAD);
    else if constexpr (sizeof(T) == 2)
        return static_cast<T>(0xDEAD);
    else if constexpr (sizeof(T) == 4)
        return static_cast<T>(0xDEADBEEF);
    else
        return static_cast<T>(0xDEADBEEFDEADBEEFULL);
}

TEST(ALP, RoundtripRealDouble) {
    ScopedDoubleConfig config("Alp.MaxCompressRatio", 100.0);
    constexpr size_t NUM_VALUES = BLOCK_SIZE * 100;
    using T = double;
    std::vector<T> data = compressible_doubles(NUM_VALUES);
    auto wrapper = from_vector(data, make_scalar_type(DataType::FLOAT64));

    alp::state<T> state;
    std::array<T, alp::config::VECTOR_SIZE> sample_buf;
    alp::encoder<T>::init(data.data(), 0, NUM_VALUES, sample_buf.data(), state);
    alp::rd_encoder<T>::init(data.data(), 0, NUM_VALUES, sample_buf.data(), state);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP_RD);

    ALPCompressData<T> compress_data{std::move(state)};
    ALPCompressor<T> compressor(std::move(compress_data));

    size_t num_blocks = NUM_VALUES / alp::config::VECTOR_SIZE;
    auto col_header_sz = RealDoubleColumnHeader<T>(compressor.data_.state_).total_size();
    size_t worst_block_sz = worst_case_required_rd_size<T>();
    size_t expected_bytes = sizeof(arcticdb::ALPHeader<T>) + col_header_sz + num_blocks * worst_block_sz;

    size_t num_elements = expected_bytes / sizeof(T);
    std::vector<T> compressed(num_elements + 1, T{0});
    compressed[num_elements] = red_zone_value<T>();

    (void)compressor.compress(wrapper.data_, compressed.data(), expected_bytes);
    ASSERT_EQ(compressed[num_elements], red_zone_value<T>()) << "Red zone overwritten";
    ALPDecompressor<T> decompressor;
    decompressor.init(compressed.data());
    std::vector<T> output(NUM_VALUES, T{0});
    auto dr = decompressor.decompress(compressed.data(), output.data());
    ASSERT_EQ(dr.uncompressed_, NUM_VALUES * sizeof(T));
    ASSERT_EQ(output, data);
}

TEST(ALP, RoundtripALPDecimal) {
    ScopedDoubleConfig config("Alp.MaxCompressRatio", 100.0);
    constexpr size_t NUM_VALUES = BLOCK_SIZE * 100;
    using T = double;
    std::vector<T> data = random_decimal_floats(0, 1000, 2, NUM_VALUES);
    auto wrapper = from_vector(data, make_scalar_type(DataType::FLOAT64));

    alp::state<T> state;
    std::array<T, alp::config::VECTOR_SIZE> sample_buf;
    alp::encoder<T>::init(data.data(), 0, NUM_VALUES, sample_buf.data(), state);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP);

    ALPCompressData<T> compress_data{std::move(state)};
    ALPCompressor<T> compressor(std::move(compress_data));

    size_t num_blocks = NUM_VALUES / alp::config::VECTOR_SIZE;
    auto col_header_sz = sizeof(ALPDecimalColumnHeader<T>);  // or use a member function if available
    size_t worst_block_sz = worst_case_required_alp_size<T>();
    size_t expected_bytes = sizeof(arcticdb::ALPHeader<T>) + col_header_sz + num_blocks * worst_block_sz;

    size_t num_elements = expected_bytes / sizeof(T);
    std::vector<T> compressed(num_elements + 1, T{0});
    compressed[num_elements] = red_zone_value<T>();

    (void)compressor.compress(wrapper.data_, compressed.data(), expected_bytes);

    ASSERT_EQ(compressed[num_elements], red_zone_value<T>()) << "Red zone overwritten";

    ALPDecompressor<T> decompressor;
    decompressor.init(compressed.data());
    std::vector<T> output(NUM_VALUES, T{0});
    auto dr = decompressor.decompress(compressed.data(), output.data());
    (void) dr;
    ASSERT_EQ(output, data);
}

} //namespace arcticdb