#include <gtest/gtest.h>

#include <arcticdb/codec/compression/bitpack.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/codec/test/encoding_test_common.hpp>

namespace arcticdb {

template <typename T>
class BitPackTest : public ::testing::Test {};

using UnsignedTypes = ::testing::Types<uint8_t, uint16_t, uint32_t, uint64_t>;
TYPED_TEST_SUITE(BitPackTest, UnsignedTypes);

TYPED_TEST(BitPackTest, SingleBlock) {
    using T = TypeParam;
    const size_t size = 100; // smaller than one block (1024)
    std::vector<T> input(size);
    for (size_t i = 0; i < size; ++i)
        input[i] = static_cast<T>(i % 128);

    auto wrapper = from_vector(input, type_desc_for_type<T>());

    std::vector<T> compressed(2048, 0);
    auto bitpack_data = BitPackCompressor<T>::compute_bitwidth(wrapper.data_);
    auto expected_size = BitPackCompressor<T>::compressed_size(input.size(), bitpack_data);
    BitPackCompressor<T> bitpack(bitpack_data);
    size_t compressed_size = bitpack.compress(wrapper.data_, compressed.data(), input.size());
    EXPECT_EQ(expected_size, compressed_size);

    std::vector<T> output(input.size(), 0);
    size_t decompressed = BitPackDecompressor<T>::decompress(compressed.data(), output.data());

    EXPECT_EQ(output, input);
    EXPECT_EQ(decompressed, input.size());
}

TYPED_TEST(BitPackTest, MultiBlock) {
    using T = TypeParam;
    const size_t size = 3000;  // multiple blocks
    std::vector<T> input(size);
    for (size_t i = 0; i < size; ++i)
        input[i] = static_cast<T>((i * 37) % 256);

    auto wrapper = from_vector(input, type_desc_for_type<T>());

    std::vector<T> compressed(8192, 0);

    auto bitpack_data = BitPackCompressor<T>::compute_bitwidth(wrapper.data_);
    auto expected_size = BitPackCompressor<T>::compressed_size(input.size(), bitpack_data);
    BitPackCompressor<T> bitpack(bitpack_data);
    size_t compressed_size = bitpack.compress(wrapper.data_, compressed.data(), input.size());
    EXPECT_EQ(expected_size, compressed_size);


    std::vector<T> output(input.size(), 0);
    size_t decompressed = BitPackDecompressor<T>::decompress(compressed.data(), output.data());

    EXPECT_EQ(output, input);
    EXPECT_EQ(decompressed, input.size());
}

TYPED_TEST(BitPackTest, AllIdentical) {
    using T = TypeParam;
    const size_t size = 1500;
    std::vector<T> input(size, static_cast<T>(42));
    auto wrapper = from_vector(input, type_desc_for_type<T>());

    std::vector<T> compressed(8192, 0);

    auto bitpack_data = BitPackCompressor<T>::compute_bitwidth(wrapper.data_);
    auto expected_size = BitPackCompressor<T>::compressed_size(input.size(), bitpack_data);
    BitPackCompressor<T> bitpack(bitpack_data);
    size_t compressed_size = bitpack.compress(wrapper.data_, compressed.data(), input.size());
    EXPECT_EQ(expected_size, compressed_size);
;

    std::vector<T> output(input.size(), 0);
    size_t decompressed = BitPackDecompressor<T>::decompress(compressed.data(), output.data());

    EXPECT_EQ(output, input);
    EXPECT_EQ(decompressed, input.size());
}

TYPED_TEST(BitPackTest, Extremes) {
    using T = TypeParam;
    const size_t size = 1024; // exactly one full block
    std::vector<T> input(size);
    for (size_t i = 0; i < size; ++i)
        input[i] = (i % 2 == 0) ? 0 : std::numeric_limits<T>::max();

    auto wrapper = from_vector(input, type_desc_for_type<T>());

    std::vector<T> compressed(8192, 0);

    auto bitpack_data = BitPackCompressor<T>::compute_bitwidth(wrapper.data_);
    auto expected_size = BitPackCompressor<T>::compressed_size(input.size(), bitpack_data);
    BitPackCompressor<T> bitpack(bitpack_data);
    size_t compressed_size = bitpack.compress(wrapper.data_, compressed.data(), input.size());
    EXPECT_EQ(expected_size, compressed_size);


    std::vector<T> output(input.size(), 0);
    size_t decompressed = BitPackDecompressor<T>::decompress(compressed.data(), output.data());

    EXPECT_EQ(output, input);
    EXPECT_EQ(decompressed, input.size());
}

} // namespace arcticdb