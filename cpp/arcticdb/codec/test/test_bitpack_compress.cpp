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
    auto expected_bytes = BitPackCompressor<T>::compressed_size(bitpack_data, input.size());
    BitPackCompressor<T> bitpack(bitpack_data);
    size_t compressed_size = bitpack.compress(wrapper.data_, compressed.data(), expected_bytes);
    EXPECT_EQ(expected_bytes, compressed_size);

    std::vector<T> output(input.size(), 0);
    auto result = BitPackDecompressor<T>::decompress(compressed.data(), output.data());

    EXPECT_EQ(output, input);
    EXPECT_EQ(result.uncompressed_, input.size() * sizeof(T));
}

TYPED_TEST(BitPackTest, MultiBlock) {
    using T = TypeParam;
    const size_t size = 3000;  // multiple blocks
    std::vector<T> input(size);
    for (size_t i = 0; i < size; ++i)
        input[i] = static_cast<T>((i * 37) % 128);

    auto wrapper = from_vector(input, type_desc_for_type<T>());

    std::vector<T> compressed(8192, 0);

    auto bitpack_data = BitPackCompressor<T>::compute_bitwidth(wrapper.data_);
    auto expected_size = BitPackCompressor<T>::compressed_size(bitpack_data, input.size());
    BitPackCompressor<T> bitpack(bitpack_data);
    size_t compressed_size = bitpack.compress(wrapper.data_, compressed.data(), expected_size);
    EXPECT_EQ(expected_size, compressed_size);

    std::vector<T> output(input.size(), 0);
    auto result = BitPackDecompressor<T>::decompress(compressed.data(), output.data());

    EXPECT_EQ(output, input);
    EXPECT_EQ(result.uncompressed_, input.size() * sizeof(T));
}

TYPED_TEST(BitPackTest, AllIdentical) {
    using T = TypeParam;
    const size_t size = 1500;
    std::vector<T> input(size, static_cast<T>(42));
    auto wrapper = from_vector(input, type_desc_for_type<T>());

    std::vector<T> compressed(8192, 0);

    auto bitpack_data = BitPackCompressor<T>::compute_bitwidth(wrapper.data_);
    auto expected_bytes = BitPackCompressor<T>::compressed_size(bitpack_data, input.size());
    BitPackCompressor<T> bitpack(bitpack_data);
    size_t compressed_size = bitpack.compress(wrapper.data_, compressed.data(), expected_bytes);
    EXPECT_EQ(expected_bytes, compressed_size);

    std::vector<T> output(input.size(), 0);
    auto result = BitPackDecompressor<T>::decompress(compressed.data(), output.data());

    EXPECT_EQ(output, input);
    EXPECT_EQ(result.uncompressed_, input.size() * sizeof(T));
}

class BitPackCodecTest : public ::testing::Test {
protected:
    static constexpr size_t values_per_block = 1024;

    template<typename T>
    void verify_roundtrip(const std::vector<T>& original, TypeDescriptor type) {
        ASSERT_FALSE(original.empty());
        auto wrapper = from_vector(original, type);
        auto bp_data = BitPackCompressor<T>::compute_bitwidth(wrapper.data_);
        size_t expected_size_bytes = BitPackCompressor<T>::compressed_size(bp_data, original.size());
        size_t expected_size_elems = expected_size_bytes / sizeof(T);
        std::vector<T> compressed(expected_size_elems + 1, 0);
        T guard = static_cast<T>(0xDEADBEEF);
        compressed[expected_size_elems] = guard;
        std::vector<T> decompressed(original.size(), 0);
        BitPackCompressor<T> compressor{bp_data};
        size_t comp_bytes = compressor.compress(wrapper.data_, compressed.data(), expected_size_bytes);
        ASSERT_EQ(comp_bytes, expected_size_bytes);
        EXPECT_EQ(compressed[expected_size_elems], guard);
        const auto* header = reinterpret_cast<const BitPackHeader<T>*>(compressed.data());
        EXPECT_EQ(header->num_rows, original.size());
        EXPECT_EQ(header->bits_needed, bp_data.bits_needed_);
        auto result = BitPackDecompressor<T>::decompress(compressed.data(), decompressed.data());
        EXPECT_EQ(result.uncompressed_, original.size() * sizeof(T));
        for (size_t i = 0; i < original.size(); ++i) {
            EXPECT_EQ(original[i], decompressed[i]) << "Mismatch at index " << i;
        }
    }
};

TEST_F(BitPackCodecTest, SingleBlock) {
    std::vector<uint32_t> data(values_per_block);
    std::iota(data.begin(), data.end(), 1000);
    verify_roundtrip(data, make_scalar_type(DataType::UINT32));
}

TEST_F(BitPackCodecTest, MultipleCompleteBlocks) {
    std::vector<uint32_t> data(values_per_block * 4);
    std::iota(data.begin(), data.end(), 1000);
    verify_roundtrip(data, make_scalar_type(DataType::UINT32));
}

TEST_F(BitPackCodecTest, BlocksWithRemainder) {
    std::vector<uint32_t> data(values_per_block * 3 + values_per_block / 2);
    std::iota(data.begin(), data.end(), 1000);
    verify_roundtrip(data, make_scalar_type(DataType::UINT32));
}

TEST_F(BitPackCodecTest, SmallRange) {
    std::vector<uint32_t> data(values_per_block * 2);
    std::mt19937 rng(42);
    std::uniform_int_distribution<uint32_t> dist(1000, 1015);
    for (auto& i : data)
        i = dist(rng);
    std::sort(data.begin(), data.end());
    verify_roundtrip(data, make_scalar_type(DataType::UINT32));
}

TEST_F(BitPackCodecTest, LargeRange) {
    std::vector<uint32_t> data(values_per_block * 2);
    std::mt19937 rng(42);
    std::uniform_int_distribution<uint32_t> dist(0, 1000000);
    for (auto& i : data)
        i = dist(rng);
    std::sort(data.begin(), data.end());
    verify_roundtrip(data, make_scalar_type(DataType::UINT32));
}

TEST_F(BitPackCodecTest, DifferentTypes) {
    {
        std::vector<uint32_t> data(values_per_block * 2);
        std::iota(data.begin(), data.end(), 1000U);
        verify_roundtrip(data, make_scalar_type(DataType::UINT32));
    }
    {
        std::vector<uint64_t> data(1024/64 * 2);
        std::iota(data.begin(), data.end(), 1000ULL);
        verify_roundtrip(data, make_scalar_type(DataType::UINT64));
    }
}

TYPED_TEST(BitPackTest, ZeroBitWidth) {
    using T = TypeParam;
    const size_t size = 10000; // 10,000 rows
    std::vector<T> input(size, static_cast<T>(0));

    auto wrapper = from_vector(input, type_desc_for_type<T>());
    std::vector<T> compressed(8192, 0);

    auto bitpack_data = BitPackCompressor<T>::compute_bitwidth(wrapper.data_);
    EXPECT_EQ(bitpack_data.bits_needed_, 0);

    auto expected_bytes = BitPackCompressor<T>::compressed_size(bitpack_data, input.size());
    BitPackCompressor<T> bitpack(bitpack_data);
    size_t compressed_size = bitpack.compress(wrapper.data_, compressed.data(), expected_bytes);
    EXPECT_EQ(expected_bytes, compressed_size);

    std::vector<T> output(input.size(), 0);
    auto result = BitPackDecompressor<T>::decompress(compressed.data(), output.data());

    EXPECT_EQ(output, input);
    EXPECT_EQ(result.uncompressed_, input.size() * sizeof(T));
    EXPECT_EQ(result.compressed_, compressed_size);
}

TEST_F(BitPackCodecTest, EdgeCases) {
    {
        std::vector<uint32_t> data(values_per_block);
        std::iota(data.begin(), data.end(), 0);
        verify_roundtrip(data, make_scalar_type(DataType::UINT32));
    }
    {
        std::vector<uint32_t> data(values_per_block + 1);
        std::iota(data.begin(), data.end(), 0);
        verify_roundtrip(data, make_scalar_type(DataType::UINT32));
    }
    {
        std::vector<uint32_t> data(values_per_block * 2, 42);
        verify_roundtrip(data, make_scalar_type(DataType::UINT32));
    }
    {
        std::vector<uint32_t> data(values_per_block * 2, 42);
        data.back() = 43;
        verify_roundtrip(data, make_scalar_type(DataType::UINT32));
    }
}
} // namespace arcticdb