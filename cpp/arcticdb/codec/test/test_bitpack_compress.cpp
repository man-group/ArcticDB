#include <gtest/gtest.h>

#include <arcticdb/codec/compression/bitpack.hpp>
#include <arcticdb/entity/types.hpp>

namespace arcticdb {

struct ColumnDataWrapper {
    ColumnDataWrapper(ChunkedBuffer&& buffer, TypeDescriptor type, size_t row_count) :
        buffer_(std::move(buffer)),
        data_(&buffer_, nullptr, type, nullptr, nullptr, row_count) {
    }

    ChunkedBuffer buffer_;
    ColumnData data_;
};

template <typename T>
ColumnDataWrapper from_vector(const std::vector<T>& data, TypeDescriptor type) {
    ChunkedBuffer buffer;
    buffer.add_external_block(reinterpret_cast<const uint8_t*>(data.data()), data.size() * sizeof(T), 0);
    return {std::move(buffer), type, data.size()};
}

template <typename T>
TypeDescriptor get_type_descriptor();

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

    auto wrapper = from_vector(input, get_type_descriptor<T>());

    std::vector<T> compBuffer(2048, 0);
    size_t compSize = BitPackCompressor<T>::compress(wrapper.data_, compBuffer.data(), input.size());

    std::vector<T> output(input.size(), 0);
    size_t decompressed = BitPackDecompressor<T>::decompress(compBuffer.data(), output.data());

    EXPECT_EQ(output, input);
    EXPECT_EQ(decompressed, input.size());
}

TYPED_TEST(BitPackTest, MultiBlock) {
    using T = TypeParam;
    const size_t size = 3000;  // multiple blocks
    std::vector<T> input(size);
    for (size_t i = 0; i < size; ++i)
        input[i] = static_cast<T>((i * 37) % 256);

    auto wrapper = from_vector(input, get_type_descriptor<T>());

    std::vector<T> compBuffer(8192, 0);
    size_t compSize = BitPackCompressor<T>::compress(wrapper.data_, compBuffer.data(), input.size());

    std::vector<T> output(input.size(), 0);
    size_t decompressed = BitPackDecompressor<T>::decompress(compBuffer.data(), output.data());

    EXPECT_EQ(output, input);
    EXPECT_EQ(decompressed, input.size());
}

TYPED_TEST(BitPackTest, AllIdentical) {
    using T = TypeParam;
    const size_t size = 1500;
    std::vector<T> input(size, static_cast<T>(42));
    auto wrapper = from_vector(input, get_type_descriptor<T>());

    std::vector<T> compBuffer(8192, 0);
    size_t compSize = BitPackCompressor<T>::compress(wrapper.data_, compBuffer.data(), input.size());

    std::vector<T> output(input.size(), 0);
    size_t decompressed = BitPackDecompressor<T>::decompress(compBuffer.data(), output.data());

    EXPECT_EQ(output, input);
    EXPECT_EQ(decompressed, input.size());
}

TYPED_TEST(BitPackTest, Extremes) {
    using T = TypeParam;
    const size_t size = 1024; // exactly one full block
    std::vector<T> input(size);
    for (size_t i = 0; i < size; ++i)
        input[i] = (i % 2 == 0) ? 0 : std::numeric_limits<T>::max();

    auto wrapper = from_vector(input, get_type_descriptor<T>());

    std::vector<T> compBuffer(8192, 0);
    size_t compSize = BitPackCompressor<T>::compress(wrapper.data_, compBuffer.data(), input.size());

    std::vector<T> output(input.size(), 0);
    size_t decompressed = BitPackDecompressor<T>::decompress(compBuffer.data(), output.data());

    EXPECT_EQ(output, input);
    EXPECT_EQ(decompressed, input.size());
}

} // namespace arcticdb