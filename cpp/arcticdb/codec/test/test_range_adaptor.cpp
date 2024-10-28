#include <gtest/gtest.h>
#include <vector>
#include <array>

#include <arcticdb/column_store/column.hpp>
#include <arcticdb/codec/compression/contiguous_range_adaptor.hpp>

namespace arcticdb {

static TypeDescriptor int_type_descriptor() {
    return make_scalar_type(DataType::INT64);
}

TEST(ContiguousRangeForwardAdaptorTest, DirectSegment) {
    TypeDescriptor int_type = int_type_descriptor();
    Column col(int_type, 500, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    const int64_t numValues = 500;
    for (int64_t i = 0; i < numValues; ++i) {
        col.push_back(i);
    }
    ColumnData data = col.data();

    ContiguousRangeForwardAdaptor<int64_t, 100> adaptor(data);
    EXPECT_TRUE(adaptor.valid());

    const int64_t *seg1 = adaptor.next();
    EXPECT_NE(seg1, adaptor.buffer_.data());
    for (int64_t i = 0; i < 100; ++i) {
        EXPECT_EQ(seg1[i], i);
    }
}

TEST(ContiguousRangeForwardAdaptorTest, CrossBlockSegment) {
    TypeDescriptor int_type = int_type_descriptor();
    Column col(int_type, 2000, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    const size_t total_values = 2000;
    for (int64_t i = 0; i < int64_t(total_values); ++i) {
        col.push_back(i);
    }
    ColumnData data = col.data();
    ContiguousRangeForwardAdaptor<int64_t, 800> adaptor(data);
    EXPECT_TRUE(adaptor.valid());

    const int64_t *seg1 = adaptor.next();
    EXPECT_NE(seg1, adaptor.buffer_.data());
    for (int64_t i = 0; i < 800; ++i) {
        EXPECT_EQ(seg1[i], i);
    }

    const int64_t *seg2 = adaptor.next();
    for (int64_t i = 0; i < 800; ++i) {
        EXPECT_EQ(seg2[i], i + 800);
    }
}

TEST(ContiguousRangeForwardAdaptorTest, TraverseBlockBoundary) {
    const size_t num_blocks = 3;
    const size_t total_size = BufferSize * num_blocks;
    const size_t total_values = total_size / sizeof(uint64_t);
    auto buffer = ChunkedBuffer::presized_in_blocks(total_size);

    for (size_t i = 0; i < total_values; ++i) {
        reinterpret_cast<uint64_t&>(buffer[i * sizeof(uint64_t)]) = static_cast<uint64_t>(i);
    }

    auto column = ColumnData{&buffer, make_scalar_type(DataType::UINT64)};
    const size_t values_per_block = BufferSize / sizeof(uint64_t);
    const size_t chunk_size = values_per_block + (values_per_block / 2);
    ContiguousRangeForwardAdaptor<uint64_t, chunk_size> adaptor(std::move(column));

    size_t global_index = 0;
    while (global_index < total_values) {
        const uint64_t* chunk = adaptor.next();
        ASSERT_NE(chunk, nullptr);
        for (size_t j = 0; j < chunk_size && global_index < total_values; ++j, ++global_index) {
            EXPECT_EQ(chunk[j], static_cast<uint64_t>(global_index));
        }
    }
}


TEST(ContiguousRangeForwardAdaptorTest, TraverseWithinBlockBoundary) {
    const size_t num_blocks = 3;
    const size_t total_size = BufferSize * num_blocks;
    const size_t total_values = total_size / sizeof(uint64_t);
    auto buffer = ChunkedBuffer::presized_in_blocks(total_size);

    for (size_t i = 0; i < total_values; ++i) {
        reinterpret_cast<uint64_t&>(buffer[i * sizeof(uint64_t)]) = static_cast<uint64_t>(i);
    }

    auto column = ColumnData{&buffer, make_scalar_type(DataType::UINT64)};
    const size_t values_per_block = BufferSize / sizeof(uint64_t);
    const size_t chunk_size = values_per_block * 0.7;
    ContiguousRangeForwardAdaptor<uint64_t, chunk_size> adaptor(std::move(column));
    size_t values_to_check = chunk_size * (total_values / chunk_size);
    size_t global_index = 0;
    while (global_index < values_to_check) {
        const uint64_t* chunk = adaptor.next();
        ASSERT_NE(chunk, nullptr);
        for (size_t j = 0; j < chunk_size && global_index < total_values; ++j, ++global_index) {
            EXPECT_EQ(chunk[j], static_cast<uint64_t>(global_index));
        }
    }
}
TEST(ContiguousRangeRandomAccessAdaptorTest, DirectAccessWithinSingleBlock) {
    Column col(int_type_descriptor(), 500, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    const int64_t numValues = 500;
    for (int64_t i = 0; i < numValues; ++i) {
        col.push_back(i);
    }
    ColumnData data = col.data();

    ContiguousRangeRandomAccessAdaptor<int64_t, 100> adaptor(data);

    const int64_t* seg = adaptor.at(50);  // should deliver data[50] ... data[149]
    for (int64_t i = 0; i < 100; ++i) {
        EXPECT_EQ(seg[i], i + 50);
    }
}

TEST(ContiguousRangeRandomAccessAdaptorTest, CopyAccessSpanningMultipleBlocks) {
    Column col(int_type_descriptor(), 1500, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    const int64_t numValues = 1500;
    for (int64_t i = 0; i < numValues; ++i) {
        col.push_back(i);
    }
    ColumnData data = col.data();

    ContiguousRangeRandomAccessAdaptor<int64_t, 200> adaptor(data);

    const int64_t* seg = adaptor.at(900);

    for (int64_t i = 0; i < 200; ++i) {
        EXPECT_EQ(seg[i], i + 900);
    }
}

TEST(ContiguousRangeRandomAccessAdaptorTest, BoundaryConditions) {
    Column col(int_type_descriptor(), 600, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    const int64_t numValues = 600;
    for (int64_t i = 0; i < numValues; ++i) {
        col.push_back(i);
    }
    ColumnData data = col.data();

    ContiguousRangeRandomAccessAdaptor<int64_t, 50> adaptor(data);

    const int64_t* seg0 = adaptor.at(0);
    for (int64_t i = 0; i < 50; ++i)
        EXPECT_EQ(seg0[i], i);

    const int64_t* seg1 = adaptor.at(550); // should contain ints 550 to 599
    for (int64_t i = 0; i < 50; ++i)
        EXPECT_EQ(seg1[i], i + 550);
}
} // namespace arcticdb