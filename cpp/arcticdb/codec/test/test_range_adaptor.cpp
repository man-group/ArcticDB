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

TEST(ContiguousRangeForwardAdaptorTest, ExhaustsDataCorrectly) {
    TypeDescriptor int_type = int_type_descriptor();
    const int64_t total_values = 2500;
    // Use expected_rows = 2500 to force multiple blocks.
    Column col(int_type, total_values, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    for (int64_t i = 0; i < total_values; ++i) {
        col.push_back(i);
    }
    ColumnData data = col.data();

    ContiguousRangeForwardAdaptor<int64_t, 500> adaptor(data);

    std::vector<int64_t> collected;
    while (adaptor.valid()) {
        const int64_t *seg = adaptor.next();
        for (int64_t i = 0; i < 500; ++i) {
            if (static_cast<size_t>(col.row_count()) > collected.size())
                collected.push_back(seg[i]);
        }
    }

    ASSERT_EQ(collected.size(), static_cast<size_t>(total_values));
    for (int64_t i = 0; i < total_values; ++i) {
        EXPECT_EQ(collected[i], i);
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