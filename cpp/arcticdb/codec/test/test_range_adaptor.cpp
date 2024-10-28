#include <gtest/gtest.h>
#include <vector>
#include <array>
#include <cstring>
#include <algorithm>

// Include your Column and ContiguousRangeForwardAdaptor definitions.
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/codec/compression/contiguous_range_adaptor.hpp>

namespace arcticdb {

// For these tests we assume that the Column’s int‐type can be constructed
// via a TypeDescriptor that accepts a string (e.g. "int"). Adjust as needed.
static TypeDescriptor createIntTypeDescriptor() {
    return make_scalar_type(DataType::INT64);
}

// ---------------------------------------------------------------------
// Test 1. Direct (contiguous) segment from a Column that fits in one block.
// We create a Column with fewer than one block’s worth of ints (~500 ints)
// so that each segment (here, 100 ints per segment) is returned directly
// from the underlying block (i.e. not copied into the adaptor’s internal buffer).
// ---------------------------------------------------------------------
TEST(ContiguousRangeForwardAdaptorTest, DirectSegment) {
    // Create a Column that will use only one block.
    TypeDescriptor intType = createIntTypeDescriptor();
    // Using expected_rows = 500 ensures that only one block is allocated.
    Column col(intType, 500, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    const int numValues = 500;
    for (int i = 0; i < numValues; ++i) {
        col.push_back(i);
    }
    ColumnData data = col.data();

    // Use the adaptor with a fixed segment size of 100 ints.
    // (Since 100*4 = 400 bytes is well below the block capacity,
    // the adaptor should return a direct pointer into the current block.)
    ContiguousRangeForwardAdaptor<int, 100> adaptor(data);
    EXPECT_TRUE(adaptor.valid());

    // First segment: expect values 0..99.
    const int *seg1 = adaptor.next();
    // In the direct branch the returned pointer should not equal the internal buffer.
    EXPECT_NE(seg1, adaptor.buffer_.data());
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(seg1[i], i);
    }

    // Second segment: expect values 100..199.
    const int *seg2 = adaptor.next();
    EXPECT_NE(seg2, adaptor.buffer_.data());
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(seg2[i], i + 100);
    }

    // Third segment: expect values 200..299.
    const int *seg3 = adaptor.next();
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(seg3[i], i + 200);
    }
}

// ---------------------------------------------------------------------
// Test 2. A segment that spans a block boundary.
// We create a Column with 2000 ints so that at least two blocks are used
// (given ~992 ints per block). We choose a segment size of 800 ints.
// The first call to next() (from block0) will return a direct pointer
// (since 800 ints×4 bytes = 3200 bytes fits in block0’s ~3968 bytes).
// Then a second call is made when the current block has only 992 – 800 = 192 ints left,
// so the adaptor must copy from the remainder of block0 and then from the first part of block1.
// ---------------------------------------------------------------------
TEST(ContiguousRangeForwardAdaptorTest, CrossBlockSegment) {
    TypeDescriptor intType = createIntTypeDescriptor();
    // Expect 2000 ints to force multiple blocks.
    Column col(intType, 2000, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    const int totalValues = 2000;
    for (int i = 0; i < totalValues; ++i) {
        col.push_back(i);
    }
    ColumnData data = col.data();

    // Create the adaptor with segment size = 800 ints.
    ContiguousRangeForwardAdaptor<int, 800> adaptor(data);
    EXPECT_TRUE(adaptor.valid());

    // First call: should return a direct pointer from block0.
    const int *seg1 = adaptor.next();
    EXPECT_NE(seg1, adaptor.buffer_.data());
    for (int i = 0; i < 800; ++i) {
        EXPECT_EQ(seg1[i], i);
    }

    // At this point, block0’s consumed byte offset is:
    //    block_pos_ = 800 * sizeof(int) (i.e. 3200 bytes),
    // and block0 (capacity ≈ 3968 bytes) has only 3968 - 3200 = 768 bytes remaining.
    // A subsequent call requesting 800 ints (3200 bytes) cannot be served contiguously,
    // so the adaptor must copy from block0 and then block1.
    const int *seg2 = adaptor.next();
    // In the copy branch we expect the returned pointer equals adaptor.buffer_.data().
    EXPECT_EQ(seg2, adaptor.buffer_.data());
    // The segment should contain values 800 .. 1599.
    for (int i = 0; i < 800; ++i) {
        EXPECT_EQ(seg2[i], i + 800);
    }
}

// ---------------------------------------------------------------------
// Test 3. Repeatedly call next() until no more data remains
// and verify that the concatenation of the returned segments equals
// the entire sequence of values in the Column.
// We create a Column with 2500 ints so that multiple blocks are used.
// For simplicity we choose a segment size (500 ints) that evenly divides the total.
// ---------------------------------------------------------------------
TEST(ContiguousRangeForwardAdaptorTest, ExhaustsDataCorrectly) {
    TypeDescriptor intType = createIntTypeDescriptor();
    const int totalValues = 2500;
    // Use expected_rows = 2500 to force multiple blocks.
    Column col(intType, totalValues, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    for (int i = 0; i < totalValues; ++i) {
        col.push_back(i);
    }
    ColumnData data = col.data();

    // Use a segment size of 500 ints.
    ContiguousRangeForwardAdaptor<int, 500> adaptor(data);

    std::vector<int> collected;
    // Since our adaptor returns fixed-size segments (500 ints)
    // we repeatedly call next() until adaptor.valid() returns false.
    // (Note: depending on the design, the final segment in a non–multiple case
    // may include extra data; here totalValues (2500) is a multiple of 500.)
    while (adaptor.valid()) {
        const int *seg = adaptor.next();
        for (int i = 0; i < 500; ++i) {
            // Only add elements if we have not yet collected all values.
            if (static_cast<size_t>(col.row_count()) > collected.size())
                collected.push_back(seg[i]);
        }
    }

    // Verify that we have collected exactly totalValues elements, in order.
    ASSERT_EQ(collected.size(), static_cast<size_t>(totalValues));
    for (int i = 0; i < totalValues; ++i) {
        EXPECT_EQ(collected[i], i);
    }
}
// ---------------------------------------------------------------------
// Test 1: Direct access within a single block.
// Create a column with fewer than one block’s worth of ints (e.g. 500 ints)
// so that the data lies contiguously in one block. Request a segment that
// is completely contained within that block.
// ---------------------------------------------------------------------
TEST(ContiguousRangeRandomAccessAdaptorTest, DirectAccessWithinSingleBlock) {
    // Create a column with 500 ints (which should reside in a single block).
    Column col(createIntTypeDescriptor(), 500, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    const int numValues = 500;
    for (int i = 0; i < numValues; ++i) {
        col.push_back(i);
    }
    ColumnData data = col.data();

    // Create the adaptor with a segment size of 100 ints.
    ContiguousRangeRandomAccessAdaptor<int, 100> adaptor(data);

    // Request a segment starting at row 50.
    const int* seg = adaptor.at(50);  // should deliver data[50] ... data[149]
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(seg[i], i + 50);
    }
}

// ---------------------------------------------------------------------
// Test 2: Copy access spanning multiple blocks.
// Create a column that spans multiple blocks (e.g. 1500 ints).
// Then request a segment that begins near the end of one block so that the
// requested range spans into the next block. The adaptor should copy the data
// into its internal buffer and return that pointer.
// ---------------------------------------------------------------------
TEST(ContiguousRangeRandomAccessAdaptorTest, CopyAccessSpanningMultipleBlocks) {
    // Given the typical block capacity (nearly 992 ints per block for int),
    // a column with 1500 ints will span at least 2 blocks.
    Column col(createIntTypeDescriptor(), 1500, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    const int numValues = 1500;
    for (int i = 0; i < numValues; ++i) {
        col.push_back(i);
    }
    ColumnData data = col.data();

    // Create the adaptor with a segment size of 200 ints.
    ContiguousRangeRandomAccessAdaptor<int, 200> adaptor(data);

    // Request a segment starting at row 900.
    // Since 900+200 = 1100, and 1100 is greater than 992 (approximate number
    // of ints that can fit in the first block), the range will span into the second block.
    const int* seg = adaptor.at(900);

    // Verify that the returned segment contains ints 900 through 1099.
    for (int i = 0; i < 200; ++i) {
        EXPECT_EQ(seg[i], i + 900);
    }
}

// ---------------------------------------------------------------------
// Test 3: Boundary condition—access at beginning and near the end when
// the segment is still contained within a block.
// ---------------------------------------------------------------------
TEST(ContiguousRangeRandomAccessAdaptorTest, BoundaryConditions) {
    // Create a column with 600 ints (fits in one block).
    Column col(createIntTypeDescriptor(), 600, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    const int numValues = 600;
    for (int i = 0; i < numValues; ++i) {
        col.push_back(i);
    }
    ColumnData data = col.data();

    // Use segment size of 50 ints.
    ContiguousRangeRandomAccessAdaptor<int, 50> adaptor(data);

    // Access a segment at the very beginning.
    const int* seg0 = adaptor.at(0);
    for (int i = 0; i < 50; ++i)
        EXPECT_EQ(seg0[i], i);

    // Access a segment near the end.
    const int* seg1 = adaptor.at(550); // should contain ints 550 to 599
    for (int i = 0; i < 50; ++i)
        EXPECT_EQ(seg1[i], i + 550);
}
} // namespace arcticdb