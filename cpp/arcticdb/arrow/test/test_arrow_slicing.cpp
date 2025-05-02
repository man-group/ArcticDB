#include <gtest/gtest.h>
#include <vector>
#include <cstddef>
#include "slice_record_batches.hpp" // This should declare SliceSegment and slice_record_batches

// Helper: comparison for SliceSegment.
bool operator==(const SliceSegment& a, const SliceSegment& b) {
    return a.record_batch_index == b.record_batch_index &&
        a.start_row == b.start_row &&
        a.num_rows == b.num_rows;
}

namespace {
// Convenience function to compare two vectors of SliceSegment.
void ExpectSliceSegmentsEqual(
    const std::vector<SliceSegment>& expected,
    const std::vector<SliceSegment>& actual) {
    ASSERT_EQ(expected.size(), actual.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i], actual[i]) << "Mismatch at index " << i;
    }
}
} // anonymous namespace

// Test case 1: Empty Input – no record batches.
TEST(SliceRecordBatchesTest, EmptyInput) {
    std::vector<size_t> row_counts = {};
    size_t desired = 1000;
    size_t tolerance = 100;

    auto result = slice_record_batches(row_counts, desired, tolerance);
    EXPECT_TRUE(result.empty());
}

// Test case 2: Single record batch smaller than desired row count.
TEST(SliceRecordBatchesTest, SingleBatchSmallerThanDesired) {
    std::vector<size_t> row_counts = {500};
    size_t desired = 1000;
    size_t tolerance = 100;

    auto result = slice_record_batches(row_counts, desired, tolerance);
    ASSERT_EQ(result.size(), 1); // a single slice
    std::vector<SliceSegment> expected = { {0, 0, 500} };
    ExpectSliceSegmentsEqual(expected, result[0]);
}

// Test case 3: Single record batch larger than desired row count.
TEST(SliceRecordBatchesTest, SingleBatchLargerThanDesired) {
    std::vector<size_t> row_counts = {1500};
    size_t desired = 1000;
    size_t tolerance = 100;

    auto result = slice_record_batches(row_counts, desired, tolerance);
    ASSERT_EQ(result.size(), 2);
    // Expect first slice: first 1000 rows, second slice: remaining 500 rows.
    std::vector<SliceSegment> expected0 = { {0, 0, 1000} };
    std::vector<SliceSegment> expected1 = { {0, 1000, 500} };
    ExpectSliceSegmentsEqual(expected0, result[0]);
    ExpectSliceSegmentsEqual(expected1, result[1]);
}

// Test case 4: Multiple record batches that need to be combined.
TEST(SliceRecordBatchesTest, CombineSmallBatches) {
    // row_counts: first batch 300, second batch 400, third batch 500.
    std::vector<size_t> row_counts = {300, 400, 500};
    size_t desired = 1000;
    size_t tolerance = 100;

    auto result = slice_record_batches(row_counts, desired, tolerance);
    // Expected behavior:
    // -- First slice gets: batch0:all 300, batch1:all 400, batch2: first 300 → total 1000.
    // -- Second slice gets: remainder from batch2: 500-300=200.
    ASSERT_EQ(result.size(), 2);
    std::vector<SliceSegment> expected0 = { {0, 0, 300}, {1, 0, 400}, {2, 0, 300} };
    std::vector<SliceSegment> expected1 = { {2, 300, 200} };
    ExpectSliceSegmentsEqual(expected0, result[0]);
    ExpectSliceSegmentsEqual(expected1, result[1]);
}

// Test case 5: Multiple record batches with exact fit.
TEST(SliceRecordBatchesTest, ExactFitBatches) {
    std::vector<size_t> row_counts = {1000, 1000, 1000};
    size_t desired = 1000;
    size_t tolerance = 100;

    auto result = slice_record_batches(row_counts, desired, tolerance);
    // Each record batch should produce its own slice.
    ASSERT_EQ(result.size(), 3);
    std::vector<SliceSegment> expected0 = { {0, 0, 1000} };
    std::vector<SliceSegment> expected1 = { {1, 0, 1000} };
    std::vector<SliceSegment> expected2 = { {2, 0, 1000} };
    ExpectSliceSegmentsEqual(expected0, result[0]);
    ExpectSliceSegmentsEqual(expected1, result[1]);
    ExpectSliceSegmentsEqual(expected2, result[2]);
}

// Test case 6: Row tolerance powers combining segments.
TEST(SliceRecordBatchesTest, RowTolerance) {
    std::vector<size_t> row_counts = {800, 300};
    size_t desired = 1000;
    size_t tolerance = 200;

    auto result = slice_record_batches(row_counts, desired, tolerance);
    // Expected:
    // First slice: batch0: all 800, then batch1: first 200 (to reach 1000 exactly).
    // Second slice: remainder from batch1: {1, 200, 100}
    ASSERT_EQ(result.size(), 2);
    std::vector<SliceSegment> expected0 = { {0, 0, 800}, {1, 0, 200} };
    std::vector<SliceSegment> expected1 = { {1, 200, 100} };
    ExpectSliceSegmentsEqual(expected0, result[0]);
    ExpectSliceSegmentsEqual(expected1, result[1]);
}

// Test case 7: Large batch split with tolerance.
TEST(SliceRecordBatchesTest, LargeBatchSplitWithTolerance) {
    std::vector<size_t> row_counts = {2500};
    size_t desired = 1000;
    size_t tolerance = 100;

    auto result = slice_record_batches(row_counts, desired, tolerance);
    // Expected slices:
    // Slice 1: { {0, 0, 1000} }
    // Slice 2: { {0, 1000, 1000} }
    // Slice 3: { {0, 2000, 500} }
    ASSERT_EQ(result.size(), 3);
    std::vector<SliceSegment> expected0 = { {0, 0, 1000} };
    std::vector<SliceSegment> expected1 = { {0, 1000, 1000} };
    std::vector<SliceSegment> expected2 = { {0, 2000, 500} };
    ExpectSliceSegmentsEqual(expected0, result[0]);
    ExpectSliceSegmentsEqual(expected1, result[1]);
    ExpectSliceSegmentsEqual(expected2, result[2]);
}

// Test case 8: Complex mix of small and large record batches.
TEST(SliceRecordBatchesTest, ComplexMix) {
    // For row_counts: batch0=500, batch1=700, batch2=2000, batch3=300
    std::vector<size_t> row_counts = {500, 700, 2000, 300};
    size_t desired = 1000;
    size_t tolerance = 100;

    auto result = slice_record_batches(row_counts, desired, tolerance);
    // Expected behavior:
    // 1. From batch0 (500) and batch1 (700): first slice: use batch0: all 500, then batch1: first 500 → total 1000.
    // 2. Then continue batch1: remaining 200 → slice 2: { {1, 500, 200} } + then go to batch2: need 800; take first 800 from batch2 → slice becomes exactly 1000.
    // 3. Next, batch2 still has 2000-800=1200 remaining, so slice 3: take 1000 from batch2 → { {2, 800, 1000} }.
    // 4. Finally, slice 4: remaining from batch2: 1200-1000=200 and then batch3: entire 300 → slice 4: { {2, 1800, 200}, {3, 0, 300} } (total 500)
    ASSERT_EQ(result.size(), 4);

    std::vector<SliceSegment> expected0 = { {0, 0, 500}, {1, 0, 500} };
    std::vector<SliceSegment> expected1 = { {1, 500, 200}, {2, 0, 800} };
    std::vector<SliceSegment> expected2 = { {2, 800, 1000} };
    std::vector<SliceSegment> expected3 = { {2, 1800, 200}, {3, 0, 300} };

    ExpectSliceSegmentsEqual(expected0, result[0]);
    ExpectSliceSegmentsEqual(expected1, result[1]);
    ExpectSliceSegmentsEqual(expected2, result[2]);
    ExpectSliceSegmentsEqual(expected3, result[3]);
}