/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/version/lazy_read_helpers.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>

namespace arcticdb {

namespace {

// Helper to create a segment with an int64 index column and a float64 data column.
// Index values run [start_ts, start_ts + num_rows).
SegmentInMemory make_test_segment(size_t num_rows, timestamp start_ts = 0) {
    auto fields = std::array{scalar_field(DataType::FLOAT64, "value")};
    auto desc = get_test_descriptor<stream::TimeseriesIndex>("test", fields);
    SegmentInMemory seg(std::move(desc), num_rows);

    // Fill index column (column 0) with ascending timestamps
    auto& idx_col = seg.column(0);
    for (size_t i = 0; i < num_rows; ++i) {
        auto ts = static_cast<timestamp>(start_ts + static_cast<timestamp>(i));
        idx_col.set_scalar(static_cast<ssize_t>(i), ts);
    }

    // Fill data column (column 1) with float values
    auto& data_col = seg.column(1);
    for (size_t i = 0; i < num_rows; ++i) {
        data_col.set_scalar(static_cast<ssize_t>(i), static_cast<double>(i) + 0.5);
    }

    seg.set_row_data(num_rows - 1);
    return seg;
}

} // anonymous namespace

// --- apply_truncation tests ---

TEST(LazyReadHelpers, ApplyTruncation_DateRange_Middle) {
    // Segment with timestamps [0, 100), truncate to [25, 75] (inclusive both ends)
    auto seg = make_test_segment(100, 0);
    pipelines::RowRange slice_row_range{0, 100};
    TimestampRange date_range{25, 75};
    FilterRange filter = entity::IndexRange(date_range);

    apply_truncation(seg, slice_row_range, filter);

    // ArcticDB date ranges are inclusive: rows 25,26,...,75 = 51 rows
    EXPECT_EQ(seg.row_count(), 51u);
}

TEST(LazyReadHelpers, ApplyTruncation_DateRange_AfterAll) {
    // Date range starts entirely after the segment data — yields 0 rows.
    // Note: setup_pipeline_context() already filters segments at segment-granularity;
    // apply_truncation() only handles boundary segments. The "filter entirely after"
    // case is the one it explicitly handles (time_filter.first > last_ts).
    auto seg = make_test_segment(100, 0); // timestamps [0, 99]
    pipelines::RowRange slice_row_range{0, 100};
    TimestampRange date_range{200, 300}; // entirely past segment
    FilterRange filter = entity::IndexRange(date_range);

    apply_truncation(seg, slice_row_range, filter);

    EXPECT_EQ(seg.row_count(), 0u);
}

TEST(LazyReadHelpers, ApplyTruncation_RowRange_MiddleOfSegment) {
    // Segment at rows [200, 300), filter asks for rows [220, 280)
    auto seg = make_test_segment(100, 200);
    pipelines::RowRange slice_row_range{200, 300};
    FilterRange filter = pipelines::RowRange{220, 280};

    apply_truncation(seg, slice_row_range, filter);

    // local_start = max(0, 220-200) = 20, local_end = min(100, 280-200) = 80 → 60 rows
    EXPECT_EQ(seg.row_count(), 60u);
}

TEST(LazyReadHelpers, ApplyTruncation_RowRange_NoTruncation) {
    // Filter range covers entire segment — no change
    auto seg = make_test_segment(100, 0);
    pipelines::RowRange slice_row_range{0, 100};
    FilterRange filter = pipelines::RowRange{0, 200};

    apply_truncation(seg, slice_row_range, filter);

    EXPECT_EQ(seg.row_count(), 100u);
}

TEST(LazyReadHelpers, ApplyTruncation_Monostate_NoOp) {
    auto seg = make_test_segment(100, 0);
    pipelines::RowRange slice_row_range{0, 100};
    FilterRange filter = std::monostate{};

    apply_truncation(seg, slice_row_range, filter);

    EXPECT_EQ(seg.row_count(), 100u);
}

TEST(LazyReadHelpers, ApplyTruncation_EmptySegment) {
    auto fields = std::array{scalar_field(DataType::FLOAT64, "value")};
    auto desc = get_test_descriptor<stream::TimeseriesIndex>("test", fields);
    SegmentInMemory seg(std::move(desc), 0);

    pipelines::RowRange slice_row_range{0, 0};
    TimestampRange date_range{0, 100};
    FilterRange filter = entity::IndexRange(date_range);

    // Should not crash on empty segment
    apply_truncation(seg, slice_row_range, filter);
    EXPECT_EQ(seg.row_count(), 0u);
}

// --- estimate_segment_bytes tests ---

TEST(LazyReadHelpers, EstimateSegmentBytes_BasicCalculation) {
    auto fields = std::array{
            scalar_field(DataType::INT64, "a"),
            scalar_field(DataType::FLOAT64, "b"),
            scalar_field(DataType::INT32, "c"),
    };
    auto desc = get_test_descriptor<stream::TimeseriesIndex>("test", fields);

    // Create a SliceAndKey with known row range
    pipelines::FrameSlice slice{pipelines::ColRange{0, 4}, pipelines::RowRange{0, 1000}};
    auto key = atom_key_builder().gen_id(0).content_hash(0).creation_ts(0).start_index(0).end_index(1000).build(
            "test", KeyType::TABLE_DATA
    );
    pipelines::SliceAndKey sk{std::move(slice), std::move(key)};

    // 1000 rows × 4 columns (index + 3 data) × 8 bytes = 32000
    auto estimate = estimate_segment_bytes(sk, desc);
    EXPECT_EQ(estimate, 1000u * 4u * 8u);
}

// --- apply_filter_clause tests ---

TEST(LazyReadHelpers, ApplyFilterClause_NullContext_ReturnsTrue) {
    auto seg = make_test_segment(100, 0);
    std::shared_ptr<ExpressionContext> null_ctx;

    auto result = apply_filter_clause(seg, null_ctx, "");

    EXPECT_TRUE(result);
    EXPECT_EQ(seg.row_count(), 100u);
}

TEST(LazyReadHelpers, ApplyFilterClause_EmptySegment_ReturnsFalse) {
    auto fields = std::array{scalar_field(DataType::FLOAT64, "value")};
    auto desc = get_test_descriptor<stream::TimeseriesIndex>("test", fields);
    SegmentInMemory seg(std::move(desc), 0);

    auto ctx = std::make_shared<ExpressionContext>();

    auto result = apply_filter_clause(seg, ctx, "filter_0");

    EXPECT_FALSE(result);
}

} // namespace arcticdb
