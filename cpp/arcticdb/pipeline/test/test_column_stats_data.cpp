#include <gtest/gtest.h>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/column_stats_filter.hpp>
#include <arcticdb/util/constants.hpp>

using namespace arcticdb;

// Helper to build a column stats SegmentInMemory with the expected schema:
//   col 0: "start_index" (NANOSECONDS_UTC64)
//   col 1: "end_index"   (NANOSECONDS_UTC64)
//   col 2: "v1.0_MIN(price)" (INT64)
//   col 3: "v1.0_MAX(price)" (INT64)
//
// Each entry in |rows| is (start_index, end_index, min_price, max_price).
// If a row's start_index is std::nullopt, that row has an absent start_index
struct StatsSegmentRow {
    std::optional<timestamp> start_index;
    std::optional<timestamp> end_index;
    int64_t min_price;
    int64_t max_price;
};

SegmentInMemory build_stats_segment(const std::vector<StatsSegmentRow>& rows) {
    auto start_col = std::make_shared<Column>(make_scalar_type(DataType::NANOSECONDS_UTC64), Sparsity::PERMITTED);
    auto end_col = std::make_shared<Column>(make_scalar_type(DataType::NANOSECONDS_UTC64), Sparsity::PERMITTED);
    auto min_col = std::make_shared<Column>(make_scalar_type(DataType::INT64), Sparsity::PERMITTED);
    auto max_col = std::make_shared<Column>(make_scalar_type(DataType::INT64), Sparsity::PERMITTED);

    for (const auto& row : rows) {
        if (row.start_index.has_value()) {
            start_col->push_back<timestamp>(*row.start_index);
        } else {
            start_col->mark_absent_rows(1);
        }
        if (row.end_index.has_value()) {
            end_col->push_back<timestamp>(*row.end_index);
        } else {
            end_col->mark_absent_rows(1);
        }
        min_col->push_back<int64_t>(row.min_price);
        max_col->push_back<int64_t>(row.max_price);
    }

    auto last_row = static_cast<ssize_t>(rows.size()) - 1;
    start_col->set_row_data(last_row);
    end_col->set_row_data(last_row);
    min_col->set_row_data(last_row);
    max_col->set_row_data(last_row);

    SegmentInMemory seg;
    seg.descriptor().set_index(IndexDescriptorImpl(IndexDescriptorImpl::Type::ROWCOUNT, 0));
    seg.add_column(scalar_field(DataType::NANOSECONDS_UTC64, start_index_column_name), start_col);
    seg.add_column(scalar_field(DataType::NANOSECONDS_UTC64, end_index_column_name), end_col);
    seg.add_column(scalar_field(DataType::INT64, "v1.0_MIN(price)"), min_col);
    seg.add_column(scalar_field(DataType::INT64, "v1.0_MAX(price)"), max_col);
    seg.set_row_data(last_row);
    return seg;
}

// When all rows have valid index values, find_stats should return the correct stats for each row.
TEST(ColumnStatsData, FindStatsAllRowsPresent) {
    auto seg = build_stats_segment({
            {100, 200, 10, 20},
            {300, 400, 30, 40},
            {500, 600, 50, 60},
    });

    ColumnStatsData data(std::move(seg));
    ASSERT_FALSE(data.empty());

    auto* row0 = data.find_stats(100, 200);
    ASSERT_NE(row0, nullptr);
    auto it0 = row0->stats_for_column.find("price");
    ASSERT_NE(it0, row0->stats_for_column.end());
    ASSERT_EQ(it0->second.min->get<int64_t>(), 10);
    ASSERT_EQ(it0->second.max->get<int64_t>(), 20);

    auto* row2 = data.find_stats(500, 600);
    ASSERT_NE(row2, nullptr);
    auto it2 = row2->stats_for_column.find("price");
    ASSERT_NE(it2, row2->stats_for_column.end());
    ASSERT_EQ(it2->second.min->get<int64_t>(), 50);
    ASSERT_EQ(it2->second.max->get<int64_t>(), 60);
}

// A malformed row (absent start_index) causes the entire ColumnStatsData to be discarded,
// even if valid rows were already parsed.
TEST(ColumnStatsData, MalformedMiddleRowDiscardsAll) {
    auto seg = build_stats_segment({
            {100, 200, 10, 20},          // valid
            {std::nullopt, 400, 30, 40}, // malformed — absent start_index
            {500, 600, 50, 60},          // never reached
    });

    ColumnStatsData data(std::move(seg));
    ASSERT_TRUE(data.empty());
    ASSERT_EQ(data.find_stats(100, 200), nullptr);
    ASSERT_EQ(data.find_stats(500, 600), nullptr);
}

// Absent end_index also triggers discard.
TEST(ColumnStatsData, MalformedEndIndexDiscardsAll) {
    auto seg = build_stats_segment({
            {100, std::nullopt, 10, 20},
    });

    ColumnStatsData data(std::move(seg));
    ASSERT_TRUE(data.empty());
}

// Lookup with non-existent indices returns nullptr.
TEST(ColumnStatsData, FindStatsNonExistentIndex) {
    auto seg = build_stats_segment({
            {100, 200, 10, 20},
    });

    ColumnStatsData data(std::move(seg));
    ASSERT_EQ(data.find_stats(999, 888), nullptr);
}

// An empty segment produces empty ColumnStatsData.
TEST(ColumnStatsData, EmptySegment) {
    auto seg = build_stats_segment({});

    ColumnStatsData data(std::move(seg));
    ASSERT_TRUE(data.empty());
    ASSERT_EQ(data.find_stats(0, 0), nullptr);
}

// Two row-slices with the same (start_index, end_index) but different min/max stats.
// This can happen with timestamp-indexed symbols when two segments span identical timestamp ranges
// (e.g. segments where all rows share the same timestamp).
// Both entries are dropped from the lookup so that find_stats returns nullptr, forcing the
// segments to be read without pruning rather than using the wrong stats.
TEST(ColumnStatsData, DuplicateIndexPairDropsBothRows) {
    auto seg = build_stats_segment({
            {100, 200, 10, 20}, // row 0: price in [10, 20]
            {100, 200, 50, 60}, // row 1: same index range, price in [50, 60]
    });

    ColumnStatsData data(std::move(seg));
    ASSERT_FALSE(data.empty());

    // Neither row is reachable via find_stats because the key is ambiguous.
    ASSERT_EQ(data.find_stats(100, 200), nullptr);
}

// Duplicate keys are dropped but non-duplicate keys in the same segment are unaffected.
TEST(ColumnStatsData, DuplicateIndexPairDoesNotAffectOtherRows) {
    auto seg = build_stats_segment({
            {100, 300, 10, 20}, // row 0: unique key
            {300, 400, 30, 40}, // row 1: duplicate key (first)
            {300, 400, 50, 60}, // row 2: duplicate key (second)
            {400, 600, 70, 80}, // row 3: unique key
    });

    ColumnStatsData data(std::move(seg));
    ASSERT_FALSE(data.empty());

    // Unique rows are still reachable.
    auto* row0 = data.find_stats(100, 300);
    ASSERT_NE(row0, nullptr);
    ASSERT_EQ(row0->stats_for_column.at("price").min->get<int64_t>(), 10);
    ASSERT_EQ(row0->stats_for_column.at("price").max->get<int64_t>(), 20);

    auto* row3 = data.find_stats(400, 600);
    ASSERT_NE(row3, nullptr);
    ASSERT_EQ(row3->stats_for_column.at("price").min->get<int64_t>(), 70);
    ASSERT_EQ(row3->stats_for_column.at("price").max->get<int64_t>(), 80);

    // Duplicate key is not reachable.
    ASSERT_EQ(data.find_stats(300, 400), nullptr);
}
