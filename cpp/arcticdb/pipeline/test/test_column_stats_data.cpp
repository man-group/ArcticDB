#include <gtest/gtest.h>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/column_stats_filter.hpp>
#include <arcticdb/util/constants.hpp>
#include <google/protobuf/any.pb.h>

namespace arcticdb {
// Helper to build a column stats SegmentInMemory with the expected schema:
//   col 0: "start_index" (NANOSECONDS_UTC64)
//   col 1: "end_index"   (NANOSECONDS_UTC64)
//   col 2: "v1_MIN(price)" (INT64)
//   col 3: "v1_MAX(price)" (INT64)
//
// Each entry in |rows| is (start_index, end_index, min_price, max_price).
// If a row's start_index is std::nullopt, that row has an absent start_index
struct StatsSegmentRow {
    std::optional<timestamp> start_index;
    std::optional<timestamp> end_index;
    int64_t min_price;
    int64_t max_price;
};

// "price" is at data_col_offset=1 in the TSD (offset 0 is the timestamp index)
static constexpr uint32_t price_data_col_offset = 1;

TimeseriesDescriptor build_test_tsd() {
    TimeseriesDescriptor tsd;
    tsd.mutable_fields().add_field(scalar_field(DataType::NANOSECONDS_UTC64, "timestamp"));
    tsd.mutable_fields().add_field(scalar_field(DataType::INT64, "price"));
    return tsd;
}

SegmentInMemory build_stats_segment(const std::vector<StatsSegmentRow>& rows) {
    using namespace arcticc::pb2::column_stats_pb2;

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
    seg.add_column(scalar_field(DataType::INT64, "v1_MIN(price)"), min_col);
    seg.add_column(scalar_field(DataType::INT64, "v1_MAX(price)"), max_col);
    seg.set_row_data(last_row);

    // Attach ColumnStatsHeader proto metadata
    ColumnStatsHeader header;
    header.set_version(1);
    auto& entry_list = (*header.mutable_stats_by_column())[price_data_col_offset];
    auto* min_entry = entry_list.add_entries();
    min_entry->set_stats_seg_offset(2); // col 2 in the stats segment
    min_entry->set_type(MIN_V1);
    auto* max_entry = entry_list.add_entries();
    max_entry->set_stats_seg_offset(3); // col 3 in the stats segment
    max_entry->set_type(MAX_V1);

    google::protobuf::Any any;
    any.PackFrom(header);
    seg.set_metadata(std::move(any));

    return seg;
}

// When all rows have valid index values, find_row should return the correct stats for each row.
TEST(ColumnStatsDataTest, FindStatsAllRowsPresent) {
    auto seg = build_stats_segment({
            {100, 200, 10, 20},
            {300, 400, 30, 40},
            {500, 600, 50, 60},
    });

    auto tsd = build_test_tsd();
    ColumnStatsData data(std::move(seg), tsd);
    ASSERT_FALSE(data.empty());

    auto slot = data.slot_for_column("price");
    ASSERT_TRUE(slot.has_value());

    auto row0 = data.find_row(100, 200);
    ASSERT_TRUE(row0.has_value());
    auto v0 = data.stats_for(*slot, *row0);
    ASSERT_EQ(v0.min->get<int64_t>(), 10);
    ASSERT_EQ(v0.max->get<int64_t>(), 20);

    auto row2 = data.find_row(500, 600);
    ASSERT_TRUE(row2.has_value());
    auto v2 = data.stats_for(*slot, *row2);
    ASSERT_EQ(v2.min->get<int64_t>(), 50);
    ASSERT_EQ(v2.max->get<int64_t>(), 60);
}

// A malformed row (absent start_index) causes the entire ColumnStatsData to be discarded,
// even if valid rows were already parsed.
TEST(ColumnStatsDataTest, MalformedMiddleRowDiscardsAll) {
    auto seg = build_stats_segment({
            {100, 200, 10, 20},          // valid
            {std::nullopt, 400, 30, 40}, // malformed — absent start_index
            {500, 600, 50, 60},          // never reached
    });

    auto tsd = build_test_tsd();
    ColumnStatsData data(std::move(seg), tsd);
    ASSERT_TRUE(data.empty());
    ASSERT_FALSE(data.find_row(100, 200).has_value());
    ASSERT_FALSE(data.find_row(500, 600).has_value());
}

// Absent end_index also triggers discard.
TEST(ColumnStatsDataTest, MalformedEndIndexDiscardsAll) {
    auto seg = build_stats_segment({
            {100, std::nullopt, 10, 20},
    });

    auto tsd = build_test_tsd();
    ColumnStatsData data(std::move(seg), tsd);
    ASSERT_TRUE(data.empty());
}

// Lookup with non-existent indices returns nullopt.
TEST(ColumnStatsDataTest, FindStatsNonExistentIndex) {
    auto seg = build_stats_segment({
            {100, 200, 10, 20},
    });

    auto tsd = build_test_tsd();
    ColumnStatsData data(std::move(seg), tsd);
    ASSERT_FALSE(data.find_row(999, 888).has_value());
}

// An empty segment produces empty ColumnStatsData.
TEST(ColumnStatsDataTest, EmptySegment) {
    auto seg = build_stats_segment({});

    auto tsd = build_test_tsd();
    ColumnStatsData data(std::move(seg), tsd);
    ASSERT_TRUE(data.empty());
    ASSERT_FALSE(data.find_row(0, 0).has_value());
}

// Two row-slices with the same (start_index, end_index) but different min/max stats.
// This can happen with timestamp-indexed symbols when two segments span identical timestamp ranges
// (e.g. segments where all rows share the same timestamp).
// Both entries are dropped from the lookup so that find_row returns nullopt, forcing the
// segments to be read without pruning rather than using the wrong stats.
TEST(ColumnStatsDataTest, DuplicateIndexPairDropsBothRows) {
    auto seg = build_stats_segment({
            {100, 200, 10, 20}, // row 0: price in [10, 20]
            {100, 200, 50, 60}, // row 1: same index range, price in [50, 60]
    });

    auto tsd = build_test_tsd();
    ColumnStatsData data(std::move(seg), tsd);
    ASSERT_FALSE(data.empty());

    // Neither row is reachable via find_row because the key is ambiguous.
    ASSERT_FALSE(data.find_row(100, 200).has_value());
}

TEST(ColumnStatsDataTest, DateRangePrunesNonOverlappingRows) {
    auto seg = build_stats_segment({
            {100, 200, 10, 20},
            {300, 400, 30, 40},
            {500, 600, 50, 60},
            {700, 800, 70, 80},
    });

    auto tsd = build_test_tsd();
    ColumnStatsData data(std::move(seg), tsd, std::make_pair<timestamp, timestamp>(250, 650));
    ASSERT_FALSE(data.empty());

    auto slot = data.slot_for_column("price");
    ASSERT_TRUE(slot.has_value());

    ASSERT_FALSE(data.find_row(100, 200).has_value());
    ASSERT_FALSE(data.find_row(700, 800).has_value());

    auto row1 = data.find_row(300, 400);
    ASSERT_TRUE(row1.has_value());
    auto v1 = data.stats_for(*slot, *row1);
    ASSERT_EQ(v1.min->get<int64_t>(), 30);
    ASSERT_EQ(v1.max->get<int64_t>(), 40);

    auto row2 = data.find_row(500, 600);
    ASSERT_TRUE(row2.has_value());
    auto v2 = data.stats_for(*slot, *row2);
    ASSERT_EQ(v2.min->get<int64_t>(), 50);
    ASSERT_EQ(v2.max->get<int64_t>(), 60);
}

TEST(ColumnStatsDataTest, DateRangeBoundariesAreInclusive) {
    auto seg = build_stats_segment({
            {100, 200, 10, 20},
            {300, 400, 30, 40},
            {500, 600, 50, 60},
            {700, 800, 70, 80},
    });

    auto tsd = build_test_tsd();
    ColumnStatsData data(std::move(seg), tsd, std::make_pair<timestamp, timestamp>(200, 500));
    ASSERT_FALSE(data.empty());

    ASSERT_TRUE(data.find_row(100, 200).has_value());
    ASSERT_TRUE(data.find_row(300, 400).has_value());
    ASSERT_TRUE(data.find_row(500, 600).has_value());
    ASSERT_FALSE(data.find_row(700, 800).has_value());
}

// Duplicate keys are dropped but non-duplicate keys in the same segment are unaffected.
TEST(ColumnStatsDataTest, DuplicateIndexPairDoesNotAffectOtherRows) {
    auto seg = build_stats_segment({
            {100, 300, 10, 20}, // row 0: unique key
            {300, 400, 30, 40}, // row 1: duplicate key (first)
            {300, 400, 50, 60}, // row 2: duplicate key (second)
            {400, 600, 70, 80}, // row 3: unique key
    });

    auto tsd = build_test_tsd();
    ColumnStatsData data(std::move(seg), tsd);
    ASSERT_FALSE(data.empty());

    auto slot = data.slot_for_column("price");
    ASSERT_TRUE(slot.has_value());

    // Unique rows are still reachable.
    auto row0 = data.find_row(100, 300);
    ASSERT_TRUE(row0.has_value());
    auto v0 = data.stats_for(*slot, *row0);
    ASSERT_EQ(v0.min->get<int64_t>(), 10);
    ASSERT_EQ(v0.max->get<int64_t>(), 20);

    auto row3 = data.find_row(400, 600);
    ASSERT_TRUE(row3.has_value());
    auto v3 = data.stats_for(*slot, *row3);
    ASSERT_EQ(v3.min->get<int64_t>(), 70);
    ASSERT_EQ(v3.max->get<int64_t>(), 80);

    // Duplicate key is not reachable.
    ASSERT_FALSE(data.find_row(300, 400).has_value());
}

// Build a two-column stats segment where "volume" is absent (sparse) for certain rows.
// Verify that materialize_slot reports column_absent = true for those entries.
TEST(ColumnStatsDataTest, SparseColumnAbsentMarkedCorrectly) {
    using namespace arcticc::pb2::column_stats_pb2;

    // Two data columns: price (offset 1) and volume (offset 2) in the TSD.
    // Stats segment layout:
    //   col 0: start_index
    //   col 1: end_index
    //   col 2: v1_MIN(price)
    //   col 3: v1_MAX(price)
    //   col 4: v1_MIN(volume)
    //   col 5: v1_MAX(volume)
    //
    // Row 0: price=[10,20], volume=[100,200]  (both present)
    // Row 1: price=[30,40], volume absent      (sparse)

    auto start_col = std::make_shared<Column>(make_scalar_type(DataType::NANOSECONDS_UTC64), Sparsity::PERMITTED);
    auto end_col = std::make_shared<Column>(make_scalar_type(DataType::NANOSECONDS_UTC64), Sparsity::PERMITTED);
    auto min_price_col = std::make_shared<Column>(make_scalar_type(DataType::INT64), Sparsity::PERMITTED);
    auto max_price_col = std::make_shared<Column>(make_scalar_type(DataType::INT64), Sparsity::PERMITTED);
    auto min_vol_col = std::make_shared<Column>(make_scalar_type(DataType::INT64), Sparsity::PERMITTED);
    auto max_vol_col = std::make_shared<Column>(make_scalar_type(DataType::INT64), Sparsity::PERMITTED);

    // Row 0: all present
    start_col->push_back<timestamp>(100);
    end_col->push_back<timestamp>(200);
    min_price_col->push_back<int64_t>(10);
    max_price_col->push_back<int64_t>(20);
    min_vol_col->push_back<int64_t>(100);
    max_vol_col->push_back<int64_t>(200);

    // Row 1: volume absent
    start_col->push_back<timestamp>(300);
    end_col->push_back<timestamp>(400);
    min_price_col->push_back<int64_t>(30);
    max_price_col->push_back<int64_t>(40);
    min_vol_col->mark_absent_rows(1);
    max_vol_col->mark_absent_rows(1);

    ssize_t last_row = 1;
    SegmentInMemory seg;
    seg.descriptor().set_index(IndexDescriptorImpl(IndexDescriptorImpl::Type::ROWCOUNT, 0));
    seg.add_column(scalar_field(DataType::NANOSECONDS_UTC64, start_index_column_name), start_col);
    seg.add_column(scalar_field(DataType::NANOSECONDS_UTC64, end_index_column_name), end_col);
    seg.add_column(scalar_field(DataType::INT64, "v1_MIN(price)"), min_price_col);
    seg.add_column(scalar_field(DataType::INT64, "v1_MAX(price)"), max_price_col);
    seg.add_column(scalar_field(DataType::INT64, "v1_MIN(volume)"), min_vol_col);
    seg.add_column(scalar_field(DataType::INT64, "v1_MAX(volume)"), max_vol_col);
    seg.set_row_data(last_row);

    ColumnStatsHeader header;
    header.set_version(1);
    constexpr uint32_t price_offset = 1;
    constexpr uint32_t volume_offset = 2;

    auto& price_entries = (*header.mutable_stats_by_column())[price_offset];
    auto* p_min = price_entries.add_entries();
    p_min->set_stats_seg_offset(2);
    p_min->set_type(MIN_V1);
    auto* p_max = price_entries.add_entries();
    p_max->set_stats_seg_offset(3);
    p_max->set_type(MAX_V1);

    auto& vol_entries = (*header.mutable_stats_by_column())[volume_offset];
    auto* v_min = vol_entries.add_entries();
    v_min->set_stats_seg_offset(4);
    v_min->set_type(MIN_V1);
    auto* v_max = vol_entries.add_entries();
    v_max->set_stats_seg_offset(5);
    v_max->set_type(MAX_V1);

    google::protobuf::Any any;
    any.PackFrom(header);
    seg.set_metadata(std::move(any));

    // Build TSD with timestamp, price, volume
    TimeseriesDescriptor tsd;
    tsd.mutable_fields().add_field(scalar_field(DataType::NANOSECONDS_UTC64, "timestamp"));
    tsd.mutable_fields().add_field(scalar_field(DataType::INT64, "price"));
    tsd.mutable_fields().add_field(scalar_field(DataType::INT64, "volume"));

    ColumnStatsData data(std::move(seg), tsd);
    ASSERT_FALSE(data.empty());

    auto price_slot = data.slot_for_column("price");
    auto vol_slot = data.slot_for_column("volume");
    ASSERT_TRUE(price_slot.has_value());
    ASSERT_TRUE(vol_slot.has_value());

    auto row0 = data.find_row(100, 200);
    ASSERT_TRUE(row0.has_value());
    auto p0 = data.stats_for(*price_slot, *row0);
    ASSERT_TRUE(p0.min.has_value());
    ASSERT_EQ(p0.min->get<int64_t>(), 10);
    ASSERT_FALSE(p0.column_absent);

    auto v0 = data.stats_for(*vol_slot, *row0);
    ASSERT_TRUE(v0.min.has_value());
    ASSERT_EQ(v0.min->get<int64_t>(), 100);
    ASSERT_FALSE(v0.column_absent);

    auto row1 = data.find_row(300, 400);
    ASSERT_TRUE(row1.has_value());
    auto p1 = data.stats_for(*price_slot, *row1);
    ASSERT_TRUE(p1.min.has_value());
    ASSERT_EQ(p1.min->get<int64_t>(), 30);
    ASSERT_FALSE(p1.column_absent);

    auto v1 = data.stats_for(*vol_slot, *row1);
    ASSERT_FALSE(v1.min.has_value());
    ASSERT_FALSE(v1.max.has_value());
    ASSERT_TRUE(v1.column_absent);
}
} // namespace arcticdb
