#include <gtest/gtest.h>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/column_stats_filter.hpp>
#include <arcticdb/util/constants.hpp>
#include <google/protobuf/any.pb.h>

using namespace arcticdb;

// Helper to build a column stats SegmentInMemory with the expected schema:
//   col 0: "start_row" (UINT64)
//   col 1: "v1_MIN(price)" (INT64)
//   col 2: "v1_MAX(price)" (INT64)
//
// Each entry in |rows| is (start_row, min_price, max_price).
// If a row's start_row is std::nullopt, that row has an absent start_row.
struct StatsSegmentRow {
    std::optional<uint64_t> start_row;
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

    auto start_row_col = std::make_shared<Column>(make_scalar_type(DataType::UINT64), Sparsity::PERMITTED);
    auto min_col = std::make_shared<Column>(make_scalar_type(DataType::INT64), Sparsity::PERMITTED);
    auto max_col = std::make_shared<Column>(make_scalar_type(DataType::INT64), Sparsity::PERMITTED);

    for (const auto& row : rows) {
        if (row.start_row.has_value()) {
            start_row_col->push_back<uint64_t>(*row.start_row);
        } else {
            start_row_col->mark_absent_rows(1);
        }
        min_col->push_back<int64_t>(row.min_price);
        max_col->push_back<int64_t>(row.max_price);
    }

    auto last_row = static_cast<ssize_t>(rows.size()) - 1;
    start_row_col->set_row_data(last_row);
    min_col->set_row_data(last_row);
    max_col->set_row_data(last_row);

    SegmentInMemory seg;
    seg.descriptor().set_index(IndexDescriptorImpl(IndexDescriptorImpl::Type::ROWCOUNT, 0));
    seg.add_column(scalar_field(DataType::UINT64, start_row_column_name), start_row_col);
    seg.add_column(scalar_field(DataType::INT64, "v1_MIN(price)"), min_col);
    seg.add_column(scalar_field(DataType::INT64, "v1_MAX(price)"), max_col);
    seg.set_row_data(last_row);

    // Attach ColumnStatsHeader proto metadata
    ColumnStatsHeader header;
    header.set_version(1);
    auto& entry_list = (*header.mutable_stats_by_column())[price_data_col_offset];
    auto* min_entry = entry_list.add_entries();
    min_entry->set_stats_seg_offset(1); // col 1 in the stats segment
    min_entry->set_type(MIN_V1);
    auto* max_entry = entry_list.add_entries();
    max_entry->set_stats_seg_offset(2); // col 2 in the stats segment
    max_entry->set_type(MAX_V1);

    google::protobuf::Any any;
    any.PackFrom(header);
    seg.set_metadata(std::move(any));

    return seg;
}

// When all rows have valid start_row values, find_stats should return the correct stats for each row.
TEST(ColumnStatsData, FindStatsAllRowsPresent) {
    auto seg = build_stats_segment({
            {0, 10, 20},
            {100, 30, 40},
            {200, 50, 60},
    });

    auto tsd = build_test_tsd();
    ColumnStatsData data(std::move(seg), tsd);
    ASSERT_FALSE(data.empty());

    auto* row0 = data.find_stats(0);
    ASSERT_NE(row0, nullptr);
    auto it0 = row0->col_name_to_stat.find("price");
    ASSERT_NE(it0, row0->col_name_to_stat.end());
    ASSERT_EQ(it0->second.min->get<int64_t>(), 10);
    ASSERT_EQ(it0->second.max->get<int64_t>(), 20);

    auto* row2 = data.find_stats(200);
    ASSERT_NE(row2, nullptr);
    auto it2 = row2->col_name_to_stat.find("price");
    ASSERT_NE(it2, row2->col_name_to_stat.end());
    ASSERT_EQ(it2->second.min->get<int64_t>(), 50);
    ASSERT_EQ(it2->second.max->get<int64_t>(), 60);
}

// Row-slices that share the same index value (and therefore would have collided under the old
// (start_index, end_index) key) are keyed by distinct start_row values, so each row remains
// individually reachable with its own stats. This is the core behaviour the row-keying change enables.
TEST(ColumnStatsData, IdenticalIndexDistinctStartRowAllReachable) {
    // All five row-slices span the same timestamp range in the real symbol, but each has a distinct
    // start_row. Every row must keep its own min/max.
    auto seg = build_stats_segment({
            {0, 10, 20},
            {100, 30, 40},
            {200, 50, 60},
            {300, 70, 80},
            {400, 90, 100},
    });

    auto tsd = build_test_tsd();
    ColumnStatsData data(std::move(seg), tsd);
    ASSERT_FALSE(data.empty());

    const std::vector<std::tuple<uint64_t, int64_t, int64_t>> expected{
            {0, 10, 20}, {100, 30, 40}, {200, 50, 60}, {300, 70, 80}, {400, 90, 100}
    };
    for (const auto& [start_row, min, max] : expected) {
        auto* row = data.find_stats(start_row);
        ASSERT_NE(row, nullptr) << "start_row " << start_row << " should be reachable";
        ASSERT_EQ(row->col_name_to_stat.at("price").min->get<int64_t>(), min);
        ASSERT_EQ(row->col_name_to_stat.at("price").max->get<int64_t>(), max);
    }
}

// A malformed row (absent start_row) causes the entire ColumnStatsData to be discarded,
// even if valid rows were already parsed.
TEST(ColumnStatsData, MalformedMiddleRowDiscardsAll) {
    auto seg = build_stats_segment({
            {0, 10, 20},            // valid
            {std::nullopt, 30, 40}, // malformed — absent start_row
            {200, 50, 60},          // never reached
    });

    auto tsd = build_test_tsd();
    ColumnStatsData data(std::move(seg), tsd);
    ASSERT_TRUE(data.empty());
    ASSERT_EQ(data.find_stats(0), nullptr);
    ASSERT_EQ(data.find_stats(200), nullptr);
}

// Lookup with a non-existent start_row returns nullptr.
TEST(ColumnStatsData, FindStatsNonExistentStartRow) {
    auto seg = build_stats_segment({
            {0, 10, 20},
    });

    auto tsd = build_test_tsd();
    ColumnStatsData data(std::move(seg), tsd);
    ASSERT_EQ(data.find_stats(999), nullptr);
}

// An empty segment produces empty ColumnStatsData.
TEST(ColumnStatsData, EmptySegment) {
    auto seg = build_stats_segment({});

    auto tsd = build_test_tsd();
    ColumnStatsData data(std::move(seg), tsd);
    ASSERT_TRUE(data.empty());
    ASSERT_EQ(data.find_stats(0), nullptr);
}

// A duplicate start_row indicates a corrupt stats segment (start_row uniquely identifies a row-slice),
// so all stats are discarded and find_stats returns nullptr, forcing an unpruned read.
TEST(ColumnStatsData, DuplicateStartRowDiscardsAll) {
    auto seg = build_stats_segment({
            {100, 10, 20}, // row 0
            {100, 50, 60}, // row 1: duplicate start_row
    });

    auto tsd = build_test_tsd();
    ColumnStatsData data(std::move(seg), tsd);
    ASSERT_TRUE(data.empty());
    ASSERT_EQ(data.find_stats(100), nullptr);
}

// Build a two-column stats segment where "volume" is absent (sparse) for certain rows.
// Verify that ColumnStatsData marks those entries as column_absent = true.
TEST(ColumnStatsData, SparseColumnAbsentMarkedCorrectly) {
    using namespace arcticc::pb2::column_stats_pb2;

    // Two data columns: price (offset 1) and volume (offset 2) in the TSD.
    // Stats segment layout:
    //   col 0: start_row
    //   col 1: v1_MIN(price)
    //   col 2: v1_MAX(price)
    //   col 3: v1_MIN(volume)
    //   col 4: v1_MAX(volume)
    //
    // Row 0 (start_row 0):   price=[10,20], volume=[100,200]  (both present)
    // Row 1 (start_row 100): price=[30,40], volume absent      (sparse)

    auto start_row_col = std::make_shared<Column>(make_scalar_type(DataType::UINT64), Sparsity::PERMITTED);
    auto min_price_col = std::make_shared<Column>(make_scalar_type(DataType::INT64), Sparsity::PERMITTED);
    auto max_price_col = std::make_shared<Column>(make_scalar_type(DataType::INT64), Sparsity::PERMITTED);
    auto min_vol_col = std::make_shared<Column>(make_scalar_type(DataType::INT64), Sparsity::PERMITTED);
    auto max_vol_col = std::make_shared<Column>(make_scalar_type(DataType::INT64), Sparsity::PERMITTED);

    // Row 0: all present
    start_row_col->push_back<uint64_t>(0);
    min_price_col->push_back<int64_t>(10);
    max_price_col->push_back<int64_t>(20);
    min_vol_col->push_back<int64_t>(100);
    max_vol_col->push_back<int64_t>(200);

    // Row 1: volume absent
    start_row_col->push_back<uint64_t>(100);
    min_price_col->push_back<int64_t>(30);
    max_price_col->push_back<int64_t>(40);
    min_vol_col->mark_absent_rows(1);
    max_vol_col->mark_absent_rows(1);

    ssize_t last_row = 1;
    SegmentInMemory seg;
    seg.descriptor().set_index(IndexDescriptorImpl(IndexDescriptorImpl::Type::ROWCOUNT, 0));
    seg.add_column(scalar_field(DataType::UINT64, start_row_column_name), start_row_col);
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
    p_min->set_stats_seg_offset(1);
    p_min->set_type(MIN_V1);
    auto* p_max = price_entries.add_entries();
    p_max->set_stats_seg_offset(2);
    p_max->set_type(MAX_V1);

    auto& vol_entries = (*header.mutable_stats_by_column())[volume_offset];
    auto* v_min = vol_entries.add_entries();
    v_min->set_stats_seg_offset(3);
    v_min->set_type(MIN_V1);
    auto* v_max = vol_entries.add_entries();
    v_max->set_stats_seg_offset(4);
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

    // Row 0: both columns present, neither absent
    auto* row0 = data.find_stats(0);
    ASSERT_NE(row0, nullptr);
    auto price0_it = row0->col_name_to_stat.find("price");
    ASSERT_NE(price0_it, row0->col_name_to_stat.end());
    ASSERT_TRUE(price0_it->second.min.has_value());
    ASSERT_EQ(price0_it->second.min->get<int64_t>(), 10);
    ASSERT_FALSE(price0_it->second.column_absent);

    auto vol0_it = row0->col_name_to_stat.find("volume");
    ASSERT_NE(vol0_it, row0->col_name_to_stat.end());
    ASSERT_TRUE(vol0_it->second.min.has_value());
    ASSERT_EQ(vol0_it->second.min->get<int64_t>(), 100);
    ASSERT_FALSE(vol0_it->second.column_absent);

    // Row 1: price present, volume absent
    auto* row1 = data.find_stats(100);
    ASSERT_NE(row1, nullptr);
    auto price1_it = row1->col_name_to_stat.find("price");
    ASSERT_NE(price1_it, row1->col_name_to_stat.end());
    ASSERT_TRUE(price1_it->second.min.has_value());
    ASSERT_EQ(price1_it->second.min->get<int64_t>(), 30);
    ASSERT_FALSE(price1_it->second.column_absent);

    auto vol1_it = row1->col_name_to_stat.find("volume");
    ASSERT_NE(vol1_it, row1->col_name_to_stat.end());
    ASSERT_FALSE(vol1_it->second.min.has_value());
    ASSERT_FALSE(vol1_it->second.max.has_value());
    ASSERT_TRUE(vol1_it->second.column_absent);
}
