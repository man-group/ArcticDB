/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <google/protobuf/any.pb.h>

#include <algorithm>
#include <tuple>
#include <unordered_set>

namespace arcticdb {

namespace {
constexpr size_t price_data_col_offset = 1;
constexpr size_t volume_data_col_offset = 2;

template<typename T>
ColumnStatValue min_stat(T value, DataType data_type) {
    return ColumnStatValue{ColumnStatTypeInternal::MIN_V1, price_data_col_offset, Value{value, data_type}};
}

ColumnStatsRow column_stats_row(uint64_t start_row, uint64_t end_row, std::vector<ColumnStatValue> stats) {
    return ColumnStatsRow{pipelines::RowRange{start_row, end_row}, std::move(stats)};
}

position_t column_index(const SegmentInMemory& seg, std::string_view name) {
    auto idx = seg.column_index(name);
    util::check(idx.has_value(), "column {} missing", name);
    return static_cast<position_t>(*idx);
}

StreamDescriptor make_descriptor(DataType price_type = DataType::INT64, DataType volume_type = DataType::UINT64) {
    StreamDescriptor desc{"sym"};
    desc.add_scalar_field(DataType::UINT64, "index");
    desc.add_scalar_field(price_type, "price");
    desc.add_scalar_field(volume_type, "volume");
    return desc;
}

bool stat_values_equal(const ColumnStatValue& left, const ColumnStatValue& right) {
    return left.type == right.type && left.data_col_offset == right.data_col_offset && left.value == right.value;
}

// Stat order within a column stats row reflects protobuf map iteration order over stats_by_column, which
// is unspecified, so comparisons sort by (type, data_col_offset) first.
void expect_column_stats_rows_equal(std::vector<ColumnStatsRow> actual, std::vector<ColumnStatsRow> expected) {
    auto by_key = [](const ColumnStatValue& l, const ColumnStatValue& r) {
        return std::tie(l.type, l.data_col_offset) < std::tie(r.type, r.data_col_offset);
    };
    for (auto& c : actual) {
        std::sort(c.stats.begin(), c.stats.end(), by_key);
    }
    for (auto& c : expected) {
        std::sort(c.stats.begin(), c.stats.end(), by_key);
    }
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < actual.size(); ++i) {
        EXPECT_EQ(actual.at(i).row_range.start(), expected.at(i).row_range.start()) << "component " << i;
        EXPECT_EQ(actual.at(i).row_range.end(), expected.at(i).row_range.end()) << "component " << i;
        ASSERT_EQ(actual.at(i).stats.size(), expected.at(i).stats.size()) << "component " << i;
        for (size_t j = 0; j < actual.at(i).stats.size(); ++j) {
            EXPECT_TRUE(stat_values_equal(actual.at(i).stats.at(j), expected.at(i).stats.at(j)))
                    << "component " << i << " stat " << j;
        }
    }
}
} // namespace

TEST(ColumnStatsBuildSegmentTest, IndexColumnsAndRowOrder) {
    auto desc = make_descriptor();
    // Deliberately out of index order
    auto seg = build_column_stats_segment(
            {column_stats_row(300, 399, {min_stat<int64_t>(3, DataType::INT64)}),
             column_stats_row(100, 199, {min_stat<int64_t>(1, DataType::INT64)}),
             column_stats_row(200, 299, {min_stat<int64_t>(2, DataType::INT64)})},
            desc
    );

    ASSERT_EQ(seg.row_count(), 3);
    EXPECT_EQ(column_index(seg, start_row_column_name), 0);
    EXPECT_EQ(column_index(seg, end_row_column_name), 1);

    const auto min_col = column_index(seg, "v1_MIN(price)");
    for (auto&& [row, expected_start] : folly::enumerate(std::vector<uint64_t>{100, 200, 300})) {
        EXPECT_EQ(seg.scalar_at<uint64_t>(static_cast<position_t>(row), 0), expected_start);
        EXPECT_EQ(seg.scalar_at<uint64_t>(static_cast<position_t>(row), 1), expected_start + 99);
        EXPECT_EQ(seg.scalar_at<int64_t>(static_cast<position_t>(row), min_col), static_cast<int64_t>(row) + 1);
    }
}

TEST(ColumnStatsBuildSegmentTest, HeaderOffsetsMatchColumnPositions) {
    auto desc = make_descriptor();
    auto seg = build_column_stats_segment(
            {column_stats_row(
                    100,
                    199,
                    {min_stat<int64_t>(1, DataType::INT64),
                     ColumnStatValue{
                             ColumnStatTypeInternal::MAX_V1, price_data_col_offset, Value{int64_t{9}, DataType::INT64}
                     },
                     ColumnStatValue{
                             ColumnStatTypeInternal::MIN_V1,
                             volume_data_col_offset,
                             Value{uint64_t{5}, DataType::UINT64}
                     }}
            )},
            desc
    );

    ASSERT_TRUE(seg.metadata());
    arcticc::pb2::column_stats_pb2::ColumnStatsHeader header;
    ASSERT_TRUE(seg.metadata()->UnpackTo(&header));
    EXPECT_EQ(header.version(), 1);

    const std::unordered_map<ColumnStatTypeInternal, std::string> operator_for_type{
            {ColumnStatTypeInternal::MIN_V1, "v1_MIN"}, {ColumnStatTypeInternal::MAX_V1, "v1_MAX"}
    };
    const std::unordered_map<size_t, std::string> column_for_offset{
            {price_data_col_offset, "price"}, {volume_data_col_offset, "volume"}
    };

    size_t entries = 0;
    for (const auto& [data_col_offset, entry_list] : header.stats_by_column()) {
        for (const auto& entry : entry_list.entries()) {
            const auto expected =
                    fmt::format("{}({})", operator_for_type.at(entry.type()), column_for_offset.at(data_col_offset));
            EXPECT_EQ(seg.descriptor().field(entry.stats_seg_offset()).name(), expected);
            ++entries;
        }
    }
    EXPECT_EQ(entries, 3);
}

// The descriptor decides the stat column's on-disk type, not the row slices contributing to this
// particular create — otherwise a partial create over a subset of slices could write a narrower
// type than a whole-symbol create, making the on-disk type depend on creation order.
TEST(ColumnStatsBuildSegmentTest, StatColumnTypeComesFromDescriptor) {
    {
        auto desc = make_descriptor(DataType::UINT16);
        auto seg = build_column_stats_segment(
                {column_stats_row(100, 199, {min_stat<uint8_t>(7, DataType::UINT8)}),
                 column_stats_row(200, 299, {min_stat<uint16_t>(1000, DataType::UINT16)})},
                desc
        );
        const auto col = column_index(seg, "v1_MIN(price)");
        EXPECT_EQ(seg.column(col).type(), make_scalar_type(DataType::UINT16));
        EXPECT_EQ(seg.scalar_at<uint16_t>(0, col), 7);
        EXPECT_EQ(seg.scalar_at<uint16_t>(1, col), 1000);
    }
    {
        auto desc = make_descriptor(DataType::INT32);
        auto seg = build_column_stats_segment(
                {column_stats_row(100, 199, {min_stat<uint16_t>(1000, DataType::UINT16)}),
                 column_stats_row(200, 299, {min_stat<int16_t>(-1002, DataType::INT16)})},
                desc
        );
        const auto col = column_index(seg, "v1_MIN(price)");
        EXPECT_EQ(seg.column(col).type(), make_scalar_type(DataType::INT32));
        EXPECT_EQ(seg.scalar_at<int32_t>(0, col), 1000);
        EXPECT_EQ(seg.scalar_at<int32_t>(1, col), -1002);
    }
}

// A stat missing from some row slices stays sparse, with the absent rows marked absent rather than
// shifting the values of the rows that do have it.
TEST(ColumnStatsBuildSegmentTest, StatAbsentFromSomeComponentsIsSparse) {
    auto desc = make_descriptor();
    auto seg = build_column_stats_segment(
            {column_stats_row(100, 199, {min_stat<int64_t>(11, DataType::INT64)}),
             column_stats_row(200, 299, {}),
             column_stats_row(300, 399, {min_stat<int64_t>(33, DataType::INT64)}),
             column_stats_row(400, 499, {})},
            desc
    );

    ASSERT_EQ(seg.row_count(), 4);
    const auto col = column_index(seg, "v1_MIN(price)");
    EXPECT_TRUE(seg.column(col).is_sparse());
    EXPECT_EQ(seg.scalar_at<int64_t>(0, col), 11);
    EXPECT_FALSE(seg.scalar_at<int64_t>(1, col).has_value());
    EXPECT_EQ(seg.scalar_at<int64_t>(2, col), 33);
    EXPECT_FALSE(seg.scalar_at<int64_t>(3, col).has_value());
    // The row-range columns stay dense across all rows
    EXPECT_EQ(seg.scalar_at<uint64_t>(3, 0), 400);
    EXPECT_EQ(seg.scalar_at<uint64_t>(3, 1), 499);
}

TEST(ColumnStatsBuildSegmentTest, DuplicateRowRangeRaises) {
    auto desc = make_descriptor();
    EXPECT_THROW(
            build_column_stats_segment(
                    {column_stats_row(100, 199, {min_stat<int64_t>(1, DataType::INT64)}),
                     column_stats_row(100, 199, {min_stat<int64_t>(2, DataType::INT64)})},
                    desc
            ),
            InternalException
    );
}

TEST(ColumnStatsBuildSegmentTest, NonMonotonicRowRangeRaises) {
    auto desc = make_descriptor();
    EXPECT_THROW(
            build_column_stats_segment(
                    {column_stats_row(0, 5, {min_stat<int64_t>(1, DataType::INT64)}),
                     column_stats_row(0, 3, {min_stat<int64_t>(2, DataType::INT64)})},
                    desc
            ),
            InternalException
    );
}

TEST(ColumnStatsBuildSegmentTest, DataColOffsetOutOfRangeRaises) {
    auto desc = make_descriptor();
    EXPECT_THROW(
            build_column_stats_segment(
                    {column_stats_row(
                            100,
                            199,
                            {ColumnStatValue{ColumnStatTypeInternal::MIN_V1, 999, Value{int64_t{1}, DataType::INT64}}}
                    )},
                    desc
            ),
            InternalException
    );
}

TEST(ColumnStatsBuildSegmentTest, EmptyComponentsRaises) {
    auto desc = make_descriptor();
    EXPECT_THROW(build_column_stats_segment({}, desc), InternalException);
}

TEST(ColumnStatsBuildSegmentTest, RoundTripsSingleTypeComponents) {
    auto desc = make_descriptor();
    std::vector column_stats_rows{
            column_stats_row(100, 199, {min_stat<int64_t>(11, DataType::INT64)}),
            column_stats_row(200, 299, {}),
            column_stats_row(300, 399, {min_stat<int64_t>(33, DataType::INT64)}),
            column_stats_row(400, 499, {})
    };
    auto expected = column_stats_rows;
    auto seg = build_column_stats_segment(std::move(column_stats_rows), desc);
    auto decoded = decode_column_stats_segment(seg);
    expect_column_stats_rows_equal(std::move(decoded), std::move(expected));
}

TEST(ColumnStatsBuildSegmentTest, EmptySegmentDecodesToNoComponents) {
    SegmentInMemory empty_seg;
    EXPECT_TRUE(decode_column_stats_segment(empty_seg).empty());
}

TEST(ColumnStatsBuildSegmentTest, FutureHeaderVersionRaises) {
    auto desc = make_descriptor();
    auto seg = build_column_stats_segment({column_stats_row(100, 199, {min_stat<int64_t>(1, DataType::INT64)})}, desc);

    arcticc::pb2::column_stats_pb2::ColumnStatsHeader header;
    ASSERT_TRUE(seg.metadata()->UnpackTo(&header));
    header.set_version(99);
    google::protobuf::Any any;
    ASSERT_TRUE(any.PackFrom(header));
    seg.reset_metadata();
    seg.set_metadata(std::move(any));

    EXPECT_THROW(decode_column_stats_segment(seg), InternalException);
}

TEST(ColumnStatsBuildSegmentTest, HeaderOffsetOutOfRangeRaises) {
    auto desc = make_descriptor();
    auto seg = build_column_stats_segment({column_stats_row(100, 199, {min_stat<int64_t>(1, DataType::INT64)})}, desc);

    arcticc::pb2::column_stats_pb2::ColumnStatsHeader header;
    ASSERT_TRUE(seg.metadata()->UnpackTo(&header));
    for (auto& [data_col_offset, entry_list] : *header.mutable_stats_by_column()) {
        for (auto& entry : *entry_list.mutable_entries()) {
            entry.set_stats_seg_offset(999);
        }
    }
    google::protobuf::Any any;
    ASSERT_TRUE(any.PackFrom(header));
    seg.reset_metadata();
    seg.set_metadata(std::move(any));

    EXPECT_THROW(decode_column_stats_segment(seg), InternalException);
}

} // namespace arcticdb
