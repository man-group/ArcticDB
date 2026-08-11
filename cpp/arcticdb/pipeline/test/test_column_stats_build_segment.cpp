/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <google/protobuf/any.pb.h>

namespace arcticdb {

namespace {
constexpr size_t price_data_col_offset = 1;
constexpr size_t volume_data_col_offset = 2;

template<typename T>
ColumnStatValue min_stat(T value, DataType data_type, const char* name = "v1_MIN(price)") {
    return ColumnStatValue{name, ColumnStatTypeInternal::MIN_V1, price_data_col_offset, Value{value, data_type}};
}

ColumnStatsComponent component(timestamp start, timestamp end, std::vector<ColumnStatValue> stats) {
    return ColumnStatsComponent{start, end, std::move(stats)};
}

position_t column_index(const SegmentInMemory& seg, std::string_view name) {
    auto idx = seg.column_index(name);
    EXPECT_TRUE(idx.has_value()) << "column " << name << " missing";
    return static_cast<position_t>(*idx);
}
} // namespace

TEST(ColumnStatsBuildSegmentTest, IndexColumnsAndRowOrder) {
    // Deliberately out of index order
    auto seg = build_column_stats_segment(
            {component(300, 399, {min_stat<int64_t>(3, DataType::INT64)}),
             component(100, 199, {min_stat<int64_t>(1, DataType::INT64)}),
             component(200, 299, {min_stat<int64_t>(2, DataType::INT64)})}
    );

    ASSERT_EQ(seg.row_count(), 3);
    EXPECT_EQ(column_index(seg, start_index_column_name), 0);
    EXPECT_EQ(column_index(seg, end_index_column_name), 1);

    const auto min_col = column_index(seg, "v1_MIN(price)");
    for (auto&& [row, expected_start] : folly::enumerate(std::vector<timestamp>{100, 200, 300})) {
        EXPECT_EQ(seg.scalar_at<timestamp>(static_cast<position_t>(row), 0), expected_start);
        EXPECT_EQ(seg.scalar_at<timestamp>(static_cast<position_t>(row), 1), expected_start + 99);
        EXPECT_EQ(seg.scalar_at<int64_t>(static_cast<position_t>(row), min_col), static_cast<int64_t>(row) + 1);
    }
}

TEST(ColumnStatsBuildSegmentTest, HeaderOffsetsMatchColumnPositions) {
    auto seg = build_column_stats_segment({component(
            100,
            199,
            {min_stat<int64_t>(1, DataType::INT64),
             ColumnStatValue{
                     "v1_MAX(price)",
                     ColumnStatTypeInternal::MAX_V1,
                     price_data_col_offset,
                     Value{int64_t{9}, DataType::INT64}
             },
             ColumnStatValue{
                     "v1_MIN(volume)",
                     ColumnStatTypeInternal::MIN_V1,
                     volume_data_col_offset,
                     Value{uint64_t{5}, DataType::UINT64}
             }}
    )});

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

// Dynamic schema can give a different type per row slice for the same stat. The column must widen
// to the common type and every value must survive the cast.
TEST(ColumnStatsBuildSegmentTest, WidensToCommonTypeUnsignedIntegers) {
    auto seg = build_column_stats_segment(
            {component(100, 199, {min_stat<uint8_t>(7, DataType::UINT8)}),
             component(200, 299, {min_stat<uint16_t>(1000, DataType::UINT16)})}
    );

    const auto col = column_index(seg, "v1_MIN(price)");
    const auto expected_type =
            has_valid_common_type(make_scalar_type(DataType::UINT8), make_scalar_type(DataType::UINT16));
    ASSERT_TRUE(expected_type.has_value());
    EXPECT_EQ(seg.column(col).type(), *expected_type);
    EXPECT_EQ(seg.scalar_at<uint16_t>(0, col), 7);
    EXPECT_EQ(seg.scalar_at<uint16_t>(1, col), 1000);
}

TEST(ColumnStatsBuildSegmentTest, WidensToCommonTypeMixedSignIntegers) {
    auto seg = build_column_stats_segment(
            {component(100, 199, {min_stat<uint16_t>(1000, DataType::UINT16)}),
             component(200, 299, {min_stat<int32_t>(-1002, DataType::INT32)})}
    );

    const auto col = column_index(seg, "v1_MIN(price)");
    const auto expected_type =
            has_valid_common_type(make_scalar_type(DataType::UINT16), make_scalar_type(DataType::INT32));
    ASSERT_TRUE(expected_type.has_value());
    EXPECT_EQ(seg.column(col).type(), *expected_type);
    EXPECT_EQ(seg.scalar_at<int32_t>(0, col), 1000);
    EXPECT_EQ(seg.scalar_at<int32_t>(1, col), -1002);
}

TEST(ColumnStatsBuildSegmentTest, WidensToCommonTypeFloats) {
    auto seg = build_column_stats_segment(
            {component(100, 199, {min_stat<float>(1.5F, DataType::FLOAT32)}),
             component(200, 299, {min_stat<double>(2.25, DataType::FLOAT64)})}
    );

    const auto col = column_index(seg, "v1_MIN(price)");
    EXPECT_EQ(seg.column(col).type(), make_scalar_type(DataType::FLOAT64));
    EXPECT_EQ(seg.scalar_at<double>(0, col), 1.5);
    EXPECT_EQ(seg.scalar_at<double>(1, col), 2.25);
}

// A stat missing from some row slices stays sparse, with the absent rows marked absent rather than
// shifting the values of the rows that do have it.
TEST(ColumnStatsBuildSegmentTest, StatAbsentFromSomeComponentsIsSparse) {
    auto seg = build_column_stats_segment(
            {component(100, 199, {min_stat<int64_t>(11, DataType::INT64)}),
             component(200, 299, {}),
             component(300, 399, {min_stat<int64_t>(33, DataType::INT64)}),
             component(400, 499, {})}
    );

    ASSERT_EQ(seg.row_count(), 4);
    const auto col = column_index(seg, "v1_MIN(price)");
    EXPECT_TRUE(seg.column(col).is_sparse());
    EXPECT_EQ(seg.scalar_at<int64_t>(0, col), 11);
    EXPECT_FALSE(seg.scalar_at<int64_t>(1, col).has_value());
    EXPECT_EQ(seg.scalar_at<int64_t>(2, col), 33);
    EXPECT_FALSE(seg.scalar_at<int64_t>(3, col).has_value());
    // The index columns stay dense across all rows
    EXPECT_EQ(seg.scalar_at<timestamp>(3, 0), 400);
    EXPECT_EQ(seg.scalar_at<timestamp>(3, 1), 499);
}

TEST(ColumnStatsBuildSegmentTest, NoCommonTypeRaises) {
    EXPECT_THROW(
            build_column_stats_segment(
                    {component(100, 199, {min_stat<uint64_t>(1, DataType::UINT64)}),
                     component(200, 299, {min_stat<int64_t>(-1, DataType::INT64)})}
            ),
            InternalException
    );
}

TEST(ColumnStatsBuildSegmentTest, EmptyComponentsRaises) {
    EXPECT_THROW(build_column_stats_segment({}), InternalException);
}

} // namespace arcticdb
