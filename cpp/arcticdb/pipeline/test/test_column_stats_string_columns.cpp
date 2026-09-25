/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/string_stat_encoding.hpp>

#include <string>
#include <vector>

namespace arcticdb {

namespace {
constexpr size_t fruit_data_col_offset = 1;

const std::vector<DataType> string_data_types{
        DataType::ASCII_FIXED64,
        DataType::ASCII_DYNAMIC64,
        DataType::UTF_FIXED64,
        DataType::UTF_DYNAMIC64
};

StreamDescriptor make_descriptor(DataType fruit_type) {
    StreamDescriptor desc{"sym"};
    desc.add_scalar_field(DataType::UINT64, "index");
    desc.add_scalar_field(fruit_type, "fruit");
    return desc;
}

TimeseriesDescriptor make_tsd(const StreamDescriptor& desc) {
    proto::descriptors::NormalizationMetadata norm_meta;
    return make_timeseries_descriptor(0, desc, norm_meta, std::nullopt, std::nullopt, false);
}

ColumnStatValue packed_stat(ColumnStatTypeInternal type, std::string_view value) {
    return ColumnStatValue{type, fruit_data_col_offset, Value{pack_string_stat(value), DataType::UINT64}};
}

ColumnStatsRow string_minmax_row(uint64_t start_row, uint64_t end_row, std::string_view min, std::string_view max) {
    return ColumnStatsRow{
            pipelines::RowRange{start_row, end_row},
            {packed_stat(ColumnStatTypeInternal::MIN_STR_V1, min), packed_stat(ColumnStatTypeInternal::MAX_STR_V1, max)}
    };
}

position_t column_index(const SegmentInMemory& seg, std::string_view name) {
    auto idx = seg.column_index(name);
    util::check(idx.has_value(), "column {} missing", name);
    return static_cast<position_t>(*idx);
}
} // namespace

TEST(ColumnStatsStringColumns, EveryStringTypeIsEligibleForStats) {
    for (const auto data_type : string_data_types) {
        const auto desc = make_descriptor(data_type);
        const auto map = ColumnStats{make_tsd(desc)}.to_map();
        ASSERT_TRUE(map.contains("fruit")) << datatype_to_str(data_type);
        EXPECT_EQ(map.at("fruit"), std::unordered_set<std::string>{"MINMAX"}) << datatype_to_str(data_type);
    }
}

// UTF_DYNAMIC32 only ever exists on the Arrow output path, so it never reaches stats generation and
// admitting it would mean claiming stats for a column that cannot produce them.
TEST(ColumnStatsStringColumns, ArrowOutputOnlyStringTypeIsNotEligible) {
    const auto desc = make_descriptor(DataType::UTF_DYNAMIC32);
    const auto map = ColumnStats{make_tsd(desc)}.to_map();
    EXPECT_FALSE(map.contains("fruit"));
    // The numeric index column is still eligible, so an empty map would prove nothing here.
    EXPECT_TRUE(map.contains("index"));
}

// The reason the proto needed distinct MIN_STR_V1/MAX_STR_V1 values: the stat column's type is
// resolved from the stat type alone. Inheriting the data column's string type would store the packed
// value as a string pool offset into a segment that has no string pool.
TEST(ColumnStatsStringColumns, StringStatColumnsAreUint64NotTheDataColumnType) {
    for (const auto data_type : string_data_types) {
        const auto desc = make_descriptor(data_type);
        auto seg = build_column_stats_segment({string_minmax_row(0, 100, "apple", "cherry")}, desc);

        const auto min_col = column_index(seg, "v1_MIN_STR(fruit)");
        const auto max_col = column_index(seg, "v1_MAX_STR(fruit)");
        EXPECT_EQ(seg.column(min_col).type(), make_scalar_type(DataType::UINT64)) << datatype_to_str(data_type);
        EXPECT_EQ(seg.column(max_col).type(), make_scalar_type(DataType::UINT64)) << datatype_to_str(data_type);
        EXPECT_EQ(seg.scalar_at<uint64_t>(0, min_col), pack_string_stat("apple")) << datatype_to_str(data_type);
        EXPECT_EQ(seg.scalar_at<uint64_t>(0, max_col), pack_string_stat("cherry")) << datatype_to_str(data_type);
    }
}

TEST(ColumnStatsStringColumns, StringStatColumnNamesAreDistinctFromNumericOnes) {
    EXPECT_EQ(to_segment_column_name("fruit", ColumnStatTypeInternal::MIN_STR_V1), "v1_MIN_STR(fruit)");
    EXPECT_EQ(to_segment_column_name("fruit", ColumnStatTypeInternal::MAX_STR_V1), "v1_MAX_STR(fruit)");
    // A reader must be able to tell a packed prefix from an ordinary numeric min by name alone.
    EXPECT_EQ(to_segment_column_name("fruit", ColumnStatTypeInternal::MIN_V1), "v1_MIN(fruit)");
    EXPECT_EQ(to_segment_column_name("fruit", ColumnStatTypeInternal::MAX_V1), "v1_MAX(fruit)");
}

// A string column can hold both packed min/max and the count stats, and the counts must keep their
// own UINT64 type resolution rather than being confused with the packed ones.
TEST(ColumnStatsStringColumns, CountStatsCoexistWithPackedStatsOnAStringColumn) {
    const auto desc = make_descriptor(DataType::UTF_DYNAMIC64);
    auto seg = build_column_stats_segment(
            {ColumnStatsRow{
                    pipelines::RowRange{0, 100},
                    {packed_stat(ColumnStatTypeInternal::MIN_STR_V1, "apple"),
                     packed_stat(ColumnStatTypeInternal::MAX_STR_V1, "cherry"),
                     ColumnStatValue{
                             ColumnStatTypeInternal::NAN_COUNT_V1,
                             fruit_data_col_offset,
                             Value{uint64_t{2}, DataType::UINT64}
                     },
                     ColumnStatValue{
                             ColumnStatTypeInternal::NULL_COUNT_V1,
                             fruit_data_col_offset,
                             Value{uint64_t{3}, DataType::UINT64}
                     }}
            }},
            desc
    );

    EXPECT_EQ(seg.scalar_at<uint64_t>(0, column_index(seg, "v1_NAN_COUNT(fruit)")), 2);
    EXPECT_EQ(seg.scalar_at<uint64_t>(0, column_index(seg, "v1_NULL_COUNT(fruit)")), 3);
    EXPECT_EQ(seg.scalar_at<uint64_t>(0, column_index(seg, "v1_MIN_STR(fruit)")), pack_string_stat("apple"));
}

// Incremental extend re-reads the existing segment before rewriting it, so packed stats must survive
// the round trip unchanged.
TEST(ColumnStatsStringColumns, PackedStatsRoundTripThroughTheSegment) {
    const auto desc = make_descriptor(DataType::UTF_DYNAMIC64);
    auto seg = build_column_stats_segment(
            {string_minmax_row(0, 100, "apple", "cherry"), string_minmax_row(100, 200, "damson", "elderberry")}, desc
    );

    const auto decoded = decode_column_stats_segment(seg);
    ASSERT_EQ(decoded.size(), 2);
    for (const auto& row : decoded) {
        ASSERT_EQ(row.stats.size(), 2);
        for (const auto& stat : row.stats) {
            EXPECT_EQ(stat.data_col_offset, fruit_data_col_offset);
            EXPECT_EQ(stat.value.data_type(), DataType::UINT64);
        }
    }
    const auto expected_min = pack_string_stat("damson");
    const auto second_row_min = std::ranges::find_if(decoded.at(1).stats, [](const auto& stat) {
        return stat.type == ColumnStatTypeInternal::MIN_STR_V1;
    });
    ASSERT_NE(second_row_min, decoded.at(1).stats.end());
    EXPECT_EQ(second_row_min->value.get<uint64_t>(), expected_min);
}

// Without this the header round trip warns and drops the entry, so get_column_stats_info_experimental
// would report a string column as having no stats and an extend would not know to regenerate them.
TEST(ColumnStatsStringColumns, HeaderRoundTripReportsMinmaxForStringColumns) {
    const auto desc = make_descriptor(DataType::UTF_DYNAMIC64);
    auto seg = build_column_stats_segment({string_minmax_row(0, 100, "apple", "cherry")}, desc);

    ASSERT_TRUE(seg.metadata());
    arcticc::pb2::column_stats_pb2::ColumnStatsHeader header;
    ASSERT_TRUE(seg.metadata()->UnpackTo(&header));

    const auto map = ColumnStats{header, make_tsd(desc)}.to_map();
    ASSERT_TRUE(map.contains("fruit"));
    EXPECT_EQ(map.at("fruit"), std::unordered_set<std::string>{"MINMAX"});
}

// The stats a string column asks for must be the stats it gets back out of the header, or a create
// followed by an extend would loop forever adding stats the header never reports.
TEST(ColumnStatsStringColumns, EligibilityAndHeaderRoundTripAgree) {
    const auto desc = make_descriptor(DataType::UTF_DYNAMIC64);
    const auto tsd = make_tsd(desc);
    auto seg = build_column_stats_segment(
            {ColumnStatsRow{
                    pipelines::RowRange{0, 100},
                    {packed_stat(ColumnStatTypeInternal::MIN_STR_V1, "apple"),
                     packed_stat(ColumnStatTypeInternal::MAX_STR_V1, "cherry"),
                     ColumnStatValue{ColumnStatTypeInternal::MIN_V1, 0, Value{uint64_t{0}, DataType::UINT64}},
                     ColumnStatValue{ColumnStatTypeInternal::MAX_V1, 0, Value{uint64_t{99}, DataType::UINT64}}}
            }},
            desc
    );
    arcticc::pb2::column_stats_pb2::ColumnStatsHeader header;
    ASSERT_TRUE(seg.metadata()->UnpackTo(&header));

    const ColumnStats from_header{header, tsd};
    const ColumnStats from_eligibility{tsd};
    EXPECT_EQ(from_header, from_eligibility);
}

} // namespace arcticdb
