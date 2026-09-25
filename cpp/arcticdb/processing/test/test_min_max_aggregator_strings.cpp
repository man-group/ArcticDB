/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/processing/unsorted_aggregation.hpp>
#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/util/string_stat_encoding.hpp>
#include <arcticdb/util/string_utils.hpp>

#include <optional>
#include <vector>

namespace arcticdb {

namespace {
constexpr size_t data_col_offset = 1;

// A string column holds string pool offsets, not text. std::nullopt in |values| means the reserved
// None sentinel, and |nan_rows| names rows holding the reserved NaN sentinel instead.
ColumnWithStrings build_string_column(
        const std::vector<std::optional<std::string>>& values, DataType data_type = DataType::UTF_DYNAMIC64,
        std::optional<size_t> fixed_width = std::nullopt
) {
    auto string_pool = std::make_shared<StringPool>();
    std::vector<entity::position_t> offsets;
    offsets.reserve(values.size());
    for (const auto& value : values) {
        if (!value.has_value()) {
            offsets.emplace_back(not_a_string());
            continue;
        }
        if (data_type == DataType::UTF_FIXED64) {
            const auto utf32 = util::utf8_to_u32(*value);
            std::u32string padded{utf32};
            if (fixed_width.has_value()) {
                padded.resize(*fixed_width, char32_t{0});
            }
            const std::string_view bytes{
                    reinterpret_cast<const char*>(padded.data()), padded.size() * sizeof(char32_t)
            };
            offsets.emplace_back(string_pool->get(bytes, false).offset());
        } else if (data_type == DataType::ASCII_FIXED64 && fixed_width.has_value()) {
            std::string padded{*value};
            padded.resize(*fixed_width, '\0');
            offsets.emplace_back(string_pool->get(padded, false).offset());
        } else {
            offsets.emplace_back(string_pool->get(*value, false).offset());
        }
    }

    Column col(make_scalar_type(data_type), values.size(), AllocationType::PRESIZED, Sparsity::NOT_PERMITTED);
    memcpy(col.ptr(), offsets.data(), offsets.size() * sizeof(entity::position_t));
    col.set_row_data(static_cast<ssize_t>(values.size()) - 1);
    return {std::move(col), string_pool, "strings"};
}

// Overload placing the NaN sentinel at the given rows, which build_string_column cannot express.
ColumnWithStrings build_string_column_with_nans(
        const std::vector<std::optional<std::string>>& values, const std::vector<size_t>& nan_rows
) {
    auto column = build_string_column(values);
    for (const auto row : nan_rows) {
        column.column_->reference_at<entity::position_t>(row) = nan_placeholder();
    }
    return column;
}

std::optional<Value> stat_value(const std::vector<ColumnStatValue>& stats, ColumnStatTypeInternal type) {
    for (const auto& stat : stats) {
        if (stat.type == type) {
            return stat.value;
        }
    }
    return std::nullopt;
}

uint64_t count_stat(const std::vector<ColumnStatValue>& stats, ColumnStatTypeInternal type) {
    const auto value = stat_value(stats, type);
    return value.has_value() ? value->get<uint64_t>() : 0;
}
} // namespace

TEST(MinMaxAggregatorStrings, EmitsPackedMinMaxUnderStringStatTypes) {
    MinMaxAggregatorData aggregator{data_col_offset};
    aggregator.aggregate(build_string_column({"banana", "apple", "cherry"}));
    const auto stats = aggregator.finalize();

    // The distinct stat types are what tell a reader these cells are packed prefixes rather than
    // ordinary numeric mins in the column's own type.
    ASSERT_FALSE(stat_value(stats, ColumnStatTypeInternal::MIN_V1).has_value());
    ASSERT_FALSE(stat_value(stats, ColumnStatTypeInternal::MAX_V1).has_value());
    const auto min = stat_value(stats, ColumnStatTypeInternal::MIN_STR_V1);
    const auto max = stat_value(stats, ColumnStatTypeInternal::MAX_STR_V1);
    ASSERT_TRUE(min.has_value());
    ASSERT_TRUE(max.has_value());
    ASSERT_EQ(min->get<uint64_t>(), pack_string_stat("apple"));
    ASSERT_EQ(max->get<uint64_t>(), pack_string_stat("cherry"));
    ASSERT_EQ(min->data_type(), DataType::UINT64);
    ASSERT_EQ(max->data_type(), DataType::UINT64);
    ASSERT_EQ(count_stat(stats, ColumnStatTypeInternal::NAN_COUNT_V1), 0);
    ASSERT_EQ(count_stat(stats, ColumnStatTypeInternal::NULL_COUNT_V1), 0);
}

TEST(MinMaxAggregatorStrings, NumericColumnsStillUseNumericStatTypes) {
    MinMaxAggregatorData aggregator{data_col_offset};
    Column col(make_scalar_type(DataType::UINT64), 3, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED);
    const std::vector<uint64_t> values{7, 3, 9};
    memcpy(col.ptr(), values.data(), values.size() * sizeof(uint64_t));
    col.set_row_data(2);
    aggregator.aggregate(ColumnWithStrings{std::move(col), nullptr, "numbers"});
    const auto stats = aggregator.finalize();

    // A UINT64 column's min is bit-identical to a packed string min, so only the stat type
    // distinguishes them. This is the regression test for tagging every uint64 min as a string.
    ASSERT_EQ(stat_value(stats, ColumnStatTypeInternal::MIN_V1)->get<uint64_t>(), 3);
    ASSERT_EQ(stat_value(stats, ColumnStatTypeInternal::MAX_V1)->get<uint64_t>(), 9);
    ASSERT_FALSE(stat_value(stats, ColumnStatTypeInternal::MIN_STR_V1).has_value());
    ASSERT_FALSE(stat_value(stats, ColumnStatTypeInternal::MAX_STR_V1).has_value());
}

TEST(MinMaxAggregatorStrings, MinMaxIsBytewiseNotLengthOrdered) {
    MinMaxAggregatorData aggregator{data_col_offset};
    // "az" must beat "b" as a minimum. Packing is monotonic, so std::min on the packed values gets
    // this right without any string comparison in the aggregator.
    aggregator.aggregate(build_string_column({"b", "az", "aza"}));
    const auto stats = aggregator.finalize();

    ASSERT_EQ(stat_value(stats, ColumnStatTypeInternal::MIN_STR_V1)->get<uint64_t>(), pack_string_stat("az"));
    ASSERT_EQ(stat_value(stats, ColumnStatTypeInternal::MAX_STR_V1)->get<uint64_t>(), pack_string_stat("b"));
}

TEST(MinMaxAggregatorStrings, RepeatedOffsetsAreSkippedWithoutLosingMinMax) {
    MinMaxAggregatorData aggregator{data_col_offset};
    // Every value repeats, and both the min and the max are among the repeats. A repeated pool offset
    // is skipped, so this fails if the skip happens before the value reaches min/max.
    aggregator.aggregate(build_string_column({"cherry", "apple", "cherry", "apple", "banana", "apple"}));
    const auto stats = aggregator.finalize();

    ASSERT_EQ(stat_value(stats, ColumnStatTypeInternal::MIN_STR_V1)->get<uint64_t>(), pack_string_stat("apple"));
    ASSERT_EQ(stat_value(stats, ColumnStatTypeInternal::MAX_STR_V1)->get<uint64_t>(), pack_string_stat("cherry"));
}

TEST(MinMaxAggregatorStrings, RepeatedSentinelsAreStillCountedPerRow) {
    MinMaxAggregatorData aggregator{data_col_offset};
    // Sentinels share one reserved offset, so deduplicating offsets must not reach them: the counts
    // are per row, unlike min/max.
    aggregator.aggregate(
            build_string_column_with_nans({"alpha", std::nullopt, "alpha", std::nullopt, "nan", "nan"}, {4, 5})
    );
    const auto stats = aggregator.finalize();

    ASSERT_EQ(count_stat(stats, ColumnStatTypeInternal::NULL_COUNT_V1), 2);
    ASSERT_EQ(count_stat(stats, ColumnStatTypeInternal::NAN_COUNT_V1), 2);
    ASSERT_EQ(stat_value(stats, ColumnStatTypeInternal::MIN_STR_V1)->get<uint64_t>(), pack_string_stat("alpha"));
    ASSERT_EQ(stat_value(stats, ColumnStatTypeInternal::MAX_STR_V1)->get<uint64_t>(), pack_string_stat("alpha"));
}

TEST(MinMaxAggregatorStrings, NoneSentinelCountsAsNull) {
    MinMaxAggregatorData aggregator{data_col_offset};
    aggregator.aggregate(build_string_column({"beta", std::nullopt, "alpha", std::nullopt}));
    const auto stats = aggregator.finalize();

    ASSERT_EQ(count_stat(stats, ColumnStatTypeInternal::NULL_COUNT_V1), 2);
    ASSERT_EQ(count_stat(stats, ColumnStatTypeInternal::NAN_COUNT_V1), 0);
    // Sentinels must not participate in min/max, or the min would be a huge offset value.
    ASSERT_EQ(stat_value(stats, ColumnStatTypeInternal::MIN_STR_V1)->get<uint64_t>(), pack_string_stat("alpha"));
    ASSERT_EQ(stat_value(stats, ColumnStatTypeInternal::MAX_STR_V1)->get<uint64_t>(), pack_string_stat("beta"));
}

TEST(MinMaxAggregatorStrings, NanSentinelCountsAsNan) {
    MinMaxAggregatorData aggregator{data_col_offset};
    aggregator.aggregate(build_string_column_with_nans({"beta", "ignored", "alpha"}, {1}));
    const auto stats = aggregator.finalize();

    ASSERT_EQ(count_stat(stats, ColumnStatTypeInternal::NAN_COUNT_V1), 1);
    ASSERT_EQ(count_stat(stats, ColumnStatTypeInternal::NULL_COUNT_V1), 0);
    ASSERT_EQ(stat_value(stats, ColumnStatTypeInternal::MIN_STR_V1)->get<uint64_t>(), pack_string_stat("alpha"));
}

TEST(MinMaxAggregatorStrings, AllNoneRecordsCountsWithoutMinMax) {
    MinMaxAggregatorData aggregator{data_col_offset};
    aggregator.aggregate(build_string_column({std::nullopt, std::nullopt}));
    const auto stats = aggregator.finalize();

    // Unlike floats there is no sentinel to record as the min: a slice of only nulls legitimately
    // has no minimum, so the counts must stand alone.
    ASSERT_FALSE(stat_value(stats, ColumnStatTypeInternal::MIN_STR_V1).has_value());
    ASSERT_FALSE(stat_value(stats, ColumnStatTypeInternal::MAX_STR_V1).has_value());
    ASSERT_EQ(count_stat(stats, ColumnStatTypeInternal::NULL_COUNT_V1), 2);
}

TEST(MinMaxAggregatorStrings, AllNanRecordsCountsWithoutMinMax) {
    MinMaxAggregatorData aggregator{data_col_offset};
    aggregator.aggregate(build_string_column_with_nans({"ignored", "ignored_too"}, {0, 1}));
    const auto stats = aggregator.finalize();

    // finalize() returns early when there is no min and no nulls, which would silently drop a
    // nan-only slice's counts. Unreachable for numerics, reachable here.
    ASSERT_FALSE(stat_value(stats, ColumnStatTypeInternal::MIN_STR_V1).has_value());
    ASSERT_EQ(count_stat(stats, ColumnStatTypeInternal::NAN_COUNT_V1), 2);
    ASSERT_EQ(count_stat(stats, ColumnStatTypeInternal::NULL_COUNT_V1), 0);
}

TEST(MinMaxAggregatorStrings, AbsentColumnRecordsNothing) {
    MinMaxAggregatorData aggregator{data_col_offset};
    aggregator.aggregate(build_string_column({}));
    ASSERT_TRUE(aggregator.finalize().empty());
}

TEST(MinMaxAggregatorStrings, FixedWidthPaddingIsStripped) {
    MinMaxAggregatorData aggregator{data_col_offset};
    aggregator.aggregate(build_string_column({"ab"}, DataType::UTF_FIXED64, /*fixed_width=*/8));
    const auto stats = aggregator.finalize();

    // Without stripping, the trailing nulls would pack as a seven byte value rather than "ab", and
    // the stat would compare wrongly against a query for "ab".
    ASSERT_EQ(stat_value(stats, ColumnStatTypeInternal::MIN_STR_V1)->get<uint64_t>(), pack_string_stat("ab"));
}

TEST(MinMaxAggregatorStrings, AsciiFixedWidthPaddingIsStripped) {
    MinMaxAggregatorData aggregator{data_col_offset};
    aggregator.aggregate(build_string_column({"ab", "cd"}, DataType::ASCII_FIXED64, /*fixed_width=*/8));
    const auto stats = aggregator.finalize();

    // The pool holds eight null padded bytes per value. The aggregator hands them to the packer
    // as-is, so the packer is what has to strip them - relying on the caller to do it would make the
    // stored stat depend on how wide the caller thinks a character is.
    ASSERT_EQ(stat_value(stats, ColumnStatTypeInternal::MIN_STR_V1)->get<uint64_t>(), pack_string_stat("ab"));
    ASSERT_EQ(stat_value(stats, ColumnStatTypeInternal::MAX_STR_V1)->get<uint64_t>(), pack_string_stat("cd"));
}

TEST(MinMaxAggregatorStrings, FixedWidthRepeatedOffsetsAreSkippedWithoutLosingMinMax) {
    MinMaxAggregatorData aggregator{data_col_offset};
    aggregator.aggregate(build_string_column({"zz", "aa", "zz", "aa"}, DataType::UTF_FIXED64, /*fixed_width=*/8));
    const auto stats = aggregator.finalize();

    ASSERT_EQ(stat_value(stats, ColumnStatTypeInternal::MIN_STR_V1)->get<uint64_t>(), pack_string_stat("aa"));
    ASSERT_EQ(stat_value(stats, ColumnStatTypeInternal::MAX_STR_V1)->get<uint64_t>(), pack_string_stat("zz"));
}

TEST(MinMaxAggregatorStrings, FixedWidthUtf32PacksLikeUtf8) {
    MinMaxAggregatorData fixed_aggregator{data_col_offset};
    fixed_aggregator.aggregate(build_string_column({"日本"}, DataType::UTF_FIXED64, /*fixed_width=*/8));
    MinMaxAggregatorData dynamic_aggregator{data_col_offset};
    dynamic_aggregator.aggregate(build_string_column({"日本"}, DataType::UTF_DYNAMIC64));

    // Makes a dynamic schema column comparable across slices that differ in string type.
    ASSERT_EQ(
            stat_value(fixed_aggregator.finalize(), ColumnStatTypeInternal::MIN_STR_V1)->get<uint64_t>(),
            stat_value(dynamic_aggregator.finalize(), ColumnStatTypeInternal::MIN_STR_V1)->get<uint64_t>()
    );
}

TEST(MinMaxAggregatorStrings, FixedWidthNonAsciiOrdersByCodepointNotUtf32Bytes) {
    const std::string e_acute{"é"};  // U+00E9, UTF-8 c3 a9, UTF-32LE e9 00 00 00
    const std::string a_macron{"Ā"}; // U+0100, UTF-8 c4 80, UTF-32LE 00 01 00 00
    // These two differ above the low byte, so as little-endian UTF-32 they compare in the opposite
    // order to their codepoints. Packing the pool bytes without transcoding would swap min and max,
    // and the same values in a UTF_DYNAMIC slice of the same column would disagree.
    ASSERT_LT(pack_string_stat(e_acute), pack_string_stat(a_macron));

    MinMaxAggregatorData fixed_aggregator{data_col_offset};
    fixed_aggregator.aggregate(build_string_column({e_acute, a_macron}, DataType::UTF_FIXED64, /*fixed_width=*/4));
    const auto fixed_stats = fixed_aggregator.finalize();
    MinMaxAggregatorData dynamic_aggregator{data_col_offset};
    dynamic_aggregator.aggregate(build_string_column({e_acute, a_macron}));
    const auto dynamic_stats = dynamic_aggregator.finalize();

    ASSERT_EQ(stat_value(fixed_stats, ColumnStatTypeInternal::MIN_STR_V1)->get<uint64_t>(), pack_string_stat(e_acute));
    ASSERT_EQ(stat_value(fixed_stats, ColumnStatTypeInternal::MAX_STR_V1)->get<uint64_t>(), pack_string_stat(a_macron));
    ASSERT_EQ(
            stat_value(fixed_stats, ColumnStatTypeInternal::MIN_STR_V1)->get<uint64_t>(),
            stat_value(dynamic_stats, ColumnStatTypeInternal::MIN_STR_V1)->get<uint64_t>()
    );
    ASSERT_EQ(
            stat_value(fixed_stats, ColumnStatTypeInternal::MAX_STR_V1)->get<uint64_t>(),
            stat_value(dynamic_stats, ColumnStatTypeInternal::MAX_STR_V1)->get<uint64_t>()
    );
}

TEST(MinMaxAggregatorStrings, MaxTruncatedMidCodepointStillBracketsItsOwnValue) {
    MinMaxAggregatorData aggregator{data_col_offset};
    aggregator.aggregate(build_string_column({"日本語", "日本"}, DataType::UTF_FIXED64, /*fixed_width=*/4));
    const auto stats = aggregator.finalize();
    const auto min = stat_value(stats, ColumnStatTypeInternal::MIN_STR_V1)->get<uint64_t>();
    const auto max = stat_value(stats, ColumnStatTypeInternal::MAX_STR_V1)->get<uint64_t>();

    // 日本語 is nine UTF-8 bytes, so the max keeps seven of them and splits 語 mid-codepoint. The
    // stored bytes are then not valid UTF-8, which is fine because comparison is bytewise, but the
    // value must still sort within its own recorded range or a query for it would prune this slice.
    ASSERT_EQ(min, pack_string_stat("日本"));
    ASSERT_EQ(max, pack_string_stat("日本語"));
    ASSERT_TRUE(unpack_string(max).was_truncated);
    ASSERT_FALSE(unpack_string(min).was_truncated);
    ASSERT_GE(pack_string_stat("日本語"), min);
    ASSERT_LE(pack_string_stat("日本語"), max);
    // Anything sharing the truncated prefix is equally unprunable, which is the conservative side.
    ASSERT_LE(pack_string_stat("日本語です"), max);
}

TEST(MinMaxAggregatorStrings, EmptyTypeSliceRecordsNothing) {
    MinMaxAggregatorData aggregator{data_col_offset};
    // A row slice in which a string column is entirely None arrives as EMPTYVAL. It carries no row
    // count either - set_row_data is a no-op for empty type - so the column is absent from the
    // stats for this slice rather than present with a null count.
    Column col(make_scalar_type(DataType::EMPTYVAL), 0, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    col.set_row_data(1);
    aggregator.aggregate(ColumnWithStrings{std::move(col), std::make_shared<StringPool>(), "strings"});

    ASSERT_TRUE(aggregator.finalize().empty());
}

TEST(MinMaxAggregatorStrings, AggregatesAcrossMultipleColumnsInASlice) {
    MinMaxAggregatorData aggregator{data_col_offset};
    aggregator.aggregate(build_string_column({"m", "d"}));
    aggregator.aggregate(build_string_column({"z", "a"}));
    const auto stats = aggregator.finalize();

    // One aggregator sees every column block of the row slice, so min/max must span all of them.
    ASSERT_EQ(stat_value(stats, ColumnStatTypeInternal::MIN_STR_V1)->get<uint64_t>(), pack_string_stat("a"));
    ASSERT_EQ(stat_value(stats, ColumnStatTypeInternal::MAX_STR_V1)->get<uint64_t>(), pack_string_stat("z"));
}

} // namespace arcticdb
