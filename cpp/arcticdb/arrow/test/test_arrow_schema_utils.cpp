/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <folly/container/Enumerate.h>
#include <google/protobuf/util/message_differencer.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <arcticdb/arrow/arrow_schema_utils.hpp>

using namespace arcticdb;
using namespace arcticdb::entity;
using namespace google::protobuf::util;
using NormalizationMetadata = arcticdb::proto::descriptors::NormalizationMetadata;
using Index = NormalizationMetadata::PandasIndex;
using MultiIndex = NormalizationMetadata::PandasMultiIndex;

namespace {

// ColumnName protobuf message, plus name in stream descriptor and a data type
struct ColumnSpec {
    std::string name{""};
    DataType data_type{DataType::INT64};
    bool is_none{false};
    bool is_empty{false};
    std::string original_name{""};
    bool is_int{false};
};

enum class ObjectType { DF, SERIES };

Index row_count_index(int64_t start = 0, int64_t step = 1) {
    Index index;
    index.set_start(start);
    index.set_step(step);
    return index;
}

Index timestamp_index(
        const std::variant<std::monostate, std::string, int>& name, std::optional<std::string> tz = std::nullopt
) {
    Index index;
    index.set_is_physically_stored(true);
    if (std::holds_alternative<std::monostate>(name)) {
        index.set_fake_name(true);
    } else if (std::holds_alternative<std::string>(name)) {
        index.set_name(std::get<std::string>(name));
    } else { // int
        index.set_name(fmt::format("{}", std::get<int>(name)));
        index.set_is_int(true);
    }

    if (tz.has_value()) {
        index.set_tz(*tz);
    }
    return index;
}

MultiIndex multiindex(const std::vector<std::variant<std::monostate, std::string, int>>& names) {
    MultiIndex multiindex;
    multiindex.set_field_count(names.size() - 1);
    for (const auto& [idx, name] : folly::enumerate(names)) {
        if (idx == 0) {
            if (std::holds_alternative<std::monostate>(name)) {
                multiindex.add_fake_field_pos(0);
            } else if (std::holds_alternative<std::string>(name)) {
                multiindex.set_name(std::get<std::string>(name));
            } else { // int
                multiindex.set_name(fmt::format("{}", std::get<int>(name)));
                multiindex.set_is_int(true);
            }
        } else {
            if (std::holds_alternative<std::monostate>(name)) {
                multiindex.add_fake_field_pos(idx);
            }
        }
    }
    return multiindex;
}

OutputSchema generate_schema(
        ObjectType object_type, bool has_synthetic_columns, std::variant<Index, MultiIndex> index,
        std::vector<ColumnSpec> columns
) {
    NormalizationMetadata norm;
    NormalizationMetadata::Pandas* common;
    StreamDescriptor desc;
    if (object_type == ObjectType::DF) {
        norm.mutable_df()->set_has_synthetic_columns(has_synthetic_columns);
        common = norm.mutable_df()->mutable_common();
    } else { // SERIES
        norm.mutable_series()->set_has_synthetic_columns(has_synthetic_columns);
        common = norm.mutable_series()->mutable_common();
        if (!has_synthetic_columns) {
            common->set_has_name(true);
            common->set_name(columns.back().name == "__empty__0" ? "" : columns.back().name);
        }
    }
    bool range_index{false};
    if (std::holds_alternative<Index>(index)) {
        *common->mutable_index() = std::get<Index>(index);
        if (common->index().is_physically_stored()) {
            desc.set_index({IndexDescriptor::Type::TIMESTAMP, 1});
        } else {
            range_index = true;
            desc.set_index({IndexDescriptor::Type::ROWCOUNT, 0});
        }
    } else { // MultiIndex
        *common->mutable_multi_index() = std::get<MultiIndex>(index);
        if (columns.front().data_type == DataType::NANOSECONDS_UTC64) {
            desc.set_index({IndexDescriptor::Type::TIMESTAMP, 1});
        } else {
            desc.set_index({IndexDescriptor::Type::ROWCOUNT, 0});
        }
    }
    for (const auto& [idx, column] : folly::enumerate(columns)) {
        std::string col_name = column.name;
        if (idx > 0 || range_index) {
            (*common->mutable_col_names())[col_name].set_is_none(column.is_none);
            (*common->mutable_col_names())[col_name].set_is_empty(column.is_empty);
            (*common->mutable_col_names())[col_name].set_original_name(column.original_name);
            (*common->mutable_col_names())[col_name].set_is_int(column.is_int);
        }
        desc.add_scalar_field(column.data_type, col_name);
    }
    return {std::move(desc), std::move(norm)};
}

} // namespace

using NoOpParams = std::tuple<
        ObjectType, std::optional<std::vector<std::string>>, std::variant<Index, MultiIndex>, std::vector<ColumnSpec>>;

struct ArrowSchemaCompatNoOpFixture : public ::testing::TestWithParam<NoOpParams> {
    static ObjectType object_type() { return std::get<0>(GetParam()); }
    static std::optional<std::vector<std::string>> index_columns() { return std::get<1>(GetParam()); }
    static std::variant<Index, MultiIndex> index() { return std::get<2>(GetParam()); }
    static std::vector<ColumnSpec> columns() { return std::get<3>(GetParam()); }
};

TEST_P(ArrowSchemaCompatNoOpFixture, Test) {
    auto input_schema = generate_schema(object_type(), false, index(), columns());
    ASSERT_FALSE(make_schema_arrow_compatible(input_schema, index_columns()).has_value());
}

INSTANTIATE_TEST_SUITE_P(
        ArrowSchemaCompatNoOp, ArrowSchemaCompatNoOpFixture,
        ::testing::Values(
                // RangeIndex tests
                NoOpParams(
                        ObjectType::DF, std::nullopt, row_count_index(5, 10), {{.name = "col", .original_name = "col"}}
                ),
                NoOpParams(
                        ObjectType::SERIES, std::nullopt, row_count_index(10, 5),
                        {{.name = "col", .original_name = "col"}}
                ),
                // Timeseries tests
                // Explicit index rename not provided
                NoOpParams(
                        ObjectType::DF, std::nullopt, timestamp_index("ts", "UTC"),
                        {{.name = "ts", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}}
                ),
                NoOpParams(
                        ObjectType::SERIES, std::nullopt, timestamp_index("ts", "UTC"),
                        {{.name = "ts", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}}
                ),
                // Explicit index rename provided, but matches existing name
                NoOpParams(
                        ObjectType::DF, {{"ts"}}, timestamp_index("ts"),
                        {{.name = "ts", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}}
                ),
                NoOpParams(
                        ObjectType::SERIES, {{"ts"}}, timestamp_index("ts", "UTC"),
                        {{.name = "ts", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}}
                ),
                // MultiIndex tests
                // Explicit index renames not provided
                NoOpParams(
                        ObjectType::DF, std::nullopt, multiindex({"ts", "ticker"}),
                        {{.name = "ts", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "__idx__ticker", .original_name = "__idx__ticker"},
                         {.name = "col", .original_name = "col"}}
                ),
                NoOpParams(
                        ObjectType::SERIES, std::nullopt, multiindex({"ts", "ticker"}),
                        {{.name = "ts", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "__idx__ticker", .original_name = "__idx__ticker"},
                         {.name = "col", .original_name = "col"}}
                ),
                // Explicit index renames provided, but match existing names
                NoOpParams(
                        ObjectType::DF, {{"ts", "ticker"}}, multiindex({"ts", "ticker"}),
                        {{.name = "ts", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "__idx__ticker", .original_name = "__idx__ticker"},
                         {.name = "col", .original_name = "col"}}
                ),
                NoOpParams(
                        ObjectType::SERIES, {{"ts", "ticker"}}, multiindex({"ts", "ticker"}),
                        {{.name = "ts", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "__idx__ticker", .original_name = "__idx__ticker"},
                         {.name = "col", .original_name = "col"}}
                )
        )
);

struct ArrowSchemaCompatRaisesFixture : public ::testing::TestWithParam<NoOpParams> {
    static ObjectType object_type() { return std::get<0>(GetParam()); }
    static std::optional<std::vector<std::string>> index_columns() { return std::get<1>(GetParam()); }
    static std::variant<Index, MultiIndex> index() { return std::get<2>(GetParam()); }
    static std::vector<ColumnSpec> columns() { return std::get<3>(GetParam()); }
};

TEST_P(ArrowSchemaCompatRaisesFixture, Test) {
    auto input_schema = generate_schema(object_type(), false, index(), columns());
    ASSERT_THROW(make_schema_arrow_compatible(input_schema, index_columns()), UserInputException);
}

INSTANTIATE_TEST_SUITE_P(
        ArrowSchemaCompatRaises, ArrowSchemaCompatRaisesFixture,
        ::testing::Values(
                // Timeseries index tests
                // Explicit rename clashes with existing column
                NoOpParams(
                        ObjectType::DF, {{"ts"}}, timestamp_index(std::monostate(), "UTC"),
                        {{.name = "index", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "ts", .original_name = "ts"}}
                ),
                NoOpParams(
                        ObjectType::SERIES, {{"ts"}}, timestamp_index(std::monostate(), "UTC"),
                        {{.name = "index", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "ts", .original_name = "ts"}}
                ),
                NoOpParams(
                        ObjectType::DF, {{"ts"}}, timestamp_index("old_name", "UTC"),
                        {{.name = "old_name", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "ts", .original_name = "ts"}}
                ),
                NoOpParams(
                        ObjectType::SERIES, {{"ts"}}, timestamp_index("old_name", "UTC"),
                        {{.name = "old_name", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "ts", .original_name = "ts"}}
                ),
                // Explicit rename has the wrong number of index levels
                NoOpParams(
                        ObjectType::DF, {{"ts", "ticker"}}, timestamp_index(std::monostate(), "UTC"),
                        {{.name = "index", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}}
                ),
                // MultiIndex tests
                // Explicit rename clashes with existing column
                NoOpParams(
                        ObjectType::DF, {{"col", "level1"}}, multiindex({std::monostate(), std::monostate()}),
                        {{.name = "index"},
                         {.name = "__fkidx__1", .original_name = "__fkidx__1"},
                         {.name = "col", .original_name = "col"}}
                ),
                NoOpParams(
                        ObjectType::DF, {{"level0", "col"}}, multiindex({std::monostate(), std::monostate()}),
                        {{.name = "index"},
                         {.name = "__fkidx__1", .original_name = "__fkidx__1"},
                         {.name = "col", .original_name = "col"}}
                ),
                NoOpParams(
                        ObjectType::SERIES, {{"col", "level1"}}, multiindex({std::monostate(), std::monostate()}),
                        {{.name = "index"},
                         {.name = "__fkidx__1", .original_name = "__fkidx__1"},
                         {.name = "col", .original_name = "col"}}
                ),
                NoOpParams(
                        ObjectType::SERIES, {{"level0", "col"}}, multiindex({std::monostate(), std::monostate()}),
                        {{.name = "index"},
                         {.name = "__fkidx__1", .original_name = "__fkidx__1"},
                         {.name = "col", .original_name = "col"}}
                ),
                // Explicit rename has the wrong number of index levels
                NoOpParams(
                        ObjectType::DF, {{"ts"}}, multiindex({std::monostate(), std::monostate()}),
                        {{.name = "index"},
                         {.name = "__fkidx__1", .original_name = "__fkidx__1"},
                         {.name = "col", .original_name = "col"}}
                ),
                NoOpParams(
                        ObjectType::DF, {{"ts", "ticker", "blah"}}, multiindex({std::monostate(), std::monostate()}),
                        {{.name = "index"},
                         {.name = "__fkidx__1", .original_name = "__fkidx__1"},
                         {.name = "col", .original_name = "col"}}
                )
        )
);

using ModificationParams = std::tuple<
        ObjectType, bool, std::optional<std::vector<std::string>>, std::variant<Index, MultiIndex>,
        std::variant<Index, MultiIndex>, std::vector<ColumnSpec>, std::vector<ColumnSpec>,
        ankerl::unordered_dense::map<std::string, std::string, util::TransparentStringHash, std::equal_to<>>>;

struct ArrowSchemaCompatModificationFixture : public ::testing::TestWithParam<ModificationParams> {
    static ObjectType object_type() { return std::get<0>(GetParam()); }
    static bool has_synthetic_columns() { return std::get<1>(GetParam()); }
    static std::optional<std::vector<std::string>> index_columns() { return std::get<2>(GetParam()); }
    static std::variant<Index, MultiIndex> input_index() { return std::get<3>(GetParam()); }
    static std::variant<Index, MultiIndex> output_index() { return std::get<4>(GetParam()); }
    static std::vector<ColumnSpec> input_columns() { return std::get<5>(GetParam()); }
    static std::vector<ColumnSpec> output_columns() { return std::get<6>(GetParam()); }
    static ankerl::unordered_dense::map<std::string, std::string, util::TransparentStringHash, std::equal_to<>>
    expected_column_renames() {
        return std::get<7>(GetParam());
    }
};

TEST_P(ArrowSchemaCompatModificationFixture, Test) {
    auto input_schema = generate_schema(object_type(), has_synthetic_columns(), input_index(), input_columns());
    auto arrow_transformed_schema = make_schema_arrow_compatible(input_schema, index_columns());
    ASSERT_TRUE(arrow_transformed_schema.has_value());
    auto expected_output_schema = generate_schema(object_type(), false, output_index(), output_columns());
    ASSERT_EQ(arrow_transformed_schema->schema_.stream_descriptor(), expected_output_schema.stream_descriptor());
    ASSERT_TRUE(MessageDifferencer::Equals(
            arrow_transformed_schema->schema_.norm_metadata_, expected_output_schema.norm_metadata_
    ));
    ASSERT_EQ(arrow_transformed_schema->column_renames_, expected_column_renames());
}

INSTANTIATE_TEST_SUITE_P(
        ArrowSchemaCompatModification, ArrowSchemaCompatModificationFixture,
        ::testing::Values(
                // RangeIndex tests
                // Tests that a single column of a df or a series called None/""/an integer gets renamed correctly
                ModificationParams(
                        ObjectType::DF, false, std::nullopt, row_count_index(), row_count_index(),
                        {{.name = "__none__0", .is_none = true}}, {{.name = "None", .original_name = "None"}},
                        {{"__none__0", "None"}}
                ),
                ModificationParams(
                        ObjectType::DF, false, std::nullopt, row_count_index(), row_count_index(),
                        {{.name = "10", .original_name = "10", .is_int = true}},
                        {{.name = "10", .original_name = "10"}}, {}
                ),
                ModificationParams(
                        ObjectType::DF, false, std::nullopt, row_count_index(), row_count_index(),
                        {{.name = "__empty__0", .is_empty = true}},
                        {{.name = "__empty__", .original_name = "__empty__"}}, {{"__empty__0", "__empty__"}}
                ),
                ModificationParams(
                        // Series with an empty name have has_synthetic_columns set to true
                        ObjectType::SERIES, true, std::nullopt, row_count_index(), row_count_index(), {{.name = "0"}},
                        {{.name = "None", .original_name = "None"}}, {{"0", "None"}}
                ),
                ModificationParams(
                        ObjectType::SERIES, false, std::nullopt, row_count_index(), row_count_index(),
                        {{.name = "10", .original_name = "10", .is_int = true}},
                        {{.name = "10", .original_name = "10"}}, {}
                ),
                ModificationParams(
                        ObjectType::SERIES, false, std::nullopt, row_count_index(), row_count_index(),
                        {{.name = "__empty__0", .is_empty = true}},
                        {{.name = "__empty__", .original_name = "__empty__"}}, {{"__empty__0", "__empty__"}}
                ),
                // Duplicate column name test
                ModificationParams(
                        ObjectType::DF, false, std::nullopt, row_count_index(), row_count_index(),
                        {{.name = "__col_a__0", .original_name = "a"},
                         {.name = "__col_a__1", .original_name = "a"},
                         {.name = "__col_a__2", .original_name = "a"},
                         {.name = "__empty__3", .is_empty = true},
                         {.name = "__empty__4", .is_empty = true},
                         {.name = "__empty__5", .is_empty = true},
                         {.name = "__none__6", .is_none = true},
                         {.name = "__none__7", .is_none = true},
                         {.name = "None", .original_name = "None"}},
                        {{.name = "a", .original_name = "a"},
                         {.name = "_a_", .original_name = "_a_"},
                         {.name = "__a__", .original_name = "__a__"},
                         {.name = "__empty__", .original_name = "__empty__"},
                         {.name = "___empty___", .original_name = "___empty___"},
                         {.name = "____empty____", .original_name = "____empty____"},
                         {.name = "None", .original_name = "None"},
                         {.name = "_None_", .original_name = "_None_"},
                         {.name = "__None__", .original_name = "__None__"}},
                        {{"__col_a__0", "a"},
                         {"__col_a__1", "_a_"},
                         {"__col_a__2", "__a__"},
                         {"__empty__3", "__empty__"},
                         {"__empty__4", "___empty___"},
                         {"__empty__5", "____empty____"},
                         {"__none__6", "None"},
                         {"__none__7", "_None_"},
                         {"None", "__None__"}}
                ),
                // Synthetic columns test
                ModificationParams(
                        ObjectType::DF, true, std::nullopt, row_count_index(), row_count_index(),
                        {{.name = "0", .original_name = "0"}, {.name = "1", .original_name = "1"}},
                        {{.name = "0", .original_name = "0"}, {.name = "1", .original_name = "1"}}, {}
                ),
                // Timeseries index tests
                // Auto-rename an int named index column
                // No Clashes
                ModificationParams(
                        ObjectType::DF, false, std::nullopt, timestamp_index(10, "UTC"), timestamp_index("10", "UTC"),
                        {{.name = "10", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "10", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}},
                        {}
                ),
                ModificationParams(
                        ObjectType::SERIES, false, std::nullopt, timestamp_index(10, "UTC"),
                        timestamp_index("10", "UTC"),
                        {{.name = "10", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "10", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}},
                        {}
                ),
                // With clashes
                ModificationParams(
                        ObjectType::DF, false, std::nullopt, timestamp_index(10, "UTC"), timestamp_index("10", "UTC"),
                        {{.name = "10", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "__col_10__0", .original_name = "10"}},
                        {{.name = "10", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "_10_", .original_name = "_10_"}},
                        {{"__col_10__0", "_10_"}}
                ),
                ModificationParams(
                        ObjectType::SERIES, false, std::nullopt, timestamp_index(10, "UTC"),
                        timestamp_index("10", "UTC"),
                        {{.name = "10", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "__col_10__0", .original_name = "10"}},
                        {{.name = "10", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "_10_", .original_name = "_10_"}},
                        {{"__col_10__0", "_10_"}}
                ),
                // Auto-rename an index column with name None
                // No clashes
                ModificationParams(
                        ObjectType::DF, false, std::nullopt, timestamp_index(std::monostate(), "UTC"),
                        timestamp_index("__index__", "UTC"),
                        {{.name = "index", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "__index__", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}},
                        {{"index", "__index__"}}
                ),
                ModificationParams(
                        ObjectType::SERIES, false, std::nullopt, timestamp_index(std::monostate(), "UTC"),
                        timestamp_index("__index__", "UTC"),
                        {{.name = "index", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "__index__", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}},
                        {{"index", "__index__"}}
                ),
                // One clash
                ModificationParams(
                        ObjectType::DF, false, std::nullopt, timestamp_index(std::monostate(), "UTC"),
                        timestamp_index("___index___", "UTC"),
                        {{.name = "index", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "__index__", .original_name = "__index__"}},
                        {{.name = "___index___", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "__index__", .original_name = "__index__"}},
                        {{"index", "___index___"}}
                ),
                ModificationParams(
                        ObjectType::SERIES, false, std::nullopt, timestamp_index(std::monostate(), "UTC"),
                        timestamp_index("___index___", "UTC"),
                        {{.name = "index", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "__index__", .original_name = "__index__"}},
                        {{.name = "___index___", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "__index__", .original_name = "__index__"}},
                        {{"index", "___index___"}}
                ),
                // Multiple clashes
                ModificationParams(
                        ObjectType::DF, false, std::nullopt, timestamp_index(std::monostate(), "UTC"),
                        timestamp_index("___index___", "UTC"),
                        {{.name = "index", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "__col___index____0", .original_name = "__index__"},
                         {.name = "__col___index____1", .original_name = "__index__"}},
                        {{.name = "___index___", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "__index__", .original_name = "__index__"},
                         {.name = "____index____", .original_name = "____index____"}},
                        {{"index", "___index___"},
                         {"__col___index____0", "__index__"},
                         {"__col___index____1", "____index____"}}
                ),
                // Auto-rename an index column with name ""
                // No clashes
                ModificationParams(
                        ObjectType::DF, false, std::nullopt, timestamp_index("", "UTC"),
                        timestamp_index("__empty__", "UTC"),
                        {{.name = "", .data_type = DataType::NANOSECONDS_UTC64}, {.name = "col", .original_name = "col"}
                        },
                        {{.name = "__empty__", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}},
                        {{"", "__empty__"}}
                ),
                ModificationParams(
                        ObjectType::SERIES, false, std::nullopt, timestamp_index("", "UTC"),
                        timestamp_index("__empty__", "UTC"),
                        {{.name = "", .data_type = DataType::NANOSECONDS_UTC64}, {.name = "col", .original_name = "col"}
                        },
                        {{.name = "__empty__", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}},
                        {{"", "__empty__"}}
                ),
                // One clash
                ModificationParams(
                        ObjectType::DF, false, std::nullopt, timestamp_index("", "UTC"),
                        timestamp_index("__empty__", "UTC"),
                        {{.name = "", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "__empty__0", .is_empty = true, .original_name = ""}},
                        {{.name = "__empty__", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "___empty___", .original_name = "___empty___"}},
                        {{"", "__empty__"}, {"__empty__0", "___empty___"}}
                ),
                ModificationParams(
                        ObjectType::SERIES, false, std::nullopt, timestamp_index("", "UTC"),
                        timestamp_index("__empty__", "UTC"),
                        {{.name = "", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "__empty__0", .is_empty = true, .original_name = ""}},
                        {{.name = "__empty__", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "___empty___", .original_name = "___empty___"}},
                        {{"", "__empty__"}, {"__empty__0", "___empty___"}}
                ),
                // Multiple clashes
                ModificationParams(
                        ObjectType::DF, false, std::nullopt, timestamp_index("", "UTC"),
                        timestamp_index("__empty__", "UTC"),
                        {{.name = "", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "__empty__0", .is_empty = true, .original_name = ""},
                         {.name = "__empty__1", .is_empty = true, .original_name = ""}},
                        {{.name = "__empty__", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "___empty___", .original_name = "___empty___"},
                         {.name = "____empty____", .original_name = "____empty____"}},
                        {{"", "__empty__"}, {"__empty__0", "___empty___"}, {"__empty__1", "____empty____"}}
                ),
                // Explicit renaming to "ts"
                // No clash
                // Original index is named None
                ModificationParams(
                        ObjectType::DF, false, {{"ts"}}, timestamp_index(std::monostate(), "UTC"),
                        timestamp_index("ts", "UTC"),
                        {{.name = "index", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "ts", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}},
                        {{"index", "ts"}}
                ),
                ModificationParams(
                        ObjectType::SERIES, false, {{"ts"}}, timestamp_index(std::monostate(), "UTC"),
                        timestamp_index("ts", "UTC"),
                        {{.name = "index", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "ts", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}},
                        {{"index", "ts"}}
                ),
                // Original index is named "old_ts"
                ModificationParams(
                        ObjectType::DF, false, {{"ts"}}, timestamp_index("old_ts", "UTC"), timestamp_index("ts", "UTC"),
                        {{.name = "old_ts", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "ts", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}},
                        {{"old_ts", "ts"}}
                ),
                ModificationParams(
                        ObjectType::SERIES, false, {{"ts"}}, timestamp_index("old_ts", "UTC"),
                        timestamp_index("ts", "UTC"),
                        {{.name = "old_ts", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "ts", .data_type = DataType::NANOSECONDS_UTC64},
                         {.name = "col", .original_name = "col"}},
                        {{"old_ts", "ts"}}
                ),
                // MultiIndex tests
                // Auto-rename an int named primary index column with no clashes (clashes imply corrupted data, see
                // Monday issues 9715738171 and 12909663080)
                ModificationParams(
                        ObjectType::DF, false, std::nullopt, multiindex({10, "level1"}), multiindex({"10", "level1"}),
                        {{.name = "10"},
                         {.name = "__idx__level1", .original_name = "__idx__level1"},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "10"},
                         {.name = "__idx__level1", .original_name = "__idx__level1"},
                         {.name = "col", .original_name = "col"}},
                        {}
                ),
                ModificationParams(
                        ObjectType::SERIES, false, std::nullopt, multiindex({10, "level1"}),
                        multiindex({"10", "level1"}),
                        {{.name = "10"},
                         {.name = "__idx__level1", .original_name = "__idx__level1"},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "10"},
                         {.name = "__idx__level1", .original_name = "__idx__level1"},
                         {.name = "col", .original_name = "col"}},
                        {}
                ),
                // Auto-rename nameless multiindex with no clash (clashes are tested in Python layer)
                ModificationParams(
                        ObjectType::DF, false, std::nullopt, multiindex({std::monostate(), std::monostate()}),
                        multiindex({"__index_level_0__", "__index_level_1__"}),
                        {{.name = "index"},
                         {.name = "__fkidx__1", .original_name = "__fkidx__1"},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "__index_level_0__"},
                         {.name = "__idx____index_level_1__", .original_name = "__idx____index_level_1__"},
                         {.name = "col", .original_name = "col"}},
                        {{"index", "__index_level_0__"}, {"__fkidx__1", "__idx____index_level_1__"}}
                ),
                ModificationParams(
                        ObjectType::SERIES, false, std::nullopt, multiindex({std::monostate(), std::monostate()}),
                        multiindex({"__index_level_0__", "__index_level_1__"}),
                        {{.name = "index"},
                         {.name = "__fkidx__1", .original_name = "__fkidx__1"},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "__index_level_0__"},
                         {.name = "__idx____index_level_1__", .original_name = "__idx____index_level_1__"},
                         {.name = "col", .original_name = "col"}},
                        {{"index", "__index_level_0__"}, {"__fkidx__1", "__idx____index_level_1__"}}
                ),
                // Explicit rename nameless multiindex with no clash
                ModificationParams(
                        ObjectType::DF, false, {{"level0", "level1"}}, multiindex({std::monostate(), std::monostate()}),
                        multiindex({"level0", "level1"}),
                        {{.name = "index"},
                         {.name = "__fkidx__1", .original_name = "__fkidx__1"},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "level0"},
                         {.name = "__idx__level1", .original_name = "__idx__level1"},
                         {.name = "col", .original_name = "col"}},
                        {{"index", "level0"}, {"__fkidx__1", "__idx__level1"}}
                ),
                ModificationParams(
                        ObjectType::SERIES, false, {{"level0", "level1"}},
                        multiindex({std::monostate(), std::monostate()}), multiindex({"level0", "level1"}),
                        {{.name = "index"},
                         {.name = "__fkidx__1", .original_name = "__fkidx__1"},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "level0"},
                         {.name = "__idx__level1", .original_name = "__idx__level1"},
                         {.name = "col", .original_name = "col"}},
                        {{"index", "level0"}, {"__fkidx__1", "__idx__level1"}}
                ),
                // Explicit rename named multiindex with no clash
                ModificationParams(
                        ObjectType::DF, false, {{"level0", "level1"}}, multiindex({"old_name0", "old_name1"}),
                        multiindex({"level0", "level1"}),
                        {{.name = "old_name0"},
                         {.name = "__idx__old_name1", .original_name = "__idx__old_name1"},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "level0"},
                         {.name = "__idx__level1", .original_name = "__idx__level1"},
                         {.name = "col", .original_name = "col"}},
                        {{"old_name0", "level0"}, {"__idx__old_name1", "__idx__level1"}}
                ),
                ModificationParams(
                        ObjectType::SERIES, false, {{"level0", "level1"}}, multiindex({"old_name0", "old_name1"}),
                        multiindex({"level0", "level1"}),
                        {{.name = "old_name0"},
                         {.name = "__idx__old_name1", .original_name = "__idx__old_name1"},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "level0"},
                         {.name = "__idx__level1", .original_name = "__idx__level1"},
                         {.name = "col", .original_name = "col"}},
                        {{"old_name0", "level0"}, {"__idx__old_name1", "__idx__level1"}}
                ),
                // Explicit rename named multiindex. The specified new index names clash with existing index names,
                // which is allowed
                ModificationParams(
                        ObjectType::DF, false, {{"blah", "level1"}}, multiindex({"level0", "level1"}),
                        multiindex({"blah", "level1"}),
                        {{.name = "level0"},
                         {.name = "__idx__level1", .original_name = "__idx__level1"},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "blah"},
                         {.name = "__idx__level1", .original_name = "__idx__level1"},
                         {.name = "col", .original_name = "col"}},
                        {{"level0", "blah"}}
                ),
                ModificationParams(
                        ObjectType::DF, false, {{"level0", "blah"}}, multiindex({"level0", "level1"}),
                        multiindex({"level0", "blah"}),
                        {{.name = "level0"},
                         {.name = "__idx__level1", .original_name = "__idx__level1"},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "level0"},
                         {.name = "__idx__blah", .original_name = "__idx__blah"},
                         {.name = "col", .original_name = "col"}},
                        {{"__idx__level1", "__idx__blah"}}
                ),
                ModificationParams(
                        ObjectType::DF, false, {{"blah", "level1"}}, multiindex({"level1", "level0"}),
                        multiindex({"blah", "level1"}),
                        {{.name = "level1"},
                         {.name = "__idx__level0", .original_name = "__idx__level0"},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "blah"},
                         {.name = "__idx__level1", .original_name = "__idx__level1"},
                         {.name = "col", .original_name = "col"}},
                        {{"level1", "blah"}, {"__idx__level0", "__idx__level1"}}
                ),
                ModificationParams(
                        ObjectType::DF, false, {{"level0", "blah"}}, multiindex({"level1", "level0"}),
                        multiindex({"level0", "blah"}),
                        {{.name = "level1"},
                         {.name = "__idx__level0", .original_name = "__idx__level0"},
                         {.name = "col", .original_name = "col"}},
                        {{.name = "level0"},
                         {.name = "__idx__blah", .original_name = "__idx__blah"},
                         {.name = "col", .original_name = "col"}},
                        {{"level1", "level0"}, {"__idx__level0", "__idx__blah"}}
                )
        )
);