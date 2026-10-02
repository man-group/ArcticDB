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
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::IsEmpty;
using NormalizationMetadata = arcticdb::proto::descriptors::NormalizationMetadata;
using Index = NormalizationMetadata::PandasIndex;
using MultiIndex = NormalizationMetadata::PandasMultiIndex;

namespace {

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

Index timestamp_index(const std::variant<std::monostate, std::string, int>& name, std::optional<std::string> tz) {
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

struct ArrowSchemaCompatibleBasic
    : public ::testing::TestWithParam<
              std::tuple<ObjectType, ColumnSpec, ColumnSpec, ankerl::unordered_dense::map<std::string, std::string>>> {
    ObjectType object_type() const { return std::get<0>(GetParam()); }
    ColumnSpec input_column_spec() const { return std::get<1>(GetParam()); }
    ColumnSpec output_column_spec() const { return std::get<2>(GetParam()); }
    ankerl::unordered_dense::map<std::string, std::string> expected_column_renames() const {
        return std::get<3>(GetParam());
    }
};

TEST_P(ArrowSchemaCompatibleBasic, Basic) {
    auto index = row_count_index();
    const bool has_synthetic_columns = object_type() == ObjectType::SERIES && input_column_spec().name == "0";
    auto original_schema = generate_schema(object_type(), has_synthetic_columns, index, {input_column_spec()});
    auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema);
    ASSERT_TRUE(changed);
    auto expected_schema = generate_schema(object_type(), false, index, {output_column_spec()});
    // TODO: Remove report throughout this file once all tests passing
    std::string report;
    MessageDifferencer differ;
    differ.ReportDifferencesToString(&report);
    auto same = differ.Compare(new_schema.norm_metadata_, expected_schema.norm_metadata_);
    ASSERT_TRUE(same);
    ASSERT_EQ(column_renames, expected_column_renames());
}

INSTANTIATE_TEST_SUITE_P(
        ArrowSchemaCompatibleBasicTests, ArrowSchemaCompatibleBasic,
        ::testing::Values(
                std::make_tuple(
                        ObjectType::DF, ColumnSpec{.name = "__none__0", .is_none = true},
                        ColumnSpec{.name = "None", .original_name = "None"},
                        ankerl::unordered_dense::map<std::string, std::string>{{"__none__0", "None"}}
                ),
                std::make_tuple(
                        ObjectType::DF, ColumnSpec{.name = "10", .original_name = "10", .is_int = true},
                        ColumnSpec{.name = "10", .original_name = "10"},
                        ankerl::unordered_dense::map<std::string, std::string>{}
                ),
                std::make_tuple(
                        ObjectType::DF, ColumnSpec{.name = "__empty__0", .is_empty = true},
                        ColumnSpec{.name = "__empty__", .original_name = "__empty__"},
                        ankerl::unordered_dense::map<std::string, std::string>{{"__empty__0", "__empty__"}}
                ),
                std::make_tuple(
                        ObjectType::SERIES, ColumnSpec{.name = "0"},
                        ColumnSpec{.name = "None", .original_name = "None"},
                        ankerl::unordered_dense::map<std::string, std::string>{{"0", "None"}}
                ),
                std::make_tuple(
                        ObjectType::SERIES, ColumnSpec{.name = "10", .original_name = "10", .is_int = true},
                        ColumnSpec{.name = "10", .original_name = "10"},
                        ankerl::unordered_dense::map<std::string, std::string>{}
                ),
                std::make_tuple(
                        ObjectType::SERIES, ColumnSpec{.name = "__empty__0", .is_empty = true},
                        ColumnSpec{.name = "__empty__", .original_name = "__empty__"},
                        ankerl::unordered_dense::map<std::string, std::string>{{"__empty__0", "__empty__"}}
                )
        )
);

TEST(ArrowSchemaCompatible, Duplicates) {
    auto index = row_count_index();
    auto original_schema = generate_schema(
            ObjectType::DF,
            false,
            index,
            {{.name = "__col_a__0", .original_name = "a"},
             {.name = "__col_a__1", .original_name = "a"},
             {.name = "__col_a__2", .original_name = "a"},
             {.name = "__empty__3", .is_empty = true},
             {.name = "__empty__4", .is_empty = true},
             {.name = "__empty__5", .is_empty = true},
             {.name = "__none__6", .is_none = true},
             {.name = "__none__7", .is_none = true},
             {.name = "None", .original_name = "None"}}
    );
    auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema);
    ASSERT_TRUE(changed);
    auto expected_schema = generate_schema(
            ObjectType::DF,
            false,
            index,
            {{.name = "a", .original_name = "a"},
             {.name = "_a_", .original_name = "_a_"},
             {.name = "__a__", .original_name = "__a__"},
             {.name = "__empty__", .original_name = "__empty__"},
             {.name = "___empty___", .original_name = "___empty___"},
             {.name = "____empty____", .original_name = "____empty____"},
             {.name = "None", .original_name = "None"},
             {.name = "_None_", .original_name = "_None_"},
             {.name = "__None__", .original_name = "__None__"}}
    );
    std::string report;
    MessageDifferencer differ;
    differ.ReportDifferencesToString(&report);
    auto same = differ.Compare(new_schema.norm_metadata_, expected_schema.norm_metadata_);
    ASSERT_TRUE(same);
    ankerl::unordered_dense::map<std::string, std::string> expected_column_renames{
            {"__col_a__0", "a"},
            {"__col_a__1", "_a_"},
            {"__col_a__2", "__a__"},
            {"__empty__3", "__empty__"},
            {"__empty__4", "___empty___"},
            {"__empty__5", "____empty____"},
            {"__none__6", "None"},
            {"__none__7", "_None_"},
            {"None", "__None__"}
    };
    ASSERT_EQ(column_renames, expected_column_renames);
}

TEST(ArrowSchemaCompatible, SyntheticColumns) {
    auto index = row_count_index();
    auto original_schema = generate_schema(
            ObjectType::DF, true, index, {{.name = "0", .original_name = "0"}, {.name = "1", .original_name = "1"}}
    );
    auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema);
    ASSERT_TRUE(changed);
    auto expected_schema = generate_schema(
            ObjectType::DF, false, index, {{.name = "0", .original_name = "0"}, {.name = "1", .original_name = "1"}}
    );
    std::string report;
    MessageDifferencer differ;
    differ.ReportDifferencesToString(&report);
    auto same = differ.Compare(new_schema.norm_metadata_, expected_schema.norm_metadata_);
    ASSERT_TRUE(same);
    ASSERT_TRUE(column_renames.empty());
}

struct ArrowSchemaCompatibleAutoRenameIntIndex : public ::testing::TestWithParam<std::tuple<
                                                         ObjectType, std::vector<ColumnSpec>, std::vector<ColumnSpec>,
                                                         ankerl::unordered_dense::map<std::string, std::string>>> {
    ObjectType object_type() const { return std::get<0>(GetParam()); }
    std::vector<ColumnSpec> input_column_spec() const { return std::get<1>(GetParam()); }
    std::vector<ColumnSpec> output_column_spec() const { return std::get<2>(GetParam()); }
    ankerl::unordered_dense::map<std::string, std::string> expected_column_renames() const {
        return std::get<3>(GetParam());
    }
};

TEST_P(ArrowSchemaCompatibleAutoRenameIntIndex, Basic) {
    auto original_index = timestamp_index(10, "UTC");
    auto original_schema = generate_schema(object_type(), false, original_index, input_column_spec());
    auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema);
    ASSERT_TRUE(changed);
    auto expected_index = timestamp_index("10", "UTC");
    auto expected_schema = generate_schema(object_type(), false, expected_index, output_column_spec());
    std::string report;
    MessageDifferencer differ;
    differ.ReportDifferencesToString(&report);
    auto same = differ.Compare(new_schema.norm_metadata_, expected_schema.norm_metadata_);
    ASSERT_TRUE(same);
    ASSERT_EQ(column_renames, expected_column_renames());
}

INSTANTIATE_TEST_SUITE_P(
        ArrowSchemaCompatibleAutoRenameIntIndexTests, ArrowSchemaCompatibleAutoRenameIntIndex,
        ::testing::Values(
                std::make_tuple(
                        ObjectType::DF,
                        std::vector<ColumnSpec>{
                                {.name = "10", .data_type = DataType::NANOSECONDS_UTC64},
                                {.name = "col", .original_name = "col"}
                        },
                        std::vector<ColumnSpec>{
                                {.name = "10", .data_type = DataType::NANOSECONDS_UTC64},
                                {.name = "col", .original_name = "col"}
                        },
                        ankerl::unordered_dense::map<std::string, std::string>{}
                ),
                std::make_tuple(
                        ObjectType::DF,
                        std::vector<ColumnSpec>{
                                {.name = "10", .data_type = DataType::NANOSECONDS_UTC64},
                                {.name = "__col_10__0", .original_name = "10"}
                        },
                        std::vector<ColumnSpec>{
                                {.name = "10", .data_type = DataType::NANOSECONDS_UTC64},
                                {.name = "_10_", .original_name = "_10_"}
                        },
                        ankerl::unordered_dense::map<std::string, std::string>{{"__col_10__0", "_10_"}}
                ),
                std::make_tuple(
                        ObjectType::SERIES,
                        std::vector<ColumnSpec>{
                                {.name = "10", .data_type = DataType::NANOSECONDS_UTC64},
                                {.name = "col", .original_name = "col"}
                        },
                        std::vector<ColumnSpec>{
                                {.name = "10", .data_type = DataType::NANOSECONDS_UTC64},
                                {.name = "col", .original_name = "col"}
                        },
                        ankerl::unordered_dense::map<std::string, std::string>{}
                ),
                std::make_tuple(
                        ObjectType::SERIES,
                        std::vector<ColumnSpec>{
                                {.name = "10", .data_type = DataType::NANOSECONDS_UTC64},
                                {.name = "__col_10__0", .original_name = "10"}
                        },
                        std::vector<ColumnSpec>{
                                {.name = "10", .data_type = DataType::NANOSECONDS_UTC64},
                                {.name = "_10_", .original_name = "_10_"}
                        },
                        ankerl::unordered_dense::map<std::string, std::string>{{"__col_10__0", "_10_"}}
                )
        )
);

TEST(ArrowSchemaCompatible, AutoRenameNamelessIndexNoClash) {
    for (auto object_type : std::array{ObjectType::DF, ObjectType::SERIES}) {
        auto original_index = timestamp_index(std::monostate(), "UTC");
        auto original_schema = generate_schema(
                object_type,
                false,
                original_index,
                {{.name = "index", .data_type = DataType::NANOSECONDS_UTC64}, {.name = "col", .original_name = "col"}}
        );
        auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema);
        ASSERT_TRUE(changed);
        auto expected_index = timestamp_index("__index__", "UTC");
        auto expected_schema = generate_schema(
                object_type,
                false,
                expected_index,
                {{.name = "__index__", .data_type = DataType::NANOSECONDS_UTC64},
                 {.name = "col", .original_name = "col"}}
        );
        std::string report;
        MessageDifferencer differ;
        differ.ReportDifferencesToString(&report);
        auto same = differ.Compare(new_schema.norm_metadata_, expected_schema.norm_metadata_);
        ASSERT_TRUE(same);
        ankerl::unordered_dense::map<std::string, std::string> expected_column_renames{{"index", "__index__"}};
        ASSERT_EQ(column_renames, expected_column_renames);
    }
}

TEST(ArrowSchemaCompatible, AutoRenameNamelessIndexOneClash) {
    for (auto object_type : std::array{ObjectType::DF, ObjectType::SERIES}) {
        auto original_index = timestamp_index(std::monostate(), "UTC");
        auto original_schema = generate_schema(
                object_type,
                false,
                original_index,
                {{.name = "index", .data_type = DataType::NANOSECONDS_UTC64},
                 {.name = "__index__", .original_name = "__index__"}}
        );
        auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema);
        ASSERT_TRUE(changed);
        auto expected_index = timestamp_index("___index___", "UTC");
        auto expected_schema = generate_schema(
                object_type,
                false,
                expected_index,
                {{.name = "___index___", .data_type = DataType::NANOSECONDS_UTC64},
                 {.name = "__index__", .original_name = "__index__"}}
        );
        std::string report;
        MessageDifferencer differ;
        differ.ReportDifferencesToString(&report);
        auto same = differ.Compare(new_schema.norm_metadata_, expected_schema.norm_metadata_);
        ASSERT_TRUE(same);
        ankerl::unordered_dense::map<std::string, std::string> expected_column_renames{{"index", "___index___"}};
        ASSERT_EQ(column_renames, expected_column_renames);
    }
}

TEST(ArrowSchemaCompatible, AutoRenameNamelessIndexMultipleClashes) {
    auto original_index = timestamp_index(std::monostate(), "UTC");
    auto original_schema = generate_schema(
            ObjectType::DF,
            false,
            original_index,
            {{.name = "index", .data_type = DataType::NANOSECONDS_UTC64},
             {.name = "__col___index____0", .original_name = "__index__"},
             {.name = "__col___index____1", .original_name = "__index__"}}
    );
    auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema);
    ASSERT_TRUE(changed);
    auto expected_index = timestamp_index("___index___", "UTC");
    auto expected_schema = generate_schema(
            ObjectType::DF,
            false,
            expected_index,
            {{.name = "___index___", .data_type = DataType::NANOSECONDS_UTC64},
             {.name = "__index__", .original_name = "__index__"},
             {.name = "____index____", .original_name = "____index____"}}
    );
    std::string report;
    MessageDifferencer differ;
    differ.ReportDifferencesToString(&report);
    auto same = differ.Compare(new_schema.norm_metadata_, expected_schema.norm_metadata_);
    ASSERT_TRUE(same);
    ankerl::unordered_dense::map<std::string, std::string> expected_column_renames{
            {"index", "___index___"}, {"__col___index____0", "__index__"}, {"__col___index____1", "____index____"}
    };
    ASSERT_EQ(column_renames, expected_column_renames);
}

TEST(ArrowSchemaCompatible, AutoRenameEmptyStringIndexNoClash) {
    for (auto object_type : std::array{ObjectType::DF, ObjectType::SERIES}) {
        auto original_index = timestamp_index("", "UTC");
        auto original_schema = generate_schema(
                object_type,
                false,
                original_index,
                {{.name = "", .data_type = DataType::NANOSECONDS_UTC64}, {.name = "col", .original_name = "col"}}
        );
        auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema);
        ASSERT_TRUE(changed);
        auto expected_index = timestamp_index("__empty__", "UTC");
        auto expected_schema = generate_schema(
                object_type,
                false,
                expected_index,
                {{.name = "__empty__", .data_type = DataType::NANOSECONDS_UTC64},
                 {.name = "col", .original_name = "col"}}
        );
        std::string report;
        MessageDifferencer differ;
        differ.ReportDifferencesToString(&report);
        auto same = differ.Compare(new_schema.norm_metadata_, expected_schema.norm_metadata_);
        ASSERT_TRUE(same);
        ankerl::unordered_dense::map<std::string, std::string> expected_column_renames{{"", "__empty__"}};
        ASSERT_EQ(column_renames, expected_column_renames);
    }
}

TEST(ArrowSchemaCompatible, AutoRenameEmptyStringIndexOneClash) {
    for (auto object_type : std::array{ObjectType::DF, ObjectType::SERIES}) {
        auto original_index = timestamp_index("", "UTC");
        auto original_schema = generate_schema(
                object_type,
                false,
                original_index,
                {{.name = "", .data_type = DataType::NANOSECONDS_UTC64},
                 {.name = "__empty__0", .is_empty = true, .original_name = ""}}
        );
        auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema);
        ASSERT_TRUE(changed);
        auto expected_index = timestamp_index("__empty__", "UTC");
        auto expected_schema = generate_schema(
                object_type,
                false,
                expected_index,
                {{.name = "__empty__", .data_type = DataType::NANOSECONDS_UTC64},
                 {.name = "___empty___", .original_name = "___empty___"}}
        );
        std::string report;
        MessageDifferencer differ;
        differ.ReportDifferencesToString(&report);
        auto same = differ.Compare(new_schema.norm_metadata_, expected_schema.norm_metadata_);
        ASSERT_TRUE(same);
        ankerl::unordered_dense::map<std::string, std::string> expected_column_renames{
                {"", "__empty__"}, {"__empty__0", "___empty___"}
        };
        ASSERT_EQ(column_renames, expected_column_renames);
    }
}

TEST(ArrowSchemaCompatible, AutoRenameEmptyStringIndexmultipleClashes) {
    for (auto object_type : std::array{ObjectType::DF, ObjectType::SERIES}) {
        auto original_index = timestamp_index("", "UTC");
        auto original_schema = generate_schema(
                object_type,
                false,
                original_index,
                {{.name = "", .data_type = DataType::NANOSECONDS_UTC64},
                 {.name = "__empty__0", .is_empty = true, .original_name = ""},
                 {.name = "__empty__1", .is_empty = true, .original_name = ""}}
        );
        auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema);
        ASSERT_TRUE(changed);
        auto expected_index = timestamp_index("__empty__", "UTC");
        auto expected_schema = generate_schema(
                object_type,
                false,
                expected_index,
                {{.name = "__empty__", .data_type = DataType::NANOSECONDS_UTC64},
                 {.name = "___empty___", .original_name = "___empty___"},
                 {.name = "____empty____", .original_name = "____empty____"}}
        );
        std::string report;
        MessageDifferencer differ;
        differ.ReportDifferencesToString(&report);
        auto same = differ.Compare(new_schema.norm_metadata_, expected_schema.norm_metadata_);
        ASSERT_TRUE(same);
        ankerl::unordered_dense::map<std::string, std::string> expected_column_renames{
                {"", "__empty__"}, {"__empty__0", "___empty___"}, {"__empty__1", "____empty____"}
        };
        ASSERT_EQ(column_renames, expected_column_renames);
    }
}

TEST(ArrowSchemaCompatible, ExplicitRenameIndexNoClash) {
    for (auto object_type : std::array{ObjectType::DF, ObjectType::SERIES}) {
        for (auto original_index_name : std::vector<std::string>{"None", "old_ts", "ts"}) {
            bool unnamed_index = original_index_name == "None";
            auto original_index = unnamed_index ? timestamp_index(std::monostate(), "UTC")
                                                : timestamp_index(original_index_name, "UTC");
            auto original_schema = generate_schema(
                    object_type,
                    false,
                    original_index,
                    {{.name = unnamed_index ? "index" : original_index_name, .data_type = DataType::NANOSECONDS_UTC64},
                     {.name = "col", .original_name = "col"}}
            );
            auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema, {{"ts"}});
            ASSERT_EQ(changed, original_index_name != "ts");
            auto expected_index = timestamp_index("ts", "UTC");
            auto expected_schema = generate_schema(
                    object_type,
                    false,
                    expected_index,
                    {{.name = "ts", .data_type = DataType::NANOSECONDS_UTC64}, {.name = "col", .original_name = "col"}}
            );
            std::string report;
            MessageDifferencer differ;
            differ.ReportDifferencesToString(&report);
            auto same = differ.Compare(new_schema.norm_metadata_, expected_schema.norm_metadata_);
            ASSERT_TRUE(same);
            ankerl::unordered_dense::map<std::string, std::string> expected_column_renames;
            if (original_index_name != "ts") {
                expected_column_renames.emplace(unnamed_index ? "index" : original_index_name, "ts");
            }
            ASSERT_EQ(column_renames, expected_column_renames);
        }
    }
}

TEST(ArrowSchemaCompatible, ExplicitRenameIndexClash) {
    for (auto object_type : std::array{ObjectType::DF, ObjectType::SERIES}) {
        auto original_index = timestamp_index(std::monostate(), "UTC");
        auto original_schema = generate_schema(
                object_type,
                false,
                original_index,
                {{.name = "index", .data_type = DataType::NANOSECONDS_UTC64}, {.name = "ts", .original_name = "ts"}}
        );
        ASSERT_THROW(make_schema_arrow_compatible(original_schema, {{"ts"}}), UserInputException);
    }
}

TEST(ArrowSchemaCompatible, ExplicitRenameIndexTooManyIndexNames) {
    for (auto object_type : std::array{ObjectType::DF, ObjectType::SERIES}) {
        auto original_index = timestamp_index(std::monostate(), "UTC");
        auto original_schema = generate_schema(
                object_type,
                false,
                original_index,
                {{.name = "index", .data_type = DataType::NANOSECONDS_UTC64}, {.name = "col", .original_name = "ts"}}
        );
        ASSERT_THROW(make_schema_arrow_compatible(original_schema, {{"level0", "level1"}}), UserInputException);
    }
}

TEST(ArrowSchemaCompatible, ExplicitRenameNamelessIndexClash) {
    for (auto object_type : std::array{ObjectType::DF, ObjectType::SERIES}) {
        auto original_index = timestamp_index(std::monostate(), "UTC");
        auto original_schema = generate_schema(
                object_type,
                false,
                original_index,
                {{.name = "index", .data_type = DataType::NANOSECONDS_UTC64}, {.name = "ts", .original_name = "ts"}}
        );
        ASSERT_THROW(make_schema_arrow_compatible(original_schema, {{"ts"}}), UserInputException);
    }
}

TEST(ArrowSchemaCompatible, AutoRenameIntMultiIndexNoClash) {
    for (auto object_type : std::array{ObjectType::DF, ObjectType::SERIES}) {
        auto original_index = multiindex({10, "level1"});
        auto original_schema = generate_schema(
                object_type,
                false,
                original_index,
                {{.name = "10"},
                 {.name = "__idx__level1", .original_name = "__idx__level1"},
                 {.name = "col", .original_name = "col"}}
        );
        auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema);
        ASSERT_TRUE(changed);
        auto expected_index = multiindex({"10", "level1"});
        auto expected_schema = generate_schema(
                object_type,
                false,
                expected_index,
                {{.name = "10"},
                 {.name = "__idx__level1", .original_name = "__idx__level1"},
                 {.name = "col", .original_name = "col"}}
        );
        std::string report;
        MessageDifferencer differ;
        differ.ReportDifferencesToString(&report);
        auto same = differ.Compare(new_schema.norm_metadata_, expected_schema.norm_metadata_);
        ASSERT_TRUE(same);
        ASSERT_TRUE(column_renames.empty());
    }
}

TEST(ArrowSchemaCompatible, AutoRenameNamelessMultiIndexNoClash) {
    for (auto object_type : std::array{ObjectType::DF, ObjectType::SERIES}) {
        auto original_index = multiindex({std::monostate(), std::monostate()});
        auto original_schema = generate_schema(
                object_type,
                false,
                original_index,
                {{.name = "index"},
                 {.name = "__fkidx__1", .original_name = "__fkidx__1"},
                 {.name = "col", .original_name = "col"}}
        );
        auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema);
        ASSERT_TRUE(changed);
        auto expected_index = multiindex({"__index_level_0__", "__index_level_1__"});
        auto expected_schema = generate_schema(
                object_type,
                false,
                expected_index,
                {{.name = "__index_level_0__"},
                 {.name = "__idx____index_level_1__", .original_name = "__idx____index_level_1__"},
                 {.name = "col", .original_name = "col"}}
        );
        std::string report;
        MessageDifferencer differ;
        differ.ReportDifferencesToString(&report);
        auto same = differ.Compare(new_schema.norm_metadata_, expected_schema.norm_metadata_);
        ASSERT_TRUE(same);
        ankerl::unordered_dense::map<std::string, std::string> expected_column_renames{
                {"index", "__index_level_0__"}, {"__fkidx__1", "__idx____index_level_1__"}
        };
        ASSERT_EQ(column_renames, expected_column_renames);
    }
}

TEST(ArrowSchemaCompatible, ExplicitRenameNamelessMultiIndexNoClash) {
    for (auto object_type : std::array{ObjectType::DF, ObjectType::SERIES}) {
        auto original_index = multiindex({std::monostate(), std::monostate()});
        auto original_schema = generate_schema(
                object_type,
                false,
                original_index,
                {{.name = "index"},
                 {.name = "__fkidx__1", .original_name = "__fkidx__1"},
                 {.name = "col", .original_name = "col"}}
        );
        auto [changed, new_schema, column_renames] =
                make_schema_arrow_compatible(original_schema, {{"level0", "level1"}});
        ASSERT_TRUE(changed);
        auto expected_index = multiindex({"level0", "level1"});
        auto expected_schema = generate_schema(
                object_type,
                false,
                expected_index,
                {{.name = "level0"},
                 {.name = "__idx__level1", .original_name = "__idx__level1"},
                 {.name = "col", .original_name = "col"}}
        );
        std::string report;
        MessageDifferencer differ;
        differ.ReportDifferencesToString(&report);
        auto same = differ.Compare(new_schema.norm_metadata_, expected_schema.norm_metadata_);
        ASSERT_TRUE(same);
        ankerl::unordered_dense::map<std::string, std::string> expected_column_renames{
                {"index", "level0"}, {"__fkidx__1", "__idx__level1"}
        };
        ASSERT_EQ(column_renames, expected_column_renames);
    }
}

TEST(ArrowSchemaCompatible, ExplicitRenameNamedMultiIndexNoClash) {
    for (auto object_type : std::array{ObjectType::DF, ObjectType::SERIES}) {
        auto original_index = multiindex({"old_name0", "old_name1"});
        auto original_schema = generate_schema(
                object_type,
                false,
                original_index,
                {{.name = "old_name0"},
                 {.name = "__idx__old_name1", .original_name = "__idx__old_name1"},
                 {.name = "col", .original_name = "col"}}
        );
        auto [changed, new_schema, column_renames] =
                make_schema_arrow_compatible(original_schema, {{"level0", "level1"}});
        ASSERT_TRUE(changed);
        auto expected_index = multiindex({"level0", "level1"});
        auto expected_schema = generate_schema(
                object_type,
                false,
                expected_index,
                {{.name = "level0"},
                 {.name = "__idx__level1", .original_name = "__idx__level1"},
                 {.name = "col", .original_name = "col"}}
        );
        std::string report;
        MessageDifferencer differ;
        differ.ReportDifferencesToString(&report);
        auto same = differ.Compare(new_schema.norm_metadata_, expected_schema.norm_metadata_);
        ASSERT_TRUE(same);
        ankerl::unordered_dense::map<std::string, std::string> expected_column_renames{
                {"old_name0", "level0"}, {"__idx__old_name1", "__idx__level1"}
        };
        ASSERT_EQ(column_renames, expected_column_renames);
    }
}

// TODO: Add test that explicit renaming works if the new names appear in index names that will be overridden
TEST(ArrowSchemaCompatible, ExplicitRenameMultiIndexClash) {
    for (auto object_type : std::array{ObjectType::DF, ObjectType::SERIES}) {
        for (const auto& index_names : std::vector<std::vector<std::string>>{{"col", "level1"}, {"level0", "col"}}) {
            auto original_index = multiindex({std::monostate(), std::monostate()});
            auto original_schema = generate_schema(
                    object_type,
                    false,
                    original_index,
                    {{.name = "index"},
                     {.name = "__fkidx__1", .original_name = "__fkidx__1"},
                     {.name = "col", .original_name = "col"}}
            );
            ASSERT_THROW(make_schema_arrow_compatible(original_schema, index_names), UserInputException);
        }
    }
}

TEST(ArrowSchemaCompatible, ExplicitRenameMultiIndexClashInOverwrittenIndexNames) {
    for (auto object_type : std::array{ObjectType::DF, ObjectType::SERIES}) {
        for (const auto& original_index :
             std::vector<MultiIndex>{multiindex({"level0", "level1"}), multiindex({"level1", "level0"})}) {
            auto original_primary = original_index.name();
            auto original_secondary = original_primary == "level0" ? "__idx__level1" : "__idx__level0";
            auto original_schema = generate_schema(
                    object_type,
                    false,
                    original_index,
                    {{.name = original_primary},
                     {.name = original_secondary, .original_name = original_secondary},
                     {.name = "col", .original_name = "col"}}
            );
            for (const auto& index_names :
                 std::vector<std::vector<std::string>>{{"blah", "level1"}, {"level0", "blah"}}) {
                auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema, index_names);
                ASSERT_TRUE(changed);
                auto expected_primary = index_names.at(0);
                auto expected_secondary = fmt::format("__idx__{}", index_names.at(1));
                auto expected_index = multiindex({expected_primary, expected_secondary});
                auto expected_schema = generate_schema(
                        object_type,
                        false,
                        expected_index,
                        {{.name = expected_primary},
                         {.name = expected_secondary, .original_name = expected_secondary},
                         {.name = "col", .original_name = "col"}}
                );
                std::string report;
                MessageDifferencer differ;
                differ.ReportDifferencesToString(&report);
                auto same = differ.Compare(new_schema.norm_metadata_, expected_schema.norm_metadata_);
                ASSERT_TRUE(same);
                ankerl::unordered_dense::map<std::string, std::string> expected_column_renames{
                        {original_primary, expected_primary}, {original_secondary, expected_secondary}
                };
                // TODO: Don't even add these in in the first place
                for (auto it = expected_column_renames.begin(); it != expected_column_renames.end();) {
                    if (it->first == it->second) {
                        it = expected_column_renames.erase(it);
                    } else {
                        ++it;
                    }
                }
                ASSERT_EQ(column_renames, expected_column_renames);
            }
        }
    }
}

TEST(ArrowSchemaCompatible, ExplicitRenameMultiIndexIncorrectIndexCount) {
    for (auto object_type : std::array{ObjectType::DF, ObjectType::SERIES}) {
        for (const auto& index_names :
             std::vector<std::vector<std::string>>{{"level0"}, {"level0", "level1", "level2"}}) {
            auto original_index = multiindex({std::monostate(), std::monostate()});
            auto original_schema = generate_schema(
                    object_type,
                    false,
                    original_index,
                    {{.name = "index"},
                     {.name = "__fkidx__1", .original_name = "__fkidx__1"},
                     {.name = "col", .original_name = "col"}}
            );
            ASSERT_THROW(make_schema_arrow_compatible(original_schema, index_names), UserInputException);
        }
    }
}

TEST(ArrowSchemaCompatibleValidSchema, RangeIndexDf) {
    auto index = row_count_index(10, 2);
    auto original_schema = generate_schema(ObjectType::DF, false, index, {{.name = "col", .original_name = "col"}});
    auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema);
    ASSERT_FALSE(changed);
    ASSERT_TRUE(MessageDifferencer::Equals(new_schema.norm_metadata_, original_schema.norm_metadata_));
    ASSERT_TRUE(column_renames.empty());
}

TEST(ArrowSchemaCompatibleValidSchema, TimeseriesDf) {
    auto index = timestamp_index("ts", "UTC");
    auto original_schema = generate_schema(
            ObjectType::DF,
            false,
            index,
            {{.name = "ts", .data_type = DataType::NANOSECONDS_UTC64}, {.name = "col", .original_name = "col"}}
    );
    auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema);
    ASSERT_FALSE(changed);
    ASSERT_TRUE(MessageDifferencer::Equals(new_schema.norm_metadata_, original_schema.norm_metadata_));
    ASSERT_TRUE(column_renames.empty());
}

TEST(ArrowSchemaCompatibleValidSchema, MultiIndexDf) {
    auto index = multiindex({"ts", "ticker"});
    index.set_tz("UTC");
    auto original_schema = generate_schema(
            ObjectType::DF,
            false,
            index,
            {{.name = "ts", .data_type = DataType::NANOSECONDS_UTC64},
             {.name = "__idx__ticker", .original_name = "__idx__ticker"},
             {.name = "col", .original_name = "col"}}
    );
    auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema);
    ASSERT_FALSE(changed);
    ASSERT_TRUE(MessageDifferencer::Equals(new_schema.norm_metadata_, original_schema.norm_metadata_));
    ASSERT_TRUE(column_renames.empty());
}

TEST(ArrowSchemaCompatibleValidSchema, RangeIndexSeries) {
    auto index = row_count_index(10, 2);
    auto original_schema = generate_schema(ObjectType::SERIES, false, index, {{.name = "col", .original_name = "col"}});
    auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema);
    ASSERT_FALSE(changed);
    ASSERT_TRUE(MessageDifferencer::Equals(new_schema.norm_metadata_, original_schema.norm_metadata_));
    ASSERT_TRUE(column_renames.empty());
}

TEST(ArrowSchemaCompatibleValidSchema, TimeseriesSeries) {
    auto index = timestamp_index("ts", "UTC");
    auto original_schema = generate_schema(
            ObjectType::SERIES,
            false,
            index,
            {{.name = "ts", .data_type = DataType::NANOSECONDS_UTC64}, {.name = "col", .original_name = "col"}}
    );
    auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema);
    ASSERT_FALSE(changed);
    ASSERT_TRUE(MessageDifferencer::Equals(new_schema.norm_metadata_, original_schema.norm_metadata_));
    ASSERT_TRUE(column_renames.empty());
}

TEST(ArrowSchemaCompatibleValidSchema, MultiIndexSeries) {
    auto index = multiindex({"ts", "ticker"});
    index.set_tz("UTC");
    auto original_schema = generate_schema(
            ObjectType::SERIES,
            false,
            index,
            {{.name = "ts", .data_type = DataType::NANOSECONDS_UTC64},
             {.name = "__idx__ticker", .original_name = "__idx__ticker"},
             {.name = "col", .original_name = "col"}}
    );
    auto [changed, new_schema, column_renames] = make_schema_arrow_compatible(original_schema);
    ASSERT_FALSE(changed);
    ASSERT_TRUE(MessageDifferencer::Equals(new_schema.norm_metadata_, original_schema.norm_metadata_));
    ASSERT_TRUE(column_renames.empty());
}
