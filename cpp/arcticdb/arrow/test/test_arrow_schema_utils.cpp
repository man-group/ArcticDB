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
    std::string name;
    DataType data_type{DataType::INT64};
    bool is_none{false};
    bool is_empty{false};
    std::string original_name{""};
    bool is_int{false};
};

enum class ObjectType { DF, SERIES };

Index row_count_index() {
    Index index;
    index.set_step(1);
    return index;
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
                        ObjectType::DF, ColumnSpec{"__none__0", .is_none = true},
                        ColumnSpec{"None", .original_name = "None"},
                        ankerl::unordered_dense::map<std::string, std::string>{{"__none__0", "None"}}
                ),
                std::make_tuple(
                        ObjectType::DF, ColumnSpec{"10", .original_name = "10", .is_int = true},
                        ColumnSpec{"10", .original_name = "10"},
                        ankerl::unordered_dense::map<std::string, std::string>{}
                ),
                std::make_tuple(
                        ObjectType::DF, ColumnSpec{"__empty__0", .is_empty = true},
                        ColumnSpec{"__empty__", .original_name = "__empty__"},
                        ankerl::unordered_dense::map<std::string, std::string>{{"__empty__0", "__empty__"}}
                ),
                std::make_tuple(
                        ObjectType::SERIES, ColumnSpec{"0"}, ColumnSpec{"__empty__", .original_name = "__empty__"},
                        ankerl::unordered_dense::map<std::string, std::string>{{"0", "__empty__"}}
                ),
                std::make_tuple(
                        ObjectType::SERIES, ColumnSpec{"10", .original_name = "10", .is_int = true},
                        ColumnSpec{"10", .original_name = "10"},
                        ankerl::unordered_dense::map<std::string, std::string>{}
                ),
                std::make_tuple(
                        ObjectType::SERIES, ColumnSpec{"__empty__0", .is_empty = true},
                        ColumnSpec{"__empty__", .original_name = "__empty__"},
                        ankerl::unordered_dense::map<std::string, std::string>{{"__empty__0", "__empty__"}}
                )
        )
);
