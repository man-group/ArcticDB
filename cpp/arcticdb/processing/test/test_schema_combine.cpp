/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <google/protobuf/util/message_differencer.h>

#include <gtest/gtest.h>
#include <arcticdb/processing/schema_combine.hpp>

using namespace arcticdb;
using namespace arcticdb::entity;
using namespace google::protobuf::util;
using NormalizationMetadata = arcticdb::proto::descriptors::NormalizationMetadata;

namespace {

// Build a timeseries-indexed DataFrame OutputSchema with the given index field and data columns.
OutputSchema timeseries_df(
        const std::string& index_name, const std::vector<std::pair<std::string, DataType>>& columns
) {
    StreamDescriptor desc{StreamId{}, IndexDescriptorImpl{IndexDescriptor::Type::TIMESTAMP, 1}};
    desc.add_scalar_field(DataType::NANOSECONDS_UTC64, index_name);
    for (const auto& [name, type] : columns) {
        desc.add_scalar_field(type, name);
    }
    NormalizationMetadata norm;
    auto* index = norm.mutable_df()->mutable_common()->mutable_index();
    index->set_is_physically_stored(true);
    index->set_name(index_name);
    return {std::move(desc), std::move(norm)};
}

std::vector<std::pair<std::string, DataType>> columns_of(const OutputSchema& schema) {
    std::vector<std::pair<std::string, DataType>> out;
    for (const auto& field : schema.stream_descriptor().fields()) {
        out.emplace_back(std::string(field.name()), field.type().data_type());
    }
    return out;
}

} // namespace

TEST(CombineSchema, ConcatOuterUnionOfColumns) {
    auto base = timeseries_df("ts", {{"a", DataType::FLOAT64}, {"b", DataType::FLOAT64}});
    auto other = timeseries_df("ts", {{"b", DataType::FLOAT64}, {"c", DataType::FLOAT64}});
    auto combined = combine_schema(base, other, concat_outer_options());
    // Union in base-first, then other-unique order.
    std::vector<std::pair<std::string, DataType>> expected{
            {"ts", DataType::NANOSECONDS_UTC64},
            {"a", DataType::FLOAT64},
            {"b", DataType::FLOAT64},
            {"c", DataType::FLOAT64}
    };
    ASSERT_EQ(columns_of(combined), expected);
}

TEST(CombineSchema, ConcatInnerIntersectionOfColumns) {
    auto base = timeseries_df("ts", {{"a", DataType::FLOAT64}, {"b", DataType::FLOAT64}});
    auto other = timeseries_df("ts", {{"b", DataType::FLOAT64}, {"c", DataType::FLOAT64}});
    auto combined = combine_schema(base, other, concat_inner_options());
    std::vector<std::pair<std::string, DataType>> expected{
            {"ts", DataType::NANOSECONDS_UTC64}, {"b", DataType::FLOAT64}
    };
    ASSERT_EQ(columns_of(combined), expected);
}

TEST(CombineSchema, ConcatOuterTypePromotion) {
    auto base = timeseries_df("ts", {{"a", DataType::INT32}});
    auto other = timeseries_df("ts", {{"a", DataType::INT64}});
    auto combined = combine_schema(base, other, concat_outer_options());
    ASSERT_EQ(combined.stream_descriptor().field(1).type().data_type(), DataType::INT64);
}

TEST(CombineSchema, ConcatMismatchedIndexNameReconciledToFake) {
    auto base = timeseries_df("ts1", {{"a", DataType::FLOAT64}});
    auto other = timeseries_df("ts2", {{"a", DataType::FLOAT64}});
    auto combined = combine_schema(base, other, concat_outer_options());
    // Scalar index name mismatch reconciles the index field to "index".
    ASSERT_EQ(combined.stream_descriptor().field(0).name(), "index");
}

TEST(CombineSchema, AppendStaticSameColumnsSucceeds) {
    auto base = timeseries_df("ts", {{"a", DataType::FLOAT64}, {"b", DataType::INT64}});
    auto other = timeseries_df("ts", {{"a", DataType::FLOAT64}, {"b", DataType::INT64}});
    auto combined = combine_schema(base, other, append_static_schema_options());
    ASSERT_EQ(columns_of(combined), columns_of(base));
}

TEST(CombineSchema, AppendStaticMissingColumnRaises) {
    auto base = timeseries_df("ts", {{"a", DataType::FLOAT64}, {"b", DataType::INT64}});
    auto other = timeseries_df("ts", {{"a", DataType::FLOAT64}});
    ASSERT_THROW(combine_schema(base, other, append_static_schema_options()), SchemaException);
}

TEST(CombineSchema, AppendStaticEmptyToConcretePromotion) {
    auto base = timeseries_df("ts", {{"a", DataType::EMPTYVAL}});
    auto other = timeseries_df("ts", {{"a", DataType::FLOAT64}});
    auto combined = combine_schema(base, other, append_static_schema_options());
    ASSERT_EQ(combined.stream_descriptor().field(1).type().data_type(), DataType::FLOAT64);
}

TEST(CombineSchema, AppendStaticFixedToDynamicStringPromotion) {
    auto base = timeseries_df("ts", {{"a", DataType::UTF_FIXED64}});
    auto other = timeseries_df("ts", {{"a", DataType::UTF_DYNAMIC64}});
    auto combined = combine_schema(base, other, append_static_schema_options());
    ASSERT_EQ(combined.stream_descriptor().field(1).type().data_type(), DataType::UTF_DYNAMIC64);
}

TEST(CombineSchema, AppendMismatchedIndexNameRaises) {
    auto base = timeseries_df("ts1", {{"a", DataType::FLOAT64}});
    auto other = timeseries_df("ts2", {{"a", DataType::FLOAT64}});
    ASSERT_THROW(combine_schema(base, other, append_static_schema_options()), SchemaException);
}

TEST(CombineSchema, AppendDynamicKeepsUnionAndPromotes) {
    auto base = timeseries_df("ts", {{"a", DataType::INT32}});
    auto other = timeseries_df("ts", {{"a", DataType::INT64}, {"b", DataType::FLOAT64}});
    auto combined = combine_schema(base, other, append_dynamic_schema_options());
    std::vector<std::pair<std::string, DataType>> expected{
            {"ts", DataType::NANOSECONDS_UTC64}, {"a", DataType::INT64}, {"b", DataType::FLOAT64}
    };
    ASSERT_EQ(columns_of(combined), expected);
}
