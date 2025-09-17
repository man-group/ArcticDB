/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <google/protobuf/util/message_differencer.h>

#include <gtest/gtest.h>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/processing/grouper.hpp>

using namespace arcticdb;
using namespace arcticdb::pipelines;
using namespace google::protobuf::util;

TEST(OutputSchema, NonModifyingClauses) {
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({IndexDescriptor::Type::TIMESTAMP, 1});
    stream_desc.add_scalar_field(DataType::NANOSECONDS_UTC64, "timestamp");
    stream_desc.add_scalar_field(DataType::INT64, "col");

    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_name("timestamp");
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(true);

    PassthroughClause passthrough_clause;
    auto output_schema = passthrough_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    RowRangeClause row_range_clause{0, 5};
    output_schema = row_range_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    DateRangeClause date_range_clause{{}, {}};
    output_schema = date_range_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    RemoveColumnPartitioningClause remove_column_partitioning_clause;
    output_schema = remove_column_partitioning_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    SplitClause split_clause{{}};
    output_schema = split_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    SortClause sort_clause{{}, {}};
    output_schema = sort_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    MergeClause merge_clause{stream::EmptyIndex{}, {}, {}, {}, {}};
    output_schema = merge_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    ConcatClause concat_clause{{}};
    output_schema = concat_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));
}

TEST(OutputSchema, DateRangeClauseRequiresTimeseries) {
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({IndexDescriptor::Type::ROWCOUNT, 0});
    stream_desc.add_scalar_field(DataType::INT64, "col");

    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(false);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_start(0);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_step(1);

    DateRangeClause date_range_clause{0, 5};
    ASSERT_THROW(date_range_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);
}

TEST(OutputSchema, MergeClauseRequiresTimeseries) {
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({IndexDescriptor::Type::ROWCOUNT, 0});
    stream_desc.add_scalar_field(DataType::INT64, "col");

    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(false);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_start(0);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_step(1);

    MergeClause merge_clause{stream::EmptyIndex{}, {}, {}, {}, {}};
    ASSERT_THROW(merge_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);
}

TEST(OutputSchema, FilterClauseColumnPresence) {
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({IndexDescriptor::Type::ROWCOUNT, 0});
    stream_desc.add_scalar_field(DataType::INT64, "col1");
    stream_desc.add_scalar_field(DataType::INT64, "col2");
    stream_desc.add_scalar_field(DataType::INT64, "col3");

    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(false);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_start(0);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_step(1);

    // All required columns present in StreamDescriptor
    auto node = std::make_shared<ExpressionNode>(ColumnName("col1"), ColumnName("col3"), OperationType::EQ);
    ExpressionContext ec;
    ec.root_node_name_ = ExpressionName("blah");
    ec.add_expression_node("blah", node);
    FilterClause filter_clause{{"col1", "col3"}, ec, {}};
    auto output_schema = filter_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    // Some, but not all required columns present in StreamDescriptor
    node = std::make_shared<ExpressionNode>(ColumnName("col1"), ColumnName("col4"), OperationType::EQ);
    ec = ExpressionContext();
    ec.root_node_name_ = ExpressionName("blah");
    ec.add_expression_node("blah", node);
    filter_clause = FilterClause{{"col1", "col4"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);

    // No required columns present in StreamDescriptor
    node = std::make_shared<ExpressionNode>(ColumnName("col4"), ColumnName("col5"), OperationType::EQ);
    ec = ExpressionContext();
    ec.root_node_name_ = ExpressionName("blah");
    ec.add_expression_node("blah", node);
    filter_clause = FilterClause{{"col4", "col5"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);
}

TEST(OutputSchema, ProjectClauseColumnPresence) {
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({IndexDescriptor::Type::ROWCOUNT, 0});
    stream_desc.add_scalar_field(DataType::INT64, "col1");
    stream_desc.add_scalar_field(DataType::INT64, "col2");
    stream_desc.add_scalar_field(DataType::INT64, "col3");

    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(false);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_start(0);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_step(1);

    // All required columns present in StreamDescriptor
    auto node = std::make_shared<ExpressionNode>(ColumnName("col1"), ColumnName("col3"), OperationType::ADD);
    ExpressionContext ec;
    ec.root_node_name_ = ExpressionName("root");
    ec.add_expression_node("root", node);
    ProjectClause project_clause{{"col1", "col3"}, "root", ec};
    auto output_schema = project_clause.modify_schema({stream_desc.clone(), norm_meta});
    stream_desc.add_scalar_field(DataType::INT64, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    // Some, but not all required columns present in StreamDescriptor
    node = std::make_shared<ExpressionNode>(ColumnName("col1"), ColumnName("col4"), OperationType::ADD);
    ec = ExpressionContext();
    ec.root_node_name_ = ExpressionName("root");
    ec.add_expression_node("root", node);
    project_clause = ProjectClause{{"col1", "col4"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);

    // No required columns present in StreamDescriptor
    node = std::make_shared<ExpressionNode>(ColumnName("col4"), ColumnName("col5"), OperationType::ADD);
    ec = ExpressionContext();
    ec.root_node_name_ = ExpressionName("root");
    ec.add_expression_node("root", node);
    project_clause = ProjectClause{{"col4", "col5"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);
}

TEST(OutputSchema, PartitionClause) {
    using GroupByClause = PartitionClause<arcticdb::grouping::HashingGroupers, arcticdb::grouping::ModuloBucketizer>;
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({IndexDescriptor::Type::ROWCOUNT, 0});
    stream_desc.add_scalar_field(DataType::INT64, "col");

    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(false);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_start(0);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_step(1);

    // Grouping column present in StreamDescriptor
    GroupByClause groupby_clause{"col"};
    auto output_schema = groupby_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    // Grouping column not present in StreamDescriptor
    groupby_clause = GroupByClause{"col1"};
    ASSERT_THROW(groupby_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);
}

TEST(OutputSchema, AggregationClauseColumnPresence) {
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({IndexDescriptor::Type::ROWCOUNT, 0});
    stream_desc.add_scalar_field(DataType::INT64, "to_group");
    stream_desc.add_scalar_field(DataType::INT64, "to_agg");

    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(false);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_start(0);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_step(1);

    // Grouping column not present in StreamDescriptor
    AggregationClause aggregation_clause{"dummy", {{"sum", "to_agg", "aggregated"}}};
    ASSERT_THROW(aggregation_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);

    // Aggregation column not present in StreamDescriptor
    aggregation_clause = AggregationClause{"to_group", {{"sum", "dummy", "aggregated"}}};
    ASSERT_THROW(aggregation_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);

    // Both columns present in StreamDescriptor
    aggregation_clause = AggregationClause{"to_group", {{"sum", "to_agg", "aggregated"}}};
    auto output_schema = aggregation_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor().field(0), stream_desc.field(0));
    ASSERT_EQ(output_schema.stream_descriptor().field(1).name(), "aggregated");
    ASSERT_EQ(output_schema.norm_metadata_.df().common().index().name(), "to_group");
    ASSERT_FALSE(output_schema.norm_metadata_.df().common().index().fake_name());
    ASSERT_TRUE(output_schema.norm_metadata_.df().common().index().is_physically_stored());
}

TEST(OutputSchema, ResampleClauseRequiresTimeseries) {
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({IndexDescriptor::Type::ROWCOUNT, 0});
    stream_desc.add_scalar_field(DataType::INT64, "to_agg");

    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(false);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_start(0);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_step(1);

    auto resample_clause = generate_resample_clause({{"sum", "to_agg", "aggregated"}});
    ASSERT_THROW(resample_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);
}

TEST(OutputSchema, ResampleClauseColumnPresence) {
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({IndexDescriptor::Type::TIMESTAMP, 1});
    stream_desc.add_scalar_field(DataType::NANOSECONDS_UTC64, "timestamp");
    stream_desc.add_scalar_field(DataType::INT64, "to_agg");

    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_name("timestamp");
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(true);

    // Aggregation column not present in StreamDescriptor
    auto resample_clause = generate_resample_clause({{"sum", "dummy", "aggregated"}});
    ASSERT_THROW(resample_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);

    // Aggregation column present in StreamDescriptor
    resample_clause = generate_resample_clause({{"sum", "to_agg", "aggregated"}});
    auto output_schema = resample_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor().field(0), stream_desc.field(0));
    ASSERT_EQ(output_schema.stream_descriptor().field(1).name(), "aggregated");
    ASSERT_EQ(output_schema.norm_metadata_.df().common().index().name(), "timestamp");
    ASSERT_FALSE(output_schema.norm_metadata_.df().common().index().fake_name());
    ASSERT_TRUE(output_schema.norm_metadata_.df().common().index().is_physically_stored());
}

TEST(OutputSchema, ResampleClauseMultiindex) {
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({IndexDescriptor::Type::TIMESTAMP, 2});
    stream_desc.add_scalar_field(DataType::NANOSECONDS_UTC64, "timestamp");
    stream_desc.add_scalar_field(DataType::INT64, "second_level_index");
    stream_desc.add_scalar_field(DataType::INT64, "to_agg");

    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    norm_meta.mutable_df()->mutable_common()->mutable_multi_index()->set_name("timestamp");
    norm_meta.mutable_df()->mutable_common()->mutable_multi_index()->set_field_count(2);

    // Aggregating non-index column
    auto resample_clause = generate_resample_clause({{"sum", "to_agg", "aggregated"}});
    auto output_schema = resample_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor().index(), IndexDescriptorImpl(IndexDescriptor::Type::TIMESTAMP, 1));
    ASSERT_EQ(output_schema.stream_descriptor().field(0), stream_desc.field(0));
    ASSERT_EQ(output_schema.stream_descriptor().field(1).name(), "aggregated");
    ASSERT_TRUE(output_schema.norm_metadata_.df().common().has_index());
    ASSERT_EQ(output_schema.norm_metadata_.df().common().index().name(), "timestamp");
    ASSERT_FALSE(output_schema.norm_metadata_.df().common().index().fake_name());
    ASSERT_TRUE(output_schema.norm_metadata_.df().common().index().is_physically_stored());

    // Aggregating secondary index column
    resample_clause = generate_resample_clause({{"sum", "second_level_index", "aggregated"}});
    output_schema = resample_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor().index(), IndexDescriptorImpl(IndexDescriptor::Type::TIMESTAMP, 1));
    ASSERT_EQ(output_schema.stream_descriptor().field(0), stream_desc.field(0));
    ASSERT_EQ(output_schema.stream_descriptor().field(1).name(), "aggregated");
    ASSERT_TRUE(output_schema.norm_metadata_.df().common().has_index());
    ASSERT_EQ(output_schema.norm_metadata_.df().common().index().name(), "timestamp");
    ASSERT_FALSE(output_schema.norm_metadata_.df().common().index().fake_name());
    ASSERT_TRUE(output_schema.norm_metadata_.df().common().index().is_physically_stored());
}
