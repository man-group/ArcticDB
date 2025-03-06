/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <google/protobuf/util/message_differencer.h>

#include <gtest/gtest.h>
#include <arcticdb/processing/clause.hpp>

using namespace arcticdb;
using namespace arcticdb::pipelines;
using namespace google::protobuf::util;

// TODO: Move into fixture?
TEST(OutputSchema, NonModifyingClauses) {
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({1, IndexDescriptor::Type::TIMESTAMP});
    stream_desc.add_scalar_field(DataType::NANOSECONDS_UTC64, "timestamp");
    stream_desc.add_scalar_field(DataType::INT64, "col");

    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_name("timestamp");
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(true);

    PassthroughClause passthrough_clause;
    auto output_schema = passthrough_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor_, stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    RowRangeClause row_range_clause{0, 5};
    output_schema = row_range_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor_, stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    DateRangeClause date_range_clause{{}, {}};
    output_schema = date_range_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor_, stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    RemoveColumnPartitioningClause remove_column_partitioning_clause;
    output_schema = remove_column_partitioning_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor_, stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    SplitClause split_clause{{}};
    output_schema = split_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor_, stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    SortClause sort_clause{{}, {}};
    output_schema = sort_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor_, stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    MergeClause merge_clause{stream::EmptyIndex{}, {}, {}, {}, {}};
    output_schema = merge_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor_, stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));
}

TEST(OutputSchema, DateRangeClauseRequiresTimeseries) {
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({0, IndexDescriptor::Type::ROWCOUNT});
    stream_desc.add_scalar_field(DataType::INT64, "col");

    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(false);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_start(0);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_step(1);

    DateRangeClause date_range_clause{0, 5};
    EXPECT_THROW(date_range_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);
}

TEST(OutputSchema, MergeClauseRequiresTimeseries) {
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({0, IndexDescriptor::Type::ROWCOUNT});
    stream_desc.add_scalar_field(DataType::INT64, "col");

    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(false);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_start(0);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_step(1);

    MergeClause merge_clause{stream::EmptyIndex{}, {}, {}, {}, {}};
    EXPECT_THROW(merge_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);
}

TEST(OutputSchema, FilterClauseColumnPresence) {
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({0, IndexDescriptor::Type::ROWCOUNT});
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
    ASSERT_EQ(output_schema.stream_descriptor_, stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    // Some, but not all required columns present in StreamDescriptor
    node = std::make_shared<ExpressionNode>(ColumnName("col1"), ColumnName("col4"), OperationType::EQ);
    ec = ExpressionContext();
    ec.root_node_name_ = ExpressionName("blah");
    ec.add_expression_node("blah", node);
    filter_clause = FilterClause{{"col1", "col4"}, ec, {}};
    EXPECT_THROW(filter_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);

    // No required columns present in StreamDescriptor
    node = std::make_shared<ExpressionNode>(ColumnName("col4"), ColumnName("col5"), OperationType::EQ);
    ec = ExpressionContext();
    ec.root_node_name_ = ExpressionName("blah");
    ec.add_expression_node("blah", node);
    filter_clause = FilterClause{{"col4", "col5"}, ec, {}};
    EXPECT_THROW(filter_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);
}

TEST(OutputSchema, ProjectClauseColumnPresence) {
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({0, IndexDescriptor::Type::ROWCOUNT});
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
    ec.root_node_name_ = ExpressionName("new col");
    ec.add_expression_node("new col", node);
    ProjectClause project_clause{{"col1", "col3"}, "new col", ec};
    auto output_schema = project_clause.modify_schema({stream_desc.clone(), norm_meta});
    stream_desc.add_scalar_field(DataType::INT64, "new col");
    ASSERT_EQ(output_schema.stream_descriptor_, stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    // Some, but not all required columns present in StreamDescriptor
    node = std::make_shared<ExpressionNode>(ColumnName("col1"), ColumnName("col4"), OperationType::ADD);
    ec = ExpressionContext();
    ec.root_node_name_ = ExpressionName("new col");
    ec.add_expression_node("new col", node);
    project_clause = ProjectClause{{"col1", "col4"}, "new col", ec};
    EXPECT_THROW(project_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);

    // No required columns present in StreamDescriptor
    node = std::make_shared<ExpressionNode>(ColumnName("col4"), ColumnName("col5"), OperationType::ADD);
    ec = ExpressionContext();
    ec.root_node_name_ = ExpressionName("new col");
    ec.add_expression_node("new col", node);
    project_clause = ProjectClause{{"col4", "col5"}, "new col", ec};
    EXPECT_THROW(project_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);
}

// Type promotion rules are tested exhaustively in test_arithmetic_type_promotion.cpp, so we can just exercise the major
// code paths once each here
class AstParsingOutputTypesTest : public testing::Test {
protected:
    void SetUp() override {
        initial_stream_desc_.set_id(StreamId("test symbol"));
        initial_stream_desc_.add_scalar_field(DataType::INT32, "int32");
        initial_stream_desc_.add_scalar_field(DataType::UINT8, "uint8");
        initial_stream_desc_.add_scalar_field(DataType::UTF_DYNAMIC64, "string");
        initial_stream_desc_.add_scalar_field(DataType::BOOL8, "bool");
    }

    OutputSchema initial_schema() {
        return {initial_stream_desc_.clone(), {}};
    }
    StreamDescriptor initial_stream_desc_;
};

TEST_F(AstParsingOutputTypesTest, FilterNotBitset) {
    ExpressionContext ec;
    auto node_1 = std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("uint8"), OperationType::EQ);
    ec.add_expression_node("bitset", node_1);
    auto node_2 = std::make_shared<ExpressionNode>(ExpressionName("bitset"), OperationType::NOT);
    ec.add_expression_node("root", node_2);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor_, initial_schema().stream_descriptor_);
}

TEST_F(AstParsingOutputTypesTest, FilterNotBoolColumn) {
    ExpressionContext ec;
    auto node = std::make_shared<ExpressionNode>(ColumnName("bool"), OperationType::NOT);
    ec.add_expression_node("root", node);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"bool"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor_, initial_schema().stream_descriptor_);
}

TEST_F(AstParsingOutputTypesTest, FilterNotNumericColumn) {
    ExpressionContext ec;
    auto node = std::make_shared<ExpressionNode>(ColumnName("int32"), OperationType::NOT);
    ec.add_expression_node("root", node);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"int32"}, ec, {}};
    EXPECT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterIsNullStringColumn) {
    ExpressionContext ec;
    auto node = std::make_shared<ExpressionNode>(ColumnName("string"), OperationType::ISNULL);
    ec.add_expression_node("root", node);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"string"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor_, initial_schema().stream_descriptor_);
}

TEST_F(AstParsingOutputTypesTest, FilterIsNullNumericColumn) {
    ExpressionContext ec;
    auto node = std::make_shared<ExpressionNode>(ColumnName("int32"), OperationType::ISNULL);
    ec.add_expression_node("root", node);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"int32"}, ec, {}};
    EXPECT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterIsNullBitset) {
    ExpressionContext ec;
    auto node_1 = std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("uint8"), OperationType::EQ);
    ec.add_expression_node("bitset", node_1);
    auto node_2 = std::make_shared<ExpressionNode>(ExpressionName("bitset"), OperationType::ISNULL);
    ec.add_expression_node("root", node_2);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    EXPECT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterEqNumericCols) {
    ExpressionContext ec;
    auto node = std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("uint8"), OperationType::EQ);
    ec.add_expression_node("root", node);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor_, initial_schema().stream_descriptor_);
}

TEST_F(AstParsingOutputTypesTest, FilterEqStringColStringVal) {
    ExpressionContext ec;
    auto value = std::make_shared<Value>(construct_string_value("hello"));
    ec.add_value("value", value);
    auto node = std::make_shared<ExpressionNode>(ColumnName("string"), ValueName("value"), OperationType::EQ);
    ec.add_expression_node("root", node);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"string"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor_, initial_schema().stream_descriptor_);
}

TEST_F(AstParsingOutputTypesTest, FilterEqNumericColStringVal) {
    ExpressionContext ec;
    auto value = std::make_shared<Value>(construct_string_value("hello"));
    ec.add_value("value", value);
    auto node = std::make_shared<ExpressionNode>(ColumnName("int32"), ValueName("value"), OperationType::EQ);
    ec.add_expression_node("root", node);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"int32"}, ec, {}};
    EXPECT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterEqColValueSet) {
    ExpressionContext ec;
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.add_value_set("value_set", value_set);
    auto node = std::make_shared<ExpressionNode>(ColumnName("int32"), ValueSetName("value_set"), OperationType::EQ);
    ec.add_expression_node("root", node);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"int32"}, ec, {}};
    EXPECT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterLessThanNumericColNumericVal) {
    ExpressionContext ec;
    auto value = std::make_shared<Value>(construct_value<uint16_t>(5));
    ec.add_value("value", value);
    auto node = std::make_shared<ExpressionNode>(ColumnName("int32"), ValueName("value"), OperationType::LT);
    ec.add_expression_node("root", node);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"int32"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor_, initial_schema().stream_descriptor_);
}

TEST_F(AstParsingOutputTypesTest, FilterLessThanStringColStringVal) {
    ExpressionContext ec;
    auto value = std::make_shared<Value>(construct_string_value("hello"));
    ec.add_value("value", value);
    auto node = std::make_shared<ExpressionNode>(ColumnName("string"), ValueName("value"), OperationType::LT);
    ec.add_expression_node("root", node);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"string"}, ec, {}};
    EXPECT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterLessThanNumericColBitset) {
    ExpressionContext ec;
    auto node_1 = std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("uint8"), OperationType::EQ);
    ec.add_expression_node("bitset", node_1);
    auto node_2 = std::make_shared<ExpressionNode>(ColumnName("int32"), ExpressionName("bitset"), OperationType::LT);
    ec.add_expression_node("root", node_2);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    EXPECT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterIsInNumericColNumericValueSet) {
    ExpressionContext ec;
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.add_value_set("value_set", value_set);
    auto node = std::make_shared<ExpressionNode>(ColumnName("int32"), ValueSetName("value_set"), OperationType::ISIN);
    ec.add_expression_node("root", node);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"int32"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor_, initial_schema().stream_descriptor_);
}

TEST_F(AstParsingOutputTypesTest, FilterIsInStringColStringValueSet) {
    ExpressionContext ec;
    std::vector<std::string> raw_set{"hello", "goodbye"};
    auto value_set = std::make_shared<ValueSet>(std::move(raw_set));
    ec.add_value_set("value_set", value_set);
    auto node = std::make_shared<ExpressionNode>(ColumnName("string"), ValueSetName("value_set"), OperationType::ISIN);
    ec.add_expression_node("root", node);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"string"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor_, initial_schema().stream_descriptor_);
}

TEST_F(AstParsingOutputTypesTest, FilterIsInNumericColEmptyValueSet) {
    ExpressionContext ec;
    std::vector<std::string> raw_set;
    auto value_set = std::make_shared<ValueSet>(std::move(raw_set));
    ec.add_value_set("value_set", value_set);
    auto node = std::make_shared<ExpressionNode>(ColumnName("int32"), ValueSetName("value_set"), OperationType::ISIN);
    ec.add_expression_node("root", node);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"int32"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor_, initial_schema().stream_descriptor_);
}

TEST_F(AstParsingOutputTypesTest, FilterIsInBitsetValueSet) {
    ExpressionContext ec;
    auto node_1 = std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("uint8"), OperationType::EQ);
    ec.add_expression_node("bitset", node_1);
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.add_value_set("value_set", value_set);
    auto node_2 = std::make_shared<ExpressionNode>(ExpressionName("bitset"), ValueSetName("value_set"), OperationType::ISIN);
    ec.add_expression_node("root", node_2);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    EXPECT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterIsInNumericColNumericValue) {
    ExpressionContext ec;
    auto value = std::make_shared<Value>(construct_value<uint16_t>(5));
    ec.add_value("value", value);
    auto node = std::make_shared<ExpressionNode>(ColumnName("int32"), ValueName("value"), OperationType::ISIN);
    ec.add_expression_node("root", node);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"int32"}, ec, {}};
    EXPECT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterAndBitsetBitset) {
    ExpressionContext ec;
    auto node_1 = std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("uint8"), OperationType::EQ);
    ec.add_expression_node("bitset_1", node_1);
    auto node_2 = std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("uint8"), OperationType::LT);
    ec.add_expression_node("bitset_2", node_2);
    auto node_3 = std::make_shared<ExpressionNode>(ExpressionName("bitset_1"), ExpressionName("bitset_2"), OperationType::AND);
    ec.add_expression_node("root", node_3);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor_, initial_schema().stream_descriptor_);
}

TEST_F(AstParsingOutputTypesTest, FilterAndBitsetBoolColumn) {
    ExpressionContext ec;
    auto node_1 = std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("uint8"), OperationType::EQ);
    ec.add_expression_node("bitset", node_1);
    auto node_2 = std::make_shared<ExpressionNode>(ExpressionName("bitset"), ColumnName("bool"), OperationType::AND);
    ec.add_expression_node("root", node_2);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"int32", "uint8", "bool"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor_, initial_schema().stream_descriptor_);
}

TEST_F(AstParsingOutputTypesTest, FilterAndNumericCols) {
    ExpressionContext ec;
    auto node = std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("uint8"), OperationType::AND);
    ec.add_expression_node("root", node);
    ec.root_node_name_ = ExpressionName("root");
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    EXPECT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAbsNumeric) {
    ExpressionContext ec;
    auto node = std::make_shared<ExpressionNode>(ColumnName("int32"), OperationType::ABS);
    ec.add_expression_node("new col", node);
    ec.root_node_name_ = ExpressionName("new col");
    ProjectClause project_clause{{"int32"}, "new col", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor_;
    stream_desc.add_scalar_field(DataType::INT64, "new col");
    ASSERT_EQ(output_schema.stream_descriptor_, stream_desc);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAbsString) {
    ExpressionContext ec;
    auto node = std::make_shared<ExpressionNode>(ColumnName("string"), OperationType::ABS);
    ec.add_expression_node("new col", node);
    ec.root_node_name_ = ExpressionName("new col");
    ProjectClause project_clause{{"string"}, "new col", ec};
    EXPECT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAbsBitset) {
    ExpressionContext ec;
    auto node_1 = std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("uint8"), OperationType::EQ);
    ec.add_expression_node("bitset", node_1);
    auto node_2 = std::make_shared<ExpressionNode>(ExpressionName("bitset"), OperationType::ABS);
    ec.add_expression_node("new col", node_2);
    ec.root_node_name_ = ExpressionName("new col");
    ProjectClause project_clause{{"int32", "uint8"}, "new col", ec};
    EXPECT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAddNumericCols) {
    ExpressionContext ec;
    auto node = std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("uint8"), OperationType::ADD);
    ec.add_expression_node("new col", node);
    ec.root_node_name_ = ExpressionName("new col");
    ProjectClause project_clause{{"int32", "uint8"}, "new col", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor_;
    stream_desc.add_scalar_field(DataType::INT64, "new col");
    ASSERT_EQ(output_schema.stream_descriptor_, stream_desc);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAddNumericColVal) {
    ExpressionContext ec;
    auto value = std::make_shared<Value>(construct_value<uint16_t>(5));
    ec.add_value("5", value);
    auto node = std::make_shared<ExpressionNode>(ColumnName("uint8"), ValueName("5"), OperationType::ADD);
    ec.add_expression_node("new col", node);
    ec.root_node_name_ = ExpressionName("new col");
    ProjectClause project_clause{{"uint8"}, "new col", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor_;
    stream_desc.add_scalar_field(DataType::UINT32, "new col");
    ASSERT_EQ(output_schema.stream_descriptor_, stream_desc);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAddNumericColStringCol) {
    ExpressionContext ec;
    auto node = std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("string"), OperationType::ADD);
    ec.add_expression_node("new col", node);
    ec.root_node_name_ = ExpressionName("new col");
    ProjectClause project_clause{{"int32", "string"}, "new col", ec};
    EXPECT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAddNumericColBitset) {
    ExpressionContext ec;
    auto node_1 = std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("uint8"), OperationType::EQ);
    ec.add_expression_node("bitset", node_1);
    auto node_2 = std::make_shared<ExpressionNode>(ColumnName("int32"), ExpressionName("bitset"), OperationType::ADD);
    ec.add_expression_node("new col", node_2);
    ec.root_node_name_ = ExpressionName("new col");
    ProjectClause project_clause{{"int32", "uint8"}, "new col", ec};
    EXPECT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAddNumericColValueSet) {
    ExpressionContext ec;
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.add_value_set("value_set", value_set);
    auto node = std::make_shared<ExpressionNode>(ColumnName("uint8"), ValueSetName("value_set"), OperationType::ADD);
    ec.add_expression_node("new col", node);
    ec.root_node_name_ = ExpressionName("new col");
    ProjectClause project_clause{{"uint8"}, "new col", ec};
    EXPECT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAddNumericColEmptyValueSet) {
    ExpressionContext ec;
    std::unordered_set<uint8_t> raw_set;
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.add_value_set("value_set", value_set);
    auto node = std::make_shared<ExpressionNode>(ColumnName("uint8"), ValueSetName("value_set"), OperationType::ADD);
    ec.add_expression_node("new col", node);
    ec.root_node_name_ = ExpressionName("new col");
    ProjectClause project_clause{{"uint8"}, "new col", ec};
    EXPECT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST(OutputSchema, PartitionClause) {
    using GroupByClause = PartitionClause<arcticdb::grouping::HashingGroupers, arcticdb::grouping::ModuloBucketizer>;
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({0, IndexDescriptor::Type::ROWCOUNT});
    stream_desc.add_scalar_field(DataType::INT64, "col");

    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(false);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_start(0);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_step(1);

    // Grouping column present in StreamDescriptor
    GroupByClause groupby_clause{"col"};
    auto output_schema = groupby_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor_, stream_desc);
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, norm_meta));

    // Grouping column not present in StreamDescriptor
    groupby_clause = GroupByClause{"col1"};
    EXPECT_THROW(groupby_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);
}

TEST(OutputSchema, AggregationClauseColumnPresence) {
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({0, IndexDescriptor::Type::ROWCOUNT});
    stream_desc.add_scalar_field(DataType::INT64, "to_group");
    stream_desc.add_scalar_field(DataType::INT64, "to_agg");

    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(false);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_start(0);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_step(1);

    // Grouping column not present in StreamDescriptor
    AggregationClause aggregation_clause{"dummy", {{"sum", "to_agg", "aggregated"}}};
    EXPECT_THROW(aggregation_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);

    // Aggregation column not present in StreamDescriptor
    aggregation_clause = AggregationClause{"to_group", {{"sum", "dummy", "aggregated"}}};
    EXPECT_THROW(aggregation_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);

    // Both columns present in StreamDescriptor
    aggregation_clause = AggregationClause{"to_group", {{"sum", "to_agg", "aggregated"}}};
    auto output_schema = aggregation_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor_.field(0), stream_desc.field(0));
    ASSERT_EQ(output_schema.stream_descriptor_.field(1).name(), "aggregated");
    ASSERT_EQ(output_schema.norm_metadata_.df().common().index().name(), "to_group");
    ASSERT_FALSE(output_schema.norm_metadata_.df().common().index().fake_name());
    ASSERT_TRUE(output_schema.norm_metadata_.df().common().index().is_physically_stored());
}

class AggregationClauseOutputTypesTest : public testing::Test {
protected:
    void SetUp() override {
        initial_stream_desc_.set_id(StreamId("test symbol"));
        initial_stream_desc_.set_index({0, IndexDescriptor::Type::ROWCOUNT});
        initial_stream_desc_.add_scalar_field(DataType::INT64, "to_group");
        initial_stream_desc_.add_scalar_field(DataType::INT8, "int8");
        initial_stream_desc_.add_scalar_field(DataType::INT16, "int16");
        initial_stream_desc_.add_scalar_field(DataType::INT32, "int32");
        initial_stream_desc_.add_scalar_field(DataType::INT64, "int64");
        initial_stream_desc_.add_scalar_field(DataType::UINT8, "uint8");
        initial_stream_desc_.add_scalar_field(DataType::UINT16, "uint16");
        initial_stream_desc_.add_scalar_field(DataType::UINT32, "uint32");
        initial_stream_desc_.add_scalar_field(DataType::UINT64, "uint64");
        initial_stream_desc_.add_scalar_field(DataType::FLOAT32, "float32");
        initial_stream_desc_.add_scalar_field(DataType::FLOAT64, "float64");
        initial_stream_desc_.add_scalar_field(DataType::BOOL8, "bool");
        initial_stream_desc_.add_scalar_field(DataType::NANOSECONDS_UTC64, "timestamp");
        initial_stream_desc_.add_scalar_field(DataType::UTF_DYNAMIC64, "string");
    }

    std::vector<NamedAggregator> generate_aggregators(
            const std::string& agg,
            bool timestamp_supported = true,
            bool string_supported = false) const {
        std::vector<NamedAggregator> res
                {
                        {agg, "int8", "int8_agg"},
                        {agg, "int16", "int16_agg"},
                        {agg, "int32", "int32_agg"},
                        {agg, "int64", "int64_agg"},
                        {agg, "uint8", "uint8_agg"},
                        {agg, "uint16", "uint16_agg"},
                        {agg, "uint32", "uint32_agg"},
                        {agg, "uint64", "uint64_agg"},
                        {agg, "float32", "float32_agg"},
                        {agg, "float64", "float64_agg"},
                        {agg, "bool", "bool_agg"}
                };
        if (timestamp_supported) {
            res.emplace_back(agg, "timestamp", "timestamp_agg");
        }
        if (string_supported) {
            res.emplace_back(agg, "string", "string_agg");
        }
        return res;
    }

    void check_output_column_names(
            const StreamDescriptor& stream_desc,
            bool timestamp_supported = true,
            bool string_supported = false) const {
        ASSERT_EQ(stream_desc.field(0).name(), "to_group");
        ASSERT_EQ(stream_desc.field(1).name(), "int8_agg");
        ASSERT_EQ(stream_desc.field(2).name(), "int16_agg");
        ASSERT_EQ(stream_desc.field(3).name(), "int32_agg");
        ASSERT_EQ(stream_desc.field(4).name(), "int64_agg");
        ASSERT_EQ(stream_desc.field(5).name(), "uint8_agg");
        ASSERT_EQ(stream_desc.field(6).name(), "uint16_agg");
        ASSERT_EQ(stream_desc.field(7).name(), "uint32_agg");
        ASSERT_EQ(stream_desc.field(8).name(), "uint64_agg");
        ASSERT_EQ(stream_desc.field(9).name(), "float32_agg");
        ASSERT_EQ(stream_desc.field(10).name(), "float64_agg");
        ASSERT_EQ(stream_desc.field(11).name(), "bool_agg");
        if (timestamp_supported) {
            ASSERT_EQ(stream_desc.field(12).name(), "timestamp_agg");
        }
        if (string_supported) {
            ASSERT_EQ(stream_desc.field(timestamp_supported ? 13 : 12).name(), "string_agg");
        }
    }

    OutputSchema initial_schema() {
        return {initial_stream_desc_.clone(), {}};
    }
    StreamDescriptor initial_stream_desc_;
};

TEST_F(AggregationClauseOutputTypesTest, Sum) {
    AggregationClause aggregation_clause{"to_group", generate_aggregators("sum", false)};
    auto output_schema = aggregation_clause.modify_schema(initial_schema());
    const auto& stream_desc = output_schema.stream_descriptor_;
    check_output_column_names(stream_desc, false);
    ASSERT_EQ(stream_desc.field(0).type().data_type(), DataType::INT64); // grouping column
    ASSERT_EQ(stream_desc.field(1).type().data_type(), DataType::INT64); // int8
    ASSERT_EQ(stream_desc.field(2).type().data_type(), DataType::INT64); // int16
    ASSERT_EQ(stream_desc.field(3).type().data_type(), DataType::INT64); // int32
    ASSERT_EQ(stream_desc.field(4).type().data_type(), DataType::INT64); // int64
    ASSERT_EQ(stream_desc.field(5).type().data_type(), DataType::UINT64); // uint8
    ASSERT_EQ(stream_desc.field(6).type().data_type(), DataType::UINT64); // uint16
    ASSERT_EQ(stream_desc.field(7).type().data_type(), DataType::UINT64); // uint32
    ASSERT_EQ(stream_desc.field(8).type().data_type(), DataType::UINT64); // uint64
    ASSERT_EQ(stream_desc.field(9).type().data_type(), DataType::FLOAT64); // float32
    ASSERT_EQ(stream_desc.field(10).type().data_type(), DataType::FLOAT64); // float64
    ASSERT_EQ(stream_desc.field(11).type().data_type(), DataType::BOOL8); // bool

    aggregation_clause = AggregationClause{"to_group", {{"sum", "timestamp", "timestamp_sum"}}};
    EXPECT_THROW(aggregation_clause.modify_schema(initial_schema()), SchemaException);

    aggregation_clause = AggregationClause{"to_group", {{"sum", "string", "string_sum"}}};
    EXPECT_THROW(aggregation_clause.modify_schema(initial_schema()), SchemaException);
}

TEST_F(AggregationClauseOutputTypesTest, Max) {
    AggregationClause aggregation_clause{"to_group", generate_aggregators("max")};
    auto output_schema = aggregation_clause.modify_schema(initial_schema());
    const auto& stream_desc = output_schema.stream_descriptor_;
    check_output_column_names(stream_desc);
    for (size_t idx = 0; idx < 13; ++idx) {
        ASSERT_EQ(stream_desc.field(idx).type().data_type(), initial_stream_desc_.field(idx).type().data_type());
    }

    aggregation_clause = AggregationClause{"to_group", {{"max", "string", "string_agg"}}};
    EXPECT_THROW(aggregation_clause.modify_schema(initial_schema()), SchemaException);
}

TEST_F(AggregationClauseOutputTypesTest, Min) {
    AggregationClause aggregation_clause{"to_group", generate_aggregators("min")};
    auto output_schema = aggregation_clause.modify_schema(initial_schema());
    const auto& stream_desc = output_schema.stream_descriptor_;
    check_output_column_names(stream_desc);
    for (size_t idx = 0; idx < 13; ++idx) {
        ASSERT_EQ(stream_desc.field(idx).type().data_type(), initial_stream_desc_.field(idx).type().data_type());
    }

    aggregation_clause = AggregationClause{"to_group", {{"min", "string", "string_agg"}}};
    EXPECT_THROW(aggregation_clause.modify_schema(initial_schema()), SchemaException);
}

TEST_F(AggregationClauseOutputTypesTest, Mean) {
    AggregationClause aggregation_clause{"to_group", generate_aggregators("mean")};
    auto output_schema = aggregation_clause.modify_schema(initial_schema());
    const auto& stream_desc = output_schema.stream_descriptor_;
    check_output_column_names(stream_desc);
    ASSERT_EQ(stream_desc.field(0).type().data_type(), DataType::INT64); // grouping column
    for (size_t idx = 1; idx < 13; ++idx) {
        ASSERT_EQ(stream_desc.field(idx).type().data_type(), DataType::FLOAT64);
    }

    aggregation_clause = AggregationClause{"to_group", {{"mean", "string", "string_agg"}}};
    EXPECT_THROW(aggregation_clause.modify_schema(initial_schema()), SchemaException);
}

TEST_F(AggregationClauseOutputTypesTest, Count) {
    AggregationClause aggregation_clause{"to_group", generate_aggregators("count", true, true)};
    auto output_schema = aggregation_clause.modify_schema(initial_schema());
    const auto& stream_desc = output_schema.stream_descriptor_;
    check_output_column_names(stream_desc, true, true);
    ASSERT_EQ(stream_desc.field(0).type().data_type(), DataType::INT64); // grouping column
    for (size_t idx = 1; idx < 14; ++idx) {
        ASSERT_EQ(stream_desc.field(idx).type().data_type(), DataType::UINT64);
    }
}

ResampleClause<ResampleBoundary::LEFT> generate_resample_clause(const std::vector<NamedAggregator>& named_aggregators) {
    ResampleClause<ResampleBoundary::LEFT> res{
        "dummy_rule",
        ResampleBoundary::LEFT,
        [](timestamp, timestamp, std::string_view, ResampleBoundary, timestamp, ResampleOrigin) -> std::vector<timestamp> { return {}; },
        0,
        "dummy_origin"
    };
    res.set_aggregations(named_aggregators);
    return res;
}

TEST(OutputSchema, ResampleClauseRequiresTimeseries) {
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({0, IndexDescriptor::Type::ROWCOUNT});
    stream_desc.add_scalar_field(DataType::INT64, "to_agg");

    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(false);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_start(0);
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_step(1);

    auto resample_clause = generate_resample_clause({{"sum", "to_agg", "aggregated"}});
    EXPECT_THROW(resample_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);
}

TEST(OutputSchema, ResampleClauseColumnPresence) {
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({1, IndexDescriptor::Type::TIMESTAMP});
    stream_desc.add_scalar_field(DataType::NANOSECONDS_UTC64, "timestamp");
    stream_desc.add_scalar_field(DataType::INT64, "to_agg");

    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_name("timestamp");
    norm_meta.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(true);

    // Aggregation column not present in StreamDescriptor
    auto resample_clause = generate_resample_clause({{"sum", "dummy", "aggregated"}});
    EXPECT_THROW(resample_clause.modify_schema({stream_desc.clone(), norm_meta}), SchemaException);

    // Aggregation column present in StreamDescriptor
    resample_clause = generate_resample_clause({{"sum", "to_agg", "aggregated"}});
    auto output_schema = resample_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor_.field(0), stream_desc.field(0));
    ASSERT_EQ(output_schema.stream_descriptor_.field(1).name(), "aggregated");
    ASSERT_EQ(output_schema.norm_metadata_.df().common().index().name(), "timestamp");
    ASSERT_FALSE(output_schema.norm_metadata_.df().common().index().fake_name());
    ASSERT_TRUE(output_schema.norm_metadata_.df().common().index().is_physically_stored());
}

TEST(OutputSchema, ResampleClauseMultiindex) {
    StreamDescriptor stream_desc(StreamId("test symbol"));
    stream_desc.set_index({2, IndexDescriptor::Type::TIMESTAMP});
    stream_desc.add_scalar_field(DataType::NANOSECONDS_UTC64, "timestamp");
    stream_desc.add_scalar_field(DataType::INT64, "second_level_index");
    stream_desc.add_scalar_field(DataType::INT64, "to_agg");

    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    norm_meta.mutable_df()->mutable_common()->mutable_multi_index()->set_name("timestamp");
    norm_meta.mutable_df()->mutable_common()->mutable_multi_index()->set_field_count(2);

    // Aggregating non-index column
    auto resample_clause = generate_resample_clause({{"sum", "to_agg", "aggregated"}});
    auto output_schema = resample_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor_.index(), IndexDescriptorImpl(1, IndexDescriptor::Type::TIMESTAMP));
    ASSERT_EQ(output_schema.stream_descriptor_.field(0), stream_desc.field(0));
    ASSERT_EQ(output_schema.stream_descriptor_.field(1).name(), "aggregated");
    ASSERT_TRUE(output_schema.norm_metadata_.df().common().has_index());
    ASSERT_EQ(output_schema.norm_metadata_.df().common().index().name(), "timestamp");
    ASSERT_FALSE(output_schema.norm_metadata_.df().common().index().fake_name());
    ASSERT_TRUE(output_schema.norm_metadata_.df().common().index().is_physically_stored());

    // Aggregating secondary index column
    resample_clause = generate_resample_clause({{"sum", "second_level_index", "aggregated"}});
    output_schema = resample_clause.modify_schema({stream_desc.clone(), norm_meta});
    ASSERT_EQ(output_schema.stream_descriptor_.index(), IndexDescriptorImpl(1, IndexDescriptor::Type::TIMESTAMP));
    ASSERT_EQ(output_schema.stream_descriptor_.field(0), stream_desc.field(0));
    ASSERT_EQ(output_schema.stream_descriptor_.field(1).name(), "aggregated");
    ASSERT_TRUE(output_schema.norm_metadata_.df().common().has_index());
    ASSERT_EQ(output_schema.norm_metadata_.df().common().index().name(), "timestamp");
    ASSERT_FALSE(output_schema.norm_metadata_.df().common().index().fake_name());
    ASSERT_TRUE(output_schema.norm_metadata_.df().common().index().is_physically_stored());
}

class ResampleClauseOutputTypesTest : public testing::Test {
protected:
    void SetUp() override {
        initial_stream_desc_.set_id(StreamId("test symbol"));
        initial_stream_desc_.set_index({1, IndexDescriptor::Type::TIMESTAMP});
        initial_stream_desc_.add_scalar_field(DataType::NANOSECONDS_UTC64, "index");
        initial_stream_desc_.add_scalar_field(DataType::INT8, "int8");
        initial_stream_desc_.add_scalar_field(DataType::INT16, "int16");
        initial_stream_desc_.add_scalar_field(DataType::INT32, "int32");
        initial_stream_desc_.add_scalar_field(DataType::INT64, "int64");
        initial_stream_desc_.add_scalar_field(DataType::UINT8, "uint8");
        initial_stream_desc_.add_scalar_field(DataType::UINT16, "uint16");
        initial_stream_desc_.add_scalar_field(DataType::UINT32, "uint32");
        initial_stream_desc_.add_scalar_field(DataType::UINT64, "uint64");
        initial_stream_desc_.add_scalar_field(DataType::FLOAT32, "float32");
        initial_stream_desc_.add_scalar_field(DataType::FLOAT64, "float64");
        initial_stream_desc_.add_scalar_field(DataType::BOOL8, "bool");
        initial_stream_desc_.add_scalar_field(DataType::NANOSECONDS_UTC64, "timestamp");
        initial_stream_desc_.add_scalar_field(DataType::UTF_DYNAMIC64, "string");

        initial_norm_meta_.mutable_df()->mutable_common()->mutable_index()->set_name("index");
        initial_norm_meta_.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(true);
    }

    std::vector<NamedAggregator> generate_aggregators(
            const std::string &agg,
            bool timestamp_supported = true,
            bool string_supported = false) const {
        std::vector<NamedAggregator> res
                {
                        {agg, "int8",    "int8_agg"},
                        {agg, "int16",   "int16_agg"},
                        {agg, "int32",   "int32_agg"},
                        {agg, "int64",   "int64_agg"},
                        {agg, "uint8",   "uint8_agg"},
                        {agg, "uint16",  "uint16_agg"},
                        {agg, "uint32",  "uint32_agg"},
                        {agg, "uint64",  "uint64_agg"},
                        {agg, "float32", "float32_agg"},
                        {agg, "float64", "float64_agg"},
                        {agg, "bool",    "bool_agg"}
                };
        if (timestamp_supported) {
            res.emplace_back(agg, "timestamp", "timestamp_agg");
        }
        if (string_supported) {
            res.emplace_back(agg, "string", "string_agg");
        }
        return res;
    }

    void check_output_column_names(
            const StreamDescriptor &stream_desc,
            bool timestamp_supported = true,
            bool string_supported = false) const {
        ASSERT_EQ(stream_desc.field(0).name(), "index");
        ASSERT_EQ(stream_desc.field(1).name(), "int8_agg");
        ASSERT_EQ(stream_desc.field(2).name(), "int16_agg");
        ASSERT_EQ(stream_desc.field(3).name(), "int32_agg");
        ASSERT_EQ(stream_desc.field(4).name(), "int64_agg");
        ASSERT_EQ(stream_desc.field(5).name(), "uint8_agg");
        ASSERT_EQ(stream_desc.field(6).name(), "uint16_agg");
        ASSERT_EQ(stream_desc.field(7).name(), "uint32_agg");
        ASSERT_EQ(stream_desc.field(8).name(), "uint64_agg");
        ASSERT_EQ(stream_desc.field(9).name(), "float32_agg");
        ASSERT_EQ(stream_desc.field(10).name(), "float64_agg");
        ASSERT_EQ(stream_desc.field(11).name(), "bool_agg");
        if (timestamp_supported) {
            ASSERT_EQ(stream_desc.field(12).name(), "timestamp_agg");
        }
        if (string_supported) {
            ASSERT_EQ(stream_desc.field(timestamp_supported ? 13 : 12).name(), "string_agg");
        }
    }

    OutputSchema initial_schema() {
        return {initial_stream_desc_.clone(), initial_norm_meta_};
    }

    StreamDescriptor initial_stream_desc_;;
    arcticdb::proto::descriptors::NormalizationMetadata initial_norm_meta_;
};

TEST_F(ResampleClauseOutputTypesTest, Sum) {
    auto resample_clause = generate_resample_clause(generate_aggregators("sum", false));
    auto output_schema = resample_clause.modify_schema(initial_schema());
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, initial_norm_meta_));
    const auto& stream_desc = output_schema.stream_descriptor_;
    check_output_column_names(stream_desc, false);
    ASSERT_EQ(stream_desc.field(0).type().data_type(), DataType::NANOSECONDS_UTC64); // index column
    ASSERT_EQ(stream_desc.field(1).type().data_type(), DataType::INT64); // int8
    ASSERT_EQ(stream_desc.field(2).type().data_type(), DataType::INT64); // int16
    ASSERT_EQ(stream_desc.field(3).type().data_type(), DataType::INT64); // int32
    ASSERT_EQ(stream_desc.field(4).type().data_type(), DataType::INT64); // int64
    ASSERT_EQ(stream_desc.field(5).type().data_type(), DataType::UINT64); // uint8
    ASSERT_EQ(stream_desc.field(6).type().data_type(), DataType::UINT64); // uint16
    ASSERT_EQ(stream_desc.field(7).type().data_type(), DataType::UINT64); // uint32
    ASSERT_EQ(stream_desc.field(8).type().data_type(), DataType::UINT64); // uint64
    ASSERT_EQ(stream_desc.field(9).type().data_type(), DataType::FLOAT64); // float32
    ASSERT_EQ(stream_desc.field(10).type().data_type(), DataType::FLOAT64); // float64
    ASSERT_EQ(stream_desc.field(11).type().data_type(), DataType::UINT64); // bool

    resample_clause = generate_resample_clause({{"sum", "timestamp", "timestamp_agg"}});
    EXPECT_THROW(resample_clause.modify_schema(initial_schema()), SchemaException);

    resample_clause = generate_resample_clause({{"sum", "string", "string_agg"}});
    EXPECT_THROW(resample_clause.modify_schema(initial_schema()), SchemaException);
}

TEST_F(ResampleClauseOutputTypesTest, Max) {
    auto resample_clause = generate_resample_clause(generate_aggregators("max"));
    auto output_schema = resample_clause.modify_schema(initial_schema());
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, initial_norm_meta_));
    const auto& stream_desc = output_schema.stream_descriptor_;
    check_output_column_names(stream_desc);
    for (size_t idx = 0; idx < 13; ++idx) {
        ASSERT_EQ(stream_desc.field(idx).type().data_type(), initial_stream_desc_.field(idx).type().data_type());
    }

    resample_clause = generate_resample_clause({{"max", "string", "string_agg"}});
    EXPECT_THROW(resample_clause.modify_schema(initial_schema()), SchemaException);
}

TEST_F(ResampleClauseOutputTypesTest, Min) {
    auto resample_clause = generate_resample_clause(generate_aggregators("min"));
    auto output_schema = resample_clause.modify_schema(initial_schema());
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, initial_norm_meta_));
    const auto& stream_desc = output_schema.stream_descriptor_;
    check_output_column_names(stream_desc);
    for (size_t idx = 0; idx < 13; ++idx) {
        ASSERT_EQ(stream_desc.field(idx).type().data_type(), initial_stream_desc_.field(idx).type().data_type());
    }

    resample_clause = generate_resample_clause({{"min", "string", "string_agg"}});
    EXPECT_THROW(resample_clause.modify_schema(initial_schema()), SchemaException);
}

TEST_F(ResampleClauseOutputTypesTest, Mean) {
    auto resample_clause = generate_resample_clause(generate_aggregators("mean"));
    auto output_schema = resample_clause.modify_schema(initial_schema());
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, initial_norm_meta_));
    const auto& stream_desc = output_schema.stream_descriptor_;
    check_output_column_names(stream_desc);
    ASSERT_EQ(stream_desc.field(0).type().data_type(), DataType::NANOSECONDS_UTC64); // index column
    for (size_t idx = 1; idx < 12; ++idx) {
        ASSERT_EQ(stream_desc.field(idx).type().data_type(), DataType::FLOAT64);
    }
    ASSERT_EQ(stream_desc.field(12).type().data_type(), DataType::NANOSECONDS_UTC64); // timestamp column

    resample_clause = generate_resample_clause({{"mean", "string", "string_agg"}});
    EXPECT_THROW(resample_clause.modify_schema(initial_schema()), SchemaException);
}

TEST_F(ResampleClauseOutputTypesTest, Count) {
    auto resample_clause = generate_resample_clause(generate_aggregators("count", true, true));
    auto output_schema = resample_clause.modify_schema(initial_schema());
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, initial_norm_meta_));
    const auto& stream_desc = output_schema.stream_descriptor_;
    check_output_column_names(stream_desc, true, true);
    ASSERT_EQ(stream_desc.field(0).type().data_type(), DataType::NANOSECONDS_UTC64); // index column
    for (size_t idx = 1; idx < 14; ++idx) {
        ASSERT_EQ(stream_desc.field(idx).type().data_type(), DataType::UINT64);
    }
}

TEST_F(ResampleClauseOutputTypesTest, First) {
    auto resample_clause = generate_resample_clause(generate_aggregators("first", true, true));
    auto output_schema = resample_clause.modify_schema(initial_schema());
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, initial_norm_meta_));
    const auto& stream_desc = output_schema.stream_descriptor_;
    check_output_column_names(stream_desc, true, true);
    for (size_t idx = 0; idx < 14; ++idx) {
        ASSERT_EQ(stream_desc.field(idx).type().data_type(), initial_stream_desc_.field(idx).type().data_type());
    }
}

TEST_F(ResampleClauseOutputTypesTest, Last) {
    auto resample_clause = generate_resample_clause(generate_aggregators("last", true, true));
    auto output_schema = resample_clause.modify_schema(initial_schema());
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, initial_norm_meta_));
    const auto& stream_desc = output_schema.stream_descriptor_;
    check_output_column_names(stream_desc, true, true);
    for (size_t idx = 0; idx < 14; ++idx) {
        ASSERT_EQ(stream_desc.field(idx).type().data_type(), initial_stream_desc_.field(idx).type().data_type());
    }
}
