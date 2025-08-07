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

// Type promotion rules are tested exhaustively in test_arithmetic_type_promotion.cpp, so we can just exercise the major
// code paths once each here
class AstParsingOutputTypesTest : public testing::Test {
protected:
    void SetUp() override {
        initial_stream_desc.set_id(StreamId("test symbol"));
        initial_stream_desc.add_scalar_field(DataType::INT32, "int32");
        initial_stream_desc.add_scalar_field(DataType::UINT8, "uint8");
        initial_stream_desc.add_scalar_field(DataType::UTF_DYNAMIC64, "string");
        initial_stream_desc.add_scalar_field(DataType::BOOL8, "bool");

        ec = ExpressionContext();
        ec.root_node_name_ = ExpressionName("root");
    }

    OutputSchema initial_schema() {
        return {initial_stream_desc.clone(), {}};
    }

    std::shared_ptr<ExpressionNode> bitset_node() const {
        return std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("uint8"), OperationType::EQ);
    }

    StreamDescriptor initial_stream_desc;
    ExpressionContext ec;
};

TEST_F(AstParsingOutputTypesTest, FilterNotBitset) {
    ec.add_expression_node("bitset", bitset_node());
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ExpressionName("bitset"), OperationType::NOT));
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterNotBoolColumn) {
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("bool"), OperationType::NOT));
    FilterClause filter_clause{{"bool"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterNotNumericColumn) {
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("int32"), OperationType::NOT));
    FilterClause filter_clause{{"int32"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterIsNullStringColumn) {
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("string"), OperationType::ISNULL));
    FilterClause filter_clause{{"string"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterIsNullNumericColumn) {
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("int32"), OperationType::ISNULL));
    FilterClause filter_clause{{"int32"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterIsNullBitset) {
    ec.add_expression_node("bitset", bitset_node());
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ExpressionName("bitset"), OperationType::ISNULL));
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterEqNumericCols) {
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("uint8"), OperationType::EQ));
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterEqStringColStringVal) {
    auto value = std::make_shared<Value>(construct_string_value("hello"));
    ec.add_value("value", value);
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("string"), ValueName("value"), OperationType::EQ));
    FilterClause filter_clause{{"string"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterEqNumericColStringVal) {
    auto value = std::make_shared<Value>(construct_string_value("hello"));
    ec.add_value("value", value);
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("int32"), ValueName("value"), OperationType::EQ));
    FilterClause filter_clause{{"int32"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterEqColValueSet) {
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.add_value_set("value_set", value_set);
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("int32"), ValueSetName("value_set"), OperationType::EQ));
    FilterClause filter_clause{{"int32"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterLessThanNumericColNumericVal) {
    auto value = std::make_shared<Value>(construct_value<uint16_t>(5));
    ec.add_value("value", value);
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("int32"), ValueName("value"), OperationType::LT));
    FilterClause filter_clause{{"int32"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterLessThanStringColStringVal) {
    auto value = std::make_shared<Value>(construct_string_value("hello"));
    ec.add_value("value", value);
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("string"), ValueName("value"), OperationType::LT));
    FilterClause filter_clause{{"string"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterLessThanNumericColBitset) {
    ec.add_expression_node("bitset", bitset_node());
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("int32"), ExpressionName("bitset"), OperationType::LT));
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterIsInNumericColNumericValueSet) {
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.add_value_set("value_set", value_set);
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("int32"), ValueSetName("value_set"), OperationType::ISIN));
    FilterClause filter_clause{{"int32"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterIsInStringColStringValueSet) {
    std::vector<std::string> raw_set{"hello", "goodbye"};
    auto value_set = std::make_shared<ValueSet>(std::move(raw_set));
    ec.add_value_set("value_set", value_set);
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("string"), ValueSetName("value_set"), OperationType::ISIN));
    FilterClause filter_clause{{"string"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterIsInNumericColEmptyValueSet) {
    std::vector<std::string> raw_set;
    auto value_set = std::make_shared<ValueSet>(std::move(raw_set));
    ec.add_value_set("value_set", value_set);
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("int32"), ValueSetName("value_set"), OperationType::ISIN));
    FilterClause filter_clause{{"int32"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterIsInBitsetValueSet) {
    ec.add_expression_node("bitset", bitset_node());
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.add_value_set("value_set", value_set);
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ExpressionName("bitset"), ValueSetName("value_set"), OperationType::ISIN));
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterIsInNumericColNumericValue) {
    auto value = std::make_shared<Value>(construct_value<uint16_t>(5));
    ec.add_value("value", value);
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("int32"), ValueName("value"), OperationType::ISIN));
    FilterClause filter_clause{{"int32"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterAndBitsetBitset) {
    ec.add_expression_node("bitset_1", bitset_node());
    ec.add_expression_node("bitset_2", bitset_node());
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ExpressionName("bitset_1"), ExpressionName("bitset_2"), OperationType::AND));
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterAndBitsetBoolColumn) {
    ec.add_expression_node("bitset", bitset_node());
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ExpressionName("bitset"), ColumnName("bool"), OperationType::AND));
    FilterClause filter_clause{{"int32", "uint8", "bool"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterAndNumericCols) {
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("uint8"), OperationType::AND));
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterComplexExpression) {
    // Equivalent to x AND (y OR z) where x, y, and z are bitsets
    ec.add_expression_node("bitset_1", bitset_node());
    ec.add_expression_node("bitset_2", bitset_node());
    ec.add_expression_node("bitset_3", bitset_node());
    ec.add_expression_node("bitset_4", std::make_shared<ExpressionNode>(ExpressionName("bitset_1"), ExpressionName("bitset_2"), OperationType::OR));
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ExpressionName("bitset_3"), ExpressionName("bitset_4"), OperationType::AND));
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterValue) {
    auto value = std::make_shared<Value>(construct_value<uint16_t>(2));
    ec.add_value("2", value);
    ec.root_node_name_ = ValueName("2");
    ASSERT_THROW(FilterClause({}, ec, {}), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAbsNumeric) {
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("int32"), OperationType::ABS));
    ProjectClause project_clause{{"int32"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::INT64, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAbsString) {
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("string"), OperationType::ABS));
    ProjectClause project_clause{{"string"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAbsBitset) {
    ec.add_expression_node("bitset", std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("uint8"), OperationType::EQ));
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ExpressionName("bitset"), OperationType::ABS));
    ProjectClause project_clause{{"int32", "uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAddNumericCols) {
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("uint8"), OperationType::ADD));
    ProjectClause project_clause{{"int32", "uint8"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::INT64, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAddNumericColVal) {
    auto value = std::make_shared<Value>(construct_value<uint16_t>(5));
    ec.add_value("5", value);
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("uint8"), ValueName("5"), OperationType::ADD));
    ProjectClause project_clause{{"uint8"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::UINT32, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAddNumericColStringCol) {
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("string"), OperationType::ADD));
    ProjectClause project_clause{{"int32", "string"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAddNumericColBitset) {
    ec.add_expression_node("bitset", bitset_node());
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("int32"), ExpressionName("bitset"), OperationType::ADD));
    ProjectClause project_clause{{"int32", "uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAddNumericColValueSet) {
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.add_value_set("value_set", value_set);
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("uint8"), ValueSetName("value_set"), OperationType::ADD));
    ProjectClause project_clause{{"uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAddNumericColEmptyValueSet) {
    std::unordered_set<uint8_t> raw_set;
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.add_value_set("value_set", value_set);
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("uint8"), ValueSetName("value_set"), OperationType::ADD));
    ProjectClause project_clause{{"uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionComplexExpression) {
    // Equivalent to (col1 + col2) * 2
    ec.add_expression_node("product", std::make_shared<ExpressionNode>(ColumnName("int32"), ColumnName("uint8"), OperationType::ADD));
    auto value = std::make_shared<Value>(construct_value<uint16_t>(2));
    ec.add_value("2", value);
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ExpressionName("product"), ValueName("2"), OperationType::MUL));
    ProjectClause project_clause{{"int32", "uint8"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::INT64, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, ProjectionValue) {
    auto value = std::make_shared<Value>(construct_value<uint16_t>(2));
    ec.add_value("2", value);
    ec.root_node_name_ = ValueName("2");
    ProjectClause project_clause{{}, "new_col", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::UINT16, "new_col");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, TernaryValueSetCondition) {
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.add_value_set("value_set", value_set);
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ValueSetName("value_set"), ColumnName("uint8"), ColumnName("uint8"), OperationType::TERNARY));
    ProjectClause project_clause{{"uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, TernaryValueSetLeft) {
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.add_value_set("value_set", value_set);
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("bool"), ValueSetName("value_set"), ColumnName("uint8"), OperationType::TERNARY));
    ProjectClause project_clause{{"uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, TernaryValueSetRight) {
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.add_value_set("value_set", value_set);
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("bool"), ColumnName("uint8"), ValueSetName("value_set"), OperationType::TERNARY));
    ProjectClause project_clause{{"uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, TernaryNonBoolColCondition) {
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("uint8"), ColumnName("uint8"), ColumnName("uint8"), OperationType::TERNARY));
    ProjectClause project_clause{{"uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, TernaryBitsetCondition) {
    ec.add_expression_node("bitset", bitset_node());
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ExpressionName("bitset"), ColumnName("int32"), ColumnName("uint8"), OperationType::TERNARY));
    ProjectClause project_clause{{"int32", "uint8"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::INT32, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, TernaryStringColCol) {
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("bool"), ColumnName("string"), ColumnName("string"), OperationType::TERNARY));
    ProjectClause project_clause{{"string"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::UTF_DYNAMIC64, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, TernaryNumericColVal) {
    auto value = std::make_shared<Value>(construct_value<uint16_t>(5));
    ec.add_value("5", value);
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("bool"), ColumnName("uint8"), ValueName("5"), OperationType::TERNARY));
    ProjectClause project_clause{{"uint8"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::UINT16, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, TernaryNumericColStringCol) {
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("bool"), ColumnName("int32"), ColumnName("string"), OperationType::TERNARY));
    ProjectClause project_clause{{"int32", "string"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, TernaryNumericColBitset) {
    ec.add_expression_node("bitset", bitset_node());
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("bool"), ColumnName("int32"), ExpressionName("bitset"), OperationType::TERNARY));
    ProjectClause project_clause{{"int32", "uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, TernaryBitsetBoolCol) {
    ec.add_expression_node("bitset", bitset_node());
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("bool"), ExpressionName("bitset"), ColumnName("bool"), OperationType::TERNARY));
    FilterClause filter_clause{{"int32", "uint8", "bool"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, TernaryBitsetBitset) {
    ec.add_expression_node("bitset_1", bitset_node());
    ec.add_expression_node("bitset_2", bitset_node());
    ec.add_expression_node("root", std::make_shared<ExpressionNode>(ColumnName("bool"), ExpressionName("bitset_1"), ExpressionName("bitset_2"), OperationType::TERNARY));
    FilterClause filter_clause{{"int32", "uint8", "bool"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}
