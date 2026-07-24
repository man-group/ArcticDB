/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <google/protobuf/util/message_differencer.h>

#include <gtest/gtest.h>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/processing/test/ast_test_helpers.hpp>

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
    }

    OutputSchema initial_schema() { return {initial_stream_desc.clone(), {}}; }

    ExpressionChild bitset_node() const { return node(col("int32"), col("uint8"), OperationType::EQ); }

    StreamDescriptor initial_stream_desc;
    ExpressionContext ec;
};

TEST_F(AstParsingOutputTypesTest, FilterNotBitset) {
    ec.root_ = node(bitset_node(), OperationType::NOT);
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterNotBoolColumn) {
    ec.root_ = node(col("bool"), OperationType::NOT);
    FilterClause filter_clause{{"bool"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterNotNumericColumn) {
    ec.root_ = node(col("int32"), OperationType::NOT);
    FilterClause filter_clause{{"int32"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterIsNullStringColumn) {
    ec.root_ = node(col("string"), OperationType::ISNULL);
    FilterClause filter_clause{{"string"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterIsNullNumericColumn) {
    ec.root_ = node(col("int32"), OperationType::ISNULL);
    FilterClause filter_clause{{"int32"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterIsNullBitset) {
    ec.root_ = node(bitset_node(), OperationType::ISNULL);
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterEqNumericCols) {
    ec.root_ = node(col("int32"), col("uint8"), OperationType::EQ);
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterEqStringColStringVal) {
    auto value = std::make_shared<Value>(construct_string_value("hello"));
    ec.root_ = node(col("string"), val(value), OperationType::EQ);
    FilterClause filter_clause{{"string"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterEqNumericColStringVal) {
    auto value = std::make_shared<Value>(construct_string_value("hello"));
    ec.root_ = node(col("int32"), val(value), OperationType::EQ);
    FilterClause filter_clause{{"int32"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterEqColValueSet) {
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.root_ = node(col("int32"), vset(value_set), OperationType::EQ);
    FilterClause filter_clause{{"int32"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterLessThanNumericColNumericVal) {
    auto value = std::make_shared<Value>(construct_value<uint16_t>(5));
    ec.root_ = node(col("int32"), val(value), OperationType::LT);
    FilterClause filter_clause{{"int32"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterLessThanStringColStringVal) {
    auto value = std::make_shared<Value>(construct_string_value("hello"));
    ec.root_ = node(col("string"), val(value), OperationType::LT);
    FilterClause filter_clause{{"string"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterLessThanNumericColBitset) {
    ec.root_ = node(col("int32"), bitset_node(), OperationType::LT);
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterIsInNumericColNumericValueSet) {
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.root_ = node(col("int32"), vset(value_set), OperationType::ISIN);
    FilterClause filter_clause{{"int32"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterIsInStringColStringValueSet) {
    std::vector<std::string> raw_set{"hello", "goodbye"};
    auto value_set = std::make_shared<ValueSet>(std::move(raw_set));
    ec.root_ = node(col("string"), vset(value_set), OperationType::ISIN);
    FilterClause filter_clause{{"string"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, ProjectionPowUintUintCols) {
    // uint ^ uint -> uint64
    ec.root_ = node(col("uint8"), col("uint8"), OperationType::POW);
    ProjectClause project_clause{{"uint8"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::UINT64, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, ProjectionPowIntUintCols) {
    // int ^ uint -> int64
    ec.root_ = node(col("int32"), col("uint8"), OperationType::POW);
    ProjectClause project_clause{{"int32", "uint8"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::INT64, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, ProjectionPowUintIntCols) {
    // uint ^ int -> double (negative exponent can produce fractional results)
    ec.root_ = node(col("uint8"), col("int32"), OperationType::POW);
    ProjectClause project_clause{{"uint8", "int32"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::FLOAT64, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, ProjectionPowIntIntCols) {
    // int ^ int -> double (negative exponent can produce fractional results)
    ec.root_ = node(col("int32"), col("int32"), OperationType::POW);
    ProjectClause project_clause{{"int32"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::FLOAT64, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, ProjectionPowNumericColVal) {
    // uint8 ^ uint16 value -> uint64 (unsigned base, unsigned exponent)
    auto value = std::make_shared<Value>(construct_value<uint16_t>(3));
    ec.root_ = node(col("uint8"), val(value), OperationType::POW);
    ProjectClause project_clause{{"uint8"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::UINT64, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, ProjectionPowNumericColStringCol) {
    ec.root_ = node(col("int32"), col("string"), OperationType::POW);
    ProjectClause project_clause{{"int32", "string"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionPowNumericColBitset) {
    ec.root_ = node(col("int32"), bitset_node(), OperationType::POW);
    ProjectClause project_clause{{"int32", "uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionPowNumericColValueSet) {
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.root_ = node(col("uint8"), vset(value_set), OperationType::POW);
    ProjectClause project_clause{{"uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionPowBitsetNumericCol) {
    ec.root_ = node(bitset_node(), col("int32"), OperationType::POW);
    ProjectClause project_clause{{"int32", "uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionPowValueSetNumericCol) {
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.root_ = node(vset(value_set), col("uint8"), OperationType::POW);
    ProjectClause project_clause{{"uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterIsInNumericColEmptyValueSet) {
    std::vector<std::string> raw_set;
    auto value_set = std::make_shared<ValueSet>(std::move(raw_set));
    ec.root_ = node(col("int32"), vset(value_set), OperationType::ISIN);
    FilterClause filter_clause{{"int32"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterIsInBitsetValueSet) {
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.root_ = node(bitset_node(), vset(value_set), OperationType::ISIN);
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterIsInNumericColNumericValue) {
    auto value = std::make_shared<Value>(construct_value<uint16_t>(5));
    ec.root_ = node(col("int32"), val(value), OperationType::ISIN);
    FilterClause filter_clause{{"int32"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterAndBitsetBitset) {
    ec.root_ = node(bitset_node(), bitset_node(), OperationType::AND);
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterAndBitsetBoolColumn) {
    ec.root_ = node(bitset_node(), col("bool"), OperationType::AND);
    FilterClause filter_clause{{"int32", "uint8", "bool"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterAndNumericCols) {
    ec.root_ = node(col("int32"), col("uint8"), OperationType::AND);
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    ASSERT_THROW(filter_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, FilterComplexExpression) {
    // Equivalent to x AND (y OR z) where x, y, and z are bitsets
    auto bitset_4 = node(bitset_node(), bitset_node(), OperationType::OR);
    ec.root_ = node(bitset_node(), bitset_4, OperationType::AND);
    FilterClause filter_clause{{"int32", "uint8"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, FilterValue) {
    auto value = std::make_shared<Value>(construct_value<uint16_t>(2));
    ec.root_ = val(value);
    ASSERT_THROW(FilterClause({}, ec, {}), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAbsNumeric) {
    ec.root_ = node(col("int32"), OperationType::ABS);
    ProjectClause project_clause{{"int32"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::INT64, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAbsString) {
    ec.root_ = node(col("string"), OperationType::ABS);
    ProjectClause project_clause{{"string"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAbsBitset) {
    ec.root_ = node(node(col("int32"), col("uint8"), OperationType::EQ), OperationType::ABS);
    ProjectClause project_clause{{"int32", "uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAddNumericCols) {
    ec.root_ = node(col("int32"), col("uint8"), OperationType::ADD);
    ProjectClause project_clause{{"int32", "uint8"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::INT64, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAddNumericColVal) {
    auto value = std::make_shared<Value>(construct_value<uint16_t>(5));
    ec.root_ = node(col("uint8"), val(value), OperationType::ADD);
    ProjectClause project_clause{{"uint8"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::UINT32, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAddNumericColStringCol) {
    ec.root_ = node(col("int32"), col("string"), OperationType::ADD);
    ProjectClause project_clause{{"int32", "string"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAddNumericColBitset) {
    ec.root_ = node(col("int32"), bitset_node(), OperationType::ADD);
    ProjectClause project_clause{{"int32", "uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAddNumericColValueSet) {
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.root_ = node(col("uint8"), vset(value_set), OperationType::ADD);
    ProjectClause project_clause{{"uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionAddNumericColEmptyValueSet) {
    std::unordered_set<uint8_t> raw_set;
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.root_ = node(col("uint8"), vset(value_set), OperationType::ADD);
    ProjectClause project_clause{{"uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, ProjectionComplexExpression) {
    // Equivalent to (col1 + col2) * 2
    auto product = node(col("int32"), col("uint8"), OperationType::ADD);
    auto value = std::make_shared<Value>(construct_value<uint16_t>(2));
    ec.root_ = node(product, val(value), OperationType::MUL);
    ProjectClause project_clause{{"int32", "uint8"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::INT64, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, ProjectionValue) {
    auto value = std::make_shared<Value>(construct_value<uint16_t>(2));
    ec.root_ = val(value);
    ProjectClause project_clause{{}, "new_col", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::UINT16, "new_col");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, TernaryValueSetCondition) {
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.root_ = node(vset(value_set), col("uint8"), col("uint8"), OperationType::TERNARY);
    ProjectClause project_clause{{"uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, TernaryValueSetLeft) {
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.root_ = node(col("bool"), vset(value_set), col("uint8"), OperationType::TERNARY);
    ProjectClause project_clause{{"uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, TernaryValueSetRight) {
    std::unordered_set<uint8_t> raw_set{1, 2, 3};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<uint8_t>>(std::move(raw_set)));
    ec.root_ = node(col("bool"), col("uint8"), vset(value_set), OperationType::TERNARY);
    ProjectClause project_clause{{"uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, TernaryNonBoolColCondition) {
    ec.root_ = node(col("uint8"), col("uint8"), col("uint8"), OperationType::TERNARY);
    ProjectClause project_clause{{"uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, TernaryBitsetCondition) {
    ec.root_ = node(bitset_node(), col("int32"), col("uint8"), OperationType::TERNARY);
    ProjectClause project_clause{{"int32", "uint8"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::INT32, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, TernaryStringColCol) {
    ec.root_ = node(col("bool"), col("string"), col("string"), OperationType::TERNARY);
    ProjectClause project_clause{{"string"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::UTF_DYNAMIC64, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, TernaryNumericColVal) {
    auto value = std::make_shared<Value>(construct_value<uint16_t>(5));
    ec.root_ = node(col("bool"), col("uint8"), val(value), OperationType::TERNARY);
    ProjectClause project_clause{{"uint8"}, "root", ec};
    auto output_schema = project_clause.modify_schema(initial_schema());
    auto stream_desc = initial_schema().stream_descriptor();
    stream_desc.add_scalar_field(DataType::UINT16, "root");
    ASSERT_EQ(output_schema.stream_descriptor(), stream_desc);
}

TEST_F(AstParsingOutputTypesTest, TernaryNumericColStringCol) {
    ec.root_ = node(col("bool"), col("int32"), col("string"), OperationType::TERNARY);
    ProjectClause project_clause{{"int32", "string"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, TernaryNumericColBitset) {
    ec.root_ = node(col("bool"), col("int32"), bitset_node(), OperationType::TERNARY);
    ProjectClause project_clause{{"int32", "uint8"}, "root", ec};
    ASSERT_THROW(project_clause.modify_schema(initial_schema()), UserInputException);
}

TEST_F(AstParsingOutputTypesTest, TernaryBitsetBoolCol) {
    ec.root_ = node(col("bool"), bitset_node(), col("bool"), OperationType::TERNARY);
    FilterClause filter_clause{{"int32", "uint8", "bool"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}

TEST_F(AstParsingOutputTypesTest, TernaryBitsetBitset) {
    ec.root_ = node(col("bool"), bitset_node(), bitset_node(), OperationType::TERNARY);
    FilterClause filter_clause{{"int32", "uint8", "bool"}, ec, {}};
    auto output_schema = filter_clause.modify_schema(initial_schema());
    ASSERT_EQ(output_schema.stream_descriptor(), initial_schema().stream_descriptor());
}
