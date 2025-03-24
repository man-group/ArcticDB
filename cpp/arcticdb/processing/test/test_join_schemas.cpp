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
using NormalizationMetadata = arcticdb::proto::descriptors::NormalizationMetadata;

class GenerateIndexDescriptorTest : public testing::Test {
protected:
    IndexDescriptorImpl rowcount_0{0, IndexDescriptor::Type::ROWCOUNT};
    IndexDescriptorImpl timestamp_1{1, IndexDescriptor::Type::TIMESTAMP};
    IndexDescriptorImpl timestamp_2{2, IndexDescriptor::Type::TIMESTAMP};
    IndexDescriptorImpl string_1{1, IndexDescriptor::Type::STRING};
};

TEST_F(GenerateIndexDescriptorTest, SingleDescriptor) {
    ASSERT_EQ(generate_index_descriptor({timestamp_2}), timestamp_2);
}

TEST_F(GenerateIndexDescriptorTest, IdenticalDescriptors) {
    ASSERT_EQ(generate_index_descriptor({timestamp_2, timestamp_2}), timestamp_2);
}

TEST_F(GenerateIndexDescriptorTest, SameTypeDifferentFieldCount) {
    ASSERT_THROW(generate_index_descriptor({timestamp_2, timestamp_1}), SchemaException);
    ASSERT_THROW(generate_index_descriptor({timestamp_1, timestamp_2}), SchemaException);
}

TEST_F(GenerateIndexDescriptorTest, SameFieldCountDifferentType) {
    ASSERT_THROW(generate_index_descriptor({timestamp_1, string_1}), SchemaException);
    ASSERT_THROW(generate_index_descriptor({string_1, timestamp_1}), SchemaException);
}

TEST_F(GenerateIndexDescriptorTest, DifferentFieldCountDifferentType) {
    ASSERT_THROW(generate_index_descriptor({timestamp_1, rowcount_0}), SchemaException);
    ASSERT_THROW(generate_index_descriptor({rowcount_0, timestamp_1}), SchemaException);
}

class GenerateNormMetaTest : public testing::Test {
protected:
    NormalizationMetadata single_index(
            const std::string& name = "",
            bool is_int = false,
            bool is_physically_stored = true,
            const std::string& tz = "",
            int start = 0,
            int step = 0
            ) {
        NormalizationMetadata norm_meta;
        auto* index = norm_meta.mutable_df()->mutable_common()->mutable_index();
        index->set_is_physically_stored(is_physically_stored);
        index->set_name(name);
        index->set_is_int(is_int);
        index->set_tz(tz);
        index->set_start(start);
        index->set_step(step);
        return norm_meta;
    }

    NormalizationMetadata multi_index(
            uint32_t field_count,
            const std::string& name = "",
            bool is_int = false,
            const std::string& tz = ""
    ) {
        NormalizationMetadata norm_meta;
        auto* index = norm_meta.mutable_df()->mutable_common()->mutable_multi_index();
        index->set_name(name);
        index->set_field_count(field_count);
        index->set_is_int(is_int);
        index->set_tz(tz);
        return norm_meta;
    }
};

TEST_F(GenerateNormMetaTest, SingleNormMeta) {
    auto single = single_index("ts", false, true, "UTC");
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({single}), single));
    auto multi = multi_index(2, "ts", false, "UTC");
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({multi}), multi));
}

TEST_F(GenerateNormMetaTest, IdenticalNormMetas) {
    auto single = single_index("ts", false, true, "UTC");
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({single, single}), single));
    auto multi = multi_index(2, "ts", false, "UTC");
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({multi, multi}), multi));
}

TEST_F(GenerateNormMetaTest, DifferentNames) {
    // Different index names (either in the name or is_int fields) should lead to an empty string index name
    auto str_1 = single_index("1", false, true, "UTC");
    auto str_2 = single_index("2", false, true, "UTC");
    auto int_1 = single_index("1", true, true, "UTC");
    auto int_2 = single_index("2", true, true, "UTC");
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, str_2}), single_index("", false, true, "UTC")));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({int_1, int_2}), single_index("", false, true, "UTC")));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, int_1}), single_index("", false, true, "UTC")));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({int_1, str_1}), single_index("", false, true, "UTC")));
    str_1 = multi_index(2, "1", false, "UTC");
    str_2 = multi_index(2, "2", false, "UTC");
    int_1 = multi_index(2, "1", true, "UTC");
    int_2 = multi_index(2, "2", true, "UTC");
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, str_2}), multi_index(2, "", false, "UTC")));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({int_1, int_2}), multi_index(2, "", false, "UTC")));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, int_1}), multi_index(2, "", false, "UTC")));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({int_1, str_1}), multi_index(2, "", false, "UTC")));
}

TEST_F(GenerateNormMetaTest, DifferentTimezones) {
    // Different index timezones should lead to an empty string index timezone
    auto single_utc = single_index("ts", false, true, "UTC");
    auto single_est = single_index("ts", false, true, "EST");
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({single_utc, single_est}), single_index("ts", false, true, "")));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({single_est, single_utc}), single_index("ts", false, true, "")));
    auto multi_utc = multi_index(2, "ts", false, "UTC");
    auto multi_est = multi_index(2, "ts", false, "EST");
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({multi_utc, multi_est}), multi_index(2, "ts", false, "")));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({multi_est, multi_utc}), multi_index(2, "ts", false, "")));
}

TEST_F(GenerateNormMetaTest, DifferentIsPhysicallyStored) {
    ASSERT_THROW(generate_norm_meta({single_index("ts"), single_index("ts", false, false)}), SchemaException);
}

TEST_F(GenerateNormMetaTest, DifferentFieldCount) {
    ASSERT_THROW(generate_norm_meta({multi_index(2, "ts", false, "UTC"), multi_index(3, "ts", false, "UTC")}), SchemaException);
}

TEST_F(GenerateNormMetaTest, RangeIndexBasic) {
    auto first = single_index("", false, false, "", 0, 1);
    auto second = single_index("", false, false, "", 0, 1);
    auto third = single_index("", false, false, "", 0, 1);
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({first, second, third}), first));
}

TEST_F(GenerateNormMetaTest, RangeIndexNonDefaultStep) {
    auto first = single_index("", false, false, "", 10, 2);
    auto second = single_index("", false, false, "", 5, 2);
    auto third = single_index("", false, false, "", 12, 2);
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({first, second, third}), first));
}

TEST_F(GenerateNormMetaTest, RangeIndexNonMatchingStep) {
    auto a = single_index("", false, false, "", 10, 2);
    auto b = single_index("", false, false, "", 5, 3);
    auto c = single_index("", false, false, "", 12, 2);
    auto expected_result = single_index("", false, false, "", 0, 1);
    // Order that the steps are seen in should not matter
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({a, b, c}), expected_result));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({b, c, a}), expected_result));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({c, a, b}), expected_result));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({a, c, b}), expected_result));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({c, b, a}), expected_result));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({b, a, c}), expected_result));
}

ankerl::unordered_dense::map<std::string, DataType> generate_column_types(const FieldCollection& fields) {
    ankerl::unordered_dense::map<std::string, DataType> res;
    res.reserve(fields.size());
    for (const auto& field: fields) {
        res.emplace(field.name(), field.type().data_type());
    }
    return res;
}

TEST(InnerJoin, SingleSchema) {
    auto fields = std::make_shared<FieldCollection>();
    fields->add_field(make_scalar_type(DataType::INT64), "first");
    fields->add_field(make_scalar_type(DataType::FLOAT64), "second");
    fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "third");
    Columns columns{fields, generate_column_types(*fields)};
    std::vector<Columns> input(1, columns);
    auto res = inner_join(input);
    ASSERT_EQ(res, *columns.fields);
}

TEST(InnerJoin, ExactlyMatching) {
    auto fields = std::make_shared<FieldCollection>();
    fields->add_field(make_scalar_type(DataType::INT64), "first");
    fields->add_field(make_scalar_type(DataType::FLOAT64), "second");
    fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "third");
    Columns columns{fields, generate_column_types(*fields)};
    std::vector<Columns> input(4, columns);
    auto res = inner_join(input);
    ASSERT_EQ(res, *columns.fields);
}

TEST(InnerJoin, MatchingNamesCompatibleTypes) {
    auto first_fields = std::make_shared<FieldCollection>();
    first_fields->add_field(make_scalar_type(DataType::INT64), "first");
    first_fields->add_field(make_scalar_type(DataType::UINT8), "second");
    Columns first_columns{first_fields, generate_column_types(*first_fields)};
    auto second_fields = std::make_shared<FieldCollection>();
    second_fields->add_field(make_scalar_type(DataType::INT32), "first");
    second_fields->add_field(make_scalar_type(DataType::FLOAT64), "second");
    Columns second_columns{second_fields, generate_column_types(*second_fields)};
    std::vector<Columns> input{first_columns, second_columns};
    auto res = inner_join(input);
    FieldCollection expected;
    expected.add_field(make_scalar_type(DataType::INT64), "first");
    expected.add_field(make_scalar_type(DataType::FLOAT64), "second");
    ASSERT_EQ(res, expected);
}

TEST(InnerJoin, MatchingNamesIncompatibleTypes) {
    auto first_fields = std::make_shared<FieldCollection>();
    first_fields->add_field(make_scalar_type(DataType::INT64), "first");
    first_fields->add_field(make_scalar_type(DataType::UINT8), "second");
    Columns first_columns{first_fields, generate_column_types(*first_fields)};
    auto second_fields = std::make_shared<FieldCollection>();
    second_fields->add_field(make_scalar_type(DataType::INT32), "first");
    second_fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "second");
    Columns second_columns{second_fields, generate_column_types(*second_fields)};
    std::vector<Columns> input{first_columns, second_columns};
    ASSERT_THROW(inner_join(input), SchemaException);
}

TEST(InnerJoin, DisJointColumnNames) {
    auto first_fields = std::make_shared<FieldCollection>();
    first_fields->add_field(make_scalar_type(DataType::INT64), "first");
    first_fields->add_field(make_scalar_type(DataType::UINT8), "second");
    first_fields->add_field(make_scalar_type(DataType::FLOAT64), "third");
    Columns first_columns{first_fields, generate_column_types(*first_fields)};
    auto second_fields = std::make_shared<FieldCollection>();
    second_fields->add_field(make_scalar_type(DataType::FLOAT64), "third");
    second_fields->add_field(make_scalar_type(DataType::UINT8), "second");
    second_fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "fourth");
    Columns second_columns{second_fields, generate_column_types(*second_fields)};
    std::vector<Columns> input{first_columns, second_columns};
    auto res = inner_join(input);
    FieldCollection expected;
    expected.add_field(make_scalar_type(DataType::UINT8), "second");
    expected.add_field(make_scalar_type(DataType::FLOAT64), "third");
    ASSERT_EQ(res, expected);
    input = std::vector<Columns>{second_columns, first_columns};
    res = inner_join(input);
    expected = FieldCollection();
    expected.add_field(make_scalar_type(DataType::FLOAT64), "third");
    expected.add_field(make_scalar_type(DataType::UINT8), "second");
    ASSERT_EQ(res, expected);
}

TEST(OuterJoin, SingleSchema) {
    auto fields = std::make_shared<FieldCollection>();
    fields->add_field(make_scalar_type(DataType::INT64), "first");
    fields->add_field(make_scalar_type(DataType::FLOAT64), "second");
    fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "third");
    Columns columns{fields, generate_column_types(*fields)};
    std::vector<Columns> input(1, columns);
    auto res = inner_join(input);
    ASSERT_EQ(res, *columns.fields);
}

TEST(OuterJoin, ExactlyMatching) {
    auto fields = std::make_shared<FieldCollection>();
    fields->add_field(make_scalar_type(DataType::INT64), "first");
    fields->add_field(make_scalar_type(DataType::FLOAT64), "second");
    fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "third");
    Columns columns{fields, generate_column_types(*fields)};
    std::vector<Columns> input(4, columns);
    auto res = inner_join(input);
    ASSERT_EQ(res, *columns.fields);
}

TEST(OuterJoin, MatchingNamesCompatibleTypes) {
    auto first_fields = std::make_shared<FieldCollection>();
    first_fields->add_field(make_scalar_type(DataType::INT64), "first");
    first_fields->add_field(make_scalar_type(DataType::UINT8), "second");
    Columns first_columns{first_fields, generate_column_types(*first_fields)};
    auto second_fields = std::make_shared<FieldCollection>();
    second_fields->add_field(make_scalar_type(DataType::INT32), "first");
    second_fields->add_field(make_scalar_type(DataType::FLOAT64), "second");
    Columns second_columns{second_fields, generate_column_types(*second_fields)};
    std::vector<Columns> input{first_columns, second_columns};
    auto res = inner_join(input);
    FieldCollection expected;
    expected.add_field(make_scalar_type(DataType::INT64), "first");
    expected.add_field(make_scalar_type(DataType::FLOAT64), "second");
    ASSERT_EQ(res, expected);
}

TEST(OuterJoin, MatchingNamesIncompatibleTypes) {
    auto first_fields = std::make_shared<FieldCollection>();
    first_fields->add_field(make_scalar_type(DataType::INT64), "first");
    first_fields->add_field(make_scalar_type(DataType::UINT8), "second");
    Columns first_columns{first_fields, generate_column_types(*first_fields)};
    auto second_fields = std::make_shared<FieldCollection>();
    second_fields->add_field(make_scalar_type(DataType::INT32), "first");
    second_fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "second");
    Columns second_columns{second_fields, generate_column_types(*second_fields)};
    std::vector<Columns> input{first_columns, second_columns};
    ASSERT_THROW(inner_join(input), SchemaException);
}

TEST(OuterJoin, DisJointColumnNames) {
    auto first_fields = std::make_shared<FieldCollection>();
    first_fields->add_field(make_scalar_type(DataType::INT64), "first");
    first_fields->add_field(make_scalar_type(DataType::UINT8), "second");
    first_fields->add_field(make_scalar_type(DataType::FLOAT64), "third");
    Columns first_columns{first_fields, generate_column_types(*first_fields)};
    auto second_fields = std::make_shared<FieldCollection>();
    second_fields->add_field(make_scalar_type(DataType::FLOAT64), "third");
    second_fields->add_field(make_scalar_type(DataType::UINT8), "second");
    second_fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "fourth");
    Columns second_columns{second_fields, generate_column_types(*second_fields)};
    std::vector<Columns> input{first_columns, second_columns};
    auto res = inner_join(input);
    FieldCollection expected;
    first_fields->add_field(make_scalar_type(DataType::INT64), "first");
    expected.add_field(make_scalar_type(DataType::UINT8), "second");
    expected.add_field(make_scalar_type(DataType::FLOAT64), "third");
    second_fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "fourth");
    ASSERT_EQ(res, expected);
    input = std::vector<Columns>{second_columns, first_columns};
    res = inner_join(input);
    expected = FieldCollection();
    expected.add_field(make_scalar_type(DataType::FLOAT64), "third");
    expected.add_field(make_scalar_type(DataType::UINT8), "second");
    second_fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "fourth");
    first_fields->add_field(make_scalar_type(DataType::INT64), "first");
    ASSERT_EQ(res, expected);
}
