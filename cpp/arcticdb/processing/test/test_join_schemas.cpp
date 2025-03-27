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

//class GenerateIndexDescriptorTest : public testing::Test {
//protected:
//    IndexDescriptorImpl timestamp_2_index{2, IndexDescriptor::Type::TIMESTAMP};
//    OutputSchema rowcount_0{{{}, IndexDescriptorImpl{0, IndexDescriptor::Type::ROWCOUNT}}, {}};
//    OutputSchema timestamp_1{{{}, IndexDescriptorImpl{1, IndexDescriptor::Type::TIMESTAMP}}, {}};
//    OutputSchema timestamp_2{{{}, timestamp_2_index}, {}};
//    OutputSchema string_1{{{}, IndexDescriptorImpl{1, IndexDescriptor::Type::STRING}}, {}};
//};
//
//TEST_F(GenerateIndexDescriptorTest, SingleDescriptor) {
//    ASSERT_EQ(generate_index_descriptor({timestamp_2}), timestamp_2_index);
//}
//
//TEST_F(GenerateIndexDescriptorTest, IdenticalDescriptors) {
//    ASSERT_EQ(generate_index_descriptor({timestamp_2, timestamp_2}), timestamp_2_index);
//}
//
//TEST_F(GenerateIndexDescriptorTest, SameTypeDifferentFieldCount) {
//    ASSERT_THROW(generate_index_descriptor({timestamp_2, timestamp_1}), SchemaException);
//    ASSERT_THROW(generate_index_descriptor({timestamp_1, timestamp_2}), SchemaException);
//}
//
//TEST_F(GenerateIndexDescriptorTest, SameFieldCountDifferentType) {
//    ASSERT_THROW(generate_index_descriptor({timestamp_1, string_1}), SchemaException);
//    ASSERT_THROW(generate_index_descriptor({string_1, timestamp_1}), SchemaException);
//}
//
//TEST_F(GenerateIndexDescriptorTest, DifferentFieldCountDifferentType) {
//    ASSERT_THROW(generate_index_descriptor({timestamp_1, rowcount_0}), SchemaException);
//    ASSERT_THROW(generate_index_descriptor({rowcount_0, timestamp_1}), SchemaException);
//}

class AddIndexFieldsTest : public testing::Test {
protected:
    void SetUp() override {
        one_datetime_field.add_scalar_field(DataType::NANOSECONDS_UTC64, "index");
        one_float_field.add_scalar_field(DataType::FLOAT64, "index");
        one_datetime_one_float_field.add_scalar_field(DataType::NANOSECONDS_UTC64, "index");
        one_datetime_one_float_field.add_scalar_field(DataType::FLOAT64, "level2");
        one_datetime_one_string_field.add_scalar_field(DataType::NANOSECONDS_UTC64, "index");
        one_datetime_one_string_field.add_scalar_field(DataType::UTF_DYNAMIC64, "level2");
    }

    StreamDescriptor zero_index_fields{{}, IndexDescriptorImpl{0, IndexDescriptor::Type::ROWCOUNT}};
    StreamDescriptor one_index_field{{}, IndexDescriptorImpl{1, IndexDescriptor::Type::TIMESTAMP}};
    StreamDescriptor two_index_fields{{}, IndexDescriptorImpl{2, IndexDescriptor::Type::TIMESTAMP}};
    StreamDescriptor one_datetime_field;
    StreamDescriptor one_float_field;
    StreamDescriptor one_datetime_one_float_field;
    StreamDescriptor one_datetime_one_string_field;
//    IndexDescriptorImpl timestamp_2_index{2, IndexDescriptor::Type::TIMESTAMP};
//    OutputSchema rowcount_0{{{}, IndexDescriptorImpl{0, IndexDescriptor::Type::ROWCOUNT}}, {}};
//    OutputSchema timestamp_1{{{}, IndexDescriptorImpl{1, IndexDescriptor::Type::TIMESTAMP}}, {}};
//    OutputSchema timestamp_2{{{}, timestamp_2_index}, {}};
//    OutputSchema string_1{{{}, IndexDescriptorImpl{1, IndexDescriptor::Type::STRING}}, {}};
};

TEST_F(AddIndexFieldsTest, ZeroIndexFields) {
    auto stream_desc = zero_index_fields.clone();
    std::vector<OutputSchema> output_schemas;
    auto non_matching_name_indices = add_index_fields(stream_desc, output_schemas);
    ASSERT_EQ(stream_desc, zero_index_fields);
    ASSERT_TRUE(non_matching_name_indices.empty());
}

TEST_F(AddIndexFieldsTest, NotEnoughIndexFields) {
    std::vector<OutputSchema> output_schemas(1);
    ASSERT_THROW(add_index_fields(one_index_field, output_schemas), SchemaException);
}

TEST_F(AddIndexFieldsTest, IncompatibleFirstType) {
    std::vector<OutputSchema> output_schemas{{one_datetime_field, {}}, {one_float_field, {}}};
    ASSERT_THROW(add_index_fields(one_index_field, output_schemas), SchemaException);
}

TEST_F(AddIndexFieldsTest, IncompatibleSecondType) {
    std::vector<OutputSchema> output_schemas{{one_datetime_one_float_field, {}}, {one_datetime_one_string_field, {}}};
    ASSERT_THROW(add_index_fields(two_index_fields, output_schemas), SchemaException);
}

//class GenerateNormMetaTest : public testing::Test {
//protected:
//    struct single_index_params {
//        std::string name = "";
//        bool is_int = false;
//        bool fake_name = false;
//        bool is_physically_stored = true;
//        std::string tz = "";
//        int start = 0;
//        int step = 0;
//    };
//
//    NormalizationMetadata single_index(const single_index_params& params) {
//        NormalizationMetadata norm_meta;
//        auto* index = norm_meta.mutable_df()->mutable_common()->mutable_index();
//        index->set_is_physically_stored(params.is_physically_stored);
//        index->set_name(params.name);
//        index->set_is_int(params.is_int);
//        index->set_fake_name(params.fake_name);
//        index->set_tz(params.tz);
//        index->set_start(params.start);
//        index->set_step(params.step);
//        return norm_meta;
//    }
//
//    struct multi_index_params {
//        uint32_t field_count = 2;
//        std::string name = "";
//        bool is_int = false;
//        std::string tz = "";
//    };
//
//    NormalizationMetadata multi_index(const multi_index_params& params) {
//        NormalizationMetadata norm_meta;
//        auto* index = norm_meta.mutable_df()->mutable_common()->mutable_multi_index();
//        index->set_name(params.name);
//        index->set_field_count(params.field_count);
//        index->set_is_int(params.is_int);
//        index->set_tz(params.tz);
//        return norm_meta;
//    }
//};
//
//TEST_F(GenerateNormMetaTest, SingleNormMeta) {
//    auto single = single_index({.name="ts", .tz="UTC"});
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({single}), single));
//    auto multi = multi_index({.name="ts", .tz="UTC"});
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({multi}), multi));
//}
//
//TEST_F(GenerateNormMetaTest, IdenticalNormMetas) {
//    auto single = single_index({.name="ts", .tz="UTC"});
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({single, single}), single));
//    auto multi = multi_index({.name="ts", .tz="UTC"});
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({multi, multi}), multi));
//}
//
//TEST_F(GenerateNormMetaTest, DifferentNames) {
//    // Different index names (either in the name or is_int fields) should lead to an empty string index name
//    // If fake_name is true for any index the output must also have this as true
//    auto str_1 = single_index({.name="1", .tz="UTC"});
//    auto str_2 = single_index({.name="2", .tz="UTC"});
//    auto int_1 = single_index({.name="1", .is_int=true, .tz="UTC"});
//    auto int_2 = single_index({.name="2", .is_int=true, .tz="UTC"});
//    auto fake_name = single_index({.fake_name=true, .tz="UTC"});
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, str_2}), fake_name));
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({int_1, int_2}), fake_name));
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, int_1}), fake_name));
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({int_1, str_1}), fake_name));
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, fake_name}), fake_name));
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({fake_name, int_1}), fake_name));
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, fake_name, int_1}), fake_name));
//    str_1 = multi_index({.name="1", .tz="UTC"});
//    str_2 = multi_index({.name="2", .tz="UTC"});
//    int_1 = multi_index({.name="1", .is_int=true, .tz="UTC"});
//    int_2 = multi_index({.name="2", .is_int=true, .tz="UTC"});
//    auto no_name = multi_index({.tz="UTC"});
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, str_2}), no_name));
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({int_1, int_2}), no_name));
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, int_1}), no_name));
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({int_1, str_1}), no_name));
//}
//
//TEST_F(GenerateNormMetaTest, DifferentTimezones) {
//    // Different index timezones should lead to an empty string index timezone
//    auto single_utc = single_index({.name="ts", .tz="UTC"});
//    auto single_est = single_index({.name="ts", .tz="EST"});
//    auto expected = single_index({.name="ts"});
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({single_utc, single_est}), expected));
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({single_est, single_utc}), expected));
//    auto multi_utc = multi_index({.name="ts", .tz="UTC"});
//    auto multi_est = multi_index({.name="ts", .tz="EST"});
//    expected = multi_index({.name="ts"});
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({multi_utc, multi_est}), expected));
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({multi_est, multi_utc}), expected));
//}
//
//TEST_F(GenerateNormMetaTest, DifferentIsPhysicallyStored) {
//    ASSERT_THROW(generate_norm_meta({single_index({.name="ts"}), single_index({.name="ts", .is_physically_stored=false})}), SchemaException);
//}
//
//TEST_F(GenerateNormMetaTest, DifferentFieldCount) {
//    ASSERT_THROW(generate_norm_meta({multi_index({.name="ts", .tz="UTC"}), multi_index({.field_count=3, .name="ts", .tz="UTC"})}), SchemaException);
//}
//
//TEST_F(GenerateNormMetaTest, RangeIndexBasic) {
//    auto index = single_index({.fake_name=true, .is_physically_stored=false, .step=1});
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({index, index, index}), index));
//}
//
//TEST_F(GenerateNormMetaTest, RangeIndexNonDefaultStep) {
//    auto first = single_index({.fake_name=true, .is_physically_stored=false, .start=10, .step=2});
//    auto second = single_index({.fake_name=true, .is_physically_stored=false, .start=5, .step=2});
//    auto third = single_index({.fake_name=true, .is_physically_stored=false, .start=12, .step=2});
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({first, second, third}), first));
//}
//
//TEST_F(GenerateNormMetaTest, RangeIndexNonMatchingStep) {
//    auto a = single_index({.fake_name=true, .is_physically_stored=false, .start=10, .step=2});
//    auto b = single_index({.fake_name=true, .is_physically_stored=false, .start=5, .step=3});
//    auto c = single_index({.fake_name=true, .is_physically_stored=false, .start=12, .step=2});
//    auto expected_result = single_index({.fake_name=true, .is_physically_stored=false, .step=1});
//    // Order that the steps are seen in should not matter
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({a, b, c}), expected_result));
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({b, c, a}), expected_result));
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({c, a, b}), expected_result));
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({a, c, b}), expected_result));
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({c, b, a}), expected_result));
//    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({b, a, c}), expected_result));
//}
//
//ankerl::unordered_dense::map<std::string, DataType> generate_column_types(const FieldCollection& fields) {
//    ankerl::unordered_dense::map<std::string, DataType> res;
//    res.reserve(fields.size());
//    for (const auto& field: fields) {
//        res.emplace(field.name(), field.type().data_type());
//    }
//    return res;
//}
//
//TEST(InnerJoin, SingleSchema) {
//    auto fields = std::make_shared<FieldCollection>();
//    fields->add_field(make_scalar_type(DataType::INT64), "first");
//    fields->add_field(make_scalar_type(DataType::FLOAT64), "second");
//    fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "third");
//    Columns columns{fields, generate_column_types(*fields)};
//    std::vector<Columns> input(1, columns);
//    auto res = inner_join(input);
//    ASSERT_EQ(res, *columns.fields);
//}
//
//TEST(InnerJoin, ExactlyMatching) {
//    auto fields = std::make_shared<FieldCollection>();
//    fields->add_field(make_scalar_type(DataType::INT64), "first");
//    fields->add_field(make_scalar_type(DataType::FLOAT64), "second");
//    fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "third");
//    Columns columns{fields, generate_column_types(*fields)};
//    std::vector<Columns> input(4, columns);
//    auto res = inner_join(input);
//    ASSERT_EQ(res, *columns.fields);
//}
//
//TEST(InnerJoin, MatchingNamesCompatibleTypes) {
//    auto first_fields = std::make_shared<FieldCollection>();
//    first_fields->add_field(make_scalar_type(DataType::INT64), "first");
//    first_fields->add_field(make_scalar_type(DataType::UINT8), "second");
//    Columns first_columns{first_fields, generate_column_types(*first_fields)};
//    auto second_fields = std::make_shared<FieldCollection>();
//    second_fields->add_field(make_scalar_type(DataType::INT32), "first");
//    second_fields->add_field(make_scalar_type(DataType::FLOAT64), "second");
//    Columns second_columns{second_fields, generate_column_types(*second_fields)};
//    std::vector<Columns> input{first_columns, second_columns};
//    auto res = inner_join(input);
//    FieldCollection expected;
//    expected.add_field(make_scalar_type(DataType::INT64), "first");
//    expected.add_field(make_scalar_type(DataType::FLOAT64), "second");
//    ASSERT_EQ(res, expected);
//}
//
//TEST(InnerJoin, MatchingNamesIncompatibleTypes) {
//    auto first_fields = std::make_shared<FieldCollection>();
//    first_fields->add_field(make_scalar_type(DataType::INT64), "first");
//    first_fields->add_field(make_scalar_type(DataType::UINT8), "second");
//    Columns first_columns{first_fields, generate_column_types(*first_fields)};
//    auto second_fields = std::make_shared<FieldCollection>();
//    second_fields->add_field(make_scalar_type(DataType::INT32), "first");
//    second_fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "second");
//    Columns second_columns{second_fields, generate_column_types(*second_fields)};
//    std::vector<Columns> input{first_columns, second_columns};
//    ASSERT_THROW(inner_join(input), SchemaException);
//}
//
//TEST(InnerJoin, DisJointColumnNames) {
//    auto first_fields = std::make_shared<FieldCollection>();
//    first_fields->add_field(make_scalar_type(DataType::INT64), "first");
//    first_fields->add_field(make_scalar_type(DataType::UINT8), "second");
//    first_fields->add_field(make_scalar_type(DataType::FLOAT64), "third");
//    Columns first_columns{first_fields, generate_column_types(*first_fields)};
//    auto second_fields = std::make_shared<FieldCollection>();
//    second_fields->add_field(make_scalar_type(DataType::FLOAT64), "third");
//    second_fields->add_field(make_scalar_type(DataType::UINT8), "second");
//    second_fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "fourth");
//    Columns second_columns{second_fields, generate_column_types(*second_fields)};
//    std::vector<Columns> input{first_columns, second_columns};
//    auto res = inner_join(input);
//    FieldCollection expected;
//    expected.add_field(make_scalar_type(DataType::UINT8), "second");
//    expected.add_field(make_scalar_type(DataType::FLOAT64), "third");
//    ASSERT_EQ(res, expected);
//    input = std::vector<Columns>{second_columns, first_columns};
//    res = inner_join(input);
//    expected = FieldCollection();
//    expected.add_field(make_scalar_type(DataType::FLOAT64), "third");
//    expected.add_field(make_scalar_type(DataType::UINT8), "second");
//    ASSERT_EQ(res, expected);
//}
//
//TEST(OuterJoin, SingleSchema) {
//    auto fields = std::make_shared<FieldCollection>();
//    fields->add_field(make_scalar_type(DataType::INT64), "first");
//    fields->add_field(make_scalar_type(DataType::FLOAT64), "second");
//    fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "third");
//    Columns columns{fields, generate_column_types(*fields)};
//    std::vector<Columns> input(1, columns);
//    auto res = inner_join(input);
//    ASSERT_EQ(res, *columns.fields);
//}
//
//TEST(OuterJoin, ExactlyMatching) {
//    auto fields = std::make_shared<FieldCollection>();
//    fields->add_field(make_scalar_type(DataType::INT64), "first");
//    fields->add_field(make_scalar_type(DataType::FLOAT64), "second");
//    fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "third");
//    Columns columns{fields, generate_column_types(*fields)};
//    std::vector<Columns> input(4, columns);
//    auto res = inner_join(input);
//    ASSERT_EQ(res, *columns.fields);
//}
//
//TEST(OuterJoin, MatchingNamesCompatibleTypes) {
//    auto first_fields = std::make_shared<FieldCollection>();
//    first_fields->add_field(make_scalar_type(DataType::INT64), "first");
//    first_fields->add_field(make_scalar_type(DataType::UINT8), "second");
//    Columns first_columns{first_fields, generate_column_types(*first_fields)};
//    auto second_fields = std::make_shared<FieldCollection>();
//    second_fields->add_field(make_scalar_type(DataType::INT32), "first");
//    second_fields->add_field(make_scalar_type(DataType::FLOAT64), "second");
//    Columns second_columns{second_fields, generate_column_types(*second_fields)};
//    std::vector<Columns> input{first_columns, second_columns};
//    auto res = inner_join(input);
//    FieldCollection expected;
//    expected.add_field(make_scalar_type(DataType::INT64), "first");
//    expected.add_field(make_scalar_type(DataType::FLOAT64), "second");
//    ASSERT_EQ(res, expected);
//}
//
//TEST(OuterJoin, MatchingNamesIncompatibleTypes) {
//    auto first_fields = std::make_shared<FieldCollection>();
//    first_fields->add_field(make_scalar_type(DataType::INT64), "first");
//    first_fields->add_field(make_scalar_type(DataType::UINT8), "second");
//    Columns first_columns{first_fields, generate_column_types(*first_fields)};
//    auto second_fields = std::make_shared<FieldCollection>();
//    second_fields->add_field(make_scalar_type(DataType::INT32), "first");
//    second_fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "second");
//    Columns second_columns{second_fields, generate_column_types(*second_fields)};
//    std::vector<Columns> input{first_columns, second_columns};
//    ASSERT_THROW(inner_join(input), SchemaException);
//}
//
//TEST(OuterJoin, DisJointColumnNames) {
//    auto first_fields = std::make_shared<FieldCollection>();
//    first_fields->add_field(make_scalar_type(DataType::INT64), "first");
//    first_fields->add_field(make_scalar_type(DataType::UINT8), "second");
//    first_fields->add_field(make_scalar_type(DataType::FLOAT64), "third");
//    Columns first_columns{first_fields, generate_column_types(*first_fields)};
//    auto second_fields = std::make_shared<FieldCollection>();
//    second_fields->add_field(make_scalar_type(DataType::FLOAT64), "third");
//    second_fields->add_field(make_scalar_type(DataType::UINT8), "second");
//    second_fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "fourth");
//    Columns second_columns{second_fields, generate_column_types(*second_fields)};
//    std::vector<Columns> input{first_columns, second_columns};
//    auto res = inner_join(input);
//    FieldCollection expected;
//    first_fields->add_field(make_scalar_type(DataType::INT64), "first");
//    expected.add_field(make_scalar_type(DataType::UINT8), "second");
//    expected.add_field(make_scalar_type(DataType::FLOAT64), "third");
//    second_fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "fourth");
//    ASSERT_EQ(res, expected);
//    input = std::vector<Columns>{second_columns, first_columns};
//    res = inner_join(input);
//    expected = FieldCollection();
//    expected.add_field(make_scalar_type(DataType::FLOAT64), "third");
//    expected.add_field(make_scalar_type(DataType::UINT8), "second");
//    second_fields->add_field(make_scalar_type(DataType::UTF_DYNAMIC64), "fourth");
//    first_fields->add_field(make_scalar_type(DataType::INT64), "first");
//    ASSERT_EQ(res, expected);
//}
