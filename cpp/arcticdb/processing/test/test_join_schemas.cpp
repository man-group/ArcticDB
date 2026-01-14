/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <google/protobuf/util/message_differencer.h>

#include <gtest/gtest.h>
#include <arcticdb/processing/clause_utils.hpp>

using namespace arcticdb;
using namespace arcticdb::pipelines;
using namespace google::protobuf::util;
using NormalizationMetadata = arcticdb::proto::descriptors::NormalizationMetadata;

class GenerateIndexDescriptorTest : public testing::Test {
  protected:
    IndexDescriptorImpl timestamp_2_index{IndexDescriptor::Type::TIMESTAMP, 2};
    OutputSchema rowcount_0{{{}, IndexDescriptorImpl{IndexDescriptor::Type::ROWCOUNT, 0}}, {}};
    OutputSchema timestamp_1{{{}, IndexDescriptorImpl{IndexDescriptor::Type::TIMESTAMP, 1}}, {}};
    OutputSchema timestamp_2{{{}, timestamp_2_index}, {}};
    OutputSchema string_1{{{}, IndexDescriptorImpl{IndexDescriptor::Type::STRING, 1}}, {}};
};

TEST_F(GenerateIndexDescriptorTest, SingleDescriptor) {
    ASSERT_EQ(generate_index_descriptor({timestamp_2}), timestamp_2_index);
}

TEST_F(GenerateIndexDescriptorTest, IdenticalDescriptors) {
    ASSERT_EQ(generate_index_descriptor({timestamp_2, timestamp_2}), timestamp_2_index);
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

class AddIndexFieldsTest : public testing::Test {
  protected:
    void SetUp() override {
        one_datetime_field.add_scalar_field(DataType::NANOSECONDS_UTC64, "my index");
        one_datetime_field_rename.add_scalar_field(DataType::NANOSECONDS_UTC64, "ts");
        one_float_field.add_scalar_field(DataType::FLOAT64, "my index");

        one_datetime_one_int_field.add_scalar_field(DataType::NANOSECONDS_UTC64, "my index");
        one_datetime_one_int_field.add_scalar_field(DataType::INT32, "level2");

        one_datetime_one_int_field_rename_first.add_scalar_field(DataType::NANOSECONDS_UTC64, "ts");
        one_datetime_one_int_field_rename_first.add_scalar_field(DataType::INT32, "level2");

        one_datetime_one_int_field_rename_second.add_scalar_field(DataType::NANOSECONDS_UTC64, "my index");
        one_datetime_one_int_field_rename_second.add_scalar_field(DataType::INT32, "int");

        one_datetime_one_int_field_rename_both.add_scalar_field(DataType::NANOSECONDS_UTC64, "ts");
        one_datetime_one_int_field_rename_both.add_scalar_field(DataType::INT32, "int");

        one_datetime_one_float_field.add_scalar_field(DataType::NANOSECONDS_UTC64, "my index");
        one_datetime_one_float_field.add_scalar_field(DataType::FLOAT64, "level2");

        one_datetime_one_string_field.add_scalar_field(DataType::NANOSECONDS_UTC64, "my index");
        one_datetime_one_string_field.add_scalar_field(DataType::UTF_DYNAMIC64, "level2");
    }

    StreamDescriptor one_index_field{{}, IndexDescriptorImpl{IndexDescriptor::Type::TIMESTAMP, 1}};
    StreamDescriptor two_index_fields{{}, IndexDescriptorImpl{IndexDescriptor::Type::TIMESTAMP, 2}};
    StreamDescriptor one_datetime_field;
    StreamDescriptor one_datetime_field_rename;
    StreamDescriptor one_float_field;
    StreamDescriptor one_datetime_one_int_field;
    StreamDescriptor one_datetime_one_int_field_rename_first;
    StreamDescriptor one_datetime_one_int_field_rename_second;
    StreamDescriptor one_datetime_one_int_field_rename_both;
    StreamDescriptor one_datetime_one_float_field;
    StreamDescriptor one_datetime_one_string_field;
};

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

TEST_F(AddIndexFieldsTest, TypePromotion) {
    auto stream_desc = two_index_fields.clone();
    std::vector<OutputSchema> output_schemas{{one_datetime_one_float_field, {}}, {one_datetime_one_int_field, {}}};
    auto non_matching_name_indices = add_index_fields(stream_desc, output_schemas);
    ASSERT_EQ(stream_desc.fields(), one_datetime_one_float_field.fields());
    ASSERT_TRUE(non_matching_name_indices.empty());
    // Should be independent of the input schemas ordering
    stream_desc = two_index_fields.clone();
    output_schemas = std::vector<OutputSchema>{{one_datetime_one_float_field, {}}, {one_datetime_one_int_field, {}}};
    non_matching_name_indices = add_index_fields(stream_desc, output_schemas);
    ASSERT_EQ(stream_desc.fields(), one_datetime_one_float_field.fields());
    ASSERT_TRUE(non_matching_name_indices.empty());
}

TEST_F(AddIndexFieldsTest, ScalarIndexNonMatchingNames) {
    auto stream_desc = one_index_field.clone();
    std::vector<OutputSchema> output_schemas{{one_datetime_field, {}}, {one_datetime_field_rename, {}}};
    auto non_matching_name_indices = add_index_fields(stream_desc, output_schemas);
    FieldCollection expected;
    expected.add_field(make_scalar_type(DataType::NANOSECONDS_UTC64), "index");
    ASSERT_EQ(stream_desc.fields(), expected);
    ASSERT_TRUE(non_matching_name_indices.size() == 1);
    ASSERT_TRUE(non_matching_name_indices.contains(0));
}

TEST_F(AddIndexFieldsTest, MultiIndexNonMatchingNames) {
    auto stream_desc = two_index_fields.clone();
    std::vector<OutputSchema> output_schemas{
            {one_datetime_one_int_field, {}}, {one_datetime_one_int_field_rename_first, {}}
    };
    auto non_matching_name_indices = add_index_fields(stream_desc, output_schemas);
    FieldCollection expected;
    expected.add_field(make_scalar_type(DataType::NANOSECONDS_UTC64), "index");
    expected.add_field(make_scalar_type(DataType::INT32), "level2");
    ASSERT_EQ(stream_desc.fields(), expected);
    ASSERT_TRUE(non_matching_name_indices.size() == 1);
    ASSERT_TRUE(non_matching_name_indices.contains(0));

    stream_desc = two_index_fields.clone();
    output_schemas =
            std::vector<OutputSchema>{{one_datetime_one_int_field_rename_first, {}}, {one_datetime_one_int_field, {}}};
    non_matching_name_indices = add_index_fields(stream_desc, output_schemas);
    expected = FieldCollection();
    expected.add_field(make_scalar_type(DataType::NANOSECONDS_UTC64), "index");
    expected.add_field(make_scalar_type(DataType::INT32), "level2");
    ASSERT_EQ(stream_desc.fields(), expected);
    ASSERT_TRUE(non_matching_name_indices.size() == 1);
    ASSERT_TRUE(non_matching_name_indices.contains(0));

    stream_desc = two_index_fields.clone();
    output_schemas =
            std::vector<OutputSchema>{{one_datetime_one_int_field, {}}, {one_datetime_one_int_field_rename_second, {}}};
    non_matching_name_indices = add_index_fields(stream_desc, output_schemas);
    expected = FieldCollection();
    expected.add_field(make_scalar_type(DataType::NANOSECONDS_UTC64), "my index");
    expected.add_field(make_scalar_type(DataType::INT32), "__fkidx__1");
    ASSERT_EQ(stream_desc.fields(), expected);
    ASSERT_TRUE(non_matching_name_indices.size() == 1);
    ASSERT_TRUE(non_matching_name_indices.contains(1));

    stream_desc = two_index_fields.clone();
    output_schemas =
            std::vector<OutputSchema>{{one_datetime_one_int_field_rename_second, {}}, {one_datetime_one_int_field, {}}};
    non_matching_name_indices = add_index_fields(stream_desc, output_schemas);
    expected = FieldCollection();
    expected.add_field(make_scalar_type(DataType::NANOSECONDS_UTC64), "my index");
    expected.add_field(make_scalar_type(DataType::INT32), "__fkidx__1");
    ASSERT_EQ(stream_desc.fields(), expected);
    ASSERT_TRUE(non_matching_name_indices.size() == 1);
    ASSERT_TRUE(non_matching_name_indices.contains(1));

    stream_desc = two_index_fields.clone();
    output_schemas =
            std::vector<OutputSchema>{{one_datetime_one_int_field, {}}, {one_datetime_one_int_field_rename_both, {}}};
    non_matching_name_indices = add_index_fields(stream_desc, output_schemas);
    expected = FieldCollection();
    expected.add_field(make_scalar_type(DataType::NANOSECONDS_UTC64), "index");
    expected.add_field(make_scalar_type(DataType::INT32), "__fkidx__1");
    ASSERT_EQ(stream_desc.fields(), expected);
    ASSERT_TRUE(non_matching_name_indices.size() == 2);
    ASSERT_TRUE(non_matching_name_indices.contains(0) && non_matching_name_indices.contains(1));

    stream_desc = two_index_fields.clone();
    output_schemas =
            std::vector<OutputSchema>{{one_datetime_one_int_field_rename_both, {}}, {one_datetime_one_int_field, {}}};
    non_matching_name_indices = add_index_fields(stream_desc, output_schemas);
    expected = FieldCollection();
    expected.add_field(make_scalar_type(DataType::NANOSECONDS_UTC64), "index");
    expected.add_field(make_scalar_type(DataType::INT32), "__fkidx__1");
    ASSERT_EQ(stream_desc.fields(), expected);
    ASSERT_TRUE(non_matching_name_indices.size() == 2);
    ASSERT_TRUE(non_matching_name_indices.contains(0) && non_matching_name_indices.contains(1));
}

class GenerateNormMetaTest : public testing::Test {
  protected:
    enum class PandasClass { DATAFRAME, SERIES };

    struct single_index_params {
        PandasClass pandas_class = PandasClass::DATAFRAME;
        std::string name = "";
        bool is_int = false;
        bool fake_name = false;
        bool is_physically_stored = true;
        std::string tz = "";
        int start = 0;
        int step = 0;
    };

    OutputSchema single_index(const single_index_params& params) {
        NormalizationMetadata norm_meta;
        auto* index = params.pandas_class == PandasClass::DATAFRAME
                              ? norm_meta.mutable_df()->mutable_common()->mutable_index()
                              : norm_meta.mutable_series()->mutable_common()->mutable_index();
        index->set_is_physically_stored(params.is_physically_stored);
        index->set_name(params.name);
        index->set_is_int(params.is_int);
        index->set_fake_name(params.fake_name);
        index->set_tz(params.tz);
        index->set_start(params.start);
        index->set_step(params.step);
        return {{}, norm_meta};
    }

    struct multi_index_params {
        PandasClass pandas_class = PandasClass::DATAFRAME;
        uint32_t field_count = 2;
        std::string name = "";
        bool is_int = false;
        std::string tz = "";
        std::vector<uint32_t> fake_field_pos = std::vector<uint32_t>();
    };

    OutputSchema multi_index(const multi_index_params& params) {
        NormalizationMetadata norm_meta;
        auto* index = params.pandas_class == PandasClass::DATAFRAME
                              ? norm_meta.mutable_df()->mutable_common()->mutable_multi_index()
                              : norm_meta.mutable_series()->mutable_common()->mutable_multi_index();
        index->set_name(params.name);
        index->set_field_count(params.field_count);
        index->set_is_int(params.is_int);
        index->set_tz(params.tz);
        for (auto idx : params.fake_field_pos) {
            index->add_fake_field_pos(idx);
        }
        return {{}, norm_meta};
    }
};

TEST_F(GenerateNormMetaTest, SingleNormMeta) {
    auto single = single_index({.name = "ts", .tz = "UTC"});
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({single}, {}), single.norm_metadata_));
    auto multi = multi_index({.name = "ts", .tz = "UTC"});
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({multi}, {}), multi.norm_metadata_));
}

TEST_F(GenerateNormMetaTest, IdenticalNormMetas) {
    auto single_df = single_index({.name = "ts", .tz = "UTC"});
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({single_df, single_df}, {}), single_df.norm_metadata_));
    auto multi_df = multi_index({.name = "ts", .tz = "UTC"});
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({multi_df, multi_df}, {}), multi_df.norm_metadata_));
    auto single_series = single_index({.pandas_class = PandasClass::SERIES, .name = "ts", .tz = "UTC"});
    ASSERT_TRUE(MessageDifferencer::Equals(
            generate_norm_meta({single_series, single_series}, {}), single_series.norm_metadata_
    ));
    auto multi_series = multi_index({.pandas_class = PandasClass::SERIES, .name = "ts", .tz = "UTC"});
    ASSERT_TRUE(MessageDifferencer::Equals(
            generate_norm_meta({multi_series, multi_series}, {}), multi_series.norm_metadata_
    ));
}

TEST_F(GenerateNormMetaTest, DataFrameDifferentNames) {
    // Different index names (either in the name or is_int fields) should lead to an empty string index name
    // If fake_name is true for any index the output must also have this as true
    auto str_1 = single_index({.name = "1", .tz = "UTC"});
    auto str_2 = single_index({.name = "2", .tz = "UTC"});
    auto int_1 = single_index({.name = "1", .is_int = true, .tz = "UTC"});
    auto int_2 = single_index({.name = "2", .is_int = true, .tz = "UTC"});
    auto fake_name = single_index({.name = "index", .fake_name = true, .tz = "UTC"});
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, str_2}, {}), fake_name.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({int_1, int_2}, {}), fake_name.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, int_1}, {}), fake_name.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({int_1, str_1}, {}), fake_name.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, fake_name}, {}), fake_name.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({fake_name, int_1}, {}), fake_name.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, fake_name, int_1}, {}), fake_name.norm_metadata_)
    );
    str_1 = multi_index({.name = "1", .tz = "UTC"});
    str_2 = multi_index({.name = "2", .tz = "UTC"});
    int_1 = multi_index({.name = "1", .is_int = true, .tz = "UTC"});
    int_2 = multi_index({.name = "2", .is_int = true, .tz = "UTC"});
    auto no_name = multi_index({.tz = "UTC"});
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, str_2}, {}), no_name.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({int_1, int_2}, {}), no_name.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, int_1}, {}), no_name.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({int_1, str_1}, {}), no_name.norm_metadata_));
}

TEST_F(GenerateNormMetaTest, SeriesDifferentNames) {
    // Different index names (either in the name or is_int fields) should lead to an empty string index name
    // If fake_name is true for any index the output must also have this as true
    auto str_1 = single_index({.pandas_class = PandasClass::SERIES, .name = "1", .tz = "UTC"});
    auto str_2 = single_index({.pandas_class = PandasClass::SERIES, .name = "2", .tz = "UTC"});
    auto int_1 = single_index({.pandas_class = PandasClass::SERIES, .name = "1", .is_int = true, .tz = "UTC"});
    auto int_2 = single_index({.pandas_class = PandasClass::SERIES, .name = "2", .is_int = true, .tz = "UTC"});
    auto fake_name =
            single_index({.pandas_class = PandasClass::SERIES, .name = "index", .fake_name = true, .tz = "UTC"});
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, str_2}, {}), fake_name.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({int_1, int_2}, {}), fake_name.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, int_1}, {}), fake_name.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({int_1, str_1}, {}), fake_name.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, fake_name}, {}), fake_name.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({fake_name, int_1}, {}), fake_name.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, fake_name, int_1}, {}), fake_name.norm_metadata_)
    );
    str_1 = multi_index({.pandas_class = PandasClass::SERIES, .name = "1", .tz = "UTC"});
    str_2 = multi_index({.pandas_class = PandasClass::SERIES, .name = "2", .tz = "UTC"});
    int_1 = multi_index({.pandas_class = PandasClass::SERIES, .name = "1", .is_int = true, .tz = "UTC"});
    int_2 = multi_index({.pandas_class = PandasClass::SERIES, .name = "2", .is_int = true, .tz = "UTC"});
    auto no_name = multi_index({.pandas_class = PandasClass::SERIES, .tz = "UTC"});
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, str_2}, {}), no_name.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({int_1, int_2}, {}), no_name.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({str_1, int_1}, {}), no_name.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({int_1, str_1}, {}), no_name.norm_metadata_));
}

TEST_F(GenerateNormMetaTest, MultiIndexFakeFieldPos) {
    // Fake field positions in multi-indexes are basically unioned together with the second argument to
    // generate_norm_meta
    const auto no_fake_fields = multi_index({.name = "ts"});
    const auto first_field_fake = multi_index({.name = "index", .fake_field_pos{0}});
    const auto second_field_fake = multi_index({.name = "ts", .fake_field_pos{1}});
    const auto both_fields_fake = multi_index({.name = "index", .fake_field_pos{0, 1}});
    // Ordering in fake_field_pos should not matter
    const auto both_fields_fake_reversed = multi_index({.name = "index", .fake_field_pos{1, 0}});
    // Feels like there should be a simpler way to get the correct FieldDescriptor* to pass to TreatAsSet to ignore the
    // ordering of fake_field_pos, but couldn't readily find examples
    MessageDifferencer md;
    const auto reflection = first_field_fake.norm_metadata_.df().common().multi_index().GetReflection();
    std::vector<const google::protobuf::FieldDescriptor*> fields;
    reflection->ListFields(first_field_fake.norm_metadata_.df().common().multi_index(), &fields);
    for (auto field : fields) {
        if (field->name() == "fake_field_pos") {
            md.TreatAsSet(field);
            break;
        }
    }
    std::vector<OutputSchema> schemas{
            no_fake_fields, first_field_fake, second_field_fake, both_fields_fake, both_fields_fake_reversed
    };
    std::vector<std::unordered_set<size_t>> non_matching_name_indices_vec{{}, {0}, {1}, {0, 1}};
    for (const auto& first_schema : schemas) {
        const auto& first_norm = first_schema.norm_metadata_;
        for (const auto& second_schema : schemas) {
            const auto& second_norm = second_schema.norm_metadata_;
            // Deliberately take a copy because generate_norm_meta takes this argument as an rvalue
            for (auto non_matching_name_indices : non_matching_name_indices_vec) {
                const bool first_fake = md.Compare(first_norm, first_field_fake.norm_metadata_) ||
                                        md.Compare(first_norm, both_fields_fake.norm_metadata_) ||
                                        md.Compare(first_norm, both_fields_fake_reversed.norm_metadata_) ||
                                        md.Compare(second_norm, first_field_fake.norm_metadata_) ||
                                        md.Compare(second_norm, both_fields_fake.norm_metadata_) ||
                                        md.Compare(second_norm, both_fields_fake_reversed.norm_metadata_) ||
                                        non_matching_name_indices.contains(0);
                const bool second_fake = md.Compare(first_norm, second_field_fake.norm_metadata_) ||
                                         md.Compare(first_norm, both_fields_fake.norm_metadata_) ||
                                         md.Compare(first_norm, both_fields_fake_reversed.norm_metadata_) ||
                                         md.Compare(second_norm, second_field_fake.norm_metadata_) ||
                                         md.Compare(second_norm, both_fields_fake.norm_metadata_) ||
                                         md.Compare(second_norm, both_fields_fake_reversed.norm_metadata_) ||
                                         non_matching_name_indices.contains(1);
                NormalizationMetadata expected;
                if (first_fake && second_fake) {
                    expected = both_fields_fake.norm_metadata_;
                } else if (first_fake) {
                    expected = first_field_fake.norm_metadata_;
                } else if (second_fake) {
                    expected = second_field_fake.norm_metadata_;
                } else {
                    // Neither fake
                    expected = no_fake_fields.norm_metadata_;
                }
                ASSERT_TRUE(md.Compare(
                        generate_norm_meta({first_schema, second_schema}, std::move(non_matching_name_indices)),
                        expected
                ));
            }
        }
    }
}

TEST_F(GenerateNormMetaTest, DifferentTimezones) {
    // Different index timezones should lead to an empty string index timezone
    auto single_utc = single_index({.name = "ts", .tz = "UTC"});
    auto single_est = single_index({.name = "ts", .tz = "EST"});
    auto expected = single_index({.name = "ts"});
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({single_utc, single_est}, {}), expected.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({single_est, single_utc}, {}), expected.norm_metadata_));
    auto multi_utc = multi_index({.name = "ts", .tz = "UTC"});
    auto multi_est = multi_index({.name = "ts", .tz = "EST"});
    expected = multi_index({.name = "ts"});
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({multi_utc, multi_est}, {}), expected.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({multi_est, multi_utc}, {}), expected.norm_metadata_));
}

TEST_F(GenerateNormMetaTest, DifferentIsPhysicallyStored) {
    ASSERT_THROW(
            generate_norm_meta(
                    {single_index({.name = "ts"}), single_index({.name = "ts", .is_physically_stored = false})}, {}
            ),
            SchemaException
    );
}

TEST_F(GenerateNormMetaTest, DifferentFieldCount) {
    ASSERT_THROW(
            generate_norm_meta(
                    {multi_index({.name = "ts", .tz = "UTC"}),
                     multi_index({.field_count = 3, .name = "ts", .tz = "UTC"})},
                    {}
            ),
            SchemaException
    );
}

TEST_F(GenerateNormMetaTest, RangeIndexBasic) {
    auto index = single_index({.name = "index", .fake_name = true, .is_physically_stored = false, .step = 1});
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({index, index, index}, {}), index.norm_metadata_));
}

TEST_F(GenerateNormMetaTest, RangeIndexNonDefaultStep) {
    auto first =
            single_index({.name = "index", .fake_name = true, .is_physically_stored = false, .start = 10, .step = 2});
    auto second =
            single_index({.name = "index", .fake_name = true, .is_physically_stored = false, .start = 5, .step = 2});
    auto third =
            single_index({.name = "index", .fake_name = true, .is_physically_stored = false, .start = 12, .step = 2});
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({first, second, third}, {}), first.norm_metadata_));
}

TEST_F(GenerateNormMetaTest, RangeIndexNonMatchingStep) {
    auto a = single_index({.name = "index", .fake_name = true, .is_physically_stored = false, .start = 10, .step = 2});
    auto b = single_index({.name = "index", .fake_name = true, .is_physically_stored = false, .start = 5, .step = 3});
    auto c = single_index({.name = "index", .fake_name = true, .is_physically_stored = false, .start = 12, .step = 2});
    auto expected_result = single_index({.name = "index", .fake_name = true, .is_physically_stored = false, .step = 1});
    // Order that the steps are seen in should not matter
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({a, b, c}, {}), expected_result.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({b, c, a}, {}), expected_result.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({c, a, b}, {}), expected_result.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({a, c, b}, {}), expected_result.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({c, b, a}, {}), expected_result.norm_metadata_));
    ASSERT_TRUE(MessageDifferencer::Equals(generate_norm_meta({b, a, c}, {}), expected_result.norm_metadata_));
}

TEST_F(GenerateNormMetaTest, SeriesAndDataFrame) {
    auto series_single = single_index({.pandas_class = PandasClass::SERIES});
    auto df_single = single_index({.pandas_class = PandasClass::DATAFRAME});
    ASSERT_THROW(generate_norm_meta({series_single, df_single}, {}), SchemaException);
    ASSERT_THROW(generate_norm_meta({df_single, series_single}, {}), SchemaException);
    auto series_multi = multi_index({.pandas_class = PandasClass::SERIES});
    auto df_multi = multi_index({.pandas_class = PandasClass::DATAFRAME});
    ASSERT_THROW(generate_norm_meta({series_multi, df_multi}, {}), SchemaException);
    ASSERT_THROW(generate_norm_meta({df_multi, series_multi}, {}), SchemaException);
}

ankerl::unordered_dense::map<std::string, DataType> generate_column_types(const FieldCollection& fields) {
    ankerl::unordered_dense::map<std::string, DataType> res;
    res.reserve(fields.size());
    for (const auto& field : fields) {
        res.emplace(field.name(), field.type().data_type());
    }
    return res;
}

TEST(Join, SingleSchema) {
    StreamDescriptor input;
    input.add_scalar_field(DataType::INT64, "first");
    input.add_scalar_field(DataType::FLOAT64, "second");
    input.add_scalar_field(DataType::UTF_DYNAMIC64, "third");
    std::vector<OutputSchema> schemas;
    schemas.emplace_back(input.clone(), NormalizationMetadata());
    StreamDescriptor res_inner;
    inner_join(res_inner, schemas);
    ASSERT_EQ(res_inner, input);
    StreamDescriptor res_outer;
    outer_join(res_outer, schemas);
    ASSERT_EQ(res_outer, input);
}

TEST(Join, MultipleSchema) {
    StreamDescriptor input;
    input.add_scalar_field(DataType::INT64, "first");
    input.add_scalar_field(DataType::FLOAT64, "second");
    input.add_scalar_field(DataType::UTF_DYNAMIC64, "third");
    std::vector<OutputSchema> schemas;
    schemas.emplace_back(input.clone(), NormalizationMetadata());
    schemas.emplace_back(input.clone(), NormalizationMetadata());
    StreamDescriptor res_inner;
    inner_join(res_inner, schemas);
    ASSERT_EQ(res_inner, input);
    StreamDescriptor res_outer;
    outer_join(res_outer, schemas);
    ASSERT_EQ(res_outer, input);
}

TEST(Join, MatchingNamesCompatibleTypes) {
    StreamDescriptor first_input;
    first_input.add_scalar_field(DataType::INT64, "first");
    first_input.add_scalar_field(DataType::UINT8, "second");
    StreamDescriptor second_input;
    second_input.add_scalar_field(DataType::INT32, "first");
    second_input.add_scalar_field(DataType::FLOAT64, "second");
    std::vector<OutputSchema> schemas;
    schemas.emplace_back(first_input.clone(), NormalizationMetadata());
    schemas.emplace_back(second_input.clone(), NormalizationMetadata());
    StreamDescriptor expected;
    expected.add_scalar_field(DataType::INT64, "first");
    expected.add_scalar_field(DataType::FLOAT64, "second");
    StreamDescriptor res_inner;
    inner_join(res_inner, schemas);
    ASSERT_EQ(res_inner, expected);
    StreamDescriptor res_outer;
    outer_join(res_outer, schemas);
    ASSERT_EQ(res_outer, expected);
}

TEST(Join, MatchingNamesIncompatibleTypes) {
    StreamDescriptor first_input;
    first_input.add_scalar_field(DataType::INT64, "first");
    first_input.add_scalar_field(DataType::UINT8, "second");
    StreamDescriptor second_input;
    second_input.add_scalar_field(DataType::INT32, "first");
    second_input.add_scalar_field(DataType::UTF_DYNAMIC64, "second");
    std::vector<OutputSchema> schemas;
    schemas.emplace_back(first_input.clone(), NormalizationMetadata());
    schemas.emplace_back(second_input.clone(), NormalizationMetadata());
    StreamDescriptor res_inner;
    ASSERT_THROW(inner_join(res_inner, schemas), SchemaException);
    StreamDescriptor res_outer;
    ASSERT_THROW(outer_join(res_outer, schemas), SchemaException);
}

TEST(Join, DisJointColumnNames) {
    StreamDescriptor first_input;
    first_input.add_scalar_field(DataType::INT64, "first");
    first_input.add_scalar_field(DataType::UINT8, "second");
    first_input.add_scalar_field(DataType::FLOAT64, "third");
    StreamDescriptor second_input;
    second_input.add_scalar_field(DataType::FLOAT64, "third");
    second_input.add_scalar_field(DataType::UINT8, "second");
    second_input.add_scalar_field(DataType::UTF_DYNAMIC64, "fourth");
    std::vector<OutputSchema> schemas;
    schemas.emplace_back(first_input.clone(), NormalizationMetadata());
    schemas.emplace_back(second_input.clone(), NormalizationMetadata());
    StreamDescriptor expected_inner;
    expected_inner.add_scalar_field(DataType::UINT8, "second");
    expected_inner.add_scalar_field(DataType::FLOAT64, "third");
    StreamDescriptor res_inner;
    inner_join(res_inner, schemas);
    ASSERT_EQ(res_inner, expected_inner);
    StreamDescriptor expected_outer;
    expected_outer.add_scalar_field(DataType::INT64, "first");
    expected_outer.add_scalar_field(DataType::UINT8, "second");
    expected_outer.add_scalar_field(DataType::FLOAT64, "third");
    expected_outer.add_scalar_field(DataType::UTF_DYNAMIC64, "fourth");
    StreamDescriptor res_outer;
    outer_join(res_outer, schemas);
    ASSERT_EQ(res_outer, expected_outer);
}

TEST(InnerJoin, MatchingNamesIncompatibleTypesOnUnusedColumns) {
    StreamDescriptor first_input;
    first_input.add_scalar_field(DataType::INT64, "first");
    first_input.add_scalar_field(DataType::INT64, "second");
    StreamDescriptor second_input;
    second_input.add_scalar_field(DataType::INT64, "first");
    second_input.add_scalar_field(DataType::UTF_DYNAMIC64, "second");
    StreamDescriptor third_input;
    third_input.add_scalar_field(DataType::INT64, "first");
    std::vector<OutputSchema> schemas;
    schemas.emplace_back(first_input.clone(), NormalizationMetadata());
    schemas.emplace_back(second_input.clone(), NormalizationMetadata());
    schemas.emplace_back(third_input.clone(), NormalizationMetadata());
    StreamDescriptor res;
    inner_join(res, schemas);
    StreamDescriptor expected_res;
    expected_res.add_scalar_field(DataType::INT64, "first");
    ASSERT_EQ(res, expected_res);
}
