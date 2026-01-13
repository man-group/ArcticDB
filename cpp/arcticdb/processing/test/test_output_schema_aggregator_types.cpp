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
#include <arcticdb/util/test/generators.hpp>

using namespace arcticdb;
using namespace arcticdb::pipelines;
using namespace google::protobuf::util;

class AggregationClauseOutputTypesTest : public testing::Test {
  protected:
    void SetUp() override {
        initial_stream_desc_.set_id(StreamId("test symbol"));
        initial_stream_desc_.set_index({IndexDescriptor::Type::ROWCOUNT, 0});
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
            const std::string& agg, bool timestamp_supported = true, bool string_supported = false
    ) const {
        std::vector<NamedAggregator> res{
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
            const StreamDescriptor& stream_desc, bool timestamp_supported = true, bool string_supported = false
    ) const {
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

    OutputSchema initial_schema() { return {initial_stream_desc_.clone(), {}}; }
    StreamDescriptor initial_stream_desc_;
};

TEST_F(AggregationClauseOutputTypesTest, Sum) {
    AggregationClause aggregation_clause{"to_group", generate_aggregators("sum", false)};
    auto output_schema = aggregation_clause.modify_schema(initial_schema());
    const auto& stream_desc = output_schema.stream_descriptor();
    check_output_column_names(stream_desc, false);
    ASSERT_EQ(stream_desc.field(0).type().data_type(), DataType::INT64);    // grouping column
    ASSERT_EQ(stream_desc.field(1).type().data_type(), DataType::INT64);    // int8
    ASSERT_EQ(stream_desc.field(2).type().data_type(), DataType::INT64);    // int16
    ASSERT_EQ(stream_desc.field(3).type().data_type(), DataType::INT64);    // int32
    ASSERT_EQ(stream_desc.field(4).type().data_type(), DataType::INT64);    // int64
    ASSERT_EQ(stream_desc.field(5).type().data_type(), DataType::UINT64);   // uint8
    ASSERT_EQ(stream_desc.field(6).type().data_type(), DataType::UINT64);   // uint16
    ASSERT_EQ(stream_desc.field(7).type().data_type(), DataType::UINT64);   // uint32
    ASSERT_EQ(stream_desc.field(8).type().data_type(), DataType::UINT64);   // uint64
    ASSERT_EQ(stream_desc.field(9).type().data_type(), DataType::FLOAT64);  // float32
    ASSERT_EQ(stream_desc.field(10).type().data_type(), DataType::FLOAT64); // float64
    ASSERT_EQ(stream_desc.field(11).type().data_type(), DataType::UINT64);  // bool

    aggregation_clause = AggregationClause{"to_group", {{"sum", "timestamp", "timestamp_sum"}}};
    ASSERT_THROW(aggregation_clause.modify_schema(initial_schema()), SchemaException);

    aggregation_clause = AggregationClause{"to_group", {{"sum", "string", "string_sum"}}};
    ASSERT_THROW(aggregation_clause.modify_schema(initial_schema()), SchemaException);
}

TEST_F(AggregationClauseOutputTypesTest, Max) {
    AggregationClause aggregation_clause{"to_group", generate_aggregators("max")};
    auto output_schema = aggregation_clause.modify_schema(initial_schema());
    const auto& stream_desc = output_schema.stream_descriptor();
    check_output_column_names(stream_desc);
    for (size_t idx = 0; idx < 13; ++idx) {
        ASSERT_EQ(stream_desc.field(idx).type().data_type(), initial_stream_desc_.field(idx).type().data_type());
    }

    aggregation_clause = AggregationClause{"to_group", {{"max", "string", "string_agg"}}};
    ASSERT_THROW(aggregation_clause.modify_schema(initial_schema()), SchemaException);
}

TEST_F(AggregationClauseOutputTypesTest, Min) {
    AggregationClause aggregation_clause{"to_group", generate_aggregators("min")};
    auto output_schema = aggregation_clause.modify_schema(initial_schema());
    const auto& stream_desc = output_schema.stream_descriptor();
    check_output_column_names(stream_desc);
    for (size_t idx = 0; idx < 13; ++idx) {
        ASSERT_EQ(stream_desc.field(idx).type().data_type(), initial_stream_desc_.field(idx).type().data_type());
    }

    aggregation_clause = AggregationClause{"to_group", {{"min", "string", "string_agg"}}};
    ASSERT_THROW(aggregation_clause.modify_schema(initial_schema()), SchemaException);
}

TEST_F(AggregationClauseOutputTypesTest, Mean) {
    AggregationClause aggregation_clause{"to_group", generate_aggregators("mean")};
    auto output_schema = aggregation_clause.modify_schema(initial_schema());
    const auto& stream_desc = output_schema.stream_descriptor();
    check_output_column_names(stream_desc);
    ASSERT_EQ(stream_desc.field(0).type().data_type(), DataType::INT64); // grouping column
    for (size_t idx = 1; idx < 13; ++idx) {
        const DataType field_data_type = stream_desc.field(idx).type().data_type();
        if (is_time_type(field_data_type)) {
            ASSERT_EQ(field_data_type, initial_stream_desc_.field(idx).type().data_type());
        } else {
            ASSERT_EQ(field_data_type, DataType::FLOAT64);
        }
    }
    aggregation_clause = AggregationClause{"to_group", {{"mean", "string", "string_agg"}}};
    ASSERT_THROW(aggregation_clause.modify_schema(initial_schema()), SchemaException);
}

TEST_F(AggregationClauseOutputTypesTest, Count) {
    AggregationClause aggregation_clause{"to_group", generate_aggregators("count", true, true)};
    auto output_schema = aggregation_clause.modify_schema(initial_schema());
    const auto& stream_desc = output_schema.stream_descriptor();
    check_output_column_names(stream_desc, true, true);
    ASSERT_EQ(stream_desc.field(0).type().data_type(), DataType::INT64); // grouping column
    for (size_t idx = 1; idx < 14; ++idx) {
        ASSERT_EQ(stream_desc.field(idx).type().data_type(), DataType::UINT64);
    }
}

class ResampleClauseOutputTypesTest : public testing::Test {
  protected:
    void SetUp() override {
        initial_stream_desc_.set_id(StreamId("test symbol"));
        initial_stream_desc_.set_index({IndexDescriptor::Type::TIMESTAMP, 1});
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
            const std::string& agg, bool timestamp_supported = true, bool string_supported = false
    ) const {
        std::vector<NamedAggregator> res{
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
            const StreamDescriptor& stream_desc, bool timestamp_supported = true, bool string_supported = false
    ) const {
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

    OutputSchema initial_schema() { return {initial_stream_desc_.clone(), initial_norm_meta_}; }

    StreamDescriptor initial_stream_desc_;
    ;
    arcticdb::proto::descriptors::NormalizationMetadata initial_norm_meta_;
};

TEST_F(ResampleClauseOutputTypesTest, Sum) {
    auto resample_clause = generate_resample_clause(generate_aggregators("sum", false));
    auto output_schema = resample_clause.modify_schema(initial_schema());
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, initial_norm_meta_));
    const auto& stream_desc = output_schema.stream_descriptor();
    check_output_column_names(stream_desc, false);
    ASSERT_EQ(stream_desc.field(0).type().data_type(), DataType::NANOSECONDS_UTC64); // index column
    ASSERT_EQ(stream_desc.field(1).type().data_type(), DataType::INT64);             // int8
    ASSERT_EQ(stream_desc.field(2).type().data_type(), DataType::INT64);             // int16
    ASSERT_EQ(stream_desc.field(3).type().data_type(), DataType::INT64);             // int32
    ASSERT_EQ(stream_desc.field(4).type().data_type(), DataType::INT64);             // int64
    ASSERT_EQ(stream_desc.field(5).type().data_type(), DataType::UINT64);            // uint8
    ASSERT_EQ(stream_desc.field(6).type().data_type(), DataType::UINT64);            // uint16
    ASSERT_EQ(stream_desc.field(7).type().data_type(), DataType::UINT64);            // uint32
    ASSERT_EQ(stream_desc.field(8).type().data_type(), DataType::UINT64);            // uint64
    ASSERT_EQ(stream_desc.field(9).type().data_type(), DataType::FLOAT64);           // float32
    ASSERT_EQ(stream_desc.field(10).type().data_type(), DataType::FLOAT64);          // float64
    ASSERT_EQ(stream_desc.field(11).type().data_type(), DataType::UINT64);           // bool

    resample_clause = generate_resample_clause({{"sum", "timestamp", "timestamp_agg"}});
    ASSERT_THROW(resample_clause.modify_schema(initial_schema()), SchemaException);

    resample_clause = generate_resample_clause({{"sum", "string", "string_agg"}});
    ASSERT_THROW(resample_clause.modify_schema(initial_schema()), SchemaException);
}

TEST_F(ResampleClauseOutputTypesTest, Max) {
    auto resample_clause = generate_resample_clause(generate_aggregators("max"));
    auto output_schema = resample_clause.modify_schema(initial_schema());
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, initial_norm_meta_));
    const auto& stream_desc = output_schema.stream_descriptor();
    check_output_column_names(stream_desc);
    for (size_t idx = 0; idx < 13; ++idx) {
        ASSERT_EQ(stream_desc.field(idx).type().data_type(), initial_stream_desc_.field(idx).type().data_type());
    }

    resample_clause = generate_resample_clause({{"max", "string", "string_agg"}});
    ASSERT_THROW(resample_clause.modify_schema(initial_schema()), SchemaException);
}

TEST_F(ResampleClauseOutputTypesTest, Min) {
    auto resample_clause = generate_resample_clause(generate_aggregators("min"));
    auto output_schema = resample_clause.modify_schema(initial_schema());
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, initial_norm_meta_));
    const auto& stream_desc = output_schema.stream_descriptor();
    check_output_column_names(stream_desc);
    for (size_t idx = 0; idx < 13; ++idx) {
        ASSERT_EQ(stream_desc.field(idx).type().data_type(), initial_stream_desc_.field(idx).type().data_type());
    }

    resample_clause = generate_resample_clause({{"min", "string", "string_agg"}});
    ASSERT_THROW(resample_clause.modify_schema(initial_schema()), SchemaException);
}

TEST_F(ResampleClauseOutputTypesTest, Mean) {
    auto resample_clause = generate_resample_clause(generate_aggregators("mean"));
    auto output_schema = resample_clause.modify_schema(initial_schema());
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, initial_norm_meta_));
    const auto& stream_desc = output_schema.stream_descriptor();
    check_output_column_names(stream_desc);
    ASSERT_EQ(stream_desc.field(0).type().data_type(), DataType::NANOSECONDS_UTC64); // index column
    for (size_t idx = 1; idx < 12; ++idx) {
        ASSERT_EQ(stream_desc.field(idx).type().data_type(), DataType::FLOAT64);
    }
    ASSERT_EQ(stream_desc.field(12).type().data_type(), DataType::NANOSECONDS_UTC64); // timestamp column

    resample_clause = generate_resample_clause({{"mean", "string", "string_agg"}});
    ASSERT_THROW(resample_clause.modify_schema(initial_schema()), SchemaException);
}

TEST_F(ResampleClauseOutputTypesTest, Count) {
    auto resample_clause = generate_resample_clause(generate_aggregators("count", true, true));
    auto output_schema = resample_clause.modify_schema(initial_schema());
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, initial_norm_meta_));
    const auto& stream_desc = output_schema.stream_descriptor();
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
    const auto& stream_desc = output_schema.stream_descriptor();
    check_output_column_names(stream_desc, true, true);
    for (size_t idx = 0; idx < 14; ++idx) {
        ASSERT_EQ(stream_desc.field(idx).type().data_type(), initial_stream_desc_.field(idx).type().data_type());
    }
}

TEST_F(ResampleClauseOutputTypesTest, Last) {
    auto resample_clause = generate_resample_clause(generate_aggregators("last", true, true));
    auto output_schema = resample_clause.modify_schema(initial_schema());
    ASSERT_TRUE(MessageDifferencer::Equals(output_schema.norm_metadata_, initial_norm_meta_));
    const auto& stream_desc = output_schema.stream_descriptor();
    check_output_column_names(stream_desc, true, true);
    for (size_t idx = 0; idx < 14; ++idx) {
        ASSERT_EQ(stream_desc.field(idx).type().data_type(), initial_stream_desc_.field(idx).type().data_type());
    }
}
