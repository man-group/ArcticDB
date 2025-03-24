/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/processing/clause.hpp>

using namespace arcticdb;
using namespace arcticdb::pipelines;

ankerl::unordered_dense::map<std::string, DataType> generate_column_types(const FieldCollection& fields) {
    ankerl::unordered_dense::map<std::string, DataType> res;
    res.reserve(fields.size());
    for (const auto& field: fields) {
        res.emplace(field.name(), field.type().data_type());
    }
    return res;
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