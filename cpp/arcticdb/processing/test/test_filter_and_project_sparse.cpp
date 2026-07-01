/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/processing/test/ast_test_helpers.hpp>
#include <arcticdb/util/test/generators.hpp>

using namespace arcticdb;

class FilterProjectSparse : public testing::Test {
  protected:
    void SetUp() override {
        auto input_segment = generate_filter_and_project_testing_sparse_segment();
        sparse_floats_1 = input_segment.column_ptr(input_segment.column_index("sparse_floats_1").value());
        sparse_floats_2 = input_segment.column_ptr(input_segment.column_index("sparse_floats_2").value());
        dense_floats_1 = input_segment.column_ptr(input_segment.column_index("dense_floats_1").value());
        dense_floats_2 = input_segment.column_ptr(input_segment.column_index("dense_floats_2").value());
        sparse_bools = input_segment.column_ptr(input_segment.column_index("sparse_bools").value());
        proc_unit = ProcessingUnit(std::move(input_segment));
        proc_unit.set_expression_context(expression_context);
    }

    std::shared_ptr<Column> unary_projection(std::string_view input_column_name, OperationType op) {
        auto root = node(col(input_column_name), op);
        auto variant_data = root->compute(proc_unit);
        return std::get<ColumnWithStrings>(variant_data).column_;
    }

    util::BitSet unary_filter(std::string_view input_column_name, OperationType op) {
        auto root = node(col(input_column_name), op);
        auto variant_data = root->compute(proc_unit);
        return std::get<util::BitSet>(variant_data);
    }

    util::BitSet binary_filter(
            std::string_view left_column_name,
            const std::variant<std::string_view, double, std::unordered_set<double>>& right_input, OperationType op
    ) {
        ExpressionChild right;
        util::variant_match(
                right_input,
                [&](std::string_view right_column_name) { right = col(right_column_name); },
                [&](double value) { right = val(std::make_shared<Value>(value, DataType::FLOAT64)); },
                [&](std::unordered_set<double> value_set) {
                    right = vset(std::make_shared<ValueSet>(std::make_shared<std::unordered_set<double>>(value_set)));
                }
        );
        auto root = node(col(left_column_name), right, op);
        auto variant_data = root->compute(proc_unit);
        return std::get<util::BitSet>(variant_data);
    }

    std::shared_ptr<Column> binary_projection(
            std::string_view left_column_name, const std::variant<std::string_view, double>& right_input,
            OperationType op
    ) {
        ExpressionChild right;
        util::variant_match(
                right_input,
                [&](std::string_view right_column_name) { right = col(right_column_name); },
                [&](double value) { right = val(std::make_shared<Value>(value, DataType::FLOAT64)); }
        );
        auto root = node(col(left_column_name), right, op);
        auto variant_data = root->compute(proc_unit);
        return std::get<ColumnWithStrings>(variant_data).column_;
    }

    ProcessingUnit proc_unit;
    std::shared_ptr<ExpressionContext> expression_context{std::make_shared<ExpressionContext>()};
    std::shared_ptr<Column> sparse_floats_1;
    std::shared_ptr<Column> sparse_floats_2;
    std::shared_ptr<Column> dense_floats_1;
    std::shared_ptr<Column> dense_floats_2;
    std::shared_ptr<Column> sparse_bools;
};

TEST_F(FilterProjectSparse, UnaryProjection) {
    auto projected_column = unary_projection("sparse_floats_1", OperationType::NEG);

    ASSERT_EQ(sparse_floats_1->last_row(), projected_column->last_row());
    ASSERT_EQ(sparse_floats_1->row_count(), projected_column->row_count());
    ASSERT_EQ(sparse_floats_1->opt_sparse_map(), projected_column->opt_sparse_map());

    for (auto idx = 0; idx < sparse_floats_1->row_count(); idx++) {
        ASSERT_FLOAT_EQ(sparse_floats_1->reference_at<double>(idx), -projected_column->reference_at<double>(idx));
    }
}

TEST_F(FilterProjectSparse, BoolColumnIdentity) {
    auto bitset = unary_filter("sparse_bools", OperationType::IDENTITY);
    for (auto idx = 0; idx <= sparse_bools->last_row(); idx++) {
        auto opt_input_value = sparse_bools->scalar_at<bool>(idx);
        if (opt_input_value.has_value() && *opt_input_value) {
            ASSERT_TRUE(bitset.get_bit(idx));
        } else {
            ASSERT_FALSE(bitset.get_bit(idx));
        }
    }
}

TEST_F(FilterProjectSparse, BoolColumnNot) {
    auto bitset = unary_filter("sparse_bools", OperationType::NOT);
    for (auto idx = 0; idx <= sparse_bools->last_row(); idx++) {
        auto opt_input_value = sparse_bools->scalar_at<bool>(idx);
        if (opt_input_value.has_value() && *opt_input_value) {
            ASSERT_FALSE(bitset.get_bit(idx));
        } else {
            ASSERT_TRUE(bitset.get_bit(idx));
        }
    }
}

TEST_F(FilterProjectSparse, IsNull) {
    auto bitset = unary_filter("sparse_floats_2", OperationType::ISNULL);
    for (auto idx = 0; idx <= sparse_floats_2->last_row(); idx++) {
        auto opt_input_value = sparse_floats_2->scalar_at<double>(idx);
        if (opt_input_value.has_value() && !std::isnan(*opt_input_value)) {
            ASSERT_FALSE(bitset.get_bit(idx));
        } else {
            ASSERT_TRUE(bitset.get_bit(idx));
        }
    }
}

TEST_F(FilterProjectSparse, NotNull) {
    auto bitset = unary_filter("sparse_floats_2", OperationType::NOTNULL);
    for (auto idx = 0; idx <= sparse_floats_2->last_row(); idx++) {
        auto opt_input_value = sparse_floats_2->scalar_at<double>(idx);
        if (opt_input_value.has_value() && !std::isnan(*opt_input_value)) {
            ASSERT_TRUE(bitset.get_bit(idx));
        } else {
            ASSERT_FALSE(bitset.get_bit(idx));
        }
    }
}

TEST_F(FilterProjectSparse, BinaryComparisonIsIn) {
    auto bitset = binary_filter("sparse_floats_2", std::unordered_set<double>{5.0}, OperationType::ISIN);
    for (auto idx = 0; idx <= sparse_floats_2->last_row(); idx++) {
        auto opt_input_value = sparse_floats_2->scalar_at<double>(idx);
        if (opt_input_value.has_value() && *opt_input_value == double(5.0)) {
            ASSERT_TRUE(bitset.get_bit(idx));
        } else {
            ASSERT_FALSE(bitset.get_bit(idx));
        }
    }
}

TEST_F(FilterProjectSparse, BinaryComparisonIsNotIn) {
    auto bitset = binary_filter("sparse_floats_2", std::unordered_set<double>{5.0}, OperationType::ISNOTIN);
    for (auto idx = 0; idx <= sparse_floats_2->last_row(); idx++) {
        auto opt_input_value = sparse_floats_2->scalar_at<double>(idx);
        if (!opt_input_value.has_value() || *opt_input_value != double(5.0)) {
            ASSERT_TRUE(bitset.get_bit(idx));
        } else {
            ASSERT_FALSE(bitset.get_bit(idx));
        }
    }
}

TEST_F(FilterProjectSparse, BinaryComparisonColValEquals) {
    auto bitset = binary_filter("sparse_floats_2", double{5.0}, OperationType::EQ);
    for (auto idx = 0; idx <= sparse_floats_2->last_row(); idx++) {
        auto opt_input_value = sparse_floats_2->scalar_at<double>(idx);
        if (opt_input_value.has_value() && *opt_input_value == double(5.0)) {
            ASSERT_TRUE(bitset.get_bit(idx));
        } else {
            ASSERT_FALSE(bitset.get_bit(idx));
        }
    }
}

TEST_F(FilterProjectSparse, BinaryComparisonColValNotEqual) {
    auto bitset = binary_filter("sparse_floats_2", double{5.0}, OperationType::NE);
    for (auto idx = 0; idx <= sparse_floats_2->last_row(); idx++) {
        auto opt_input_value = sparse_floats_2->scalar_at<double>(idx);
        if (!opt_input_value.has_value() || *opt_input_value != double(5.0)) {
            ASSERT_TRUE(bitset.get_bit(idx));
        } else {
            ASSERT_FALSE(bitset.get_bit(idx));
        }
    }
}

TEST_F(FilterProjectSparse, BinaryComparisonSparseColSparseCol) {
    auto bitset = binary_filter("sparse_floats_1", std::string_view{"sparse_floats_2"}, OperationType::LT);
    for (auto idx = 0; idx <= sparse_floats_2->last_row(); idx++) {
        auto opt_left_value = sparse_floats_1->scalar_at<double>(idx);
        auto opt_right_value = sparse_floats_2->scalar_at<double>(idx);
        if (opt_left_value.has_value() && opt_right_value.has_value()) {
            ASSERT_EQ(*opt_left_value < *opt_right_value, bitset.get_bit(idx));
        } else {
            ASSERT_FALSE(bitset.get_bit(idx));
        }
    }
}

TEST_F(FilterProjectSparse, BinaryComparisonSparseColSparseColNotEqual) {
    auto bitset = binary_filter("sparse_floats_1", std::string_view{"sparse_floats_2"}, OperationType::NE);
    for (auto idx = 0; idx <= sparse_floats_2->last_row(); idx++) {
        auto opt_left_value = sparse_floats_1->scalar_at<double>(idx);
        auto opt_right_value = sparse_floats_2->scalar_at<double>(idx);
        if (opt_left_value.has_value() && opt_right_value.has_value()) {
            ASSERT_EQ(*opt_left_value != *opt_right_value, bitset.get_bit(idx));
        } else {
            ASSERT_TRUE(bitset.get_bit(idx));
        }
    }
}

TEST_F(FilterProjectSparse, BinaryComparisonDenseColDenseCol) {
    auto bitset = binary_filter("dense_floats_1", std::string_view{"dense_floats_2"}, OperationType::EQ);
    for (auto idx = 0; idx <= dense_floats_2->last_row(); idx++) {
        if (idx <= dense_floats_1->last_row()) {
            auto opt_left_value = dense_floats_1->scalar_at<double>(idx);
            auto opt_right_value = dense_floats_2->scalar_at<double>(idx);
            if (opt_left_value.has_value() && opt_right_value.has_value()) {
                ASSERT_EQ(*opt_left_value == *opt_right_value, bitset.get_bit(idx));
            } else {
                ASSERT_FALSE(bitset.get_bit(idx));
            }
        } else {
            ASSERT_FALSE(bitset.get_bit(idx));
        }
    }
}

TEST_F(FilterProjectSparse, BinaryComparisonDenseColDenseColNotEqual) {
    auto bitset = binary_filter("dense_floats_1", std::string_view{"dense_floats_2"}, OperationType::NE);
    for (auto idx = 0; idx <= dense_floats_2->last_row(); idx++) {
        auto opt_left_value = dense_floats_1->scalar_at<double>(idx);
        auto opt_right_value = dense_floats_2->scalar_at<double>(idx);
        if (opt_left_value.has_value() && opt_right_value.has_value()) {
            ASSERT_EQ(*opt_left_value != *opt_right_value, bitset.get_bit(idx));
        } else {
            ASSERT_TRUE(bitset.get_bit(idx));
        }
    }
}

TEST_F(FilterProjectSparse, BinaryComparisonSparseColShorterThanDenseCol) {
    auto bitset = binary_filter("sparse_floats_1", std::string_view{"dense_floats_1"}, OperationType::GT);
    for (auto idx = 0; idx <= dense_floats_1->last_row(); idx++) {
        if (idx <= sparse_floats_1->last_row()) {
            auto opt_left_value = sparse_floats_1->scalar_at<double>(idx);
            auto opt_right_value = dense_floats_1->scalar_at<double>(idx);
            if (opt_left_value.has_value() && opt_right_value.has_value()) {
                ASSERT_EQ(*opt_left_value > *opt_right_value, bitset.get_bit(idx));
            } else {
                ASSERT_FALSE(bitset.get_bit(idx));
            }
        } else {
            ASSERT_FALSE(bitset.get_bit(idx));
        }
    }
}

TEST_F(FilterProjectSparse, BinaryComparisonSparseColShorterThanDenseColNotEqual) {
    auto bitset = binary_filter("sparse_floats_1", std::string_view{"dense_floats_1"}, OperationType::NE);
    for (auto idx = 0; idx <= sparse_floats_1->last_row(); idx++) {
        auto opt_left_value = sparse_floats_1->scalar_at<double>(idx);
        auto opt_right_value = dense_floats_1->scalar_at<double>(idx);
        if (opt_left_value.has_value() && opt_right_value.has_value()) {
            ASSERT_EQ(*opt_left_value != *opt_right_value, bitset.get_bit(idx));
        } else {
            ASSERT_TRUE(bitset.get_bit(idx));
        }
    }
}

TEST_F(FilterProjectSparse, BinaryComparisonSparseColLongerThanDenseColNotEqual) {
    auto bitset = binary_filter("dense_floats_1", std::string_view{"sparse_floats_1"}, OperationType::NE);
    for (auto idx = 0; idx <= sparse_floats_1->last_row(); idx++) {
        auto opt_left_value = dense_floats_1->scalar_at<double>(idx);
        auto opt_right_value = sparse_floats_1->scalar_at<double>(idx);
        if (opt_left_value.has_value() && opt_right_value.has_value()) {
            ASSERT_EQ(*opt_left_value != *opt_right_value, bitset.get_bit(idx));
        } else {
            ASSERT_TRUE(bitset.get_bit(idx));
        }
    }
}

TEST_F(FilterProjectSparse, BinaryArithmeticColVal) {
    auto projected_column = binary_projection("sparse_floats_1", double{10.0}, OperationType::MUL);
    ASSERT_EQ(sparse_floats_1->last_row(), projected_column->last_row());
    ASSERT_EQ(sparse_floats_1->row_count(), projected_column->row_count());
    ASSERT_EQ(sparse_floats_1->opt_sparse_map(), projected_column->opt_sparse_map());
    for (auto idx = 0; idx < sparse_floats_1->row_count(); idx++) {
        ASSERT_FLOAT_EQ(10.0 * sparse_floats_1->reference_at<double>(idx), projected_column->reference_at<double>(idx));
    }
}

TEST_F(FilterProjectSparse, BinaryArithmeticSparseColSparseCol) {
    auto projected_column =
            binary_projection("sparse_floats_1", std::string_view{"sparse_floats_2"}, OperationType::MUL);
    ASSERT_TRUE(projected_column->opt_sparse_map().has_value());
    ASSERT_EQ(sparse_floats_1->sparse_map() & sparse_floats_2->sparse_map(), projected_column->sparse_map());
    ASSERT_EQ(projected_column->row_count(), projected_column->sparse_map().count());

    for (auto idx = 0; idx <= projected_column->last_row(); idx++) {
        auto opt_left_value = sparse_floats_1->scalar_at<double>(idx);
        auto opt_right_value = sparse_floats_2->scalar_at<double>(idx);
        auto opt_projected_value = projected_column->scalar_at<double>(idx);
        if (opt_left_value.has_value() && opt_right_value.has_value()) {
            ASSERT_TRUE(opt_projected_value.has_value());
            if (std::isnan(*opt_left_value * *opt_right_value)) {
                ASSERT_TRUE(std::isnan(*opt_projected_value));
            } else {
                ASSERT_FLOAT_EQ(*opt_left_value * *opt_right_value, *projected_column->scalar_at<double>(idx));
            }
        } else {
            ASSERT_FALSE(projected_column->has_value_at(idx));
        }
    }
}

TEST_F(FilterProjectSparse, BinaryArithmeticDenseColDenseCol) {
    auto projected_column = binary_projection("dense_floats_1", std::string_view{"dense_floats_2"}, OperationType::MUL);
    // dense_floats_1 has fewer values than dense_floats_2
    ASSERT_EQ(dense_floats_1->last_row(), projected_column->last_row());
    ASSERT_EQ(dense_floats_1->row_count(), projected_column->row_count());
    ASSERT_FALSE(projected_column->opt_sparse_map().has_value());

    for (auto idx = 0; idx < dense_floats_1->last_row(); idx++) {
        ASSERT_FLOAT_EQ(
                dense_floats_1->reference_at<double>(idx) * dense_floats_2->reference_at<double>(idx),
                projected_column->reference_at<double>(idx)
        );
    }
}

TEST_F(FilterProjectSparse, BinaryArithmeticSparseColShorterThanDenseCol) {
    auto projected_column =
            binary_projection("sparse_floats_1", std::string_view{"dense_floats_1"}, OperationType::MUL);
    ASSERT_TRUE(projected_column->opt_sparse_map().has_value());
    ASSERT_EQ(*sparse_floats_1->opt_sparse_map(), *projected_column->opt_sparse_map());
    ASSERT_EQ(projected_column->row_count(), projected_column->sparse_map().count());
    for (auto idx = 0; idx <= projected_column->last_row(); idx++) {
        auto opt_left_value = sparse_floats_1->scalar_at<double>(idx);
        auto opt_right_value = dense_floats_1->scalar_at<double>(idx);
        auto opt_projected_value = projected_column->scalar_at<double>(idx);
        if (opt_left_value.has_value() && opt_right_value.has_value()) {
            ASSERT_TRUE(opt_projected_value.has_value());
            if (std::isnan(*opt_left_value * *opt_right_value)) {
                ASSERT_TRUE(std::isnan(*opt_projected_value));
            } else {
                ASSERT_FLOAT_EQ(*opt_left_value * *opt_right_value, *projected_column->scalar_at<double>(idx));
            }
        } else {
            ASSERT_FALSE(projected_column->has_value_at(idx));
        }
    }
}
