/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/processing/test/ast_test_helpers.hpp>
#include <arcticdb/pipeline/value_set.hpp>
#include <arcticdb/util/test/generators.hpp>

#include <unordered_set>

TEST(ExpressionNode, AddBasic) {
    using namespace arcticdb;
    StreamId symbol("test_add");
    auto wrapper =
            SinkWrapper(symbol, {scalar_field(DataType::UINT64, "thing1"), scalar_field(DataType::UINT64, "thing2")});

    for (auto j = 0; j < 20; ++j) {
        wrapper.aggregator_.start_row(timestamp(j))([&](auto&& rb) {
            rb.set_scalar(1, j);
            rb.set_scalar(2, j + 1);
        });
    }

    wrapper.aggregator_.commit();
    auto& seg = wrapper.segment();
    ProcessingUnit proc(std::move(seg));
    auto root = node(col("thing1"), col("thing2"), OperationType::ADD);
    auto expression_context = std::make_shared<ExpressionContext>();
    expression_context->root_ = root;
    proc.set_expression_context(expression_context);
    auto ret = root->compute(proc);
    const auto& result_col = std::get<ColumnWithStrings>(ret).column_;

    for (auto j = 0; j < 20; ++j) {
        auto v1 = proc.segments_->at(0)->scalar_at<uint64_t>(j, 1);
        ASSERT_EQ(v1.value(), j);
        auto v2 = proc.segments_->at(0)->scalar_at<uint64_t>(j, 2);
        ASSERT_EQ(v2.value(), j + 1);
        ASSERT_EQ(result_col->scalar_at<uint64_t>(j), v1.value() + v2.value());
    }
}

TEST(ExpressionNode, SubexpressionMemoized) {
    using namespace arcticdb;
    StreamId symbol("test_longhand");
    auto wrapper =
            SinkWrapper(symbol, {scalar_field(DataType::UINT64, "thing1"), scalar_field(DataType::UINT64, "thing2")});

    for (auto j = 0; j < 20; ++j) {
        wrapper.aggregator_.start_row(timestamp(j))([&](auto&& rb) {
            rb.set_scalar(1, j);
            rb.set_scalar(2, j + 1);
        });
    }

    wrapper.aggregator_.commit();
    auto& seg = wrapper.segment();
    ProcessingUnit proc(std::move(seg));

    auto add1 = node(col("thing1"), col("thing2"), OperationType::ADD);
    auto add2 = node(col("thing1"), col("thing2"), OperationType::ADD);
    auto root = node(add1, add2, OperationType::MUL);

    auto expression_context = std::make_shared<ExpressionContext>();
    expression_context->root_ = root;
    proc.set_expression_context(expression_context);

    auto ret = root->compute(proc);
    const auto& result_col = std::get<ColumnWithStrings>(ret).column_;

    for (auto j = 0; j < 20; ++j) {
        auto v1 = proc.segments_->at(0)->scalar_at<uint64_t>(j, 1).value();
        auto v2 = proc.segments_->at(0)->scalar_at<uint64_t>(j, 2).value();
        ASSERT_EQ(result_col->scalar_at<uint64_t>(j), (v1 + v2) * (v1 + v2));
    }

    const std::string add_label = R"((Column["thing1"] ADD Column["thing2"]))";
    ASSERT_EQ(add1->label_, add_label);
    ASSERT_EQ(add2->label_, add_label);
    ASSERT_EQ(proc.computed_data_.count(add_label), 1);
    const auto* cached_node = proc.computed_data_.at(add_label).first;
    ASSERT_TRUE(cached_node == add1.get() || cached_node == add2.get());
    ASSERT_EQ(proc.computed_data_.size(), 2);
}

TEST(ExpressionNode, NoFalseReuseOnLabelClash) {
    using namespace arcticdb;
    StreamId symbol("test_label_clash");
    auto wrapper = SinkWrapper(symbol, {scalar_field(DataType::INT64, "thing1")});

    for (auto j = 0; j < 20; ++j) {
        wrapper.aggregator_.start_row(timestamp(j))([&](auto&& rb) { rb.set_scalar(1, static_cast<int64_t>(j)); });
    }

    wrapper.aggregator_.commit();
    auto& seg = wrapper.segment();
    ProcessingUnit proc(std::move(seg));

    auto set_a = std::make_shared<ValueSet>(
            std::make_shared<std::unordered_set<int64_t>>(std::unordered_set<int64_t>{0, 1, 2})
    );
    auto set_b = std::make_shared<ValueSet>(
            std::make_shared<std::unordered_set<int64_t>>(std::unordered_set<int64_t>{3, 4, 5})
    );

    auto isin_a = node(col("thing1"), vset(set_a), OperationType::ISIN);
    auto isin_b = node(col("thing1"), vset(set_b), OperationType::ISIN);

    auto expression_context = std::make_shared<ExpressionContext>();
    expression_context->root_ = isin_a;
    proc.set_expression_context(expression_context);

    // The coarse value-set label keys only on dtype and size, so these two operations collide.
    ASSERT_EQ(isin_a->label_, isin_b->label_);

    auto bitset_a = std::get<util::BitSet>(isin_a->compute(proc));
    auto bitset_b = std::get<util::BitSet>(isin_b->compute(proc));

    for (size_t idx = 0; idx < 20; ++idx) {
        ASSERT_EQ(set_a->get_set<int64_t>()->count(static_cast<int64_t>(idx)) > 0, bitset_a.get_bit(idx));
        ASSERT_EQ(set_b->get_set<int64_t>()->count(static_cast<int64_t>(idx)) > 0, bitset_b.get_bit(idx));
    }
}
