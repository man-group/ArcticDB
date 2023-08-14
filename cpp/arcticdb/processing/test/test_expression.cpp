/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/util/test/generators.hpp>

TEST(ExpressionNode, AddBasic) {
    using namespace arcticdb;
    StreamId symbol("test_add");
    auto wrapper = SinkWrapper(symbol, {
            scalar_field(DataType::UINT64, "thing1"),
            scalar_field(DataType::UINT64, "thing2")
    });

    for(auto j = 0; j < 20; ++j ) {
        wrapper.aggregator_.start_row(timestamp(j))([&](auto &&rb) {
            rb.set_scalar(1, j);
            rb.set_scalar(2, j + 1);
        });
    }

    wrapper.aggregator_.commit();
    auto& seg = wrapper.segment();
    ProcessingUnit proc(std::move(seg));
    auto node = std::make_shared<ExpressionNode>(ColumnName("thing1"), ColumnName("thing2"), OperationType::ADD);
    auto expression_context = std::make_shared<ExpressionContext>();
    expression_context->root_node_name_ = ExpressionName("new_thing");
    expression_context->add_expression_node("new_thing", node);
    proc.set_expression_context(expression_context);
    std::shared_ptr<Store> empty;
    auto ret = proc.get(ExpressionName("new_thing"), empty);
    const auto& col = std::get<ColumnWithStrings>(ret).column_;

    for(auto j = 0; j < 20; ++j ) {
        auto v1 =proc.data_[0].segment(empty).scalar_at<uint64_t>(j, 1) ;
        ASSERT_EQ(v1.value(), j);
        auto v2 = proc.data_[0].segment(empty).scalar_at<uint64_t>(j, 2);
        ASSERT_EQ(v2.value(), j + 1);
        ASSERT_EQ(col->scalar_at<uint64_t>(j), v1.value() + v2.value());
    }
}
