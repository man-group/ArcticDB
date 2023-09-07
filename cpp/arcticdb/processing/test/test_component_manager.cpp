/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <folly/executors/ThreadedExecutor.h>
#include <gtest/gtest.h>

#include <arcticdb/processing/component_manager.hpp>

using namespace arcticdb;
using namespace arcticdb::pipelines;

TEST(ComponentManager, Synchronous) {
    ComponentManager component_manager;
    auto segment_0 = std::make_shared<SegmentInMemory>();
    auto row_range_0 = std::make_shared<RowRange>(0, 10);
    auto col_range_0 = std::make_shared<ColRange>(10, 20);
    auto key_0 = std::make_shared<AtomKey>(AtomKeyBuilder().version_id(1).build("symbol_0", KeyType::TABLE_DATA));
    uint64_t expected_get_calls_0{1};

    auto segment_1 = std::make_shared<SegmentInMemory>();
    auto row_range_1 = std::make_shared<RowRange>(20, 30);
    auto col_range_1 = std::make_shared<ColRange>(30, 40);
    auto key_1 = std::make_shared<AtomKey>(AtomKeyBuilder().version_id(2).build("symbol_1", KeyType::TABLE_DATA));
    uint64_t expected_get_calls_1{2};

    auto id_0 = component_manager.add(segment_0, std::nullopt, expected_get_calls_0);
    ASSERT_EQ(component_manager.add(row_range_0, id_0), id_0);
    ASSERT_EQ(component_manager.add(col_range_0, id_0), id_0);
    ASSERT_EQ(component_manager.add(key_0, id_0), id_0);

    auto id_1 = component_manager.add(segment_1, std::nullopt, expected_get_calls_1);
    ASSERT_EQ(component_manager.add(row_range_1, id_1), id_1);
    ASSERT_EQ(component_manager.add(col_range_1, id_1), id_1);
    ASSERT_EQ(component_manager.add(key_1, id_1), id_1);

    ASSERT_EQ(id_0, 0);
    ASSERT_EQ(id_1, 1);

    ASSERT_EQ(component_manager.get<std::shared_ptr<SegmentInMemory>>(id_0).get(), segment_0);
    ASSERT_EQ(component_manager.get<std::shared_ptr<RowRange>>(id_0).get(), row_range_0);
    ASSERT_EQ(component_manager.get<std::shared_ptr<ColRange>>(id_0).get(), col_range_0);
    ASSERT_EQ(component_manager.get<std::shared_ptr<AtomKey>>(id_0).get(), key_0);
    EXPECT_THROW(component_manager.get<std::shared_ptr<SegmentInMemory>>(id_0), InternalException);

    ASSERT_EQ(component_manager.get<std::shared_ptr<SegmentInMemory>>(id_1).get(), segment_1);
    ASSERT_EQ(component_manager.get<std::shared_ptr<SegmentInMemory>>(id_1).get(), segment_1);
    ASSERT_EQ(component_manager.get<std::shared_ptr<RowRange>>(id_1).get(), row_range_1);
    ASSERT_EQ(component_manager.get<std::shared_ptr<ColRange>>(id_1).get(), col_range_1);
    ASSERT_EQ(component_manager.get<std::shared_ptr<AtomKey>>(id_1).get(), key_1);
    EXPECT_THROW(component_manager.get<std::shared_ptr<SegmentInMemory>>(id_1), InternalException);
}

TEST(ComponentManager, Asynchronous) {
    ComponentManager component_manager;
    folly::ThreadedExecutor executor;
    auto [promise_0, future_0] = folly::makePromiseContract<SegmentAndSlice>(&executor);
    auto [promise_1, future_1] = folly::makePromiseContract<SegmentAndSlice>(&executor);

    RowRange row_range_0(0, 10);
    ColRange col_range_0(10, 20);
    auto key_0 = AtomKeyBuilder().version_id(1).build("symbol_0", KeyType::TABLE_DATA);
    SegmentAndSlice segment_and_slice_0(RangesAndKey(row_range_0, col_range_0, key_0), SegmentInMemory());

    RowRange row_range_1(20, 30);
    ColRange col_range_1(30, 40);
    auto key_1 = AtomKeyBuilder().version_id(2).build("symbol_1", KeyType::TABLE_DATA);
    SegmentAndSlice segment_and_slice_1(RangesAndKey(row_range_1, col_range_1, key_1), SegmentInMemory());
    uint64_t expected_get_calls_1{2};

    auto id_0 = component_manager.add_segment_and_slice(std::move(future_0));
    auto id_1 = component_manager.add_segment_and_slice(std::move(future_1), expected_get_calls_1);
    ASSERT_EQ(id_0, 0);
    ASSERT_EQ(id_1, 1);

    promise_0.setValue(segment_and_slice_0);
    ASSERT_EQ(*component_manager.get<std::shared_ptr<RowRange>>(id_0).get(), row_range_0);
    ASSERT_EQ(*component_manager.get<std::shared_ptr<ColRange>>(id_0).get(), col_range_0);
    ASSERT_EQ(*component_manager.get<std::shared_ptr<AtomKey>>(id_0).get(), key_0);
    component_manager.get<std::shared_ptr<SegmentInMemory>>(id_0).get();
    EXPECT_THROW(component_manager.get<std::shared_ptr<SegmentInMemory>>(id_0).get(), InternalException);

    auto seg_1_fut_1 = component_manager.get<std::shared_ptr<SegmentInMemory>>(id_1);
    auto seg_1_fut_2 = component_manager.get<std::shared_ptr<SegmentInMemory>>(id_1);
    ASSERT_FALSE(seg_1_fut_1.isReady());
    ASSERT_FALSE(seg_1_fut_2.isReady());
    promise_1.setValue(segment_and_slice_1);
    ASSERT_EQ(*component_manager.get<std::shared_ptr<RowRange>>(id_1).get(), row_range_1);
    ASSERT_EQ(*component_manager.get<std::shared_ptr<ColRange>>(id_1).get(), col_range_1);
    ASSERT_EQ(*component_manager.get<std::shared_ptr<AtomKey>>(id_1).get(), key_1);
    EXPECT_THROW(component_manager.get<std::shared_ptr<SegmentInMemory>>(id_1).get(), InternalException);
    ASSERT_TRUE(seg_1_fut_1.isReady());
    ASSERT_TRUE(seg_1_fut_2.isReady());
    std::move(seg_1_fut_1).get();
    std::move(seg_1_fut_2).get();
}




