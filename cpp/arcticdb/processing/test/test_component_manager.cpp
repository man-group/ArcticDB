/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/processing/component_manager.hpp>

using namespace arcticdb;
using namespace arcticdb::pipelines;

TEST(ComponentManager, Simple) {
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

    ASSERT_EQ(component_manager.get<std::shared_ptr<SegmentInMemory>>({id_0})[0], segment_0);
    ASSERT_EQ(component_manager.get_initial_expected_get_calls<std::shared_ptr<SegmentInMemory>>(id_0), expected_get_calls_0);
    ASSERT_EQ(component_manager.get<std::shared_ptr<RowRange>>({id_0})[0], row_range_0);
    ASSERT_EQ(component_manager.get<std::shared_ptr<ColRange>>({id_0})[0], col_range_0);
    ASSERT_EQ(component_manager.get<std::shared_ptr<AtomKey>>({id_0})[0], key_0);
    EXPECT_THROW(component_manager.get<std::shared_ptr<SegmentInMemory>>({id_0}), InternalException);

    ASSERT_EQ(component_manager.get<std::shared_ptr<SegmentInMemory>>({id_1})[0], segment_1);
    ASSERT_EQ(component_manager.get<std::shared_ptr<SegmentInMemory>>({id_1})[0], segment_1);
    ASSERT_EQ(component_manager.get_initial_expected_get_calls<std::shared_ptr<SegmentInMemory>>(id_1), expected_get_calls_1);
    ASSERT_EQ(component_manager.get<std::shared_ptr<RowRange>>({id_1})[0], row_range_1);
    ASSERT_EQ(component_manager.get<std::shared_ptr<ColRange>>({id_1})[0], col_range_1);
    ASSERT_EQ(component_manager.get<std::shared_ptr<AtomKey>>({id_1})[0], key_1);
    EXPECT_THROW(component_manager.get<std::shared_ptr<SegmentInMemory>>({id_1}), InternalException);
}
