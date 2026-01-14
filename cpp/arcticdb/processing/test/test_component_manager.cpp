/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
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
    uint64_t entity_fetch_count_0{1};

    auto segment_1 = std::make_shared<SegmentInMemory>();
    auto row_range_1 = std::make_shared<RowRange>(20, 30);
    auto col_range_1 = std::make_shared<ColRange>(30, 40);
    auto key_1 = std::make_shared<AtomKey>(AtomKeyBuilder().version_id(2).build("symbol_1", KeyType::TABLE_DATA));
    uint64_t entity_fetch_count_1{2};

    auto ids = component_manager.get_new_entity_ids(2);
    component_manager.add_entity(ids[0], segment_0, row_range_0, col_range_0, key_0, entity_fetch_count_0);

    component_manager.add_entity(ids[1], segment_1, row_range_1, col_range_1, key_1, entity_fetch_count_1);

    auto [segments, row_ranges, col_ranges, keys, entity_fetch_counts] =
            component_manager.get_entities_and_decrement_refcount<
                    std::shared_ptr<SegmentInMemory>,
                    std::shared_ptr<RowRange>,
                    std::shared_ptr<ColRange>,
                    std::shared_ptr<AtomKey>,
                    EntityFetchCount>(ids);

    ASSERT_EQ(segments[0], segment_0);
    ASSERT_EQ(row_ranges[0], row_range_0);
    ASSERT_EQ(col_ranges[0], col_range_0);
    ASSERT_EQ(keys[0], key_0);
    ASSERT_EQ(entity_fetch_counts[0], entity_fetch_count_0);

    ASSERT_EQ(segments[1], segment_1);
    ASSERT_EQ(row_ranges[1], row_range_1);
    ASSERT_EQ(col_ranges[1], col_range_1);
    ASSERT_EQ(keys[1], key_1);
    ASSERT_EQ(entity_fetch_counts[1], entity_fetch_count_1);

    // EntityFetchCount for entity with id_1 is 2, so can be fetched again without exceptions
    component_manager.get_entities_and_decrement_refcount<
            std::shared_ptr<SegmentInMemory>,
            std::shared_ptr<RowRange>,
            std::shared_ptr<ColRange>,
            std::shared_ptr<AtomKey>,
            EntityFetchCount>({ids[1]});
}
