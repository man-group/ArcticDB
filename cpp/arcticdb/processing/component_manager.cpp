/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/component_manager.hpp>

namespace arcticdb {

EntityId ComponentManager::entity_id(const std::optional<EntityId>& id) {
    // Do not use value_or as we do not want the side effect of fetch_add when id was provided by the caller
    return id.has_value() ? *id : next_entity_id_.fetch_add(1);
}

EntityId ComponentManager::add_segment_and_slice(folly::Future<SegmentAndSlice>&& segment_and_slice_fut, uint64_t expected_get_calls) {
    auto id = entity_id();
    folly::FutureSplitter future_splitter(std::move(segment_and_slice_fut));

    segment_map_.add(id,
    future_splitter.getFuture().thenValue([](auto &&segment_and_slice) {
        return std::make_shared<SegmentInMemory>(std::move(segment_and_slice.segment_in_memory_));
    }),
    expected_get_calls);
    row_range_map_.add(id,
    future_splitter.getFuture().thenValue([](auto &&segment_and_slice) {
        return std::make_shared<RowRange>(std::move(segment_and_slice.ranges_and_key_.row_range_));
    }));
    col_range_map_.add(id,
    future_splitter.getFuture().thenValue([](auto &&segment_and_slice) {
        return std::make_shared<ColRange>(std::move(segment_and_slice.ranges_and_key_.col_range_));
    }));
    atom_key_map_.add(id,
    future_splitter.getFuture().thenValue([](auto &&segment_and_slice) {
        return std::make_shared<AtomKey>(std::move(segment_and_slice.ranges_and_key_.key_));
    }));

    return id;
}

} // namespace arcticdb