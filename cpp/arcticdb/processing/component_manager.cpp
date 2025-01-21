/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */


#include <atomic>

#include <arcticdb/processing/component_manager.hpp>

namespace arcticdb {

std::vector<EntityId> ComponentManager::get_new_entity_ids(size_t count) {
    std::vector<EntityId> ids(count);
    std::unique_lock lock(mtx_);
    registry_.create(ids.begin(), ids.end());
    return ids;
}

void ComponentManager::decrement_entity_fetch_count(EntityId id) {
    if (registry_.get<std::atomic<EntityFetchCount>>(id).fetch_sub(1) == 1) {
        // This entity will never be accessed again
        // Ideally would call registry_.destroy(id), or at least registry_.erase<std::shared_ptr<SegmentInMemory>>(id)
        // at this point. However, they are both slower than this, and would require taking a unique_lock on the
        // shared_mutex, so just decrement the ref count of the only sizeable component, so that when the shared pointer
        // goes out of scope in the calling function, the memory is freed
        registry_.get<std::shared_ptr<SegmentInMemory>>(id).reset();
        ARCTICDB_DEBUG(log::memory(), "Releasing entity {}", id);
        debug::check<ErrorCode::E_ASSERTION_FAILURE>(!registry_.get<std::shared_ptr<SegmentInMemory>>(id),
                                                     "SegmentInMemory memory retained in ComponentManager");
    }
}

void ComponentManager::update_entity_fetch_count(EntityId id, EntityFetchCount count) {
    registry_.get<std::atomic<EntityFetchCount>>(id).store(count);
}


} // namespace arcticdb