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
    std::lock_guard<std::mutex> lock(mtx_);
    registry_.create(ids.begin(), ids.end());
    return ids;
}

void ComponentManager::erase_entity(EntityId id) {
    // Ideally would call registry_.destroy(id), or at least registry_.erase<std::shared_ptr<SegmentInMemory>>(id)
    // at this point. However, they are both slower than this, so just decrement the ref count of the only
    // sizeable component, so that when the shared pointer goes out of scope in the calling function, the
    // memory is freed
    registry_.get<std::shared_ptr<SegmentInMemory>>(id).reset();
}


} // namespace arcticdb