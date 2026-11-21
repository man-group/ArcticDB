/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <algorithm>
#include <atomic>
#include <optional>
#include <unordered_map>

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
        // shared_mutex, so just decrement the ref count of the only sizeable components, so that when the shared
        // pointer goes out of scope in the calling function, the memory is freed
        if (auto seg = registry_.try_get<std::shared_ptr<SegmentInMemory>>(id)) {
            seg->reset();
            ARCTICDB_DEBUG_CHECK(
                    ErrorCode::E_ASSERTION_FAILURE,
                    !registry_.get<std::shared_ptr<SegmentInMemory>>(id),
                    "SegmentInMemory memory retained in ComponentManager"
            );
        }
        if (auto seg = registry_.try_get<std::shared_ptr<Segment>>(id)) {
            seg->reset();
            ARCTICDB_DEBUG_CHECK(
                    ErrorCode::E_ASSERTION_FAILURE,
                    !registry_.get<std::shared_ptr<Segment>>(id),
                    "Segment memory retained in ComponentManager"
            );
        }
        ARCTICDB_DEBUG(log::memory(), "Releasing entity {}", id);
    }
}

void ComponentManager::update_entity_fetch_count(EntityId id, EntityFetchCount count) {
    registry_.get<std::atomic<EntityFetchCount>>(id).store(count);
}

uint64_t ComponentManager::count_rows(const std::vector<EntityId>& ids) const {
    if (ids.empty()) {
        return 0;
    }
    std::shared_lock lock(mtx_);
    auto entity_row_count = [this](EntityId id) -> std::optional<uint64_t> {
        if (!registry_.valid(id)) {
            return std::nullopt;
        }
        if (const auto* mem_seg_ptr = registry_.try_get<std::shared_ptr<SegmentInMemory>>(id);
            mem_seg_ptr && *mem_seg_ptr && !(*mem_seg_ptr)->is_null()) {
            return static_cast<uint64_t>((*mem_seg_ptr)->row_count());
        }
        if (const auto* seg_ptr = registry_.try_get<std::shared_ptr<Segment>>(id); seg_ptr && *seg_ptr) {
            return static_cast<uint64_t>((*seg_ptr)->descriptor().row_count());
        }
        return std::nullopt;
    };

    if (ids.size() == 1) {
        return entity_row_count(ids[0]).value_or(0);
    }

    uint64_t unkeyed_total = 0;
    std::unordered_map<pipelines::RowRange, uint64_t, pipelines::RowRange::Hasher> rows_by_range;
    rows_by_range.reserve(ids.size());
    for (const auto id : ids) {
        const auto rows = entity_row_count(id);
        if (!rows.has_value()) {
            continue;
        }
        if (const auto* row_range_ptr = registry_.try_get<std::shared_ptr<pipelines::RowRange>>(id);
            row_range_ptr && *row_range_ptr) {
            auto [it, inserted] = rows_by_range.emplace(**row_range_ptr, *rows);
            if (!inserted) {
                it->second = std::max(it->second, *rows);
            }
        } else {
            unkeyed_total += *rows;
        }
    }

    uint64_t total = unkeyed_total;
    for (const auto& entry : rows_by_range) {
        total += entry.second;
    }
    return total;
}

} // namespace arcticdb