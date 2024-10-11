/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <atomic>

#include <entt/entity/registry.hpp>

#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/util/constructors.hpp>

namespace arcticdb {

using namespace pipelines;
using EntityId = entt::entity;
using EntityFetchCount = uint64_t;
using bucket_id = uint8_t;

using namespace entt::literals;

constexpr auto remaining_entity_fetch_count_id = "remaining_entity_fetch_count"_hs;

class ComponentManager {
public:
    ComponentManager() = default;
    ARCTICDB_NO_MOVE_OR_COPY(ComponentManager)

    std::vector<EntityId> get_new_entity_ids(size_t count);

    // Add a single entity with the components defined by args
    template<class... Args>
    void add_entity(EntityId id, Args... args) {
        std::lock_guard<std::mutex> lock(mtx_);
        ([&]{
            registry_.emplace<Args>(id, args);
            // Store the initial entity fetch count component as a "first-class" entity, accessible by
            // registry_.get<EntityFetchCount>(id), as this is external facing (used by resample)
            // The remaining entity fetch count below will be decremented each time an entity is fetched, but is never
            // accessed externally, so make this a named component.
            if constexpr (std::is_same_v<Args, EntityFetchCount>) {
                auto&& remaining_entity_fetch_count_registry = registry_.storage<EntityFetchCount>(remaining_entity_fetch_count_id);
                remaining_entity_fetch_count_registry.emplace(id, args);
            }
        }(), ...);
    }

    // Add a collection of entities. Each element of args should be a collection of components, all of which have the
    // same number of elements
    template<class... Args>
    std::vector<EntityId> add_entities(Args... args) {
        std::vector<EntityId> ids;
        size_t entity_count{0};
        std::lock_guard<std::mutex> lock(mtx_);
        ([&]{
            if (entity_count == 0) {
                // Reserve memory for the result on the first pass
                entity_count = args.size();
                ids.resize(entity_count);
                registry_.create(ids.begin(), ids.end());
            } else {
                internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                        args.size() == entity_count,
                        "ComponentManager::add_entities received collections of differing lengths"
                        );
            }
            registry_.insert<typename Args::value_type>(ids.cbegin(), ids.cend(), args.begin());
            if constexpr (std::is_same_v<typename Args::value_type, EntityFetchCount>) {
                auto&& remaining_entity_fetch_count_registry = registry_.storage<EntityFetchCount>(remaining_entity_fetch_count_id);
                remaining_entity_fetch_count_registry.insert(ids.cbegin(), ids.cend(), args.begin());
            }
        }(), ...);
        return ids;
    }

    // Get a collection of entities. Returns a tuple of vectors, one for each component requested via Args
    template<class... Args>
    std::tuple<std::vector<Args>...> get_entities(const std::vector<EntityId>& ids) {
        std::vector<std::tuple<Args...>> tuple_res;
        tuple_res.reserve(ids.size());
        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto&& remaining_entity_fetch_count_registry = registry_.storage<EntityFetchCount>(remaining_entity_fetch_count_id);
            // Using view.get theoretically and empirically faster than registry_.get
            auto view = registry_.view<const Args...>();
            for (auto id: ids) {
                tuple_res.emplace_back(std::move(view.get(id)));
                auto& remaining_entity_fetch_count = remaining_entity_fetch_count_registry.get(id);
                // This entity will never be accessed again
                if (--remaining_entity_fetch_count == 0) {
                    erase_entity(id);
                }
            }
        }
        // Convert vector of tuples into tuple of vectors
        std::tuple<std::vector<Args>...> res;
        ([&]{
            std::get<std::vector<Args>>(res).reserve(ids.size());
        }(), ...);
        for (auto&& tuple: tuple_res) {
            ([&] {
                std::get<std::vector<Args>>(res).emplace_back(std::move(std::get<Args>(tuple)));
            }(), ...);
        }
        return res;
    }

private:
    void erase_entity(EntityId id);

    entt::registry registry_;
    std::mutex mtx_;
};

} // namespace arcticdb