/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <atomic>
#include <shared_mutex>

#include <entt/entity/registry.hpp>

#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/util/constructors.hpp>
#include <folly/container/Enumerate.h>

namespace arcticdb {

using namespace pipelines;
using EntityId = entt::entity;
using EntityFetchCount = uint64_t;
using bucket_id = uint8_t;

using namespace entt::literals;

class ComponentManager {
  public:
    ComponentManager() = default;
    ARCTICDB_NO_MOVE_OR_COPY(ComponentManager)

    std::vector<EntityId> get_new_entity_ids(size_t count);

    // Add a single entity with the components defined by args
    template<class... Args>
    void add_entity(EntityId id, Args... args) {
        std::unique_lock lock(mtx_);
        (
                [&] {
                    registry_.emplace<Args>(id, args);
                    // Store the initial entity fetch count component as a "first-class" entity, accessible by
                    // registry_.get<EntityFetchCount>(id), as this is external facing (used by resample)
                    // The remaining entity fetch count below will be decremented each time an entity is fetched, but is
                    // never accessed externally. Stored as an atomic to minimise the requirement to take the
                    // shared_mutex with a unique_lock.
                    if constexpr (std::is_same_v<Args, EntityFetchCount>) {
                        registry_.emplace<std::atomic<EntityFetchCount>>(id, args);
                    }
                }(),
                ...
        );
    }

    // Add a collection of entities. Each element of args should be a collection of components, all of which have the
    // same number of elements
    template<class... Args>
    std::vector<EntityId> add_entities(Args... args) {
        std::vector<EntityId> ids;
        size_t entity_count{0};
        ARCTICDB_SAMPLE_DEFAULT(AddEntities)
        std::unique_lock lock(mtx_);
        (
                [&] {
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
                        for (auto&& [idx, id] : folly::enumerate(ids)) {
                            registry_.emplace<std::atomic<EntityFetchCount>>(id, args[idx]);
                        }
                    }
                }(),
                ...
        );
        return ids;
    }

    template<typename T>
    void replace_entities(const std::vector<EntityId>& ids, T value) {
        ARCTICDB_SAMPLE_DEFAULT(ReplaceEntities)
        std::unique_lock lock(mtx_);
        for (auto id : ids) {
            registry_.replace<T>(id, value);
            if constexpr (std::is_same_v<T, EntityFetchCount>) {
                update_entity_fetch_count(id, value);
            }
        }
    }

    template<typename T>
    void replace_entities(const std::vector<EntityId>& ids, const std::vector<T>& values) {
        ARCTICDB_SAMPLE_DEFAULT(ReplaceEntityValues)
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                ids.size() == values.size(),
                "Received vectors of differing lengths in ComponentManager::replace_entities"
        );
        std::unique_lock lock(mtx_);
        for (auto [idx, id] : folly::enumerate(ids)) {
            registry_.replace<T>(id, values[idx]);
            if constexpr (std::is_same_v<T, EntityFetchCount>) {
                update_entity_fetch_count(id, values[idx]);
            }
        }
    }

    static_assert(sizeof(entt::entity) == sizeof(uint64_t));

    // Get a collection of entities. Returns a tuple of vectors, one for each component requested via Args
    template<class... Args>
    std::tuple<std::vector<Args>...> get_entities_and_decrement_refcount(const std::vector<EntityId>& ids) {
        return get_entities_impl<Args...>(ids, true);
    }

    // Get a collection of entities. Returns a tuple of vectors, one for each component requested via Args
    template<class... Args>
    std::tuple<std::vector<Args>...> get_entities(const std::vector<EntityId>& ids) {
        return get_entities_impl<Args...>(ids, false);
    }

  private:
    void decrement_entity_fetch_count(EntityId id);
    void update_entity_fetch_count(EntityId id, EntityFetchCount count);

    template<class... Args>
    std::tuple<std::vector<Args>...> get_entities_impl(const std::vector<EntityId>& ids, bool decrement_ref_count) {
        std::vector<std::tuple<Args...>> tuple_res;
        ARCTICDB_SAMPLE_DEFAULT(GetEntities)
        tuple_res.reserve(ids.size());
        {
            std::shared_lock lock{mtx_};
            // Using view.get theoretically and empirically faster than registry_.get
            auto view = registry_.view<const Args...>();

            for (auto id : ids) {
                tuple_res.emplace_back(std::move(view.get(id)));
            }

            if (decrement_ref_count) {
                for (auto id : ids) {
                    decrement_entity_fetch_count(id);
                }
            }
        }
        // Convert vector of tuples into tuple of vectors
        std::tuple<std::vector<Args>...> res;
        ([&] { std::get<std::vector<Args>>(res).reserve(ids.size()); }(), ...);
        for (auto&& tuple : tuple_res) {
            ([&] { std::get<std::vector<Args>>(res).emplace_back(std::move(std::get<Args>(tuple))); }(), ...);
        }
        return res;
    }

    entt::registry registry_;
    std::shared_mutex mtx_;
};

} // namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::EntityId> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::EntityId& id, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", static_cast<uint64_t>(id));
    }
};

} // namespace fmt