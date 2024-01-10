/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <atomic>
#include <unordered_map>

#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/util/constructors.hpp>

namespace arcticdb {

using namespace pipelines;
using EntityId = uint64_t;
using bucket_id = size_t;

class ComponentManager {
public:
    ComponentManager() = default;
    ARCTICDB_NO_MOVE_OR_COPY(ComponentManager)

    void set_next_entity_id(EntityId id);

    template<typename T>
    EntityId add(T component, std::optional<EntityId> id=std::nullopt, std::optional<uint64_t> expected_get_calls=std::nullopt) {
        auto insertion_id = entity_id(id);
        if constexpr(std::is_same_v<T, std::shared_ptr<SegmentInMemory>>) {
            segment_map_.add(insertion_id, std::move(component), expected_get_calls);
        } else if constexpr(std::is_same_v<T, std::shared_ptr<RowRange>>) {
            row_range_map_.add(insertion_id, std::move(component));
        } else if constexpr(std::is_same_v<T, std::shared_ptr<ColRange>>) {
            col_range_map_.add(insertion_id, std::move(component));
        } else if constexpr(std::is_same_v<T, std::shared_ptr<AtomKey>>) {
            atom_key_map_.add(insertion_id, std::move(component));
        } else if constexpr(std::is_same_v<T, bucket_id>) {
            bucket_map_.add(insertion_id, std::move(component));
        } else {
            // Hacky workaround for static_assert(false) not being allowed
            // See https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2022/p2593r0.html
            static_assert(sizeof(T) == 0, "Unsupported component type passed to ComponentManager::add");
        }
        return insertion_id;
    }

    template<typename T>
    T get(EntityId id) {
        if constexpr(std::is_same_v<T, std::shared_ptr<SegmentInMemory>>) {
            return segment_map_.get(id);
        } else if constexpr(std::is_same_v<T, std::shared_ptr<RowRange>>) {
            return row_range_map_.get(id);
        } else if constexpr(std::is_same_v<T, std::shared_ptr<ColRange>>) {
            return col_range_map_.get(id);
        } else if constexpr(std::is_same_v<T, std::shared_ptr<AtomKey>>) {
            return atom_key_map_.get(id);
        } else if constexpr(std::is_same_v<T, bucket_id>) {
            return bucket_map_.get(id);
        } else {
            // Hacky workaround for static_assert(false) not being allowed
            // See https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2022/p2593r0.html
            static_assert(sizeof(T) == 0, "Unsupported component type passed to ComponentManager::get");
        }
    }

private:
    template<typename T>
    class ComponentMap {
    public:
        explicit ComponentMap(std::string&& entity_type, bool track_expected_gets):
                entity_type_(std::move(entity_type)),
                opt_expected_get_calls_map_(track_expected_gets ? std::make_optional<std::unordered_map<EntityId, uint64_t>>() : std::nullopt){
        };
        ARCTICDB_NO_MOVE_OR_COPY(ComponentMap)

        void add(EntityId id, T&& entity, std::optional<uint64_t> expected_get_calls=std::nullopt) {
            std::lock_guard <std::mutex> lock(mtx_);
            ARCTICDB_DEBUG(log::storage(), "Adding {} with id {}", entity_type_, id);
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(map_.try_emplace(id, std::move(entity)).second,
                                                            "Failed to insert {} with ID {}, already exists",
                                                            entity_type_, id);
            if (opt_expected_get_calls_map_.has_value()) {
                internal::check<ErrorCode::E_ASSERTION_FAILURE>(expected_get_calls.has_value() && *expected_get_calls > 0,
                                                                "Failed to insert {} with ID {}, must provide expected gets",
                                                                entity_type_, id);
                internal::check<ErrorCode::E_ASSERTION_FAILURE>(opt_expected_get_calls_map_->try_emplace(id, *expected_get_calls).second,
                                                                "Failed to insert {} with ID {}, already exists",
                                                                entity_type_, id);
            }
        }
        T get(EntityId id) {
            std::lock_guard <std::mutex> lock(mtx_);
            ARCTICDB_DEBUG(log::storage(), "Getting {} with id {}", entity_type_, id);
            auto entity_it = map_.find(id);
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(entity_it != map_.end(),
                                                            "Requested non-existent {} with ID {}",
                                                            entity_type_, id);
            auto res = entity_it->second;
            if (opt_expected_get_calls_map_.has_value()) {
                auto expected_get_calls_it = opt_expected_get_calls_map_->find(id);
                internal::check<ErrorCode::E_ASSERTION_FAILURE>(expected_get_calls_it != opt_expected_get_calls_map_->end(),
                                                                "Requested non-existent {} with ID {}",
                                                                entity_type_, id);
                if (--expected_get_calls_it->second == 0) {
                    ARCTICDB_DEBUG(log::storage(),
                                   "{} with id {} has been fetched the expected number of times, erasing from component manager",
                                   entity_type_, id);
                    map_.erase(entity_it);
                    opt_expected_get_calls_map_->erase(expected_get_calls_it);
                }
            }
            return res;
        }
    private:
        // Just used for logging/exception messages
        std::string entity_type_;
        std::unordered_map<EntityId, T> map_;
        // If not nullopt, tracks the number of calls to get for each entity id, and erases from maps when it has been
        // called this many times
        std::optional<std::unordered_map<EntityId, uint64_t>> opt_expected_get_calls_map_;
        std::mutex mtx_;
    };

    ComponentMap<std::shared_ptr<SegmentInMemory>> segment_map_{"segment", true};
    ComponentMap<std::shared_ptr<RowRange>> row_range_map_{"row range", false};
    ComponentMap<std::shared_ptr<ColRange>> col_range_map_{"col range", false};
    ComponentMap<std::shared_ptr<AtomKey>> atom_key_map_{"atom key", false};
    ComponentMap<bucket_id> bucket_map_{"bucket", false};

    // The next ID to use when inserting elements into any of the maps
    std::atomic<EntityId> next_entity_id_{0};
    EntityId entity_id(const std::optional<EntityId>& id=std::nullopt);
};

} // namespace arcticdb