/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/version/version_map.hpp>
#include <arcticdb/version/version_store_objects.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/async/base_task.hpp>
#include <arcticdb/version/version_tasks.hpp>
#include <folly/futures/Future.h>

namespace arcticdb {

inline std::optional<AtomKey> get_latest_undeleted_version(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map, const StreamId& stream_id
) {
    ARCTICDB_RUNTIME_SAMPLE(GetLatestUndeletedVersion, 0)
    LoadStrategy load_strategy{LoadType::LATEST, LoadObjective::UNDELETED_ONLY};
    const auto entry = version_map->check_reload(store, stream_id, load_strategy, __FUNCTION__);
    return entry->get_first_index(false).first;
}

inline std::pair<std::optional<AtomKey>, bool> get_latest_version(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map, const StreamId& stream_id
) {
    ARCTICDB_SAMPLE(GetLatestVersion, 0)
    LoadStrategy load_strategy{LoadType::LATEST, LoadObjective::INCLUDE_DELETED};
    auto entry = version_map->check_reload(store, stream_id, load_strategy, __FUNCTION__);
    return entry->get_first_index(true);
}

// The next version ID returned will be 0 for brand new symbols, or one greater than the largest ever version created so
// far
inline version_store::UpdateInfo get_latest_undeleted_version_and_next_version_id(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map, const StreamId& stream_id
) {
    ARCTICDB_SAMPLE(GetLatestUndeletedVersionAndHighestVersionId, 0)
    LoadStrategy load_strategy{LoadType::LATEST, LoadObjective::UNDELETED_ONLY};
    auto entry = version_map->check_reload(store, stream_id, load_strategy, __FUNCTION__);
    auto latest_version = entry->get_first_index(true).first;
    auto latest_undeleted_version = entry->get_first_index(false).first;
    VersionId next_version_id = latest_version.has_value() ? latest_version->version_id() + 1 : 0;
    return {latest_undeleted_version, next_version_id};
}

inline std::vector<AtomKey> get_all_versions(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map, const StreamId& stream_id
) {
    ARCTICDB_SAMPLE(GetAllVersions, 0)
    LoadStrategy load_strategy{LoadType::ALL, LoadObjective::UNDELETED_ONLY};
    auto entry = version_map->check_reload(store, stream_id, load_strategy, __FUNCTION__);
    return entry->get_indexes(false);
}

inline std::optional<AtomKey> get_specific_version(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map, const StreamId& stream_id,
        SignedVersionId signed_version_id, bool include_deleted = false
) {
    LoadStrategy load_strategy{LoadType::DOWNTO, LoadObjective::UNDELETED_ONLY, signed_version_id};
    auto entry = version_map->check_reload(store, stream_id, load_strategy, __FUNCTION__);
    VersionId version_id;
    if (signed_version_id >= 0) {
        version_id = static_cast<VersionId>(signed_version_id);
    } else {
        auto opt_latest = entry->get_first_index(true).first;
        if (opt_latest.has_value()) {
            auto opt_version_id = get_version_id_negative_index(opt_latest->version_id(), signed_version_id);
            if (opt_version_id.has_value()) {
                version_id = *opt_version_id;
            } else {
                return std::nullopt;
            }
        } else {
            return std::nullopt;
        }
    }
    return find_index_key_for_version_id(version_id, entry, include_deleted);
}

template<typename MatchingAcceptor, typename PrevAcceptor, typename NextAcceptor, typename KeyFilter>
bool get_matching_prev_and_next_versions(
        const std::shared_ptr<VersionMapEntry>& entry, VersionId version_id, MatchingAcceptor matching_acceptor,
        PrevAcceptor prev_acceptor, NextAcceptor next_acceptor, KeyFilter key_filter
) {
    bool found_version = false;
    const IndexTypeKey* last = nullptr;

    for (const auto& item : entry->keys_) {
        if (key_filter(item, entry)) {
            if (item.version_id() == version_id) {
                found_version = true;
                matching_acceptor(item);
                if (last) {
                    next_acceptor(*last); // This is the next version as keys_ are descending
                }
            } else if (found_version) {
                prev_acceptor(item);
                break;
            } else {
                last = &item;
            }
        }
    }

    // If we didn't find the version but there is a last version
    // we need to pass it to the next acceptor
    // because it might share data with the version we are looking for
    // This is the case when we are deleting a snapshot and the version we are deleting is already deleted
    if (!found_version && last) {
        next_acceptor(*last);
    }
    return found_version;
}

inline bool has_undeleted_version(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map, const StreamId& id
) {
    auto maybe_undeleted = get_latest_undeleted_version(store, version_map, id);
    return static_cast<bool>(maybe_undeleted);
}

template<stream::StreamIdSet R>
void insert_if_undeleted(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map, const VariantKey& key,
        R& res
) {
    auto id = variant_key_id(key);
    if (has_undeleted_version(store, version_map, id))
        res.insert(std::move(id));
}

inline std::unordered_map<VersionId, bool> get_all_tombstoned_versions(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map, const StreamId& stream_id
) {
    LoadStrategy load_strategy{LoadType::ALL, LoadObjective::INCLUDE_DELETED};
    auto entry = version_map->check_reload(store, stream_id, load_strategy, __FUNCTION__);
    std::unordered_map<VersionId, bool> result;
    for (auto key : entry->get_tombstoned_indexes())
        result[key.version_id()] = store->key_exists(key).get();

    return result;
}

inline version_store::TombstoneVersionResult populate_tombstone_result(
        const std::shared_ptr<VersionMapEntry>& entry, const std::unordered_set<VersionId>& version_ids,
        const StreamId& stream_id, const std::shared_ptr<Store>& store
) {
    version_store::TombstoneVersionResult res(entry->empty(), stream_id);
    auto latest_key = entry->get_first_index(true).first;

    for (auto version_id : version_ids) {
        bool found = false;
        get_matching_prev_and_next_versions(
                entry,
                version_id,
                [&res, &found](auto& matching) {
                    res.keys_to_delete.push_back(matching);
                    found = true;
                },
                [&res](auto& prev) { res.could_share_data.emplace(prev); },
                [&res](auto& next) { res.could_share_data.emplace(next); },
                is_live_index_type_key // Entry could be cached with deleted keys even if LOAD_UNDELETED
        );

        // It is possible to have a tombstone key without a corresponding index_key
        // This scenario can happen in case of DR sync
        if (entry->is_tombstoned(version_id)) {
            missing_data::raise<ErrorCode::E_NO_SUCH_VERSION>(
                    "Version {} for symbol {} is already deleted", version_id, stream_id
            );
        } else {
            if (!latest_key || latest_key->version_id() < version_id) {
                missing_data::raise<ErrorCode::E_NO_SUCH_VERSION>(
                        "Can't delete version {} for symbol {} - it's higher than the latest version",
                        version_id,
                        stream_id
                );
            }
        }

        // This should never happen but we are keeping it for backwards compatibility
        if (!found) {
            log::version().debug(
                    "Trying to tombstone version {} for symbol {} that is not in the version map", version_id, stream_id
            );
            res.keys_to_delete.emplace_back(atom_key_builder()
                                                    .version_id(version_id)
                                                    .creation_ts(store->current_timestamp())
                                                    .content_hash(3)
                                                    .start_index(4)
                                                    .end_index(5)
                                                    .build(stream_id, KeyType::TABLE_INDEX));
        }
    }

    storage::check<ErrorCode::E_KEY_NOT_FOUND>(
            res.keys_to_delete.size() == version_ids.size(),
            "Expected {} index keys to be marked for deletion, got {} keys: {}",
            version_ids.size(),
            res.keys_to_delete.size(),
            fmt::format("{}", res.keys_to_delete)
    );
    return res;
}

inline folly::Future<version_store::TombstoneVersionResult> finalize_tombstone_result(
        version_store::TombstoneVersionResult&& res, const std::shared_ptr<VersionMap>& version_map,
        std::shared_ptr<VersionMapEntry>&& entry, [[maybe_unused]] AtomKey tombstone_key
) {
    ARCTICDB_DEBUG(log::version(), "Finalizing result for tombstone key {}", tombstone_key);
    // Update the result with final state
    if (version_map->validate())
        entry->validate();

    res.no_undeleted_left = !entry->get_first_index(false).first.has_value();
    res.latest_version_ = entry->get_first_index(true).first->version_id();
    return res;
}

inline folly::Future<version_store::TombstoneVersionResult> finalize_tombstone_all_result(
        const std::shared_ptr<VersionMap>& version_map, std::shared_ptr<VersionMapEntry>&& entry,
        std::pair<VersionId, std::vector<AtomKey>> tombstone_result
) {
    ARCTICDB_DEBUG(log::version(), "Finalizing result for tombstone key {}", tombstone_result.first);
    // Update the result with final state
    if (version_map->validate())
        entry->validate();

    version_store::TombstoneVersionResult res{true, entry->head_->id()};
    res.keys_to_delete = std::move(tombstone_result.second);

    res.no_undeleted_left = true;
    res.latest_version_ = tombstone_result.first;
    return res;
}

inline folly::Future<version_store::TombstoneVersionResult> process_tombstone_all_versions(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map, const StreamId& stream_id,
        std::shared_ptr<VersionMapEntry> entry
) {
    // Submit the write tombstone task
    return async::submit_io_task(TombstoneAllTask{store, version_map, stream_id, std::nullopt, entry})
            .thenValue([version_map,
                        e = std::move(entry)](std::pair<VersionId, std::vector<AtomKey>>&& tombstone_result) mutable {
                return finalize_tombstone_all_result(version_map, std::move(e), std::move(tombstone_result));
            });
}

inline folly::Future<version_store::TombstoneVersionResult> process_tombstone_versions(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map, const StreamId& stream_id,
        const std::unordered_set<VersionId>& version_ids, std::shared_ptr<VersionMapEntry> entry
) {
    // Populate the tombstone result
    version_store::TombstoneVersionResult res = populate_tombstone_result(entry, version_ids, stream_id, store);

    // Submit the write tombstone task
    return async::submit_io_task(WriteTombstonesTask{store, version_map, res.keys_to_delete, stream_id, entry})
            .thenValue([res = std::move(res), version_map, e = std::move(entry)](AtomKey&& tombstone_key) mutable {
                return finalize_tombstone_result(std::move(res), version_map, std::move(e), std::move(tombstone_key));
            });
}

inline folly::Future<version_store::TombstoneVersionResult> tombstone_versions_async(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map, const StreamId& stream_id,
        const std::unordered_set<VersionId>& version_ids
) {
    ARCTICDB_DEBUG(log::version(), "Tombstoning versions {} for stream {}", version_ids, stream_id);

    return async::submit_io_task(
                   CheckReloadTask{
                           store, version_map, stream_id, LoadStrategy{LoadType::ALL, LoadObjective::UNDELETED_ONLY}
                   }
    ).thenValue([store, version_map, stream_id, version_ids](std::shared_ptr<VersionMapEntry>&& entry) {
        return process_tombstone_versions(store, version_map, stream_id, version_ids, std::move(entry));
    });
}

inline folly::Future<version_store::TombstoneVersionResult> tombstone_all_async(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map, const StreamId& stream_id
) {
    ARCTICDB_DEBUG(log::version(), "Tombstoning all versions for stream {}", stream_id);

    return async::submit_io_task(
                   CheckReloadTask{
                           store, version_map, stream_id, LoadStrategy{LoadType::ALL, LoadObjective::UNDELETED_ONLY}
                   }
    ).thenValue([store, version_map, stream_id](std::shared_ptr<VersionMapEntry>&& entry) {
        return process_tombstone_all_versions(store, version_map, stream_id, std::move(entry));
    });
}

inline version_store::TombstoneVersionResult tombstone_versions(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map, const StreamId& stream_id,
        const std::unordered_set<VersionId>& version_ids
) {
    ARCTICDB_DEBUG(log::version(), "Tombstoning versions {} for stream {}", version_ids, stream_id);

    return tombstone_versions_async(store, version_map, stream_id, version_ids).get();
}

inline std::optional<AtomKey> get_index_key_from_time(timestamp from_time, const std::vector<AtomKey>& keys) {
    auto at_or_after =
            std::lower_bound(std::begin(keys), std::end(keys), from_time, [](const AtomKey& v_key, timestamp cmp) {
                return v_key.creation_ts() > cmp;
            });
    // If iterator points to the last element, we didn't have any versions before that
    if (at_or_after == keys.end()) {
        return std::nullopt;
    }
    return *at_or_after;
}

inline std::optional<AtomKey> load_index_key_from_time(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map, const StreamId& stream_id,
        timestamp from_time
) {
    LoadStrategy load_strategy{LoadType::FROM_TIME, LoadObjective::UNDELETED_ONLY, from_time};
    auto entry = version_map->check_reload(store, stream_id, load_strategy, __FUNCTION__);
    auto indexes = entry->get_indexes(false);
    return get_index_key_from_time(from_time, indexes);
}

inline std::vector<AtomKey> get_index_and_tombstone_keys(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map, const StreamId& stream_id
) {
    LoadStrategy load_strategy{LoadType::ALL, LoadObjective::INCLUDE_DELETED};
    const auto entry = version_map->check_reload(store, stream_id, load_strategy, __FUNCTION__);
    std::vector<AtomKey> res;
    std::copy_if(std::begin(entry->keys_), std::end(entry->keys_), std::back_inserter(res), [&](const auto& key) {
        return is_index_or_tombstone(key);
    });

    return res;
}

template<typename R = std::set<StreamId>>
R list_streams(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map,
        const std::optional<std::string>& prefix, bool all_symbols
) {
    ARCTICDB_SAMPLE(ListStreams, 0)
    R res;
    if (prefix && store->supports_prefix_matching()) {
        ARCTICDB_DEBUG(log::version(), "Storage backend supports prefix matching");
        store->iterate_type(
                KeyType::VERSION_REF,
                [&store, &res, &version_map, all_symbols](auto&& vk) {
                    auto key = std::forward<VariantKey&&>(vk);
                    util::check(!variant_key_id_empty(key), "Unexpected empty id in key {}", key);
                    if (all_symbols)
                        res.insert(variant_key_id(key));
                    else
                        insert_if_undeleted(store, version_map, key, res);
                },
                *prefix
        );
    } else {
        store->iterate_type(KeyType::VERSION_REF, [&store, &res, &version_map, all_symbols](auto&& vk) {
            const auto key = std::forward<VariantKey>(vk);
            util::check(!variant_key_id_empty(key), "Unexpected empty id in key {}", key);
            if (all_symbols)
                res.insert(variant_key_id(key));
            else
                insert_if_undeleted(store, version_map, key, res);
        });
    }
    return res;
}

} // namespace arcticdb
