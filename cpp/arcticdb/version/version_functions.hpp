/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/version/version_map.hpp>
#include <arcticdb/version/version_store_objects.hpp>


namespace arcticdb {

inline std::optional<AtomKey> get_latest_undeleted_version(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const StreamId &stream_id,
    bool skip_compat,
    bool iterate_on_failure) {
    ARCTICDB_RUNTIME_SAMPLE(GetLatestUndeletedVersion, 0)
    const auto entry = version_map->check_reload(store, stream_id, LoadParameter{LoadType::LOAD_LATEST_UNDELETED},
                                                 skip_compat, iterate_on_failure, __FUNCTION__);
    return entry->get_first_index(false);
}

inline std::optional<AtomKey> get_latest_version(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const StreamId &stream_id,
    bool skip_compat,
    bool iterate_on_failure) {
    ARCTICDB_SAMPLE(GetLatestVersion, 0)
    auto entry = version_map->check_reload(store, stream_id, LoadParameter{LoadType::LOAD_LATEST}, skip_compat,
                                           iterate_on_failure, __FUNCTION__);
    return entry->get_first_index(true);
}

// The next version ID returned will be 0 for brand new symbols, or one greater than the largest ever version created so far
inline version_store::UpdateInfo get_latest_undeleted_version_and_next_version_id(
        const std::shared_ptr<Store> &store,
        const std::shared_ptr<VersionMap> &version_map,
        const StreamId &stream_id,
        bool skip_compat,
        bool iterate_on_failure) {
    ARCTICDB_SAMPLE(GetLatestUndeletedVersionAndHighestVersionId, 0)
    auto entry = version_map->check_reload(store, stream_id, LoadParameter{LoadType::LOAD_LATEST_UNDELETED},
                                           skip_compat, iterate_on_failure, __FUNCTION__);
    auto latest_version = entry->get_first_index(true);
    auto latest_undeleted_version = entry->get_first_index(false);
    VersionId next_version_id = latest_version.has_value() ? latest_version->version_id() + 1 : 0;
    return {latest_undeleted_version, next_version_id};
}

inline std::vector<AtomKey> get_all_versions(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const StreamId &stream_id,
    bool skip_compat,
    bool iterate_on_failure) {
    ARCTICDB_SAMPLE(GetAllVersions, 0)
    const LoadParameter load_parameter(LoadType::LOAD_UNDELETED);
    auto entry = version_map->check_reload(
        store,
        stream_id,
        load_parameter,
        skip_compat,
        iterate_on_failure,
        __FUNCTION__);
    return entry->get_indexes(false);
}

inline std::optional<AtomKey> get_specific_version(
        const std::shared_ptr<Store> &store,
        const std::shared_ptr<VersionMap> &version_map,
        const StreamId &stream_id,
        SignedVersionId signed_version_id,
        bool skip_compat,
        bool iterate_on_failure,
        bool include_deleted = false) {
    ARCTICDB_SAMPLE(GetSpecificVersion, 0)
    auto entry = version_map->check_reload(store, stream_id, LoadParameter{LoadType::LOAD_DOWNTO, signed_version_id},
                                           skip_compat, iterate_on_failure, __FUNCTION__);
    VersionId version_id;
    if (signed_version_id >= 0) {
        version_id = static_cast<VersionId>(signed_version_id);
    } else {
        auto opt_latest = entry->get_first_index(true);
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
inline bool get_matching_prev_and_next_versions(
        const std::shared_ptr<VersionMapEntry> entry,
        VersionId version_id,
        MatchingAcceptor matching_acceptor,
        PrevAcceptor prev_acceptor,
        NextAcceptor next_acceptor,
        KeyFilter key_filter) {
    bool found_version = false;
    const IndexTypeKey* last = nullptr;

    for (const auto& item: entry->keys_) {
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
    return found_version;
}

inline bool has_undeleted_version(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const StreamId &id) {
    return static_cast<bool>(get_latest_undeleted_version(store, version_map, id, true, false));
}

inline void insert_if_undeleted(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const VariantKey &key,
    std::set<StreamId> &res) {
    auto id = variant_key_id(key);
    if (has_undeleted_version(store, version_map, id))
        res.insert(std::move(id));
}

inline std::unordered_map<VersionId, bool> get_all_tombstoned_versions(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const StreamId &stream_id) {
    auto entry = version_map->check_reload(store, stream_id, LoadParameter{LoadType::LOAD_ALL}, true, false,
                                           __FUNCTION__);
    std::unordered_map<VersionId, bool> result;
    for (auto key: entry->get_tombstoned_indexes())
            result[key.version_id()] = store->key_exists(key).get();

    return result;
}

inline version_store::TombstoneVersionResult tombstone_version(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const StreamId &stream_id,
    VersionId version_id,
    bool allow_tombstoning_beyond_latest_version=false,
    const std::optional<timestamp>& creation_ts=std::nullopt) {
    ARCTICDB_DEBUG(log::version(), "Tombstoning version {} for stream {}", version_id, stream_id);
    auto entry = version_map->check_reload(store, stream_id, LoadParameter{LoadType::LOAD_UNDELETED}, true, false,
                                           __FUNCTION__);
    // Might as well do the previous/next version check while we find the required version_id.
    // But if entry is empty, it's possible the load failed (since iterate_on_failure=false above), so set the flag
    // to defer the check to delete_tree() (instead of reloading in case eager delete is disabled).
    version_store::TombstoneVersionResult res(entry->empty());
    get_matching_prev_and_next_versions(entry, version_id,
            [&res](auto& matching){res.keys_to_delete.push_back(matching);},
            [&res](auto& prev){res.could_share_data.emplace(prev);},
            [&res](auto& next){res.could_share_data.emplace(next);},
            is_live_index_type_key // Entry could be cached with deleted keys even if LOAD_UNDELETED
            );

    AtomKey tombstone;
    if (res.keys_to_delete.empty()) {
        // It is possible to have a tombstone key without a corresponding index_key
        // This scenario can happen in case of DR sync
        if (entry->is_tombstoned(version_id)) {
            util::raise_rte("Version {} for symbol {} is already deleted", version_id, stream_id);
        } else {
            if (!allow_tombstoning_beyond_latest_version) {
                auto latest_key = get_latest_version(store, version_map, stream_id, true, false);
                if (!latest_key || latest_key.value().version_id() < version_id)
                    util::raise_rte("Can't delete version {} for symbol {} - it's higher than the latest version",
                            stream_id, version_id);
            }
            // We will write a tombstone key even when the index_key is not found
            tombstone = version_map->write_tombstone(store, version_id, stream_id, entry, creation_ts);
        }
    } else {
        tombstone = version_map->write_tombstone(store, res.keys_to_delete[0], stream_id, entry, creation_ts);
    }

    entry->tombstones_.try_emplace(version_id, std::move(tombstone));
    if (version_map->validate())
        entry->validate();

    res.no_undeleted_left = !entry->get_first_index(false).has_value();
    return res;
}

inline std::optional<AtomKey> get_version_key_from_time_for_versions(
    timestamp from_time,
    const std::vector<AtomKey> &version_keys) {
    // get_all_versions will hold the lock
    // Version keys are sorted in reverse order
    auto at_or_after = std::lower_bound(version_keys.begin(), version_keys.end(), from_time,
                                        [](const AtomKey &v_key, timestamp cmp) {
                                            return v_key.creation_ts() > cmp;
                                        });
    // If iterator points to the last element, we didn't have any versions before that or empty
    if (at_or_after == version_keys.end()) {
        return std::nullopt;
    }
    return *at_or_after;
}

inline std::optional<AtomKey> get_version_key_from_time(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const StreamId &stream_id,
    timestamp from_time,
    bool skip_compat,
    bool iterate_on_failure) {
    // get_all_versions will hold the lock
    auto all_versions = get_all_versions(store, version_map, stream_id, skip_compat, iterate_on_failure);
    return get_version_key_from_time_for_versions(from_time, all_versions);
}

inline std::vector<AtomKey> get_index_and_tombstone_keys(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const StreamId &stream_id) {
    const auto entry = version_map->check_reload(store, stream_id, LoadParameter{LoadType::LOAD_ALL}, true, true,
                                                 __FUNCTION__);
    std::vector<AtomKey> res;
    std::copy_if(std::begin(entry->keys_), std::end(entry->keys_), std::back_inserter(res),
                 [&](const auto &key) { return is_index_or_tombstone(key); });

    return res;
}

inline std::set<StreamId> list_streams(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const std::optional<std::string> &prefix,
    bool all_symbols
) {
    ARCTICDB_SAMPLE(ListStreams, 0)
    std::set<StreamId> res;
    if (prefix && store->supports_prefix_matching()) {
        ARCTICDB_DEBUG(log::version(), "Storage backend supports prefix matching");
        store->iterate_type(KeyType::VERSION_REF, [&store, &res, &version_map, all_symbols](auto &&vk) {
                                auto key = std::forward<VariantKey>(vk);
                                util::check(!variant_key_id_empty(key), "Unexpected empty id in key {}", key);
                                if(all_symbols)
                                    res.insert(variant_key_id(key));
                                else
                                    insert_if_undeleted(store, version_map, key, res);
                            },
                            prefix.value());
    } else {
        store->iterate_type(KeyType::VERSION_REF, [&store, &res, &version_map, all_symbols](auto &&vk) {
            const auto key = std::forward<VariantKey>(vk);
            util::check(!variant_key_id_empty(key), "Unexpected empty id in key {}", key);
            if(all_symbols)
                res.insert(variant_key_id(key));
            else
                insert_if_undeleted(store, version_map, key, res);
        });
    }
    return res;
}

}
