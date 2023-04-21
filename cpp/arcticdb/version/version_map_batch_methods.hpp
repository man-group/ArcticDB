/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/version/version_map.hpp>
#include <arcticdb/version/version_tasks.hpp>
#include <arcticdb/version/version_store_objects.hpp>

namespace arcticdb {

inline std::shared_ptr<std::unordered_map<StreamId, AtomKey>> batch_get_latest_version(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const std::vector<StreamId> &stream_ids,
    bool include_deleted) {
    ARCTICDB_SAMPLE(BatchGetLatestVersion, 0)
    const LoadParameter load_param{include_deleted ? LoadType::LOAD_LATEST : LoadType::LOAD_LATEST_UNDELETED};
    auto output = std::make_shared<std::unordered_map<StreamId, AtomKey>>();

    async::submit_tasks_for_range(stream_ids,
            [store, version_map, &load_param](auto& stream_id) {
                return async::submit_io_task(CheckReloadTask{store, version_map, stream_id, load_param});
            },
            [output, include_deleted](auto& id, auto&& entry) {
                auto index_key = entry->get_first_index(include_deleted);
                if (index_key)
                    (*output)[id] = *index_key;
            });

    return output;
}

// The logic here is the same as get_latest_undeleted_version_and_next_version_id
inline std::unordered_map<StreamId, version_store::UpdateInfo> batch_get_latest_undeleted_version_and_next_version_id(
        const std::shared_ptr<Store> &store,
        const std::shared_ptr<VersionMap> &version_map,
        const std::vector<StreamId> &stream_ids) {
    ARCTICDB_SAMPLE(BatchGetLatestUndeletedVersionAndNextVersionId, 0)
    std::unordered_map<StreamId, version_store::UpdateInfo> output;

    async::submit_tasks_for_range(stream_ids,
                                  [store, version_map](auto& stream_id) {
        return async::submit_io_task(CheckReloadTask{store,
                                                     version_map,
                                                     stream_id,
                                                     LoadParameter{LoadType::LOAD_LATEST_UNDELETED}});
        },
        [&output](auto& id, auto&& entry) {
        auto latest_version = entry->get_first_index(true);
        auto latest_undeleted_version = entry->get_first_index(false);
        VersionId next_version_id = latest_version.has_value() ? latest_version->version_id() + 1 : 0;
        output[id] =  {latest_undeleted_version, next_version_id};
    });

    return output;
}

inline std::shared_ptr<std::unordered_map<StreamId, AtomKey>> batch_get_specific_version(
    const std::shared_ptr<Store>& store,
    const std::shared_ptr<VersionMap>& version_map,
    std::map<StreamId, VersionId>& sym_versions) {
    ARCTICDB_SAMPLE(BatchGetLatestVersion, 0)
    auto output = std::make_shared<std::unordered_map<StreamId, AtomKey>>();

    async::submit_tasks_for_range(sym_versions,
                                  [store, version_map](auto& sym_version) {
        LoadParameter load_param{LoadType::LOAD_DOWNTO, sym_version.second};
        return async::submit_io_task(CheckReloadTask{store, version_map, sym_version.first, load_param});
        },
        [output](auto& sym_version, auto&& entry) {
        auto index_key = find_index_key_for_version_id(sym_version.second, entry);
        if (index_key) {
            (*output)[sym_version.first] = *index_key;
        }
    });

    return output;
}

using VersionVectorType = std::vector<VersionId>;

/**
 * Returns multiple versions for the same symbol
 * @return Does not guarantee the returned keys actually exist in storage.
 */
inline std::shared_ptr<std::unordered_map<std::pair<StreamId, VersionId>, AtomKey>> batch_get_specific_versions(
        const std::shared_ptr<Store>& store,
        const std::shared_ptr<VersionMap>& version_map,
        std::map<StreamId, VersionVectorType>& sym_versions) {
    ARCTICDB_SAMPLE(BatchGetLatestVersion, 0)
    auto output = std::make_shared<std::unordered_map<std::pair<StreamId, VersionId>, AtomKey>>();

    async::submit_tasks_for_range(sym_versions,
            [store, version_map](auto& sym_version) {
                auto first_version = *std::min_element(std::begin(sym_version.second), std::end(sym_version.second));
                LoadParameter load_param{LoadType::LOAD_DOWNTO, first_version};
                return async::submit_io_task(CheckReloadTask{store, version_map, sym_version.first, load_param});
            },
            [output, &sym_versions](auto& sym_version, auto&& entry) {
                auto sym_it = sym_versions.find(sym_version.first);
                util::check(sym_it != sym_versions.end(), "Failed to find versions for symbol");
                const auto& versions = sym_it->second;
                for(auto version : versions) {
                    auto index_key = find_index_key_for_version_id(version, entry);
                    if (index_key) {
                        (*output)[std::pair(sym_version.first, version)] = *index_key;
                    }
                }
            });

    return output;
}

inline void batch_write_version(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const std::vector<AtomKey> &keys) {
    std::vector<folly::Future<folly::Unit>> results;
    results.reserve(keys.size());
    for (const auto &key : keys) {
        results.emplace_back(async::submit_io_task(WriteVersionTask{store, version_map, key}));
    }

    folly::collect(results).wait();
}

inline void batch_write_and_prune_previous(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const std::vector<AtomKey> &keys,
    const std::unordered_map<StreamId, version_store::UpdateInfo>& stream_update_info_map) {
    std::vector<folly::Future<folly::Unit>> results;
    results.reserve(keys.size());
    for (const auto &key : keys) {
        auto previous_index_key = stream_update_info_map.at(key.id()).previous_index_key_;
        results.emplace_back(async::submit_io_task(WriteAndPrunePreviousTask{store, version_map, key, previous_index_key}));
    }

    folly::collect(results).wait();
}
} //namespace arcticdb
