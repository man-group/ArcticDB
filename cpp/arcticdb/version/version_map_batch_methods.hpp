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
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/version/version_functions.hpp>
#include <arcticdb/version/snapshot.hpp>

#include <folly/futures/FutureSplitter.h>

namespace arcticdb {

struct SymbolStatus {
    const VersionId version_id_ = 0;
    const bool exists_ = false;
    const timestamp timestamp_ = 0;

    SymbolStatus(VersionId version_id, bool exists, timestamp ts) :
        version_id_(version_id),
        exists_(exists),
        timestamp_(ts) {
    }
};

template <typename Inputs, typename TaskSubmitter, typename ResultHandler>
inline void submit_tasks_for_range(const Inputs& inputs, TaskSubmitter submitter, ResultHandler result_handler) {
    const auto window_size = async::TaskScheduler::instance()->io_thread_count() * 2;

    auto futures = folly::window(inputs, [&submitter, &result_handler](const auto &input) {
        return submitter(input).thenValue([&result_handler, &input](auto &&r) {
            auto result = std::forward<decltype(r)>(r);
            result_handler(input, std::move(result));
            return folly::Unit{};
        });
    }, window_size);

    auto collected_futs = folly::collectAll(futures).get();
    std::optional<std::string> all_exceptions;
    for (auto&& collected_fut: collected_futs) {
        if (!collected_fut.hasValue()) {
            all_exceptions = all_exceptions.value_or("").append(collected_fut.exception().what().toStdString()).append("\n");
        }
    }
    internal::check<ErrorCode::E_RUNTIME_ERROR>(!all_exceptions.has_value(), all_exceptions.value_or(""));
}

inline std::shared_ptr<std::unordered_map<StreamId, SymbolStatus>> batch_check_latest_id_and_status(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const std::shared_ptr<std::vector<StreamId>> &symbols) {
    ARCTICDB_SAMPLE(BatchGetLatestVersion, 0)
    const LoadParameter load_param{LoadType::LOAD_LATEST_UNDELETED};
    auto output = std::make_shared<std::unordered_map<StreamId, SymbolStatus>>();
    auto mutex = std::make_shared<std::mutex>();

    submit_tasks_for_range(*symbols,
        [store, version_map, &load_param](auto &symbol) {
          return async::submit_io_task(CheckReloadTask{store, version_map, symbol, load_param});
        },
        [output, mutex](const auto& id, const std::shared_ptr<VersionMapEntry> &entry) {
          auto index_key = entry->get_first_index(false);
          if (index_key) {
              std::lock_guard lock{*mutex};
              output->insert(std::make_pair<StreamId, SymbolStatus>(StreamId{id}, {index_key->version_id(), true, index_key->creation_ts()}));
          } else {
              index_key = entry->get_first_index(true);
              if (index_key) {
                  std::lock_guard lock{*mutex};
                  output->insert(std::make_pair<StreamId, SymbolStatus>(StreamId{id}, {index_key->version_id(), false, index_key->creation_ts()}));
              } else {
                  if (entry->head_ && entry->head_->type() == KeyType::TOMBSTONE_ALL) {
                      const auto& head = *entry->head_;
                      std::lock_guard lock{*mutex};
                      output->insert(std::make_pair<StreamId, SymbolStatus>(StreamId{id}, {head.version_id(), false, head.creation_ts()}));
                  }
              }
          }});

    return output;
}

inline std::shared_ptr<std::unordered_map<StreamId, AtomKey>> batch_get_latest_version(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const std::vector<StreamId> &stream_ids,
    bool include_deleted) {
    ARCTICDB_SAMPLE(BatchGetLatestVersion, 0)
    const LoadParameter load_param{include_deleted ? LoadType::LOAD_LATEST : LoadType::LOAD_LATEST_UNDELETED};
    auto output = std::make_shared<std::unordered_map<StreamId, AtomKey>>();
    auto mutex = std::make_shared<std::mutex>();

    submit_tasks_for_range(stream_ids,
            [store, version_map, &load_param](auto& stream_id) {
                return async::submit_io_task(CheckReloadTask{store, version_map, stream_id, load_param});
            },
            [output, include_deleted, mutex](auto id, auto entry) {
                auto index_key = entry->get_first_index(include_deleted);
                if (index_key) {
                    std::lock_guard lock{*mutex};
                    (*output)[id] = *index_key;
                }
            });

    return output;
}

inline std::vector<folly::Future<std::pair<std::optional<AtomKey>, std::optional<AtomKey>>>> batch_get_latest_undeleted_and_latest_versions_async(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const std::vector<StreamId> &stream_ids) {
    ARCTICDB_SAMPLE(BatchGetLatestUndeletedVersionAndNextVersionId, 0)
    std::vector<folly::Future<std::pair<std::optional<AtomKey>, std::optional<AtomKey>>>> vector_fut;
    for (auto& stream_id: stream_ids){
        vector_fut.push_back(async::submit_io_task(CheckReloadTask{store,
                                                                   version_map,
                                                                   stream_id,
                                                                   LoadParameter{LoadType::LOAD_LATEST_UNDELETED}})
                                 .thenValue([](const std::shared_ptr<VersionMapEntry>& entry){
                                     return std::make_pair(entry->get_first_index(false), entry->get_first_index(true));
                                 }));
    }
    return vector_fut;
}

inline std::vector<folly::Future<version_store::UpdateInfo>> batch_get_latest_undeleted_version_and_next_version_id_async(
        const std::shared_ptr<Store> &store,
        const std::shared_ptr<VersionMap> &version_map,
        const std::vector<StreamId> &stream_ids) {
    ARCTICDB_SAMPLE(BatchGetLatestUndeletedVersionAndNextVersionId, 0)
    std::vector<folly::Future<version_store::UpdateInfo>> vector_fut;
    for (auto& stream_id: stream_ids){
        vector_fut.push_back(async::submit_io_task(CheckReloadTask{store,
                                                     version_map,
                                                     stream_id,
                                                     LoadParameter{LoadType::LOAD_LATEST_UNDELETED}})
        .thenValue([](auto entry){
            auto latest_version = entry->get_first_index(true);
            auto latest_undeleted_version = entry->get_first_index(false);
            VersionId next_version_id = latest_version.has_value() ? latest_version->version_id() + 1 : 0;
            return version_store::UpdateInfo{latest_undeleted_version, next_version_id};
        }));
    }
    return vector_fut;
}

template <typename MapType>
struct MapRandomAccessWrapper {
    const MapType& map_;

    using iterator = typename MapType::iterator;

    explicit MapRandomAccessWrapper(const MapType& map) :
        map_(map) {
    }

    [[nodiscard]] size_t size() const {
        return map_.size();
    }

    auto& operator[](size_t pos) const {
        auto it = std::begin(map_);
        std::advance(it, pos);
        return *it;
    }
};

// This version assumes that there is only one version per symbol, so no need for the state machine below
inline std::shared_ptr<std::unordered_map<StreamId, AtomKey>> batch_get_specific_version(
    const std::shared_ptr<Store>& store,
    const std::shared_ptr<VersionMap>& version_map,
    const std::map<StreamId, VersionId>& sym_versions,
    bool include_deleted = true) {
    ARCTICDB_SAMPLE(BatchGetLatestVersion, 0)
    auto output = std::make_shared<std::unordered_map<StreamId, AtomKey>>();
    auto mutex = std::make_shared<std::mutex>();

    MapRandomAccessWrapper wrapper{sym_versions};
    submit_tasks_for_range(wrapper, [store, version_map](auto& sym_version) {
            LoadParameter load_param{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(sym_version.second)};
            return async::submit_io_task(CheckReloadTask{store, version_map, sym_version.first, load_param});
        },
        [output, include_deleted, mutex](auto sym_version, const std::shared_ptr<VersionMapEntry>& entry) {
        auto index_key = find_index_key_for_version_id(sym_version.second, entry, include_deleted);
        if (index_key) {
            std::lock_guard lock{*mutex};
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
        std::map<StreamId, VersionVectorType>& sym_versions,
        bool include_deleted = true) {
    ARCTICDB_SAMPLE(BatchGetLatestVersion, 0)
    auto output = std::make_shared<std::unordered_map<std::pair<StreamId, VersionId>, AtomKey>>();
    MapRandomAccessWrapper wrapper{sym_versions};
    auto mutex = std::make_shared<std::mutex>();

    submit_tasks_for_range(wrapper, [store, version_map](auto sym_version) {
                auto first_version = *std::min_element(std::begin(sym_version.second), std::end(sym_version.second));
                LoadParameter load_param{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(first_version)};
                return async::submit_io_task(CheckReloadTask{store, version_map, sym_version.first, load_param});
            },

            [output, &sym_versions, include_deleted, mutex](auto sym_version, const std::shared_ptr<VersionMapEntry>& entry) {
                auto sym_it = sym_versions.find(sym_version.first);
                util::check(sym_it != sym_versions.end(), "Failed to find versions for symbol {}", sym_version.first);
                const auto& versions = sym_it->second;
                for(auto version : versions) {
                    auto index_key = find_index_key_for_version_id(version, entry, include_deleted);
                    if (index_key) {
                        std::lock_guard lock{*mutex};
                        (*output)[std::pair(sym_version.first, version)] = *index_key;
                    }
                }
            });

    return output;
}
struct StreamVersionData {
    size_t count_ = 0;
    LoadParameter load_param_ = LoadParameter{LoadType::NOT_LOADED};
    folly::small_vector<SnapshotId, 1> snapshots_;

    explicit StreamVersionData(const pipelines::VersionQuery& version_query);
    StreamVersionData() = default;
    void react(const pipelines::VersionQuery& version_query);
private:
    void do_react(std::monostate);
    void do_react(const pipelines::SpecificVersionQuery& specific_version);
    void do_react(const pipelines::TimestampVersionQuery& timestamp_query);
    void do_react(const pipelines::SnapshotVersionQuery& snapshot_query);
};

std::vector<folly::Future<std::optional<AtomKey>>> batch_get_versions_async(
    const std::shared_ptr<Store>& store,
    const std::shared_ptr<VersionMap>& version_map,
    const std::vector<StreamId>& symbols,
    const std::vector<pipelines::VersionQuery>& version_queries);

inline std::vector<folly::Future<folly::Unit>> batch_write_version(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const std::vector<AtomKey> &keys) {
    std::vector<folly::Future<folly::Unit>> results;
    results.reserve(keys.size());
    for (const auto &key : keys) {
        results.emplace_back(async::submit_io_task(WriteVersionTask{store, version_map, key}));
    }

    return results;
}

inline std::vector<folly::Future<std::vector<AtomKey>>> batch_write_and_prune_previous(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const std::vector<AtomKey> &keys,
    const std::vector<version_store::UpdateInfo>& stream_update_info_vector) {
    std::vector<folly::Future<std::vector<AtomKey>>> results;
    results.reserve(keys.size());
    for(auto key : folly::enumerate(keys)){
        auto previous_index_key = stream_update_info_vector[key.index].previous_index_key_;
        results.emplace_back(async::submit_io_task(WriteAndPrunePreviousTask{store, version_map, *key, previous_index_key}));
    }
    
    return results;
}
} //namespace arcticdb
