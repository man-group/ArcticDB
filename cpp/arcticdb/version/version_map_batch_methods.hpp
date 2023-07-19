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
#include <folly/futures/FutureSplitter.h>

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
        .thenValue([](auto&& entry){
            auto latest_version = entry->get_first_index(true);
            auto latest_undeleted_version = entry->get_first_index(false);
            VersionId next_version_id = latest_version.has_value() ? latest_version->version_id() + 1 : 0;
            return version_store::UpdateInfo{latest_undeleted_version, next_version_id};
        }));
    }
    return vector_fut;
}

inline std::shared_ptr<std::unordered_map<StreamId, AtomKey>> batch_get_specific_version(
    const std::shared_ptr<Store>& store,
    const std::shared_ptr<VersionMap>& version_map,
    std::map<StreamId, VersionId>& sym_versions) {
    ARCTICDB_SAMPLE(BatchGetLatestVersion, 0)
    auto output = std::make_shared<std::unordered_map<StreamId, AtomKey>>();

    async::submit_tasks_for_range(sym_versions,
                                  [store, version_map](auto& sym_version) {
        LoadParameter load_param{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(sym_version.second)};
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
                LoadParameter load_param{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(first_version)};
                return async::submit_io_task(CheckReloadTask{store, version_map, sym_version.first, load_param});
            },
            [output, &sym_versions](auto& sym_version, auto&& entry) {
                auto sym_it = sym_versions.find(sym_version.first);
                util::check(sym_it != sym_versions.end(), "Failed to find versions for symbol {}", sym_version.first);
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

struct StreamVersionData {
    explicit StreamVersionData(const pipelines::VersionQuery& version_query) {
        react(version_query);
    }

    StreamVersionData() = default;

    size_t count_ = 0;
    LoadParameter load_param_ = LoadParameter{LoadType::LOAD_LATEST_UNDELETED};

    void react(const pipelines::VersionQuery& version_query) {
        util::variant_match(version_query.content_, [that = this](const auto &query) {
            that->do_react(query);
        });
    }

    void do_react(std::monostate) {
        ++count_;
    }

    void do_react(const pipelines::SpecificVersionQuery& specific_version) {
        ++count_;
        switch(load_param_.load_type_) {
        case LoadType::LOAD_LATEST_UNDELETED:
            load_param_ = LoadParameter{LoadType::LOAD_DOWNTO, specific_version.version_id_};
            break;
        case LoadType::LOAD_DOWNTO:
            util::check(load_param_.load_until_.has_value(), "Expect LOAD_DOWNTO to have version specificed");
            if ((specific_version.version_id_ >= 0 && load_param_.load_until_.value() >= 0) ||
                    (specific_version.version_id_ < 0 && load_param_.load_until_.value() < 0)) {
                load_param_.load_until_ = std::min(load_param_.load_until_.value(), specific_version.version_id_);
            } else {
                load_param_ = LoadParameter{LoadType::LOAD_UNDELETED};
            }
            break;
        case LoadType::LOAD_FROM_TIME:
        case LoadType::LOAD_UNDELETED:
            load_param_ = LoadParameter{LoadType::LOAD_UNDELETED};
            break;
        default:
            util::raise_rte("Unexpected load state {} applying specific version query", load_param_.load_type_);
        }
    }

    void do_react(const pipelines::TimestampVersionQuery& timestamp_query) {
        ++count_;
        switch(load_param_.load_type_) {
        case LoadType::LOAD_LATEST_UNDELETED:
            load_param_ = LoadParameter{LoadType::LOAD_FROM_TIME, timestamp_query.timestamp_};
            break;
        case LoadType::LOAD_FROM_TIME:
            util::check(load_param_.load_from_time_.has_value(), "Expect LOAD_TO_TIME to have timestamp specificed");
            load_param_.load_from_time_ = std::min(load_param_.load_from_time_.value(), timestamp_query.timestamp_);
            break;
        case LoadType::LOAD_DOWNTO:
        case LoadType::LOAD_UNDELETED:
            load_param_ = LoadParameter{LoadType::LOAD_UNDELETED};
            break;
        default:
            util::raise_rte("Unexpected load state {} applying specific version query", load_param_.load_type_);
        }
    }

    void do_react(const pipelines::SnapshotVersionQuery&) {
        util::raise_rte("Snapshot not currently supported in generic batch read version");
    }
};

inline std::optional<AtomKey> get_key_for_version_query(
    const std::shared_ptr<VersionMapEntry>& version_map_entry, 
    const pipelines::VersionQuery& version_query) {
    return util::variant_match(version_query.content_,
        [&version_map_entry] (const pipelines::SpecificVersionQuery& specific_version) -> std::optional<AtomKey> {
            auto signed_version_id = specific_version.version_id_;
            VersionId version_id;
            if (signed_version_id >= 0) {
                version_id = static_cast<VersionId>(signed_version_id);
            } else {
                auto opt_latest = version_map_entry->get_first_index(true);
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
            return find_index_key_for_version_id(version_id, version_map_entry);
        },
        [&version_map_entry] (const pipelines::TimestampVersionQuery& timestamp_version) -> std::optional<AtomKey> {
            auto version_key = get_version_key_from_time_for_versions(timestamp_version.timestamp_, version_map_entry->get_indexes(false));
            if(version_key.has_value()){
                auto version_id = version_key.value().version_id();
                return find_index_key_for_version_id(version_id, version_map_entry, false);
            }else{
                return std::nullopt;
            }
        },
        [&version_map_entry] (const std::monostate&) {
        return version_map_entry->get_first_index(false);
        },
        [] (const auto&) -> std::optional<AtomKey> {
            util::raise_rte("Unsupported version query type");
        });
}

inline std::vector<folly::Future<std::optional<AtomKey>>> batch_get_versions_async(
    const std::shared_ptr<Store>& store,
    const std::shared_ptr<VersionMap>& version_map,
    const std::vector<StreamId>& symbols,
    const std::vector<pipelines::VersionQuery>& version_queries) {
    ARCTICDB_SAMPLE(BatchGetVersion, 0)
    util::check(symbols.size() == version_queries.size(), "Symbol and version query list mismatch: {} != {}", symbols.size(), version_queries.size());

    robin_hood::unordered_flat_map<StreamId, StreamVersionData> version_data;
    for(const auto& symbol : folly::enumerate(symbols)) {
        auto it = version_data.find(*symbol);
        if(it == version_data.end())
            version_data.insert(robin_hood::pair<StreamId, StreamVersionData>(*symbol, StreamVersionData{version_queries[symbol.index]}));
        else
            it->second.react(version_queries[symbol.index]);
    }

    using SplitterType = folly::FutureSplitter<std::shared_ptr<VersionMapEntry>>;
    robin_hood::unordered_flat_map<StreamId, SplitterType> shared_futures;
    std::vector<folly::Future<std::optional<AtomKey>>> output;
    output.reserve(symbols.size());
    for(const auto& symbol : folly::enumerate(symbols)) {
        const auto it = version_data.find(*symbol);
        util::check(it != version_data.end(), "Missing version data for symbol {}", *symbol);
        auto version_entry_fut = folly::Future<std::shared_ptr<VersionMapEntry>>::makeEmpty();
        if(it->second.count_ == 1) {
            version_entry_fut = async::submit_io_task(CheckReloadTask{store, version_map, *symbol, it->second.load_param_});
        } else {
            auto fut = shared_futures.find(*symbol);
            if(fut == shared_futures.end()) {
                auto [splitter, inserted] = shared_futures.emplace(*symbol, folly::FutureSplitter(async::submit_io_task(CheckReloadTask{store, version_map, *symbol, it->second.load_param_})));

                version_entry_fut = splitter->second.getFuture();
            } else {
                version_entry_fut = fut->second.getFuture();
            }
        }
        output.push_back(std::move(version_entry_fut)
        .thenValue([version_query = version_queries[symbol.index]](auto version_map_entry) {
            return get_key_for_version_query(version_map_entry, version_query);
        }));
    }

    return output;
}

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
