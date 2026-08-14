/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/version/version_map.hpp>
#include <arcticdb/version/version_tasks.hpp>
#include <arcticdb/version/version_store_objects.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/version/snapshot.hpp>

namespace arcticdb {

struct SymbolStatus {
    const VersionId version_id_ = 0;
    const bool exists_ = false;
    const timestamp timestamp_ = 0;

    SymbolStatus(VersionId version_id, bool exists, timestamp ts) :
        version_id_(version_id),
        exists_(exists),
        timestamp_(ts) {}
};

enum class BatchGetVersionOption { LIVE_AND_TOMBSTONED_VER_REF_IN_OTHER_SNAPSHOT, ALL_VER_FOUND_IN_STORAGE, COUNT };

inline std::optional<std::string> collect_futures_exceptions(auto&& futures) {
    std::optional<std::string> all_exceptions;
    for (auto&& collected_fut : futures) {
        if (!collected_fut.hasValue()) {
            all_exceptions =
                    all_exceptions.value_or("").append(collected_fut.exception().what().toStdString()).append("\n");
        }
    }
    return all_exceptions;
}

template<typename Inputs, typename TaskSubmitter, typename ResultHandler>
inline void submit_tasks_for_range(Inputs inputs, TaskSubmitter submitter, ResultHandler result_handler) {
    const auto window_size = async::TaskScheduler::instance()->io_thread_count() * 2;

    auto futures = folly::window(
            std::move(inputs),
            [&submitter, &result_handler](const auto& input) {
                return submitter(input).thenValue([&result_handler, &input](auto&& r) {
                    auto result = std::forward<decltype(r)>(r);
                    result_handler(input, std::move(result));
                    return folly::Unit{};
                });
            },
            window_size
    );

    auto collected_futs = folly::collectAll(futures).get();
    std::optional<std::string> all_exceptions = collect_futures_exceptions(std::move(collected_futs));
    internal::check<ErrorCode::E_RUNTIME_ERROR>(!all_exceptions.has_value(), all_exceptions.value_or(""));
}

inline std::shared_ptr<std::unordered_map<StreamId, SymbolStatus>> batch_check_latest_id_and_status(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map,
        const std::shared_ptr<std::vector<StreamId>>& symbols
) {
    ARCTICDB_SAMPLE(BatchGetLatestVersion, 0)
    const LoadStrategy load_strategy{LoadType::LATEST, LoadObjective::UNDELETED_ONLY};
    auto output = std::make_shared<std::unordered_map<StreamId, SymbolStatus>>();
    auto mutex = std::make_shared<std::mutex>();

    submit_tasks_for_range(
            *symbols,
            [store, version_map, &load_strategy](auto& symbol) {
                return async::submit_io_task(CheckReloadTask{store, version_map, symbol, load_strategy});
            },
            [output, mutex](const auto& id, const std::shared_ptr<VersionMapEntry>& entry) {
                auto index_key = entry->get_first_index(false).first;
                if (index_key) {
                    std::lock_guard lock{*mutex};
                    output->insert(std::make_pair<StreamId, SymbolStatus>(
                            StreamId{id}, {index_key->version_id(), true, index_key->creation_ts()}
                    ));
                } else {
                    index_key = entry->get_first_index(true).first;
                    if (index_key) {
                        std::lock_guard lock{*mutex};
                        output->insert(std::make_pair<StreamId, SymbolStatus>(
                                StreamId{id}, {index_key->version_id(), false, index_key->creation_ts()}
                        ));
                    } else {
                        if (entry->head_ && entry->head_->type() == KeyType::TOMBSTONE_ALL) {
                            const auto& head = *entry->head_;
                            std::lock_guard lock{*mutex};
                            output->insert(std::make_pair<StreamId, SymbolStatus>(
                                    StreamId{id}, {head.version_id(), false, head.creation_ts()}
                            ));
                        }
                    }
                }
            }
    );

    return output;
}

inline std::shared_ptr<std::unordered_map<StreamId, MaybeDeletedAtomKey>> batch_get_latest_version_with_deletion_info(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map,
        const std::vector<StreamId>& stream_ids, bool include_deleted
) {
    ARCTICDB_SAMPLE(BatchGetLatestVersion, 0)
    const LoadStrategy load_strategy{
            LoadType::LATEST, include_deleted ? LoadObjective::INCLUDE_DELETED : LoadObjective::UNDELETED_ONLY
    };
    auto output = std::make_shared<std::unordered_map<StreamId, MaybeDeletedAtomKey>>();
    auto mutex = std::make_shared<std::mutex>();

    submit_tasks_for_range(
            stream_ids,
            [store, version_map, &load_strategy](auto& stream_id) {
                return async::submit_io_task(CheckReloadTask{store, version_map, stream_id, load_strategy});
            },
            [output, include_deleted, mutex](auto id, auto entry) {
                auto [index_key, deleted] = entry->get_first_index(include_deleted);
                if (index_key) {
                    std::lock_guard lock{*mutex};
                    (*output)[id] = MaybeDeletedAtomKey{*index_key, deleted};
                }
            }
    );

    return output;
}

inline std::shared_ptr<std::unordered_map<StreamId, AtomKey>> batch_get_latest_version(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map,
        const std::vector<StreamId>& stream_ids, bool include_deleted
) {
    ARCTICDB_SAMPLE(BatchGetLatestVersion, 0)
    const LoadStrategy load_strategy{
            LoadType::LATEST, include_deleted ? LoadObjective::INCLUDE_DELETED : LoadObjective::UNDELETED_ONLY
    };
    auto output = std::make_shared<std::unordered_map<StreamId, AtomKey>>();
    auto mutex = std::make_shared<std::mutex>();

    submit_tasks_for_range(
            stream_ids,
            [store, version_map, &load_strategy](auto& stream_id) {
                return async::submit_io_task(CheckReloadTask{store, version_map, stream_id, load_strategy});
            },
            [output, include_deleted, mutex](auto id, auto entry) {
                auto [index_key, deleted] = entry->get_first_index(include_deleted);
                if (index_key) {
                    std::lock_guard lock{*mutex};
                    (*output)[id] = *index_key;
                }
            }
    );

    return output;
}

inline std::vector<folly::Future<std::pair<std::optional<AtomKey>, std::optional<AtomKey>>>>
batch_get_latest_undeleted_and_latest_versions_async(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map,
        const std::vector<StreamId>& stream_ids
) {
    ARCTICDB_SAMPLE(BatchGetLatestUndeletedVersionAndNextVersionId, 0)
    std::vector<folly::Future<std::pair<std::optional<AtomKey>, std::optional<AtomKey>>>> vector_fut;
    for (auto& stream_id : stream_ids) {
        vector_fut.push_back(async::submit_io_task(CheckReloadTask{
                                                           store,
                                                           version_map,
                                                           stream_id,
                                                           LoadStrategy{LoadType::LATEST, LoadObjective::UNDELETED_ONLY}
                                                   }
        ).thenValue([](const std::shared_ptr<VersionMapEntry>& entry) {
            return std::make_pair(entry->get_first_index(false).first, entry->get_first_index(true).first);
        }));
    }
    return vector_fut;
}

inline std::vector<folly::Future<version_store::UpdateInfo>>
batch_get_next_version_id_and_optionally_latest_undeleted_version_async(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map,
        const std::vector<StreamId>& stream_ids, bool get_latest_undeleted_version = true
) {
    ARCTICDB_SAMPLE(BatchGetLatestUndeletedVersionAndNextVersionId, 0)
    std::vector<folly::Future<version_store::UpdateInfo>> vector_fut;
    for (auto& stream_id : stream_ids) {
        // Note that despite LoadObjective::UNDELETED_ONLY only loading the latest undeleted version, it will always
        // also load the latest (possibly deleted) version due to the structure of the version chain. This is because a
        // tombstone or tombstone all key deleting a version is always closer to the head of the chain than the index
        // key it is deleting.
        LoadStrategy load_strategy{
                LoadType::LATEST,
                get_latest_undeleted_version ? LoadObjective::UNDELETED_ONLY : LoadObjective::INCLUDE_DELETED
        };
        vector_fut.push_back(async::submit_io_task(CheckReloadTask{store, version_map, stream_id, load_strategy})
                                     .thenValue([](auto entry) { return populate_update_info(*entry); }));
    }
    return vector_fut;
}

using VersionVectorType = std::vector<VersionId>;
using SymbolVersion = std::pair<StreamId, VersionId>;

template<typename FoundKey>
struct BatchGetVersionResult {
    std::unordered_map<FoundKey, AtomKey> found;
    std::vector<SymbolVersion> rejected;
};

/**
 * Returns multiple versions for the same symbol.
 * Only non-negative version per symbol is supported.
 *
 * When option is LIVE_AND_TOMBSTONED_VER_REF_IN_OTHER_SNAPSHOT:
 *  - found: LIVE versions + TOMBSTONED versions referenced in another snapshot
 *  - rejected: TOMBSTONED versions not preserved in any snapshot
 * Versions that never existed (symbol or version number) are silently skipped.
 */
inline BatchGetVersionResult<SymbolVersion> batch_get_specific_versions(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map,
        const std::map<StreamId, VersionVectorType>& sym_versions,
        BatchGetVersionOption option = BatchGetVersionOption::ALL_VER_FOUND_IN_STORAGE
) {
    ARCTICDB_SAMPLE(BatchGetLatestVersion, 0)
    auto found = std::make_shared<std::unordered_map<SymbolVersion, AtomKey>>();
    auto found_mutex = std::make_shared<std::mutex>();
    auto tombstoned_vers = std::make_shared<std::vector<std::pair<SymbolVersion, AtomKey>>>();
    auto tombstoned_mutex = std::make_shared<std::mutex>();
    auto rejected = std::make_shared<std::vector<SymbolVersion>>();

    auto objective = option == BatchGetVersionOption::LIVE_AND_TOMBSTONED_VER_REF_IN_OTHER_SNAPSHOT
                             ? LoadObjective::INCLUDE_DELETED
                             : LoadObjective::UNDELETED_ONLY;

    auto tasks_input = std::vector(sym_versions.begin(), sym_versions.end());
    submit_tasks_for_range(
            std::move(tasks_input),
            [store, version_map, objective](auto sym_version) {
                auto first_version = *std::min_element(std::begin(sym_version.second), std::end(sym_version.second));
                LoadStrategy load_strategy{LoadType::DOWNTO, objective, static_cast<SignedVersionId>(first_version)};
                return async::submit_io_task(CheckReloadTask{store, version_map, sym_version.first, load_strategy});
            },

            [found, option, &sym_versions, found_mutex, tombstoned_vers, tombstoned_mutex](
                    auto sym_version, const std::shared_ptr<VersionMapEntry>& entry
            ) {
                auto sym_it = sym_versions.find(sym_version.first);
                util::check(sym_it != sym_versions.end(), "Failed to find versions for symbol {}", sym_version.first);
                const auto& versions = sym_it->second;

                for (auto version : versions) {
                    auto version_details = find_index_key_for_version_id_and_tombstone_status(version, entry);
                    if ((option == BatchGetVersionOption::ALL_VER_FOUND_IN_STORAGE &&
                         version_details.version_status_ == VersionStatus::TOMBSTONED) ||
                        version_details.version_status_ == VersionStatus::LIVE) {
                        std::lock_guard lock{*found_mutex};
                        (*found)[SymbolVersion(sym_version.first, version)] = version_details.key_.value();
                    } else if (option == BatchGetVersionOption::LIVE_AND_TOMBSTONED_VER_REF_IN_OTHER_SNAPSHOT &&
                               version_details.version_status_ == VersionStatus::TOMBSTONED) {
                        // Need to allow tombstoned version but referenced in other snapshot(s) can be "re-snapshot"
                        log::version().warn(
                                "Version {} for symbol {} is tombstoned, need to check snapshots (this can be slow)",
                                version,
                                sym_version.first
                        );
                        std::lock_guard lock{*tombstoned_mutex};
                        tombstoned_vers->emplace_back(
                                SymbolVersion(sym_version.first, version), version_details.key_.value()
                        );
                    }
                }
            }
    );

    if (!tombstoned_vers->empty()) {
        std::unordered_set<StreamId> stream_ids;
        stream_ids.reserve(tombstoned_vers->size());
        for (const auto& [sym_version, key] : *tombstoned_vers) {
            stream_ids.insert(sym_version.first);
        }
        const auto snap_map = get_master_snapshots_map(store, stream_ids);
        for (const auto& [sym_version, key] : *tombstoned_vers) {
            auto cit = snap_map.find(sym_version.first);
            if (cit != snap_map.cend() &&
                std::any_of(cit->second.cbegin(), cit->second.cend(), [&sym_version](const auto& key_and_snapshot_ids) {
                    return key_and_snapshot_ids.first.version_id() == sym_version.second;
                })) {
                (*found)[sym_version] = key;
            } else {
                rejected->push_back(sym_version);
            }
        }
    }

    return {std::move(*found), std::move(*rejected)};
}

// Convenience wrapper: assumes at most one version per symbol. Delegates to batch_get_specific_versions.
inline BatchGetVersionResult<StreamId> batch_get_specific_version(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map,
        const std::map<StreamId, VersionId>& sym_versions,
        BatchGetVersionOption option = BatchGetVersionOption::ALL_VER_FOUND_IN_STORAGE
) {
    std::map<StreamId, VersionVectorType> multi_sym_versions;
    for (const auto& [stream_id, version_id] : sym_versions) {
        multi_sym_versions[stream_id] = {version_id};
    }

    auto result = batch_get_specific_versions(store, version_map, multi_sym_versions, option);

    std::unordered_map<StreamId, AtomKey> found;
    for (auto& [sym_version, key] : result.found) {
        found[sym_version.first] = std::move(key);
    }
    return {std::move(found), std::move(result.rejected)};
}

// [StreamVersionData] is used to combine different [VersionQuery]s for a stream_id into a list of needed snapshots and
// a single [LoadStrategy] which will query the union of all version queries.
// It only ever produces load parameters where to_load=UNDELETED_ONLY.
struct StreamVersionData {
    size_t count_ = 0;
    LoadStrategy load_strategy_ = LoadStrategy{LoadType::NOT_LOADED, LoadObjective::UNDELETED_ONLY};
    boost::container::small_vector<SnapshotId, 1> snapshots_;

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
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map,
        const std::vector<StreamId>& symbols, const std::vector<pipelines::VersionQuery>& version_queries
);

} // namespace arcticdb
