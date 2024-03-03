/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/version_map_batch_methods.hpp>

namespace arcticdb {

StreamVersionData::StreamVersionData(const pipelines::VersionQuery &version_query) {
    react(version_query);
}

void StreamVersionData::react(const pipelines::VersionQuery &version_query) {
    util::variant_match(version_query.content_, [this](const auto &query) {
        do_react(query);
    });
}

void StreamVersionData::do_react(std::monostate) {
    if (load_param_.load_type_ == LoadType::NOT_LOADED)
        load_param_ = LoadParameter{LoadType::LOAD_LATEST_UNDELETED};

    ++count_;
}

void StreamVersionData::do_react(const pipelines::SpecificVersionQuery &specific_version) {
    ++count_;
    switch (load_param_.load_type_) {
    case LoadType::NOT_LOADED:
        [[fallthrough]];
    case LoadType::LOAD_LATEST_UNDELETED:
        load_param_ = LoadParameter{LoadType::LOAD_DOWNTO, specific_version.version_id_};
        break;
    case LoadType::LOAD_DOWNTO:
        util::check(load_param_.load_until_.has_value(),
                    "Expect LOAD_DOWNTO to have version specified");
        if ((specific_version.version_id_ >= 0 && is_positive_version_query(load_param_)) ||
            (specific_version.version_id_ < 0 && load_param_.load_until_.value() < 0)) {
            load_param_.load_until_ = std::min(load_param_.load_until_.value(), specific_version.version_id_);
        } else {
            load_param_ = LoadParameter{LoadType::LOAD_UNDELETED};
        }
        break;
    case LoadType::LOAD_FROM_TIME:
        [[fallthrough]];
    case LoadType::LOAD_UNDELETED:
        load_param_ = LoadParameter{LoadType::LOAD_UNDELETED};
        break;
    default:util::raise_rte("Unexpected load state {} applying specific version query", load_param_.load_type_);
    }
}

void StreamVersionData::do_react(const pipelines::TimestampVersionQuery &timestamp_query) {
    ++count_;
    switch (load_param_.load_type_) {
    case LoadType::NOT_LOADED:
        [[fallthrough]];
    case LoadType::LOAD_LATEST_UNDELETED:
        load_param_ = LoadParameter{LoadType::LOAD_FROM_TIME, timestamp_query.timestamp_};
        break;
    case LoadType::LOAD_FROM_TIME:
        util::check(load_param_.load_from_time_.has_value(),
                    "Expect LOAD_TO_TIME to have timestamp specified");
        load_param_.load_from_time_ = std::min(load_param_.load_from_time_.value(), timestamp_query.timestamp_);
        break;
    case LoadType::LOAD_DOWNTO:
        [[fallthrough]];
    case LoadType::LOAD_UNDELETED:
        load_param_ = LoadParameter{LoadType::LOAD_UNDELETED};
        break;
    default:util::raise_rte("Unexpected load state {} applying specific version query", load_param_.load_type_);
    }
}

void StreamVersionData::do_react(const pipelines::SnapshotVersionQuery &snapshot_query) {
    snapshots_.push_back(snapshot_query.name_);
}

std::optional<AtomKey> get_specific_version_from_entry(
    const std::shared_ptr<VersionMapEntry>& version_map_entry,
    const pipelines::SpecificVersionQuery& specific_version
    ) {
    auto signed_version_id = specific_version.version_id_;
    VersionId version_id;
    if (signed_version_id >= 0) {
        version_id = static_cast<VersionId>(signed_version_id);
    } else {
        auto opt_latest = version_map_entry->get_first_index(true);
        if (opt_latest.has_value()) {
            auto opt_version_id = get_version_id_negative_index(opt_latest->version_id(),
                                                                signed_version_id);
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
}

std::optional<AtomKey> get_version_map_entry_by_timestamp(
    const std::shared_ptr<VersionMapEntry>& version_map_entry,
    const pipelines::TimestampVersionQuery &timestamp_version
    ) {
    auto version_key = get_index_key_from_time(timestamp_version.timestamp_,
                                               version_map_entry->get_indexes(false));
    if (version_key.has_value()) {
        auto version_id = version_key.value().version_id();
        return find_index_key_for_version_id(version_id, version_map_entry, false);
    } else {
        return std::nullopt;
    }
}

inline std::optional<AtomKey> get_key_for_version_query(
    const std::shared_ptr<VersionMapEntry> &version_map_entry,
    const pipelines::VersionQuery &version_query) {
    return util::variant_match(version_query.content_,
        [&version_map_entry](const pipelines::SpecificVersionQuery &specific_version) {
            return get_specific_version_from_entry(version_map_entry, specific_version);
        },
        [&version_map_entry](const pipelines::TimestampVersionQuery &timestamp_version) {
            return get_version_map_entry_by_timestamp(version_map_entry, timestamp_version);
        },
        [&version_map_entry](const std::monostate &) {
           return version_map_entry->get_first_index(false);
        },
        [](const auto &) -> std::optional<AtomKey> {
           util::raise_rte("Unsupported version query type");
        });
}

struct SnapshotCountMap {
    std::unordered_map<SnapshotId, size_t> snapshot_counts_;

    explicit SnapshotCountMap(const robin_hood::unordered_flat_map<StreamId, StreamVersionData> &version_data) {
        for (const auto &[_, info] : version_data) {
            for (const auto &snapshot : info.snapshots_) {
                const auto it = snapshot_counts_.find(snapshot);
                if(it == std::end(snapshot_counts_))
                    snapshot_counts_.emplace(snapshot, 1);
                else
                    ++it->second;
            }
        }
    }

    std::vector<SnapshotId> snapshots() const {
        std::vector<SnapshotId> output;
        output.reserve(snapshot_counts_.size());
        for(const auto& [snapshot, _] : snapshot_counts_)
            output.emplace_back(snapshot);

        return output;
    }

    size_t get_size(const SnapshotId &snapshot) {
        const auto it = snapshot_counts_.find(std::cref(snapshot));
        util::check(it != snapshot_counts_.end(), "Missing snapshot data for snapshot {}", snapshot);
        return it->second;
    }
};

using SnapshotPair = std::pair<VariantKey, SegmentInMemory>;
using VersionEntryOrSnapshot = std::variant<std::shared_ptr<VersionMapEntry>, std::optional<SnapshotPair>>;
using SplitterType = folly::FutureSplitter<VersionEntryOrSnapshot>;
using SnapshotKeyMap = std::unordered_map<SnapshotId, std::optional<VariantKey>>;

folly::Future<VersionEntryOrSnapshot> set_up_snapshot_future(
    robin_hood::unordered_flat_map<StreamId, SplitterType> &snapshot_futures,
    const std::shared_ptr<SnapshotCountMap> &snapshot_count_map,
    const std::shared_ptr<SnapshotKeyMap> &snapshot_key_map,
    const pipelines::SnapshotVersionQuery &snapshot_query,
    const std::shared_ptr<Store> &store
) {
    auto num_snaps = snapshot_count_map->get_size(snapshot_query.name_);
    const auto snapshot_key = snapshot_key_map->find(snapshot_query.name_);
    util::check(snapshot_key != std::end(*snapshot_key_map),
                "Missing snapshot data for snapshot {}",
                snapshot_query.name_);
    if (!snapshot_key->second) {
        return folly::makeFuture(std::make_optional<SnapshotPair>());
    } else {
        if (num_snaps == 1) {
            return store->read(*snapshot_key->second).thenValue(
                [](SnapshotPair &&snapshot_pair) {
                    return VersionEntryOrSnapshot{std::move(snapshot_pair)};
                });
        } else {
            auto fut = snapshot_futures.find(snapshot_query.name_);
            if (fut == snapshot_futures.end()) {
                auto [splitter, _] = snapshot_futures.emplace(
                    snapshot_query.name_,
                    folly::FutureSplitter{
                        store->read(*snapshot_key->second).thenValue(
                            [snap_key =
                            *snapshot_key->second](std::pair<VariantKey, SegmentInMemory> snapshot_output) mutable -> VersionEntryOrSnapshot {
                                return SnapshotPair{std::move(snap_key), std::move(snapshot_output.second)};
                            })});

                return splitter->second.getFuture();
            } else {
                return fut->second.getFuture();
            }
        }
    }
}

folly::Future<VersionEntryOrSnapshot> set_up_version_future(
    const StreamId &symbol,
    const StreamVersionData &version_data,
    robin_hood::unordered_flat_map<StreamId, SplitterType> &version_futures,
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map
) {
    if (version_data.count_ == 1) {
        return async::submit_io_task(CheckReloadTask{store, version_map, symbol,
                                                     version_data.load_param_}).thenValue(
            [](std::shared_ptr<VersionMapEntry> version_map_entry) {
                return VersionEntryOrSnapshot{std::move(version_map_entry)};
            });
    } else {
        auto maybe_fut = version_futures.find(symbol);
        if (maybe_fut == version_futures.end()) {
            auto [splitter, inserted] = version_futures.emplace(symbol,
                folly::FutureSplitter{
                    async::submit_io_task(
                        CheckReloadTask{store,
                                        version_map,
                                        symbol,
                                        version_data.load_param_}).thenValue(
                        [](std::shared_ptr<VersionMapEntry> version_map_entry) {
                            return VersionEntryOrSnapshot{
                                std::move(version_map_entry)};
                        })});

            return splitter->second.getFuture();
        } else {
            return maybe_fut->second.getFuture();
        }
    }
}

std::vector<folly::Future<std::optional<AtomKey>>> batch_get_versions_async(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<VersionMap> &version_map,
    const std::vector<StreamId> &symbols,
    const std::vector<pipelines::VersionQuery> &version_queries) {
    ARCTICDB_SAMPLE(BatchGetVersion, 0)
    util::check(symbols.size() == version_queries.size(),
                "Symbol and version query list mismatch: {} != {}",
                symbols.size(),
                version_queries.size());

    robin_hood::unordered_flat_map<StreamId, StreamVersionData> version_data;
    for (const auto &symbol : folly::enumerate(symbols)) {
        auto it = version_data.find(*symbol);
        if (it == version_data.end()) {
            version_data.insert(robin_hood::pair<StreamId, StreamVersionData>(
                *symbol,
                StreamVersionData{version_queries[symbol.index]}));
        } else {
            it->second.react(version_queries[symbol.index]);
        }
    }

    auto snapshot_count_map = std::make_shared<SnapshotCountMap>(version_data);
    auto snapshot_key_map = std::make_shared<SnapshotKeyMap>(get_keys_for_snapshots(store, snapshot_count_map->snapshots()));

    robin_hood::unordered_flat_map<StreamId, SplitterType> snapshot_futures;
    robin_hood::unordered_flat_map<StreamId, SplitterType> version_futures;

    std::vector<folly::Future<std::optional<AtomKey>>> output;
    output.reserve(symbols.size());
    for (const auto &symbol : folly::enumerate(symbols)) {
        auto version_query = version_queries[symbol.index];
        auto version_entry_fut = folly::Future<VersionEntryOrSnapshot>::makeEmpty();
        util::variant_match(version_query.content_,
            [&version_entry_fut, &snapshot_count_map, &snapshot_key_map, &snapshot_futures, &store](
                const pipelines::SnapshotVersionQuery &snapshot_query) {
                version_entry_fut = set_up_snapshot_future(
                    snapshot_futures,
                    snapshot_count_map,
                    snapshot_key_map,
                    snapshot_query,
                    store
                );
            },
            [&version_entry_fut, &version_data, &symbol, &version_futures, &store, &version_map](
                const auto &) {
                const auto it = version_data.find(*symbol);
                util::check(it != version_data.end(), "Missing version data for symbol {}", *symbol);

                version_entry_fut = set_up_version_future(
                    *symbol,
                    it->second,
                    version_futures,
                    store,
                    version_map
                );
            });

        output.push_back(std::move(version_entry_fut)
             .thenValue([vq = version_query, sid = *symbol](auto version_or_snapshot) {
                 return util::variant_match(version_or_snapshot,
                    [&vq](const std::shared_ptr<VersionMapEntry> &version_map_entry) {
                        return get_key_for_version_query(version_map_entry, vq);
                    },
                    [&sid](std::optional<SnapshotPair> snapshot) {
                        if (!snapshot)
                            return std::make_optional<AtomKey>();

                        auto [snap_key, snap_segment] = std::move(*snapshot);
                        auto opt_id = row_id_for_stream_in_snapshot_segment(
                            snap_segment,
                            std::holds_alternative<RefKey>(snap_key),
                            sid);

                        return opt_id
                               ? std::make_optional<AtomKey>(read_key_row(snap_segment, static_cast<ssize_t>(opt_id.value())))
                               : std::nullopt;
                    });
             }));
    }
    return output;
}

} //namespace arcticdb