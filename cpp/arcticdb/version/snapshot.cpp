/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/snapshot.hpp>
#include <arcticdb/version/version_log.hpp>
#include <arcticdb/stream/index_aggregator.hpp>
#include <arcticdb/python/python_utils.hpp>

#include <algorithm>

using namespace arcticdb::entity;
using namespace arcticdb::stream;

namespace arcticdb {

void write_snapshot_entry(
        std::shared_ptr<StreamSink> store, std::vector<AtomKey>& keys, const SnapshotId& snapshot_id,
        const py::object& user_meta, bool log_changes, KeyType key_type
) {
    ARCTICDB_SAMPLE(WriteJournalEntry, 0)
    ARCTICDB_RUNTIME_DEBUG(log::snapshot(), "Command: write snapshot entry");
    IndexAggregator<RowCountIndex> snapshot_agg(
            snapshot_id,
            [&store, key_type, &snapshot_id](SegmentInMemory&& segment) {
                store->write(key_type, snapshot_id, std::move(segment)).get();
            }
    );

    ARCTICDB_DEBUG(log::snapshot(), "Constructing snapshot {}", snapshot_id);
    // Most of the searches in snapshot are for a given symbol, this helps us do a binary search on the segment
    // on read time.
    std::sort(keys.begin(), keys.end(), [](const AtomKey& l, const AtomKey& r) { return l.id() < r.id(); });

    for (const auto& key : keys) {
        ARCTICDB_DEBUG(log::snapshot(), "Adding key {}", key);
        snapshot_agg.add_key(key);
    }

    // Serialize and store the python metadata in the journal entry for snapshot.
    if (auto output = python_util::py_metadata_to_any(user_meta)) {
        snapshot_agg.set_metadata(std::move(*output));

        // Bewared: Between v4.5.0 and v5.2.1 we only saved this metadata on the
        // timeseries_descriptor user_metadata field and we need to keep support for data serialized like
        // that.
        // TimeseriesDescriptor timeseries_descriptor;
        // python_util::pb_from_python(user_meta, *timeseries_descriptor.mutable_proto().mutable_user_meta());
        // snapshot_agg.set_timeseries_descriptor(timeseries_descriptor);
    }

    snapshot_agg.finalize();
    if (log_changes) {
        log_create_snapshot(store, snapshot_id);
    }
}

void tombstone_snapshot(
        const std::shared_ptr<StreamSink>& store, const RefKey& key, SegmentInMemory&& segment_in_memory,
        bool log_changes
) {
    store->remove_key_sync(key); // Make the snapshot "disappear" to normal APIs
    if (log_changes) {
        log_delete_snapshot(store, key.id());
    }
    // Append a timestamp to the ID so that other snapshot(s) can reuse the same snapshot name before the cleanup job:
    std::string new_key = fmt::format("{}@{:x}", key, util::SysClock::coarse_nanos_since_epoch() / 1'000'000);
    store->write(KeyType::SNAPSHOT_TOMBSTONE, new_key, std::move(segment_in_memory)).get();
}

void tombstone_snapshot(
        const std::shared_ptr<StreamSink>& store, storage::KeySegmentPair& key_segment_pair, bool log_changes
) {
    store->remove_key(key_segment_pair.ref_key()).get(); // Make the snapshot "disappear" to normal APIs
    if (log_changes) {
        log_delete_snapshot(store, key_segment_pair.ref_key().id());
    }
    // Append a timestamp to the ID so that other snapshot(s) can reuse the same snapshot name before the cleanup job:
    std::string new_key =
            fmt::format("{}@{:x}", key_segment_pair.ref_key(), util::SysClock::coarse_nanos_since_epoch() / 1'000'000);
    key_segment_pair.set_key(RefKey(std::move(new_key), KeyType::SNAPSHOT_TOMBSTONE));
    store->write_compressed(std::move(key_segment_pair)).get();
}

namespace {

std::vector<VariantKey> list_snapshot_keys(const std::shared_ptr<Store>& store) {
    std::vector<VariantKey> snap_variant_keys;
    std::unordered_set<SnapshotId> seen;

    store->iterate_type(KeyType::SNAPSHOT_REF, [&snap_variant_keys, &seen](VariantKey&& vk) {
        util::check(
                std::holds_alternative<RefKey>(vk),
                "Expected snapshot ref to be reference type, got {}",
                variant_key_view(vk)
        );
        auto ref_key = std::get<RefKey>(std::move(vk));
        seen.insert(ref_key.id());
        snap_variant_keys.emplace_back(ref_key);
    });

    store->iterate_type(KeyType::SNAPSHOT, [&snap_variant_keys, &seen](VariantKey&& vk) {
        auto key = to_atom(std::move(vk));
        if (seen.find(key.id()) == seen.end()) {
            snap_variant_keys.emplace_back(key);
        }
    });
    return snap_variant_keys;
}

// True if the exception only reports that snapshot_key is missing, i.e. the snapshot was deleted since listing
bool only_snapshot_is_missing(storage::KeyNotFoundException& e, const VariantKey& snapshot_key) {
    return e.has_keys() && std::all_of(e.keys().begin(), e.keys().end(), [&snapshot_key](const VariantKey& key) {
               return key == snapshot_key;
           });
}

/**
 * Reads each snapshot and applies process(snapshot_key, snapshot_segment) to it. Snapshots deleted since they were
 * listed give std::nullopt. The reads are parallel unless on an IO thread, where blocking on the IO pool could
 * deadlock, so process may run on IO threads and must be thread safe.
 */
template<typename Processor>
auto process_snapshots(
        const std::shared_ptr<Store>& store, const std::vector<VariantKey>& snapshot_keys, const Processor& process
) {
    using Result = std::invoke_result_t<const Processor&, const VariantKey&, const SegmentInMemory&>;
    std::vector<folly::Try<Result>> tries;
    tries.reserve(snapshot_keys.size());
    if (!async::is_io_pool_thread()) {
        const auto window_size = async::TaskScheduler::instance()->io_thread_count();
        auto futures = folly::window(
                snapshot_keys,
                [store, &process](const VariantKey& snapshot_key) {
                    return store->read(snapshot_key).thenValueInline([&process](auto&& key_seg) {
                        return process(key_seg.first, key_seg.second);
                    });
                },
                window_size
        );
        tries = folly::collectAll(futures).get();
    } else {
        for (const auto& snapshot_key : snapshot_keys) {
            tries.emplace_back(folly::makeTryWith([&store, &process, &snapshot_key]() {
                auto [key, segment] = store->read_sync(snapshot_key);
                return process(key, segment);
            }));
        }
    }

    std::vector<std::optional<Result>> results;
    results.reserve(tries.size());
    for (size_t i = 0; i < tries.size(); ++i) {
        try {
            results.emplace_back(std::move(tries[i]).value());
        } catch (storage::KeyNotFoundException& e) {
            if (!only_snapshot_is_missing(e, snapshot_keys[i])) {
                throw;
            }
            ARCTICDB_DEBUG(log::version(), "Ignored snapshot {} deleted since listing", snapshot_keys[i]);
            results.emplace_back(std::nullopt);
        }
    }
    return results;
}

/**
 * The index keys in the snapshot segment, only those of stream_ids if given. Always a full scan: missing a key here can
 * delete data a snapshot needs, so this does not rely on ref key snapshots being sorted by stream id.
 */
std::vector<AtomKey> index_keys_in_snapshot_segment(
        const SegmentInMemory& seg, const std::unordered_set<StreamId>* stream_ids
) {
    std::vector<AtomKey> res;
    const auto row_count = seg.row_count();
    if (!stream_ids) {
        res.reserve(row_count);
    } else if (stream_ids->empty()) {
        return res;
    }
    for (size_t idx = 0; idx < row_count; ++idx) {
        if (!stream_ids ||
            stream_ids->contains(stream_id_from_segment<pipelines::index::Fields>(seg, static_cast<ssize_t>(idx)))) {
            res.emplace_back(read_key_row(seg, static_cast<ssize_t>(idx)));
        }
    }
    return res;
}

} // namespace

void iterate_snapshots(const std::shared_ptr<Store>& store, folly::Function<void(entity::VariantKey&)> visitor) {
    ARCTICDB_SAMPLE(IterateSnapshots, 0)

    auto snap_variant_keys = list_snapshot_keys(store);
    for (auto& vk : snap_variant_keys) {
        try {
            visitor(vk);
        } catch (storage::KeyNotFoundException& e) {
            std::for_each(e.keys().begin(), e.keys().end(), [&vk, &e](const VariantKey& key) {
                if (key != vk)
                    throw storage::KeyNotFoundException(std::move(e.keys()));
            });
            ARCTICDB_DEBUG(log::version(), "Ignored exception due to {} being deleted during iterate_snapshots().");
        }
    }
}

std::optional<size_t> row_id_for_stream_in_snapshot_segment(
        SegmentInMemory& seg, bool using_ref_key, const StreamId& stream_id, const std::optional<VersionId> version_id
) {
    if (using_ref_key) {
        // With ref keys we are sure the snapshot segment has the index atom keys sorted by stream_id.
        auto lb = std::lower_bound(std::begin(seg), std::end(seg), stream_id, [&](auto& row, StreamId t) {
            auto row_stream_id = stream_id_from_segment<pipelines::index::Fields>(seg, row.row_id_);
            return row_stream_id < t;
        });
        if (lb == std::end(seg) || stream_id_from_segment<pipelines::index::Fields>(seg, lb->row_id_) != stream_id ||
            (version_id.has_value() &&
             version_id_from_segment<pipelines::index::Fields>(seg, lb->row_id_) != *version_id)) {
            return std::nullopt;
        } else {
            return std::distance(std::begin(seg), lb);
        }
    } else {
        // Fall back to linear search for old atom key snapshots.
        for (size_t idx = 0; idx < seg.row_count(); idx++) {
            // Check that the version id matches first if provided as this does not involve materialising a string from
            // the string pool
            if (!version_id.has_value() ||
                version_id_from_segment<pipelines::index::Fields>(seg, static_cast<ssize_t>(idx)) == *version_id) {
                auto row_stream_id = stream_id_from_segment<pipelines::index::Fields>(seg, static_cast<ssize_t>(idx));
                if (row_stream_id == stream_id) {
                    return idx;
                }
            }
        }
        return std::nullopt;
    }
}

std::unordered_set<entity::AtomKey> get_index_keys_in_snapshots(
        const std::shared_ptr<Store>& store, const StreamId& stream_id
) {
    ARCTICDB_SAMPLE(GetIndexKeysInSnapshot, 0)

    const std::unordered_set<StreamId> stream_ids{stream_id};
    const auto snapshot_keys = list_snapshot_keys(store);
    auto results = process_snapshots(
            store,
            snapshot_keys,
            [&stream_ids](const VariantKey&, const SegmentInMemory& snapshot_segment) {
                return index_keys_in_snapshot_segment(snapshot_segment, &stream_ids);
            }
    );

    std::unordered_set<entity::AtomKey> index_keys_in_snapshots{};
    for (auto& keys : results) {
        if (keys) {
            index_keys_in_snapshots.insert(
                    std::make_move_iterator(keys->begin()), std::make_move_iterator(keys->end())
            );
        }
    }
    return index_keys_in_snapshots;
}

std::optional<AtomKey> index_key_for_stream_and_version_in_snapshot_segment(
        SegmentInMemory& seg, bool using_ref_key, const StreamId& stream_id, VersionId version_id
) {
    if (auto opt_row_idx = row_id_for_stream_in_snapshot_segment(seg, using_ref_key, stream_id, version_id)) {
        return read_key_row(seg, *opt_row_idx);
    } else {
        return std::nullopt;
    }
}

std::optional<AtomKey> find_index_key_in_snapshots(
        const std::shared_ptr<Store>& store, const StreamId& stream_id, VersionId version_id
) {
    std::vector<VariantKey> snapshot_keys;
    iterate_snapshots(store, [&snapshot_keys](auto&& snapshot_key) {
        snapshot_keys.emplace_back(std::move(snapshot_key));
    });
    std::optional<AtomKey> res;
    std::atomic<bool> found{false};
    const auto window_size = async::TaskScheduler::instance()->io_thread_count();
    auto futures = folly::window(
            std::move(snapshot_keys),
            [store, &stream_id, version_id, &res, &found](const VariantKey& snapshot_key) {
                if (found.load()) {
                    return folly::makeFuture();
                } else {
                    return store->read(snapshot_key)
                            .thenValueInline([&stream_id, version_id, &res, &found](auto&& key_seg) {
                                auto snapshot_key = std::move(key_seg.first);
                                auto snapshot_segment = std::move(key_seg.second);
                                auto opt_res = index_key_for_stream_and_version_in_snapshot_segment(
                                        snapshot_segment,
                                        variant_key_type(snapshot_key) == KeyType::SNAPSHOT_REF,
                                        stream_id,
                                        version_id
                                );
                                if (opt_res.has_value()) {
                                    bool f{false};
                                    if (found.compare_exchange_strong(f, true)) {
                                        res = std::move(opt_res);
                                    }
                                }
                                return folly::Unit{};
                            });
                }
            },
            window_size
    );
    // Need collectAll in case snapshot keys were deleted since the listing operation
    folly::collectAll(futures).get();
    return res;
}

/**
 * Returned pair has first: keys not in snapshots, second: keys in snapshots.
 */
std::pair<std::vector<AtomKey>, std::unordered_set<AtomKey>> get_index_keys_partitioned_by_inclusion_in_snapshots(
        const std::shared_ptr<Store>& store, const StreamId& stream_id, std::vector<entity::AtomKey>&& all_index_keys
) {
    ARCTICDB_SAMPLE(GetIndexKeysPartitionedByInclusionInSnapshots, 0)
    auto index_keys_in_snapshot = get_index_keys_in_snapshots(store, stream_id);
    std::erase_if(all_index_keys, [&index_keys_in_snapshot](const auto& index_key) {
        return index_keys_in_snapshot.count(index_key) == 1;
    });
    return {std::move(all_index_keys), std::move(index_keys_in_snapshot)};
}

VariantKey get_ref_key(const SnapshotId& snap_name) { return RefKey{snap_name, KeyType::SNAPSHOT_REF}; }

std::optional<VariantKey> get_snapshot_key(const std::shared_ptr<Store>& store, const SnapshotId& snap_name) {
    ARCTICDB_SAMPLE(getSnapshot, 0)

    if (auto maybe_ref_key = get_ref_key(snap_name); store->key_exists_sync(maybe_ref_key))
        return maybe_ref_key;

    // Fall back to iteration
    ARCTICDB_DEBUG(log::version(), "Ref key not found for snapshot, falling back to slow path: {}", snap_name);
    std::optional<std::pair<VariantKey, SegmentInMemory>> opt_segment;

    std::optional<VariantKey> ret;
    store->iterate_type(
            KeyType::SNAPSHOT,
            [&ret, &snap_name](VariantKey&& vk) {
                if (variant_key_id(vk) == snap_name) {
                    ret = to_atom(vk);
                }
            },
            fmt::format("{}", snap_name)
    );
    return ret;
}

std::unordered_map<SnapshotId, std::optional<VariantKey>> all_ref_keys(
        const std::vector<SnapshotId>& snap_names, const std::vector<VariantKey>& ref_keys
) {
    std::unordered_map<SnapshotId, std::optional<VariantKey>> output;
    output.reserve(snap_names.size());
    for (auto name : folly::enumerate(snap_names))
        output.try_emplace(*name, ref_keys[name.index]);

    return output;
}

std::unordered_map<SnapshotId, std::optional<VariantKey>> get_snapshot_keys_via_iteration(
        const std::vector<bool>& ref_key_exists, const std::vector<SnapshotId>& snap_names,
        const std::vector<VariantKey>& ref_keys, const std::shared_ptr<Store>& store
) {
    std::unordered_map<SnapshotId, std::optional<VariantKey>> output;
    for (auto snap : folly::enumerate(snap_names)) {
        if (!ref_key_exists[snap.index])
            output.try_emplace(*snap, std::nullopt);
    }

    store->iterate_type(KeyType::SNAPSHOT, [&output](VariantKey&& vk) {
        if (auto it = output.find(variant_key_id(vk)); it != output.end())
            it->second = std::move(vk);
    });

    for (auto snap : folly::enumerate(snap_names)) {
        if (ref_key_exists[snap.index])
            output.try_emplace(*snap, ref_keys[snap.index]);
    }
    return output;
}

std::unordered_map<SnapshotId, std::optional<VariantKey>> get_keys_for_snapshots(
        const std::shared_ptr<Store>& store, const std::vector<SnapshotId>& snap_names
) {
    std::vector<VariantKey> ref_keys;
    ref_keys.resize(snap_names.size());
    std::transform(std::begin(snap_names), std::end(snap_names), std::begin(ref_keys), [](const auto& name) {
        return get_ref_key(name);
    });

    auto found_keys =
            folly::collect(store->batch_key_exists(ref_keys))
                    .via(&async::io_executor())
                    .thenValue([&snap_names, &ref_keys, store](std::vector<bool> ref_key_exists) {
                        if (std::all_of(std::begin(ref_key_exists), std::end(ref_key_exists), [](bool b) {
                                return b;
                            })) {
                            return all_ref_keys(snap_names, ref_keys);
                        } else {
                            return get_snapshot_keys_via_iteration(ref_key_exists, snap_names, ref_keys, store);
                        }
                    });

    return std::move(found_keys).get();
}

std::optional<std::pair<VariantKey, SegmentInMemory>> get_snapshot(
        const std::shared_ptr<Store>& store, const SnapshotId& snap_name
) {
    ARCTICDB_SAMPLE(getSnapshot, 0)
    auto opt_snap_key = get_snapshot_key(store, snap_name);
    if (!opt_snap_key)
        return std::nullopt;

    return store->read_sync(*opt_snap_key);
}

std::vector<AtomKey> get_versions_from_segment(
        const SegmentInMemory& snapshot_segment, const std::optional<StreamId>& stream_id
) {
    std::vector<AtomKey> res;
    for (size_t idx = 0; idx < snapshot_segment.row_count(); idx++) {
        auto stream_index = read_key_row(snapshot_segment, static_cast<ssize_t>(idx));
        if (!stream_id.has_value() || *stream_id == stream_index.id()) {
            res.push_back(std::move(stream_index));
        }
    }
    return res;
}

SnapshotMap get_versions_from_snapshots(const std::shared_ptr<Store>& store, const std::optional<StreamId>& stream_id) {
    ARCTICDB_SAMPLE(GetVersionsFromSnapshot, 0)
    SnapshotMap snapshot_map;
    std::vector<VariantKey> snapshot_keys;
    iterate_snapshots(store, [&snapshot_map, &snapshot_keys](auto&& snapshot_key) {
        snapshot_map.emplace(variant_key_id(snapshot_key), std::vector<AtomKey>{});
        snapshot_keys.emplace_back(std::move(snapshot_key));
    });
    const auto window_size = async::TaskScheduler::instance()->io_thread_count();
    auto futures = folly::window(
            std::move(snapshot_keys),
            [store, &snapshot_map, &stream_id](const VariantKey& snapshot_key) {
                return store->read(snapshot_key).thenValueInline([&snapshot_map, &stream_id](auto&& key_seg) {
                    const auto& snapshot_key = key_seg.first;
                    const auto& snapshot_segment = key_seg.second;
                    SnapshotId snapshot_id{fmt::format("{}", variant_key_id(snapshot_key))};
                    snapshot_map[snapshot_id] = get_versions_from_segment(snapshot_segment, stream_id);
                    return folly::Unit{};
                });
            },
            window_size
    );
    // Need collectAll in case snapshot keys were deleted since the listing operation
    folly::collectAll(futures).get();
    return snapshot_map;
}

MasterSnapshotMapWithStats get_master_snapshots_map_with_stats(
        std::shared_ptr<Store> store, const std::optional<std::unordered_set<StreamId>>& stream_ids
) {
    const auto snapshot_keys = list_snapshot_keys(store);
    const auto* wanted = stream_ids ? &*stream_ids : nullptr;
    auto results = process_snapshots(
            store,
            snapshot_keys,
            [wanted](const VariantKey&, const SegmentInMemory& snapshot_segment) {
                return index_keys_in_snapshot_segment(snapshot_segment, wanted);
            }
    );

    MasterSnapshotMapWithStats out;
    out.total_snapshots = snapshot_keys.size();
    for (size_t i = 0; i < results.size(); ++i) {
        if (results[i]) {
            const auto snapshot_id = variant_key_id(snapshot_keys[i]);
            for (auto& key : *results[i]) {
                auto& snapshot_ids = out.map[key.id()][key];
                snapshot_ids.insert(snapshot_id);
            }
        }
    }
    return out;
}

MasterSnapshotMap get_master_snapshots_map(
        std::shared_ptr<Store> store, const std::optional<std::unordered_set<StreamId>>& stream_ids
) {
    return get_master_snapshots_map_with_stats(std::move(store), stream_ids).map;
}

MasterSnapshotMapAndKeys get_master_snapshots_map_and_keys_in_given_snapshot(
        std::shared_ptr<Store> store, const SnapshotVariantKey& given_snapshot
) {
    const auto snapshot_keys = list_snapshot_keys(store);
    auto results =
            process_snapshots(store, snapshot_keys, [](const VariantKey&, const SegmentInMemory& snapshot_segment) {
                return index_keys_in_snapshot_segment(snapshot_segment, nullptr);
            });

    MasterSnapshotMapAndKeys out;
    for (size_t i = 0; i < results.size(); ++i) {
        if (results[i]) {
            const auto snapshot_id = variant_key_id(snapshot_keys[i]);
            const bool is_given_snapshot = given_snapshot == snapshot_keys[i];
            for (auto& key : *results[i]) {
                out.map[key.id()][key].insert(snapshot_id);
                if (is_given_snapshot) {
                    out.index_keys_in_given_snapshot.push_back(key);
                }
            }
        }
    }
    return out;
}

SnapshottedIndexKeys get_snapshotted_index_keys(
        const std::shared_ptr<Store>& store, const std::unordered_set<StreamId>& stream_ids,
        const std::optional<SnapshotId>& snapshot_to_ignore
) {
    ARCTICDB_SAMPLE(GetSnapshottedIndexKeys, 0)
    SnapshottedIndexKeys out;
    if (stream_ids.empty()) {
        return out;
    }
    auto snapshot_keys = list_snapshot_keys(store);
    if (snapshot_to_ignore) {
        std::erase_if(snapshot_keys, [&snapshot_to_ignore](const VariantKey& snapshot_key) {
            return variant_key_id(snapshot_key) == *snapshot_to_ignore;
        });
    }
    auto results = process_snapshots(
            store,
            snapshot_keys,
            [&stream_ids](const VariantKey&, const SegmentInMemory& snapshot_segment) {
                return index_keys_in_snapshot_segment(snapshot_segment, &stream_ids);
            }
    );
    for (auto& keys : results) {
        if (keys) {
            for (auto& key : *keys) {
                auto& symbol_keys = out[key.id()];
                symbol_keys.insert(std::move(key));
            }
        }
    }
    return out;
}

} // namespace arcticdb
