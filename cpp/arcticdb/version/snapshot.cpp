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
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/python/python_utils.hpp>

#include <algorithm>
#include <mutex>
#include <numeric>

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

std::vector<VariantKey> list_snapshot_keys(const std::shared_ptr<Store>& store) {
    // SNAPSHOT_REF and the legacy SNAPSHOT key type live under different storage prefixes, so enumerating them
    // requires two independent listing operations. Both must happen - libraries written by older versions can still
    // hold SNAPSHOT keys - but they do not depend on each other, so run them concurrently instead of back to back.
    // Against object storage each listing costs a full round trip (~11ms on S3) that is otherwise paid twice by
    // every list_snapshots()/list_versions() call, even in a library with no snapshots at all.
    std::vector<AtomKey> legacy_keys;
    auto legacy_listing = folly::via(&async::io_executor(), [&store, &legacy_keys]() {
        store->iterate_type(KeyType::SNAPSHOT, [&legacy_keys](VariantKey&& vk) {
            legacy_keys.emplace_back(to_atom(std::move(vk)));
        });
    });

    std::vector<RefKey> ref_keys;
    std::exception_ptr ref_listing_exception;
    try {
        store->iterate_type(KeyType::SNAPSHOT_REF, [&ref_keys](VariantKey&& vk) {
            util::check(
                    std::holds_alternative<RefKey>(vk),
                    "Expected snapshot ref to be reference type, got {}",
                    variant_key_view(vk)
            );
            ref_keys.emplace_back(std::get<RefKey>(std::move(vk)));
        });
    } catch (...) {
        ref_listing_exception = std::current_exception();
    }

    // getTry() does not throw, so the background listing is always joined before its captured state goes out of
    // scope, whichever of the two listings failed.
    auto legacy_listing_result = std::move(legacy_listing).getTry();
    if (ref_listing_exception) {
        std::rethrow_exception(ref_listing_exception);
    }
    if (legacy_listing_result.hasException()) {
        legacy_listing_result.exception().throw_exception();
    }

    std::vector<VariantKey> snap_variant_keys;
    snap_variant_keys.reserve(ref_keys.size() + legacy_keys.size());
    std::unordered_set<SnapshotId> seen;
    seen.reserve(ref_keys.size());
    for (auto& ref_key : ref_keys) {
        seen.insert(ref_key.id());
        snap_variant_keys.emplace_back(std::move(ref_key));
    }
    for (auto& key : legacy_keys) {
        if (!seen.contains(key.id())) {
            snap_variant_keys.emplace_back(std::move(key));
        }
    }
    return snap_variant_keys;
}

void iterate_snapshots(const std::shared_ptr<Store>& store, folly::Function<void(entity::VariantKey&)> visitor) {
    ARCTICDB_SAMPLE(IterateSnapshots, 0)

    auto snap_variant_keys = list_snapshot_keys(store);

    for (auto& vk : snap_variant_keys) {
        try {
            visitor(vk);
        } catch (storage::KeyNotFoundException& e) {
            // An exception that names no key cannot be attributed to this snapshot, so it is not evidence that the
            // snapshot has gone. Propagate rather than dropping a snapshot that may still be there.
            if (!e.has_keys()) {
                throw;
            }
            std::for_each(e.keys().begin(), e.keys().end(), [&vk, &e](const VariantKey& key) {
                if (key != vk)
                    throw storage::KeyNotFoundException(std::move(e.keys()));
            });
            ARCTICDB_DEBUG(log::version(), "Ignored exception due to {} being deleted during iterate_snapshots().");
        }
    }
}

void check_only_deleted_snapshots_failed(
        const std::vector<folly::Try<folly::Unit>>& results, const std::vector<VariantKey>& snapshot_keys
) {
    for (size_t idx = 0; idx < results.size(); ++idx) {
        if (!results[idx].hasException()) {
            continue;
        }
        const auto& snapshot_key = snapshot_keys[idx];
        const auto* not_found = results[idx].tryGetExceptionObject<storage::KeyNotFoundException>();
        // A KeyNotFoundException that names no key is not evidence that this snapshot has gone, so it propagates
        // like any other failure rather than silently understating what the snapshots protect.
        if (!not_found || !not_found->has_keys() ||
            std::ranges::any_of(not_found->keys(), [&snapshot_key](const VariantKey& key) {
                return key != snapshot_key;
            })) {
            results[idx].exception().throw_exception();
        }
        ARCTICDB_DEBUG(
                log::version(), "Ignored {} being deleted during snapshot iteration", variant_key_view(snapshot_key)
        );
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

    std::unordered_set<entity::AtomKey> index_keys_in_snapshots{};

    iterate_snapshots(store, [&index_keys_in_snapshots, &store, &stream_id](const VariantKey& vk) {
        ARCTICDB_DEBUG(log::snapshot(), "Reading snapshot {}", vk);
        bool snapshot_using_ref = variant_key_type(vk) == KeyType::SNAPSHOT_REF;
        SegmentInMemory snapshot_segment = store->read_sync(vk).second;
        if (snapshot_segment.row_count() == 0) {
            // Snapshot has no rows, just skip this.
            ARCTICDB_DEBUG(
                    log::version(),
                    "Snapshot: {} does not have index keys (searching for symbol: {}), skipping.",
                    variant_key_id(vk),
                    stream_id
            );
            return;
        }
        auto opt_idx_for_stream_id =
                row_id_for_stream_in_snapshot_segment(snapshot_segment, snapshot_using_ref, stream_id);
        if (opt_idx_for_stream_id) {
            ARCTICDB_DEBUG(log::snapshot(), "Found index key for {} at {}", stream_id, *opt_idx_for_stream_id);
            auto stream_idx = *opt_idx_for_stream_id;
            index_keys_in_snapshots.emplace(read_key_row(snapshot_segment, static_cast<ssize_t>(stream_idx)));
        } else {
            ARCTICDB_DEBUG(log::snapshot(), "Failed to find index key for {}", stream_id);
        }
    });

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
    MasterSnapshotMapWithStats out;
    auto snapshot_keys = list_snapshot_keys(store);
    out.total_snapshots = snapshot_keys.size();

    // One blocking read per snapshot costs a storage round trip per snapshot on the calling thread. Read them
    // concurrently instead, as get_versions_from_snapshots() already does over the same segments. The map is
    // built under a lock so that only window_size segments are ever held in memory.
    std::mutex mutex;
    std::vector<size_t> indexes(snapshot_keys.size());
    std::iota(indexes.begin(), indexes.end(), 0);
    const auto window_size = async::TaskScheduler::instance()->io_thread_count();
    auto futures = folly::window(
            std::move(indexes),
            [store, &snapshot_keys, &out, &stream_ids, &mutex](size_t idx) {
                return store->read(snapshot_keys[idx]).thenValueInline([&out, &stream_ids, &mutex](auto&& key_seg) {
                    auto snapshot_id = variant_key_id(key_seg.first);
                    const auto& snapshot_segment = key_seg.second;
                    std::lock_guard lock{mutex};
                    for (size_t idx = 0; idx < snapshot_segment.row_count(); idx++) {
                        auto stream_index = read_key_row(snapshot_segment, static_cast<ssize_t>(idx));
                        if (!stream_ids || stream_ids->contains(stream_index.id())) {
                            out.map[stream_index.id()][stream_index].insert(snapshot_id);
                        }
                    }
                    return folly::Unit{};
                });
            },
            window_size
    );
    // Need collectAll in case snapshot keys were deleted since the listing operation. This map decides which
    // index keys a delete is allowed to remove, so only a snapshot that has genuinely gone may be dropped from
    // it: anything else must propagate rather than silently understate what the snapshots protect.
    check_only_deleted_snapshots_failed(folly::collectAll(futures).get(), snapshot_keys);
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
    MasterSnapshotMapAndKeys out;
    iterate_snapshots(store, [&given_snapshot, &out, &store](const VariantKey& sk) {
        auto snapshot_id = variant_key_id(sk);
        auto snapshot_segment = store->read_sync(sk).second;
        for (size_t idx = 0; idx < snapshot_segment.row_count(); idx++) {
            auto stream_index = read_key_row(snapshot_segment, static_cast<ssize_t>(idx));
            out.map[stream_index.id()][stream_index].insert(snapshot_id);
            if (given_snapshot == sk) {
                out.index_keys_in_given_snapshot.push_back(stream_index);
            }
        }
    });
    return out;
}

} // namespace arcticdb
