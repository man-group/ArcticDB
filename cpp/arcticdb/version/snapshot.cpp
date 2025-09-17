/* Copyright 2023 Man Group Operations Limited
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
    if (!user_meta.is_none()) {
        arcticdb::proto::descriptors::UserDefinedMetadata user_meta_proto;
        google::protobuf::Any output = {};
        python_util::pb_from_python(user_meta, user_meta_proto);
        output.PackFrom(user_meta_proto);
        snapshot_agg.set_metadata(std::move(output));

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

void iterate_snapshots(const std::shared_ptr<Store>& store, folly::Function<void(entity::VariantKey&)> visitor) {
    ARCTICDB_SAMPLE(IterateSnapshots, 0)

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
        SegmentInMemory& seg, bool using_ref_key, const StreamId& stream_id
) {
    if (using_ref_key) {
        // With ref keys we are sure the snapshot segment has the index atom keys sorted by stream_id.
        auto lb = std::lower_bound(std::begin(seg), std::end(seg), stream_id, [&](auto& row, StreamId t) {
            auto row_stream_id = stream_id_from_segment<pipelines::index::Fields>(seg, row.row_id_);
            return row_stream_id < t;
        });

        if (lb == std::end(seg) || stream_id_from_segment<pipelines::index::Fields>(seg, lb->row_id_) != stream_id) {
            return std::nullopt;
        }
        return std::distance(std::begin(seg), lb);
    }
    // Fall back to linear search for old atom key snapshots.
    for (size_t idx = 0; idx < seg.row_count(); idx++) {
        auto row_stream_id = stream_id_from_segment<pipelines::index::Fields>(seg, static_cast<ssize_t>(idx));
        if (row_stream_id == stream_id) {
            return idx;
        }
    }
    return std::nullopt;
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

std::set<StreamId> list_streams_in_snapshot(const std::shared_ptr<Store>& store, const SnapshotId& snap_name) {
    ARCTICDB_SAMPLE(ListStreamsInSnapshot, 0)
    std::set<StreamId> res;
    auto opt_snap_key = get_snapshot(store, snap_name);

    if (!opt_snap_key)
        throw storage::NoDataFoundException(snap_name);

    const auto& snapshot_segment = opt_snap_key->second;

    for (size_t idx = 0; idx < snapshot_segment.row_count(); idx++) {
        auto stream_index = read_key_row(snapshot_segment, static_cast<ssize_t>(idx));
        res.insert(stream_index.id());
    }
    return res;
}

std::vector<AtomKey> get_versions_from_segment(const SegmentInMemory& snapshot_segment) {
    std::vector<AtomKey> res;
    for (size_t idx = 0; idx < snapshot_segment.row_count(); idx++) {
        auto stream_index = read_key_row(snapshot_segment, static_cast<ssize_t>(idx));
        res.push_back(stream_index);
    }
    return res;
}

std::vector<AtomKey> get_versions_from_snapshot(const std::shared_ptr<Store>& store, const VariantKey& vk) {

    auto snapshot_segment = store->read_sync(vk).second;
    return get_versions_from_segment(snapshot_segment);
}

SnapshotMap get_versions_from_snapshots(const std::shared_ptr<Store>& store) {
    ARCTICDB_SAMPLE(GetVersionsFromSnapshot, 0)
    SnapshotMap res;

    iterate_snapshots(store, [&res, &store](const VariantKey& vk) {
        SnapshotId snapshot_id{fmt::format("{}", variant_key_id(vk))};
        res[snapshot_id] = get_versions_from_snapshot(store, vk);
    });

    return res;
}

MasterSnapshotMap get_master_snapshots_map(
        std::shared_ptr<Store> store,
        const std::optional<const std::tuple<const SnapshotVariantKey&, std::vector<IndexTypeKey>&>>&
                get_keys_in_snapshot
) {
    MasterSnapshotMap out;
    iterate_snapshots(store, [&get_keys_in_snapshot, &out, &store](const VariantKey& sk) {
        auto snapshot_id = variant_key_id(sk);
        auto snapshot_segment = store->read_sync(sk).second;
        for (size_t idx = 0; idx < snapshot_segment.row_count(); idx++) {
            auto stream_index = read_key_row(snapshot_segment, static_cast<ssize_t>(idx));
            out[stream_index.id()][stream_index].insert(snapshot_id);
            if (get_keys_in_snapshot) {
                auto [wanted_snap_key, sink] = *get_keys_in_snapshot;
                if (wanted_snap_key == sk) {
                    sink.push_back(stream_index);
                }
            }
        }
    });
    return out;
}

} // namespace arcticdb
