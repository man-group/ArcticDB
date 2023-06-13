/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/snapshot.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/version/version_log.hpp>

using namespace arcticdb::entity;
using namespace arcticdb::stream;

namespace arcticdb {

void write_snapshot_entry(std::shared_ptr<StreamSink> store,
    std::vector<AtomKey>& keys,
    const SnapshotId& snapshot_id,
    const py::object& user_meta,
    bool log_changes,
    KeyType key_type)
{
    ARCTICDB_SAMPLE(WriteJournalEntry, 0)
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: write snapshot entry");
    IndexAggregator<RowCountIndex> snapshot_agg(snapshot_id,
        [&](auto&& segment) { store->write(key_type, snapshot_id, std::move(segment)).get(); });

    // Most of the searches in snapshot are for a given symbol, this helps us do a binary search on the segment
    // on read time.
    std::sort(keys.begin(), keys.end(), [](const AtomKey& l, const AtomKey& r) { return l.id() < r.id(); });

    for (const auto& key : keys) {
        snapshot_agg.add_key(key);
    }
    // Serialize and store the python metadata in the journal entry for snapshot.
    if (!user_meta.is_none()) {
        arcticdb::proto::descriptors::UserDefinedMetadata user_meta_proto;
        google::protobuf::Any output = {};
        python_util::pb_from_python(user_meta, user_meta_proto);
        output.PackFrom(user_meta_proto);
        snapshot_agg.set_metadata(std::move(output));
    }

    snapshot_agg.commit();
    if (log_changes) {
        log_create_snapshot(store, snapshot_id);
    }
}

void tombstone_snapshot(std::shared_ptr<StreamSink> store,
    const RefKey& key,
    SegmentInMemory&& segment_in_memory,
    bool log_changes)
{
    store->remove_key_sync(key); // Make the snapshot "disappear" to normal APIs
    if (log_changes) {
        log_delete_snapshot(store, key.id());
    }
    // Append a timestamp to the ID so that other snapshot(s) can reuse the same snapshot name before the cleanup job:
    std::string new_key = fmt::format("{}@{:x}", key, util::SysClock::coarse_nanos_since_epoch() / 1'000'000);
    store->write(KeyType::SNAPSHOT_TOMBSTONE, new_key, std::move(segment_in_memory)).get();
}

void tombstone_snapshot(std::shared_ptr<StreamSink> store, storage::KeySegmentPair&& key_segment_pair, bool log_changes)
{
    store->remove_key(key_segment_pair.ref_key()).get(); // Make the snapshot "disappear" to normal APIs
    if (log_changes) {
        log_delete_snapshot(store, key_segment_pair.ref_key().id());
    }
    // Append a timestamp to the ID so that other snapshot(s) can reuse the same snapshot name before the cleanup job:
    std::string new_key =
        fmt::format("{}@{:x}", key_segment_pair.ref_key(), util::SysClock::coarse_nanos_since_epoch() / 1'000'000);
    key_segment_pair.ref_key() = RefKey(new_key, KeyType::SNAPSHOT_TOMBSTONE);
    store->write_compressed(std::move(key_segment_pair)).get();
}

// Returns true if snapshot with given key is successfully tombstoned, or false if the snapshot with this key doesn't exist
void tombstone_snapshot(std::shared_ptr<Store> store, const RefKey& key, bool log_changes)
{
    try {
        auto key_segment_pair = store->read_compressed(key).get();
        tombstone_snapshot(store, std::move(key_segment_pair), log_changes);
    } catch (const storage::KeyNotFoundException& e) {
        log::version().info("Cannot tombstone snapshot {}, key does not exist on the store", key);
    } catch (const std::exception& e) {
        log::version().error("Cannot tombstone snapshot {}: {}", key, e.what());
    }
}

void iterate_snapshots(std::shared_ptr<Store> store, folly::Function<void(entity::VariantKey&)> visitor)
{
    ARCTICDB_SAMPLE(IterateSnapshots, 0)

    std::vector<VariantKey> snap_variant_keys;
    std::unordered_set<SnapshotId> seen;

    store->iterate_type(KeyType::SNAPSHOT_REF, [&](VariantKey&& vk) {
        util::check(std::holds_alternative<RefKey>(vk),
            "Expected shapshot ref to be reference type, got {}",
            variant_key_view(vk));
        auto ref_key = std::get<RefKey>(vk);
        seen.insert(ref_key.id());
        snap_variant_keys.emplace_back(ref_key);
    });

    store->iterate_type(KeyType::SNAPSHOT, [&](VariantKey&& vk) {
        const auto& key = to_atom(vk);
        if (seen.find(key.id()) == seen.end()) {
            snap_variant_keys.push_back(key);
        }
    });

    for (auto& vk : snap_variant_keys) {
        try {
            visitor(vk);
        } catch (storage::KeyNotFoundException& e) {
            e.keys().broadcast([&vk, &e](const VariantKey& key) {
                if (key != vk)
                    throw storage::KeyNotFoundException(std::move(e.keys()));
            });
            log::version().info("Ignored exception due to {} being deleted during iterate_snapshots().");
        }
    }
}

std::optional<size_t>
row_id_for_stream_in_snapshot_segment(SegmentInMemory& seg, bool using_ref_key, StreamId stream_id)
{
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
        auto row_stream_id = stream_id_from_segment<pipelines::index::Fields>(seg, idx);
        if (row_stream_id == stream_id) {
            return idx;
        }
    }
    return std::nullopt;
}

std::unordered_map<VariantKey, SnapshotRefData> get_snapshots_and_index_keys(const std::shared_ptr<Store>& store)
{
    ARCTICDB_SAMPLE(GetSnapshotsAndIndexKeys, 0)
    std::unordered_map<VariantKey, SnapshotRefData> res;
    iterate_snapshots(store, [&store, &res](const VariantKey& vk) {
        try {
            auto segment = store->read_sync(vk).second;
            std::unordered_set<VariantKey> index_keys;
            for (size_t idx = 0; idx < segment.row_count(); idx++) {
                auto key = read_key_row(segment, idx);
                if (is_index_key_type(key.type())) {
                    index_keys.emplace(std::move(key));
                }
            }
            SnapshotRefData snapshot_ref_data{std::move(segment), std::move(index_keys)};
            res.try_emplace(vk, std::move(snapshot_ref_data));
        } catch (const storage::KeyNotFoundException& e) {
            log::version().info("Failed to read snapshot key: {}", e.what());
        }
    });
    return res;
}

std::unordered_set<entity::AtomKey> get_index_keys_in_snapshots(std::shared_ptr<Store> store, const StreamId& stream_id)
{
    ARCTICDB_SAMPLE(GetIndexKeysInSnapshot, 0)

    std::unordered_set<entity::AtomKey> index_keys_in_snapshots{};

    iterate_snapshots(store, [&](VariantKey& vk) {
        bool snapshot_using_ref = variant_key_type(vk) == KeyType::SNAPSHOT_REF;
        SegmentInMemory snapshot_segment = store->read_sync(vk).second;
        if (snapshot_segment.row_count() == 0) {
            // Snapshot has no rows, just skip this.
            ARCTICDB_DEBUG(log::version(),
                "Snapshot: {} does not have index keys (searching for symbol: {}), skipping.",
                variant_key_id(vk),
                stream_id);
            return;
        }
        auto opt_idx_for_stream_id =
            row_id_for_stream_in_snapshot_segment(snapshot_segment, snapshot_using_ref, stream_id);
        if (opt_idx_for_stream_id) {
            auto stream_idx = opt_idx_for_stream_id.value();
            index_keys_in_snapshots.insert(read_key_row(snapshot_segment, stream_idx));
        }
    });

    return index_keys_in_snapshots;
}

/**
 * Returned pair has first: keys not in snapshots, second: keys in snapshots.
 */
std::pair<std::vector<AtomKey>, std::unordered_set<AtomKey>> get_index_keys_partitioned_by_inclusion_in_snapshots(
    std::shared_ptr<Store> store,
    const StreamId& stream_id,
    const std::vector<entity::AtomKey>& all_index_keys)
{
    ARCTICDB_SAMPLE(GetIndexKeysPartitionedByInclusionInSnapshots, 0)
    auto index_keys_in_snapshot = get_index_keys_in_snapshots(store, stream_id);

    std::vector<entity::AtomKey> index_keys_not_in_snapshot;
    for (const auto& index_key : all_index_keys) {
        if (!index_keys_in_snapshot.count(index_key)) {
            index_keys_not_in_snapshot.emplace_back(index_key);
        }
    }

    return std::make_pair(index_keys_not_in_snapshot, index_keys_in_snapshot);
}

std::optional<VariantKey> get_snapshot_key(std::shared_ptr<Store> store, const SnapshotId& snap_name)
{
    ARCTICDB_SAMPLE(getSnapshot, 0)

    auto maybe_ref_key = RefKey{std::move(snap_name), KeyType::SNAPSHOT_REF};
    if (store->key_exists_sync(maybe_ref_key))
        return maybe_ref_key;

    // Fall back to iteration
    ARCTICDB_DEBUG(log::version(), "Ref key not found for snapshot, falling back to slow path: {}", snap_name);
    std::optional<std::pair<VariantKey, SegmentInMemory>> opt_segment = std::nullopt;

    std::optional<VariantKey> ret;
    store->iterate_type(
        KeyType::SNAPSHOT,
        [&ret, &snap_name](VariantKey&& vk) {
            auto snap_key = to_atom(vk);
            if (snap_key.id() == snap_name) {
                ret = snap_key;
            }
        },
        fmt::format("{}", snap_name));
    return ret;
}

std::optional<std::pair<VariantKey, SegmentInMemory>> get_snapshot(std::shared_ptr<Store> store,
    const SnapshotId& snap_name)
{
    ARCTICDB_SAMPLE(getSnapshot, 0)
    auto opt_snap_key = get_snapshot_key(store, snap_name);
    if (!opt_snap_key)
        return std::nullopt;

    return store->read_sync(*opt_snap_key);
}

std::set<StreamId> list_streams_in_snapshot(const std::shared_ptr<Store>& store, SnapshotId snap_name)
{
    ARCTICDB_SAMPLE(ListStreamsInSnapshot, 0)
    std::set<StreamId> res;
    auto opt_snap_key = get_snapshot(store, snap_name);

    if (!opt_snap_key)
        throw storage::NoDataFoundException(snap_name);

    auto& snapshot_segment = opt_snap_key.value().second;

    for (size_t idx = 0; idx < snapshot_segment.row_count(); idx++) {
        auto stream_index = read_key_row(snapshot_segment, idx);
        res.insert(stream_index.id());
    }
    return res;
}

namespace {
std::vector<AtomKey> get_versions_from_segment(const SegmentInMemory& snapshot_segment)
{
    std::vector<AtomKey> res;
    for (size_t idx = 0; idx < snapshot_segment.row_count(); idx++) {
        auto stream_index = read_key_row(snapshot_segment, idx);
        res.push_back(stream_index);
    }
    return res;
}

py::object get_metadata_from_segment(const SegmentInMemory& snapshot_segment)
{
    auto metadata_proto = snapshot_segment.metadata();
    py::object pyobj;
    if (metadata_proto) {
        arcticdb::proto::descriptors::UserDefinedMetadata user_meta_proto;
        metadata_proto->UnpackTo(&user_meta_proto);
        pyobj = python_util::pb_to_python(user_meta_proto);
    } else {
        pyobj = pybind11::none();
    }
    return pyobj;
}

std::vector<AtomKey> get_versions_from_snapshot(const std::shared_ptr<Store>& store, const VariantKey& vk)
{

    auto snapshot_segment = store->read_sync(vk).second;
    return get_versions_from_segment(snapshot_segment);
}
} //namespace

std::pair<std::vector<AtomKey>, py::object> get_versions_and_metadata_from_snapshot(const std::shared_ptr<Store>& store,
    const VariantKey& vk)
{
    auto snapshot_segment = store->read_sync(vk).second;
    return {get_versions_from_segment(snapshot_segment), get_metadata_from_segment(snapshot_segment)};
}

SnapshotMap get_versions_from_snapshots(const std::shared_ptr<Store>& store)
{
    ARCTICDB_SAMPLE(GetVersionsFromSnapshot, 0)
    SnapshotMap res;

    iterate_snapshots(store, [&](VariantKey& vk) {
        SnapshotId snapshot_id{fmt::format("{}", variant_key_id(vk))};
        res[snapshot_id] = get_versions_from_snapshot(store, vk);
    });

    return res;
}

//TODO I don't love having py:object here, return the protobuf and convert at the outer edge
py::object get_metadata_for_snapshot(std::shared_ptr<Store> store, VariantKey& snap_key)
{
    auto seg = store->read_sync(snap_key).second;
    return get_metadata_from_segment(seg);
}

MasterSnapshotMap get_master_snapshots_map(std::shared_ptr<Store> store,
    const std::optional<const std::tuple<const SnapshotVariantKey&, std::vector<IndexTypeKey>&>>& get_keys_in_snapshot)
{
    MasterSnapshotMap out;
    iterate_snapshots(store, [&](VariantKey& sk) {
        auto snapshot_id = variant_key_id(sk);
        auto snapshot_segment = store->read_sync(sk).second;
        for (size_t idx = 0; idx < snapshot_segment.row_count(); idx++) {
            auto stream_index = read_key_row(snapshot_segment, idx);
            out[stream_index.id()][stream_index].insert(snapshot_id);
            if (get_keys_in_snapshot) {
                auto [wanted_snap_key, sink] = get_keys_in_snapshot.value();
                if (wanted_snap_key == sk) {
                    sink.push_back(stream_index);
                }
            }
        }
    });
    return out;
}

} // namespace arcticdb
