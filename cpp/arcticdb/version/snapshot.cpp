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

#include <folly/ScopeGuard.h>
#include <folly/Try.h>

#include <algorithm>
#include <atomic>
#include <mutex>
#include <numeric>
#include <string_view>

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

namespace {

// True if the exception only reports that snapshot_key is missing, i.e. the snapshot was deleted since listing
bool only_snapshot_is_missing(storage::KeyNotFoundException& e, const VariantKey& snapshot_key) {
    return !e.keys().empty() &&
           std::ranges::all_of(e.keys(), [&snapshot_key](const VariantKey& key) { return key == snapshot_key; });
}

// Matches key rows' stream ids against stream_ids without copying string ids out of the segment's string pool
class StreamIdFilter {
  public:
    explicit StreamIdFilter(std::unordered_set<StreamId> stream_ids) : stream_ids_(std::move(stream_ids)) {
        for (const auto& stream_id : stream_ids_) {
            if (const auto* string_id = std::get_if<StringId>(&stream_id)) {
                string_ids_.emplace(*string_id);
            }
        }
    }

    StreamIdFilter(const StreamIdFilter&) = delete;
    StreamIdFilter& operator=(const StreamIdFilter&) = delete;

    // The rows of seg whose stream id is one of stream_ids
    std::vector<ssize_t> matching_rows(const SegmentInMemory& seg) const {
        // An empty segment may have no columns, which has_legacy_key_row_layout needs
        if (seg.row_count() == 0 || stream_ids_.empty()) {
            return {};
        }
        return has_legacy_key_row_layout(seg) ? matching_rows_in_layout<pipelines::index::LegacyFields>(seg)
                                              : matching_rows_in_layout<pipelines::index::Fields>(seg);
    }

  private:
    template<typename FieldType>
    std::vector<ssize_t> matching_rows_in_layout(const SegmentInMemory& seg) const {
        const auto col = static_cast<position_t>(FieldType::stream_id);
        const auto& column = seg.column(col);
        const auto data_type = seg.descriptor()[col].type().data_type();
        std::vector<ssize_t> rows;
        if (column.is_sparse() || is_fixed_string_type(data_type)) {
            for (size_t row = 0; row < seg.row_count(); ++row) {
                if (stream_ids_.contains(stream_id_from_segment<FieldType>(seg, static_cast<ssize_t>(row)))) {
                    rows.push_back(static_cast<ssize_t>(row));
                }
            }
        } else if (is_sequence_type(data_type)) {
            // One pass over the offsets, rather than a lookup per row
            const auto offsets = column.clone_scalars_to_vector<entity::position_t>();
            for (size_t row = 0; row < offsets.size(); ++row) {
                if (is_a_string(offsets[row]) && string_ids_.contains(seg.string_at_offset(offsets[row]))) {
                    rows.push_back(static_cast<ssize_t>(row));
                }
            }
        } else {
            const auto ids = column.clone_scalars_to_vector<timestamp>();
            for (size_t row = 0; row < ids.size(); ++row) {
                // As stream_id_from_segment reads it
                if (stream_ids_.contains(StreamId{ids[row]})) {
                    rows.push_back(static_cast<ssize_t>(row));
                }
            }
        }
        return rows;
    }

    std::unordered_set<StreamId> stream_ids_;
    // Views of stream_ids_' strings
    std::unordered_set<std::string_view> string_ids_;
};

// Keys are built outside the merge lock, this many at a time
constexpr size_t merge_batch_size = 4096;

void add_to_master_map(MasterSnapshotMap& map, const VariantKey& snapshot_key, AtomKey&& key) {
    auto& keys_of_symbol = map[key.id()];
    keys_of_symbol[std::move(key)].insert(variant_key_id(snapshot_key));
}

} // namespace

void read_snapshots(
        const std::shared_ptr<Store>& store, const std::vector<VariantKey>& snapshot_keys,
        SnapshotSegmentCallback on_snapshot
) {
    // Set once on_snapshot asks to stop, or on an error, so no more reads are issued
    std::atomic<bool> stop{false};
    const auto process = [&on_snapshot, &snapshot_keys, &stop](size_t idx, const auto& read_segment) {
        const auto& snapshot_key = snapshot_keys[idx];
        SCOPE_FAIL { stop.store(true); };
        std::optional<SegmentInMemory> segment;
        try {
            segment = read_segment();
        } catch (storage::KeyNotFoundException& e) {
            if (!only_snapshot_is_missing(e, snapshot_key)) {
                throw;
            }
            ARCTICDB_DEBUG(log::version(), "Ignored snapshot {} deleted since listing", snapshot_key);
            return folly::Unit{};
        }
        if (!on_snapshot(idx, snapshot_key, std::move(*segment))) {
            stop.store(true);
        }
        return folly::Unit{};
    };
    std::vector<folly::Try<folly::Unit>> tries;
    tries.reserve(snapshot_keys.size());
    if (!async::is_io_pool_thread()) {
        std::vector<size_t> indexes(snapshot_keys.size());
        std::iota(indexes.begin(), indexes.end(), 0);
        const auto window_size = async::TaskScheduler::instance()->io_thread_count();
        auto futures = folly::window(
                std::move(indexes),
                [&store, &snapshot_keys, &process, &stop](size_t idx) {
                    if (stop.load()) {
                        return folly::makeFuture();
                    }
                    return store->read(snapshot_keys[idx])
                            .thenTryInline([&process, idx](folly::Try<std::pair<VariantKey, SegmentInMemory>>&& read) {
                                return process(idx, [&read]() { return std::move(read).value().second; });
                            });
                },
                window_size
        );
        tries = folly::collectAll(futures).get();
    } else {
        // Blocking on the IO pool from one of its threads could deadlock
        for (size_t idx = 0; idx < snapshot_keys.size() && !stop.load(); ++idx) {
            tries.emplace_back(folly::makeTryWith([&store, &snapshot_keys, &process, idx]() {
                return process(idx, [&store, &snapshot_keys, idx]() {
                    return store->read_sync(snapshot_keys[idx]).second;
                });
            }));
        }
    }
    for (auto& attempt : tries) {
        attempt.throwUnlessValue();
    }
}

namespace {

/**
 * Reads each snapshot and calls merge(snapshot_key, index_key) with each of its index keys, only those of stream_ids if
 * given. It scans every row, as it collects many symbols, and reads the legacy key row layout as read_key_row does.
 * Keys are built in parallel, in batches to bound memory; merge runs under a lock, possibly on an IO thread, so it must
 * not block.
 */
template<typename Merge>
void merge_snapshots(
        const std::shared_ptr<Store>& store, const std::vector<VariantKey>& snapshot_keys,
        const std::optional<std::unordered_set<StreamId>>& stream_ids, const Merge& merge
) {
    std::optional<StreamIdFilter> filter;
    if (stream_ids) {
        filter.emplace(*stream_ids);
    }
    std::mutex merge_mutex;
    read_snapshots(
            store,
            snapshot_keys,
            [&filter, &merge, &merge_mutex](size_t, const VariantKey& snapshot_key, SegmentInMemory&& segment) {
                std::optional<std::vector<ssize_t>> rows;
                if (filter) {
                    rows = filter->matching_rows(segment);
                }
                std::vector<AtomKey> batch;
                batch.reserve(std::min(merge_batch_size, rows ? rows->size() : segment.row_count()));
                const auto merge_batch = [&batch, &merge, &merge_mutex, &snapshot_key]() {
                    if (batch.empty()) {
                        return;
                    }
                    std::lock_guard lock(merge_mutex);
                    for (auto& key : batch) {
                        merge(snapshot_key, std::move(key));
                    }
                    batch.clear();
                };
                const auto add_row = [&batch, &segment, &merge_batch](ssize_t row) {
                    batch.push_back(read_key_row(segment, row));
                    if (batch.size() == merge_batch_size) {
                        merge_batch();
                    }
                };
                if (rows) {
                    for (const auto row : *rows) {
                        add_row(row);
                    }
                } else {
                    for (size_t row = 0; row < segment.row_count(); ++row) {
                        add_row(static_cast<ssize_t>(row));
                    }
                }
                merge_batch();
                return true;
            }
    );
}

/**
 * The row of stream_id, and of version_id if given, in the snapshot segment. A ref key segment is binary searched, as
 * write_snapshot_entry sorts it by stream id, unless it has the legacy key row layout.
 */
std::optional<ssize_t> row_of_stream_in_snapshot_segment(
        SegmentInMemory& seg, bool using_ref_key, const StreamId& stream_id, const std::optional<VersionId> version_id
) {
    if (seg.row_count() == 0) {
        return std::nullopt;
    }
    if (using_ref_key && !has_legacy_key_row_layout(seg)) {
        return row_id_for_stream_in_snapshot_segment(seg, true, stream_id, version_id);
    }
    const StreamIdFilter filter(std::unordered_set<StreamId>{stream_id});
    for (const auto row : filter.matching_rows(seg)) {
        if (!version_id || read_key_row(seg, row).version_id() == *version_id) {
            return row;
        }
    }
    return std::nullopt;
}

} // namespace

void iterate_snapshots(const std::shared_ptr<Store>& store, folly::Function<void(entity::VariantKey&)> visitor) {
    ARCTICDB_SAMPLE(IterateSnapshots, 0)

    auto snap_variant_keys = list_snapshot_keys(store);
    for (auto& vk : snap_variant_keys) {
        try {
            visitor(vk);
        } catch (storage::KeyNotFoundException& e) {
            // The snapshot was deleted after we listed it: skip it. Any other missing key, or none named, is a real
            // error.
            if (!only_snapshot_is_missing(e, vk)) {
                throw;
            }
            ARCTICDB_DEBUG(log::version(), "Ignored exception due to {} being deleted during iterate_snapshots().", vk);
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
    std::unordered_set<entity::AtomKey> index_keys_in_snapshots;
    std::mutex keys_mutex;
    read_snapshots(
            store,
            list_snapshot_keys(store),
            [&stream_id,
             &index_keys_in_snapshots,
             &keys_mutex](size_t, const VariantKey& snapshot_key, SegmentInMemory&& segment) {
                const auto row = row_of_stream_in_snapshot_segment(
                        segment, variant_key_type(snapshot_key) == KeyType::SNAPSHOT_REF, stream_id, std::nullopt
                );
                if (row) {
                    auto key = read_key_row(segment, *row);
                    std::lock_guard lock(keys_mutex);
                    index_keys_in_snapshots.insert(std::move(key));
                }
                return true;
            }
    );
    return index_keys_in_snapshots;
}

std::optional<AtomKey> index_key_for_stream_and_version_in_snapshot_segment(
        SegmentInMemory& seg, bool using_ref_key, const StreamId& stream_id, VersionId version_id
) {
    if (auto row = row_of_stream_in_snapshot_segment(seg, using_ref_key, stream_id, version_id)) {
        return read_key_row(seg, *row);
    }
    return std::nullopt;
}

std::optional<AtomKey> find_index_key_in_snapshots(
        const std::shared_ptr<Store>& store, const StreamId& stream_id, VersionId version_id
) {
    std::optional<AtomKey> res;
    std::mutex res_mutex;
    read_snapshots(
            store,
            list_snapshot_keys(store),
            [&stream_id, version_id, &res, &res_mutex](
                    size_t, const VariantKey& snapshot_key, SegmentInMemory&& segment
            ) {
                auto opt_res = index_key_for_stream_and_version_in_snapshot_segment(
                        segment, variant_key_type(snapshot_key) == KeyType::SNAPSHOT_REF, stream_id, version_id
                );
                if (!opt_res) {
                    return true;
                }
                std::lock_guard lock(res_mutex);
                if (!res) {
                    res = std::move(opt_res);
                }
                return false;
            }
    );
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
    const auto snapshot_keys = list_snapshot_keys(store);
    SnapshotMap snapshot_map;
    for (const auto& snapshot_key : snapshot_keys) {
        snapshot_map.emplace(variant_key_id(snapshot_key), std::vector<AtomKey>{});
    }
    std::optional<std::unordered_set<StreamId>> stream_ids;
    if (stream_id) {
        stream_ids.emplace(std::unordered_set<StreamId>{*stream_id});
    }
    merge_snapshots(store, snapshot_keys, stream_ids, [&snapshot_map](const VariantKey& snapshot_key, AtomKey&& key) {
        snapshot_map[variant_key_id(snapshot_key)].push_back(std::move(key));
    });
    return snapshot_map;
}

MasterSnapshotMapWithStats get_master_snapshots_map_with_stats(
        std::shared_ptr<Store> store, const std::optional<std::unordered_set<StreamId>>& stream_ids
) {
    const auto snapshot_keys = list_snapshot_keys(store);
    MasterSnapshotMapWithStats out;
    out.total_snapshots = snapshot_keys.size();
    merge_snapshots(store, snapshot_keys, stream_ids, [&out](const VariantKey& snapshot_key, AtomKey&& key) {
        add_to_master_map(out.map, snapshot_key, std::move(key));
    });
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
    merge_snapshots(
            store,
            list_snapshot_keys(store),
            std::nullopt,
            [&out, &given_snapshot](const VariantKey& snapshot_key, AtomKey&& key) {
                if (given_snapshot == snapshot_key) {
                    out.index_keys_in_given_snapshot.push_back(key);
                }
                add_to_master_map(out.map, snapshot_key, std::move(key));
            }
    );
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
    merge_snapshots(store, snapshot_keys, stream_ids, [&out](const VariantKey&, AtomKey&& key) {
        auto& keys_of_symbol = out[key.id()];
        keys_of_symbol.insert(std::move(key));
    });
    return out;
}

} // namespace arcticdb
