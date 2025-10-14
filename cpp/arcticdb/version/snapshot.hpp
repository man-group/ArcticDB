/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/storage/key_segment_pair.hpp>
#include <arcticdb/storage/store.hpp>

namespace arcticdb {

void write_snapshot_entry(
        std::shared_ptr<stream::StreamSink> store, std::vector<AtomKey>& keys, const SnapshotId& snapshot_id,
        const py::object& user_meta, bool log_changes, KeyType key_type = KeyType::SNAPSHOT_REF
);

void tombstone_snapshot(
        const std::shared_ptr<stream::StreamSink>& store, const RefKey& key, SegmentInMemory&& segment_in_memory,
        bool log_changes
);

void tombstone_snapshot(
        const std::shared_ptr<stream::StreamSink>& store, storage::KeySegmentPair& key_segment_pair, bool log_changes
);

void iterate_snapshots(const std::shared_ptr<Store>& store, folly::Function<void(entity::VariantKey&)> visitor);

std::optional<size_t> row_id_for_stream_in_snapshot_segment(
        SegmentInMemory& seg, bool using_ref_key, const StreamId& stream_id,
        const std::optional<VersionId> version_id = std::nullopt
);

// Get a set of the index keys of a particular symbol that exist in any snapshot
std::unordered_set<entity::AtomKey> get_index_keys_in_snapshots(
        const std::shared_ptr<Store>& store, const StreamId& stream_id
);

// Finds an index key of specified symbol/version in all snapshots. Returns std::nullopt if no such index key exists
std::optional<AtomKey> find_index_key_in_snapshots(
        const std::shared_ptr<Store>& store, const StreamId& stream_id, VersionId version_id
);

std::pair<std::vector<AtomKey>, std::unordered_set<AtomKey>> get_index_keys_partitioned_by_inclusion_in_snapshots(
        const std::shared_ptr<Store>& store, const StreamId& stream_id, std::vector<entity::AtomKey>&& all_index_keys
);

std::vector<AtomKey> get_versions_from_segment(const SegmentInMemory& snapshot_segment);

std::optional<VariantKey> get_snapshot_key(const std::shared_ptr<Store>& store, const SnapshotId& snap_name);

std::optional<std::pair<VariantKey, SegmentInMemory>> get_snapshot(
        const std::shared_ptr<Store>& store, const SnapshotId& snap_name
);

std::set<StreamId> list_streams_in_snapshot(const std::shared_ptr<Store>& store, const SnapshotId& snap_name);

using SnapshotMap = std::unordered_map<SnapshotId, std::vector<AtomKey>>;

SnapshotMap get_versions_from_snapshots(const std::shared_ptr<Store>& store);

std::unordered_map<SnapshotId, std::optional<VariantKey>> get_keys_for_snapshots(
        const std::shared_ptr<Store>& store, const std::vector<SnapshotId>& snap_names
);

/**
 * Stream id (symbol) -> all index keys in snapshots -> which snapshot contained that key.
 */
using MasterSnapshotMap =
        std::unordered_map<StreamId, std::unordered_map<IndexTypeKey, std::unordered_set<SnapshotId>>>;

/**
 * Map the index keys in every snapshot.
 * @param get_keys_in_snapshot If set, the VariantKey in the tuple should be a SNAPSHOT[_REF] key and the index keys in
 * that snapshot will be passed to the second item.
 */
MasterSnapshotMap get_master_snapshots_map(
        std::shared_ptr<Store> store,
        const std::optional<const std::tuple<const SnapshotVariantKey&, std::vector<IndexTypeKey>&>>&
                get_keys_in_snapshot = std::nullopt
);
} // namespace arcticdb
