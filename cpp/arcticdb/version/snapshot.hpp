/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/stream/stream_writer.hpp>
#include <arcticdb/stream/stream_reader.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/storage/key_segment_pair.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/util/variant.hpp>

using namespace arcticdb::entity;
using namespace arcticdb::stream;

namespace arcticdb {

struct SnapshotRefData {
    explicit SnapshotRefData(SegmentInMemory&& snapshot_segment,
                             std::unordered_set<VariantKey>&& index_keys):
                             snapshot_segment_(std::move(snapshot_segment)),
                             index_keys_(std::move(index_keys)) {

    }
    SegmentInMemory snapshot_segment_;
    std::unordered_set<VariantKey> index_keys_;
};

void write_snapshot_entry(
    std::shared_ptr <StreamSink> store,
    std::vector <AtomKey> &keys,
    const SnapshotId &snapshot_id,
    const py::object &user_meta,
    bool log_changes,
    KeyType key_type = KeyType::SNAPSHOT_REF
);

void tombstone_snapshot(
    std::shared_ptr<StreamSink> store,
    const RefKey& key,
    SegmentInMemory&& segment_in_memory,
    bool log_changes
);

void tombstone_snapshot(
        std::shared_ptr<StreamSink> store,
        storage::KeySegmentPair&& key_segment_pair,
        bool log_changes
        );

void tombstone_snapshot(
        std::shared_ptr<Store> store,
        const RefKey& key,
        bool log_changes
        );


void iterate_snapshots(std::shared_ptr <Store> store, folly::Function<void(entity::VariantKey & )> visitor);

std::optional<size_t> row_id_for_stream_in_snapshot_segment(
    SegmentInMemory &seg,
    bool using_ref_key,
    StreamId stream_id);

// Get a set of the index keys of a particular symbol that exist in any snapshot
std::unordered_set<entity::AtomKey> get_index_keys_in_snapshots(
    std::shared_ptr <Store> store,
    const StreamId &stream_id);

std::pair<std::vector<AtomKey>, std::unordered_set<AtomKey>> get_index_keys_partitioned_by_inclusion_in_snapshots(
    std::shared_ptr <Store> store,
    const StreamId& stream_id,
    const std::vector<entity::AtomKey> &all_index_keys
);

std::pair<std::vector<AtomKey>, py::object> get_versions_and_metadata_from_snapshot(
    const std::shared_ptr<Store>& store,
    const VariantKey& vk
    );

std::optional<VariantKey> get_snapshot_key(
    std::shared_ptr <Store> store,
    const SnapshotId &snap_name);

std::optional<std::pair<VariantKey, SegmentInMemory>> get_snapshot(
    std::shared_ptr <Store> store,
    const SnapshotId &snap_name);

std::set<StreamId> list_streams_in_snapshot(
    const std::shared_ptr<Store>& store,
    SnapshotId snap_name);

using SnapshotMap = std::unordered_map<SnapshotId, std::vector<AtomKey>>;

SnapshotMap get_versions_from_snapshots(
    const std::shared_ptr<Store>& store
    );

// Returns a map from snapshot keys to it's associated segment, and the [multi-]index keys in that segment
std::unordered_map<VariantKey, SnapshotRefData> get_snapshots_and_index_keys(
    const std::shared_ptr<Store>& store
    );

py::object get_metadata_for_snapshot(std::shared_ptr <Store> store, VariantKey& snap_key);

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
    const std::optional<const std::tuple<const SnapshotVariantKey&, std::vector<IndexTypeKey>&>>& get_keys_in_snapshot =
            std::nullopt
);
}
