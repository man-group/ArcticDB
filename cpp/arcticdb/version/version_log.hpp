/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/version/op_log.hpp>
#include <arcticdb/version/version_constants.hpp>
#include <arcticdb/util/exponential_backoff.hpp>

namespace arcticdb {
using namespace arcticdb::entity;
using namespace arcticdb::stream;
// Log events for passive sync
inline StreamDescriptor log_stream_descriptor(const StreamId& event) {
    return stream_descriptor(event, RowCountIndex(), {});
};

inline void log_event(
        const std::shared_ptr<StreamSink>& store, const StreamId& id, std::string action, VersionId version_id = 0
) {
    ExponentialBackoff<StorageException>(100, 2000).go([&store, &id, &action, &version_id]() {
        SegmentInMemory seg{log_stream_descriptor(action)};
        store->write_sync(KeyType::LOG, version_id, StreamId{action}, IndexValue{id}, IndexValue{id}, std::move(seg));
    });
}

inline void log_write(const std::shared_ptr<StreamSink>& store, const StreamId& symbol, VersionId version_id) {
    log_event(store, symbol, WriteVersionId, version_id);
}

inline void log_tombstone(const std::shared_ptr<StreamSink>& store, const StreamId& symbol, VersionId version_id) {
    log_event(store, symbol, TombstoneVersionId, version_id);
}

inline void log_tombstone_all(const std::shared_ptr<StreamSink>& store, const StreamId& symbol, VersionId version_id) {
    log_event(store, symbol, TombstoneAllVersionId, version_id);
}

inline void log_create_snapshot(const std::shared_ptr<StreamSink>& store, const SnapshotId& snapshot_id) {
    log_event(store, snapshot_id, CreateSnapshotId);
}

inline void log_delete_snapshot(const std::shared_ptr<StreamSink>& store, const SnapshotId& snapshot_id) {
    log_event(store, snapshot_id, DeleteSnapshotId);
}

} // namespace arcticdb
