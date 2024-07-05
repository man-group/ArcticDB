/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/version/op_log.hpp>
#include <arcticdb/version/version_constants.hpp>

#include <folly/futures/Future.h>

namespace arcticdb {
    using namespace arcticdb::entity;
    using namespace arcticdb::stream;
    // Log events for passive sync
    inline StreamDescriptor log_stream_descriptor(const StreamId& event) {
        return stream_descriptor(event, RowCountIndex(), {});
    };

    inline void log_event(const std::shared_ptr<StreamSink>& store, const StreamId& id, std::string action, VersionId version_id=0) {
        static const auto max_trial_config = ConfigsMap::instance()->get_int("Storage.MaxOpLogWriteRetries", 2);
        auto max_trials = max_trial_config;
        while (true) {
            try {
                SegmentInMemory seg{log_stream_descriptor(action)};
                store->write_sync(KeyType::LOG, version_id, StreamId{action}, IndexValue{id}, IndexValue{id}, std::move(seg));
                break;
            } catch (const std::exception &err) {
                if (--max_trials <= 0) {
                    throw;
                }
                log::storage().warn(
                        "Failed to write op log for operation: {} for stream {} because of:{} . Retrying",
                        action, id, err.what());
                continue;
            }
        }
        
    }

    inline void log_write(const std::shared_ptr<StreamSink>& store, const StreamId& symbol,  VersionId version_id) {
        log_event(store, symbol, WriteVersionId, version_id);
    }

    inline void log_tombstone(const std::shared_ptr<StreamSink>& store, const StreamId& symbol,  VersionId version_id) {
        log_event(store, symbol, TombstoneVersionId, version_id);
    }

    inline void log_tombstone_all(const std::shared_ptr<StreamSink>& store, const StreamId& symbol,  VersionId version_id) {
        log_event(store, symbol, TombstoneAllVersionId, version_id);
    }

    inline void log_create_snapshot(const std::shared_ptr<StreamSink>& store, const SnapshotId& snapshot_id) {
        log_event(store, snapshot_id, CreateSnapshotId);
    }

    inline void log_delete_snapshot(const std::shared_ptr<StreamSink>& store, const SnapshotId& snapshot_id) {
        log_event(store, snapshot_id, DeleteSnapshotId);
    }

} //namespace arcticdb
