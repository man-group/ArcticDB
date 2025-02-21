/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <memory>

#include <arcticdb/stream/stream_source.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/entity/protobufs.hpp>

namespace arcticdb {
/*
 * The Store class aims, as much as possible, to be a fundamental, non-leaky abstraction.
 *
 * It takes SegmentInMemory and writes them to a persistent storage, returning a key.
 * It also takes keys and returns several SegmentInMemory.
 *
 * Higher level operations can read from or write to a Store without any consideration of what sort of storage
 * is being written to, what compression (if any) is being applied, the encoding to be used, etc.
 *
 * At the time of writing, the main implementation is AsyncStore.
 */
class Store : public stream::StreamSink, public stream::StreamSource, public std::enable_shared_from_this<Store> {
  public:
    virtual void set_failure_sim(const arcticdb::proto::storage::VersionStoreConfig::StorageFailureSimulator& cfg) = 0;

    virtual void move_storage(KeyType key_type, timestamp horizon, size_t storage_index) = 0;

    virtual folly::Future<VariantKey> copy(
            KeyType key_type, const StreamId& stream_id, VersionId version_id, const VariantKey& source_key
    ) = 0;

    virtual VariantKey copy_sync(
            KeyType key_type, const StreamId& stream_id, VersionId version_id, const VariantKey& source_key
    ) = 0;

    virtual std::string name() const = 0;
};

} // namespace arcticdb