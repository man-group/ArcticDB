/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <memory>

#include <arcticdb/stream/stream_source.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/entity/protobufs.hpp>

namespace arcticdb {
/*
 * Interface for a read/write compatible storage backend. At the time of writing, the main implementers are AsyncStore
 * and InMemoryStore.
 */
class Store : public stream::StreamSink, public stream::StreamSource, public std::enable_shared_from_this<Store> {
public:
    virtual void set_failure_sim(const arcticdb::proto::storage::VersionStoreConfig::StorageFailureSimulator& cfg) = 0;

    virtual void move_storage(KeyType key_type, timestamp horizon, size_t storage_index) = 0;

    virtual folly::Future<VariantKey> copy(KeyType key_type, const StreamId& stream_id, VersionId version_id, const VariantKey& source_key) = 0;

    virtual VariantKey copy_sync(KeyType key_type, const StreamId& stream_id, VersionId version_id, const VariantKey& source_key) = 0;
};

} // namespace arcticdb