/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/composite.hpp>
#include <folly/Range.h>
#include <arcticdb/storage/key_segment_pair.hpp>

namespace arcticdb::storage::memory {

class MemoryStorage final : public Storage<MemoryStorage> {

    using Parent = Storage<MemoryStorage>;
    friend Parent;

public:
    using Config = arcticdb::proto::memory_storage::Config;

    MemoryStorage(const LibraryPath& lib, OpenMode mode, const Config& conf);

protected:
    void do_write(Composite<KeySegmentPair>&& kvs);

    void do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts);

    template<class Visitor>
    void do_read(Composite<VariantKey>&& ks, Visitor&& visitor, ReadKeyOpts opts);

    void do_remove(Composite<VariantKey>&& ks, RemoveOpts opts);

    bool do_key_exists(const VariantKey& key);

    bool do_supports_prefix_matching()
    {
        return false;
    }

    inline bool do_fast_delete();

    template<class Visitor>
    void do_iterate_type(KeyType key_type, Visitor&& visitor, const std::string& prefix);

private:
    using KeyMap = std::unordered_map<VariantKey, Segment>;
    using TypeMap = std::unordered_map<KeyType, KeyMap>;
    using MutexType = std::recursive_mutex;

    TypeMap data_;
    std::unique_ptr<MutexType> mutex_; // Methods taking functions pointers may call back into the storage
};

class MemoryStorageFactory final : public StorageFactory<MemoryStorageFactory> {

    using Parent = StorageFactory<MemoryStorageFactory>;
    friend Parent;

public:
    using Config = arcticdb::proto::memory_storage::Config;
    using StorageType = MemoryStorage;

    MemoryStorageFactory(Config conf)
        : conf_(std::move(conf))
    {
    }

private:
    auto do_create_storage(LibraryPath lib, OpenMode mode)
    {
        return MemoryStorage(std::move(lib), mode, conf_);
    }

    Config conf_;
};

inline arcticdb::proto::storage::VariantStorage pack_config(uint64_t cache_size)
{
    arcticdb::proto::storage::VariantStorage output;
    arcticdb::proto::memory_storage::Config cfg;
    cfg.set_cache_size(cache_size);
    util::pack_to_any(cfg, *output.mutable_config());
    return output;
}

} // namespace arcticdb::storage::memory

#define ARCTICDB_MEMORY_STORAGE_H_
#include <arcticdb/storage/memory/memory_storage-inl.hpp>
#undef ARCTICDB_MEMORY_STORAGE_H_