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

    class MemoryStorage final : public Storage {
    public:
        using Config = arcticdb::proto::memory_storage::Config;

        MemoryStorage(const LibraryPath &lib, OpenMode mode, const Config &conf);

    private:
        void do_write(Composite<KeySegmentPair>&& kvs) final;

        void do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) final;

        void do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, ReadKeyOpts opts) final;

        void do_remove(Composite<VariantKey>&& ks, RemoveOpts opts) final;

        bool do_key_exists(const VariantKey& key) final;

        bool do_supports_prefix_matching() const final {
            return false;
        }

        inline bool do_fast_delete() final;

        void do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string & prefix) final;

        std::string do_key_path(const VariantKey&) const final { return {}; };

        using KeyMap = std::unordered_map<VariantKey, Segment>;
        using TypeMap = std::unordered_map<KeyType, KeyMap>;
        using MutexType = std::recursive_mutex;

        TypeMap data_;
        std::unique_ptr<MutexType> mutex_;  // Methods taking functions pointers may call back into the storage
    };

    inline arcticdb::proto::storage::VariantStorage pack_config(uint64_t cache_size) {
        arcticdb::proto::storage::VariantStorage output;
        arcticdb::proto::memory_storage::Config cfg;
        cfg.set_cache_size(cache_size);
        util::pack_to_any(cfg, *output.mutable_config());
        return output;
    }

}//namespace arcticdbx::storage

#define ARCTICDB_MEMORY_STORAGE_H_
#include <arcticdb/storage/memory/memory_storage-inl.hpp>
#undef ARCTICDB_MEMORY_STORAGE_H_