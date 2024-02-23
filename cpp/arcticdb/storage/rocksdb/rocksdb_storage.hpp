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

#include <rocksdb/db.h>

namespace arcticdb::storage::rocksdb {

    class RocksDBStorage final : public Storage {
    public:
        using Config = arcticdb::proto::rocksdb_storage::Config;

        RocksDBStorage(const LibraryPath &lib, OpenMode mode, const Config &conf);
        ~RocksDBStorage() override;

    private:
        void do_write(Composite<KeySegmentPair>&& kvs) final override;

        void do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) final override;

        void do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, ReadKeyOpts opts) final override;

        void do_remove(Composite<VariantKey>&& ks, RemoveOpts opts) final override;

        bool do_key_exists(const VariantKey& key) final override;

        std::string do_key_path(const VariantKey&) const final override { return {}; };

        bool do_supports_prefix_matching() const final override {
            return false;
        }

        inline bool do_fast_delete() final override;

        void do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string & prefix) final override;

        // The _internal methods remove code duplication across update, write, and read methods.
        void do_write_internal(Composite<KeySegmentPair>&& kvs);
        std::vector<VariantKey> do_remove_internal(Composite<VariantKey>&& ks, RemoveOpts opts);

        // See PR 945 for why raw pointers were used over unique ptrs here.
        ::rocksdb::DB* db_;
        using MapKeyType = std::string;
        using HandleType = ::rocksdb::ColumnFamilyHandle*;
        std::unordered_map<MapKeyType, HandleType> handles_by_key_type_;

        inline static const std::string DEFAULT_ROCKSDB_NOT_OK_ERROR = "RocksDB Unexpected Error, RocksDB status not OK: ";
    };

    inline arcticdb::proto::storage::VariantStorage pack_config() {
        arcticdb::proto::storage::VariantStorage output;
        arcticdb::proto::memory_storage::Config cfg;
        util::pack_to_any(cfg, *output.mutable_config());
        return output;
    }

} //namespace arcticdb::storage::rocksdb
