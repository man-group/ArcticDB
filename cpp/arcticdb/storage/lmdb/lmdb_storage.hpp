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

#include <folly/String.h>
#include <folly/Range.h>
#include <arcticdb/util/composite.hpp>

#ifdef ARCTICDB_USING_CONDA
    #include <lmdb++.h>
#else
    #include <third_party/lmdbxx/lmdb++.h>
#endif

#include <filesystem>

namespace fs = std::filesystem;

namespace arcticdb::storage::lmdb {

class LmdbStorage final : public Storage<LmdbStorage> {

    using Parent = Storage<LmdbStorage>;
    friend Parent;

  public:
    using Config = arcticdb::proto::lmdb_storage::Config;

    LmdbStorage(const LibraryPath &lib, OpenMode mode, const Config &conf);

  protected:
    void do_write(Composite<KeySegmentPair>&& kvs);

    void do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts);

    template<class Visitor>
    void do_read(Composite<VariantKey>&& ks, Visitor &&visitor, storage::ReadKeyOpts opts);

    void do_remove(Composite<VariantKey>&& ks, RemoveOpts opts);

    bool do_supports_prefix_matching() {
        return false;
    };

    inline bool do_fast_delete();

    template<class Visitor>
    void do_iterate_type(KeyType key_type, Visitor &&visitor, const std::string &prefix);

    bool do_key_exists(const VariantKey & key);

    ::lmdb::env& env() { return *env_;  }

private:
    // _internal methods assume the write mutex is already held
    void do_write_internal(Composite<KeySegmentPair>&& kvs, ::lmdb::txn& txn);
    std::vector<VariantKey> do_remove_internal(Composite<VariantKey>&& ks, ::lmdb::txn& txn, RemoveOpts opts);

    std::unique_ptr<std::mutex> write_mutex_;
    std::unique_ptr<::lmdb::env> env_;
};

class LmdbStorageFactory final : public StorageFactory<LmdbStorageFactory> {
    using Parent = StorageFactory<LmdbStorageFactory>;
    friend Parent;

  public:
    using Config = arcticdb::proto::lmdb_storage::Config;
    using StorageType = LmdbStorage;

    explicit LmdbStorageFactory(const Config &conf) :
            conf_(conf), root_path_(conf.path().c_str()) {
        if (!fs::exists(root_path_)) {
            fs::create_directories(root_path_);
        }
    }
  private:
    auto do_create_storage(const LibraryPath &lib, OpenMode mode) {
        return LmdbStorage(lib, mode, conf_);
    }

    Config conf_;
    fs::path root_path_;
};

inline arcticdb::proto::storage::VariantStorage pack_config(const std::string& path) {
    arcticdb::proto::storage::VariantStorage output;
    arcticdb::proto::lmdb_storage::Config cfg;
    cfg.set_path(path);
    util::pack_to_any(cfg, *output.mutable_config());
    return output;
}

}

#define ARCTICDB_LMDB_STORAGE_H_
#include <arcticdb/storage/lmdb/lmdb_storage-inl.hpp>