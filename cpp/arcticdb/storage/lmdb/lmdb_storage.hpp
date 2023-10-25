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

class LmdbStorage final : public Storage {
  public:
    using Config = arcticdb::proto::lmdb_storage::Config;

    LmdbStorage(const LibraryPath &lib, OpenMode mode, const Config &conf);

  private:
    void do_write(Composite<KeySegmentPair>&& kvs) final;

    void do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) final;

    void do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, storage::ReadKeyOpts opts) final;

    void do_remove(Composite<VariantKey>&& ks, RemoveOpts opts) final;

    bool do_supports_prefix_matching() const final {
        return false;
    };

    inline bool do_fast_delete() final;

    void do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string &prefix) final;

    bool do_key_exists(const VariantKey & key) final;

    ::lmdb::env& env() { return *env_;  }

    std::string do_key_path(const VariantKey&) const final { return {}; };

    // _internal methods assume the write mutex is already held
    void do_write_internal(Composite<KeySegmentPair>&& kvs, ::lmdb::txn& txn);
    std::vector<VariantKey> do_remove_internal(Composite<VariantKey>&& ks, ::lmdb::txn& txn, RemoveOpts opts);

    std::unique_ptr<std::mutex> write_mutex_;
    std::unique_ptr<::lmdb::env> env_;
    std::unordered_map<std::string, ::lmdb::dbi> dbi_by_key_type_;
};

inline arcticdb::proto::storage::VariantStorage pack_config(const std::string& path) {
    arcticdb::proto::storage::VariantStorage output;
    arcticdb::proto::lmdb_storage::Config cfg;
    cfg.set_path(path);
    util::pack_to_any(cfg, *output.mutable_config());
    return output;
}

}
