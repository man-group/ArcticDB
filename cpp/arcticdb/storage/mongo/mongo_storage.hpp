/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/storage/mongo/mongo_client_wrapper.hpp>
#include <arcticdb/entity/protobufs.hpp>

#include <arcticdb/util/pb_util.hpp>
#include <folly/Range.h>

namespace arcticdb::storage::mongo {

class MongoInstance;
class MongoClient;

class MongoStorage final : public Storage {
  public:
    using Config = arcticdb::proto::mongo_storage::Config;

    MongoStorage(const LibraryPath &lib, OpenMode mode, const Config &conf);

    std::string name() const final;

  private:
    void do_write(KeySegmentPair& kvs) final;

    void do_write_if_none(KeySegmentPair& kv [[maybe_unused]]) final {
        storage::raise<ErrorCode::E_UNSUPPORTED_ATOMIC_OPERATION>("Atomic operations are only supported for s3 backend");
    };

    void do_update(KeySegmentPair&& kvs, UpdateOpts opts) final;

    void do_read(VariantKey&& ks, const ReadVisitor& visitor, ReadKeyOpts opts) final;

    void do_remove(VariantKey&& ks, RemoveOpts opts) final;

    bool do_key_exists(const VariantKey& key) final;

    bool do_supports_prefix_matching() const final {
        return false;
    }

    bool do_supports_atomic_writes() const final {
        return false;
    }

    inline bool do_fast_delete() final;

    bool do_iterate_type_until_match(KeyType key_type, const IterateTypePredicate& visitor, const std::string &prefix) final;

    std::string do_key_path(const VariantKey&) const final { return {}; };

    bool do_is_path_valid(const std::string_view path) const final;

    std::string collection_name(KeyType k);

    std::shared_ptr<MongoClientWrapper> client_;
    std::string db_;
    std::string prefix_;
};

inline arcticdb::proto::storage::VariantStorage pack_config(InstanceUri uri) {
    arcticdb::proto::storage::VariantStorage output;
    arcticdb::proto::mongo_storage::Config cfg;
    cfg.set_uri(uri.value);
    util::pack_to_any(cfg, *output.mutable_config());
    return output;
}

}
