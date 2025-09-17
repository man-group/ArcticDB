/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/lmdb/lmdb_client_interface.hpp>
#include <arcticdb/codec/segment.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/mock/storage_mock_client.hpp>

namespace arcticdb::storage::lmdb {

struct LmdbKey {
    std::string db_name_;
    std::string path_;

    bool operator==(const LmdbKey& other) const {
        return std::pair(db_name_, path_) == std::pair(other.db_name_, other.path_);
    }
};

struct LmdbKeyHash {
    std::size_t operator()(const LmdbKey& k) const {
        return std::hash<std::pair<std::string, std::string>>{}(std::pair(k.db_name_, k.path_));
    }
};

class MockLmdbClient : public LmdbClientWrapper {
  public:
    static std::string get_failure_trigger(const std::string& path, StorageOperation operation_to_fail, int error_code);

    bool exists(const std::string& db_name, std::string& path, ::lmdb::txn& txn, ::lmdb::dbi& dbi) const override;

    std::optional<Segment> read(const std::string& db_name, std::string& path, ::lmdb::txn& txn, ::lmdb::dbi& dbi)
            const override;

    void write(
            const std::string& db_name, std::string& path, Segment& segment, ::lmdb::txn& txn, ::lmdb::dbi& dbi,
            int64_t overwrite_flag
    ) override;

    bool remove(const std::string& db_name, std::string& path, ::lmdb::txn& txn, ::lmdb::dbi& dbi) override;

    std::vector<VariantKey> list(
            const std::string& db_name, const std::string& prefix, ::lmdb::txn& txn, ::lmdb::dbi& dbi, KeyType key_type
    ) const override;

  private:
    std::unordered_map<LmdbKey, Segment, LmdbKeyHash> lmdb_contents_;

    bool has_key(const LmdbKey& key) const;
};

} // namespace arcticdb::storage::lmdb
