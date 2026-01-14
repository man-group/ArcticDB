/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/lmdb/lmdb_client_interface.hpp>
#include <arcticdb/entity/variant_key.hpp>

namespace arcticdb::storage::lmdb {

class RealLmdbClient : public LmdbClientWrapper {
  public:
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
};

} // namespace arcticdb::storage::lmdb
