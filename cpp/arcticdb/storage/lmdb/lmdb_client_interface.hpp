/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/codec/segment.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/variant_key.hpp>

namespace lmdb {
class txn;
class dbi;
}


namespace arcticdb::storage::lmdb {

class LmdbClientWrapper {
public:
    virtual bool exists(
            const std::string& db_name,
            std::string& path,
            ::lmdb::txn& txn,
            ::lmdb::dbi& dbi) const = 0;

    virtual std::optional<Segment> read(
            const std::string& db_name,
            std::string& path,
            ::lmdb::txn& txn,
            ::lmdb::dbi& dbi) const = 0;

    virtual void write(
            const std::string& db_name,
            std::string& path,
            Segment& segment,
            ::lmdb::txn& txn,
            ::lmdb::dbi& dbi,
            int64_t overwrite_flag) = 0;

    virtual bool remove(const std::string& db_name, std::string& path, ::lmdb::txn& txn, ::lmdb::dbi& dbi) = 0;

    virtual std::vector<VariantKey> list(
            const std::string& db_name,
            const std::string& prefix,
            ::lmdb::txn& txn,
            ::lmdb::dbi& dbi,
            KeyType key_type) const = 0;

    virtual ~LmdbClientWrapper() = default;
};

} // namespace arcticdb::storage::lmdb
