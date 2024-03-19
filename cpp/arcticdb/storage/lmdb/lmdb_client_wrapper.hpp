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
#include <arcticdb/storage/storage_utils.hpp>

// LMDB++ is using `std::is_pod` in `lmdb++.h`, which is deprecated as of C++20.
// See: https://github.com/drycpp/lmdbxx/blob/0b43ca87d8cfabba392dfe884eb1edb83874de02/lmdb%2B%2B.h#L1068
// See: https://en.cppreference.com/w/cpp/types/is_pod
// This suppresses the warning.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#ifdef ARCTICDB_USING_CONDA
#include <lmdb++.h>
#else
#include <third_party/lmdbxx/lmdb++.h>
#endif
#pragma GCC diagnostic pop


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
            Segment&& segment,
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
