/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/segment.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/lmdb/lmdb_client_impl.hpp>
#include <arcticdb/storage/lmdb/lmdb.hpp>

namespace arcticdb::storage::lmdb {

bool RealLmdbClient::exists(const std::string&, std::string& path, ::lmdb::txn& txn, ::lmdb::dbi& dbi) const {
    if (unsigned int tmp; ::mdb_dbi_flags(txn, dbi, &tmp) == EINVAL) {
        return false;
    }

    MDB_val mdb_key{path.size(), path.data()};
    MDB_val mdb_val;

    return ::lmdb::dbi_get(txn, dbi.handle(), &mdb_key, &mdb_val);
}

std::optional<Segment> RealLmdbClient::read(const std::string&, std::string& path, ::lmdb::txn& txn, ::lmdb::dbi& dbi) const {
    MDB_val mdb_key{path.size(), path.data()};
    MDB_val mdb_val;

    ARCTICDB_SUBSAMPLE(LmdbStorageGet, 0)
    if(!::lmdb::dbi_get(txn, dbi.handle(), &mdb_key, &mdb_val)) {
        return std::nullopt;
    }

    auto segment = Segment::from_bytes(reinterpret_cast<std::uint8_t *>(mdb_val.mv_data), mdb_val.mv_size);
    return segment;
}

void RealLmdbClient::write(const std::string&, std::string& path, arcticdb::Segment& seg,
                           ::lmdb::txn& txn, ::lmdb::dbi& dbi, int64_t overwrite_flag) {
    MDB_val mdb_key{path.size(), path.data()};

    MDB_val mdb_val;
    mdb_val.mv_size = seg.calculate_size();

    ARCTICDB_SUBSAMPLE(LmdbPut, 0)
    int rc = ::mdb_put(txn.handle(), dbi.handle(), &mdb_key, &mdb_val, MDB_RESERVE | overwrite_flag);
    if(rc != MDB_SUCCESS) {
        ::lmdb::error::raise("mdb_put", rc);
    }

    ARCTICDB_SUBSAMPLE(LmdbMemCpy, 0)
    // mdb_val now points to a reserved memory area we must write to
    seg.write_to(reinterpret_cast<std::uint8_t *>(mdb_val.mv_data));
}

bool RealLmdbClient::remove(const std::string&, std::string& path, ::lmdb::txn& txn, ::lmdb::dbi& dbi) {
    MDB_val mdb_key{path.size(), path.data()};

    ARCTICDB_SUBSAMPLE(LmdbStorageDel, 0)
    return ::lmdb::dbi_del(txn, dbi.handle(), &mdb_key);
}

std::vector<VariantKey> RealLmdbClient::list(const std::string&, const std::string& prefix, ::lmdb::txn& txn,
                                             ::lmdb::dbi& dbi, KeyType key_type) const {
    ARCTICDB_SUBSAMPLE(LmdbStorageOpenCursor, 0)
    auto db_cursor = ::lmdb::cursor::open(txn, dbi);

    MDB_val mdb_db_key;
    ARCTICDB_SUBSAMPLE(LmdbStorageCursorFirst, 0)
    if (!db_cursor.get(&mdb_db_key, nullptr, MDB_cursor_op::MDB_FIRST)) {
        return {};
    }

    auto prefix_matcher = [&prefix](const StreamId& id) {
        return prefix.empty() || (std::holds_alternative<std::string>(id) &&
        std::get<std::string>(id).compare(0u, prefix.size(), prefix) == 0); };
    std::vector<VariantKey> found_keys;
    do {
        auto k = variant_key_from_bytes(
                static_cast<uint8_t *>(mdb_db_key.mv_data),
                mdb_db_key.mv_size,
                key_type);

        ARCTICDB_DEBUG(log::storage(), "Iterating key {}: {}", variant_key_type(k), variant_key_view(k));
        if (prefix_matcher(variant_key_id(k))) {
            found_keys.push_back(k);
        }
        ARCTICDB_SUBSAMPLE(LmdbStorageCursorNext, 0)
    } while (db_cursor.get(&mdb_db_key, nullptr, MDB_cursor_op::MDB_NEXT));

    return found_keys;
}

} // namespace arcticdb::storage::lmdb
