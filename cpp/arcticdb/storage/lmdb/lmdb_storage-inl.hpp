/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#ifndef ARCTICDB_LMDB_STORAGE_H_
#error "This should only be included by lmdb_storage.cpp"
#endif

#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/storage/storage_utils.hpp>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <folly/gen/Base.h>

namespace arcticdb::storage::lmdb {

namespace fg = folly::gen;

inline void LmdbStorage::do_write_internal(Composite<KeySegmentPair>&& kvs, ::lmdb::txn& txn) {
    auto fmt_db = [](auto &&kv) { return kv.key_type(); };

    (fg::from(kvs.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
        auto db_name = fmt::format("{}", group.key());

        ARCTICDB_SUBSAMPLE(LmdbStorageOpenDb, 0)
        ::lmdb::dbi& dbi = dbi_by_key_type_.at(db_name);

        ARCTICDB_SUBSAMPLE(LmdbStorageWriteValues, 0)
        for (auto &kv : group.values()) {
            ARCTICDB_DEBUG(log::storage(), "Lmdb storage writing segment with key {}", kv.key_view());
            auto k = to_serialized_key(kv.variant_key());
            auto &seg = kv.segment();
            MDB_val mdb_key;
            mdb_key.mv_data = k.data();
            mdb_key.mv_size = k.size();

            std::size_t hdr_sz = seg.segment_header_bytes_size();

            MDB_val mdb_val;
            mdb_val.mv_size = seg.total_segment_size(hdr_sz);
            int64_t overwrite_flag = std::holds_alternative<RefKey>(kv.variant_key()) ? 0 : MDB_NOOVERWRITE;
            ARCTICDB_SUBSAMPLE(LmdbPut, 0)
            int res = ::mdb_put(txn.handle(), dbi.handle(), &mdb_key, &mdb_val, MDB_RESERVE | overwrite_flag);
            if (res == MDB_KEYEXIST) {
                throw DuplicateKeyException(kv.variant_key());
            } else if (res != 0) {
                throw std::runtime_error(fmt::format("Invalid lmdb error code {} while putting key {}",
                                                     res, kv.key_view()));
            }
            ARCTICDB_SUBSAMPLE(LmdbMemCpy, 0)
            // mdb_val now points to a reserved memory area we must write to
            seg.write_to(reinterpret_cast<std::uint8_t *>(mdb_val.mv_data), hdr_sz);
        }
    });
}

inline void LmdbStorage::do_write(Composite<KeySegmentPair>&& kvs) {
    ARCTICDB_SAMPLE(LmdbStorageWrite, 0)
    std::lock_guard<std::mutex> lock{*write_mutex_};
    auto txn = ::lmdb::txn::begin(env()); // scoped abort on exception, so no partial writes
    ARCTICDB_SUBSAMPLE(LmdbStorageInTransaction, 0)
    do_write_internal(std::move(kvs), txn);
    ARCTICDB_SUBSAMPLE(LmdbStorageCommit, 0)
    txn.commit();
}

inline void LmdbStorage::do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) {
    ARCTICDB_SAMPLE(LmdbStorageUpdate, 0)
    std::lock_guard<std::mutex> lock{*write_mutex_};
    auto txn = ::lmdb::txn::begin(env());
    ARCTICDB_SUBSAMPLE(LmdbStorageInTransaction, 0)
    auto keys = kvs.transform([](const auto& kv){return kv.variant_key();});
    // Deleting keys (no error is thrown if the keys already exist)
    RemoveOpts remove_opts;
    remove_opts.ignores_missing_key_ = opts.upsert_;
    auto failed_deletes = do_remove_internal(std::move(keys), txn, remove_opts);
    if(!failed_deletes.empty()) {
        ARCTICDB_SUBSAMPLE(LmdbStorageCommit, 0)
        txn.commit();
        throw KeyNotFoundException(Composite<VariantKey>(std::move(failed_deletes)));
    }
    do_write_internal(std::move(kvs), txn);
    ARCTICDB_SUBSAMPLE(LmdbStorageCommit, 0)
    txn.commit();
}

inline void LmdbStorage::do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, storage::ReadKeyOpts) {
    ARCTICDB_SAMPLE(LmdbStorageRead, 0)
    auto txn = ::lmdb::txn::begin(env(), nullptr, MDB_RDONLY);

    auto fmt_db = [](auto &&k) { return variant_key_type(k); };
    ARCTICDB_SUBSAMPLE(LmdbStorageInTransaction, 0)
    std::vector<VariantKey> failed_reads;

    (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
        auto db_name = fmt::format("{}", group.key());
        ARCTICDB_SUBSAMPLE(LmdbStorageOpenDb, 0)
        ::lmdb::dbi& dbi = dbi_by_key_type_.at(db_name);
        for (auto &k : group.values()) {
            auto stored_key = to_serialized_key(k);
            MDB_val mdb_key{stored_key.size(), stored_key.data()};
            MDB_val mdb_val;
            ARCTICDB_SUBSAMPLE(LmdbStorageGet, 0)

            if (::lmdb::dbi_get(txn, dbi.handle(), &mdb_key, &mdb_val)) {
                ARCTICDB_SUBSAMPLE(LmdbStorageVisitSegment, 0)
                visitor(k, Segment::from_bytes(reinterpret_cast<std::uint8_t *>(mdb_val.mv_data),
                                               mdb_val.mv_size));

                ARCTICDB_DEBUG(log::storage(), "Read key {}: {}, with {} bytes of data", variant_key_type(k), variant_key_view(k), mdb_val.mv_size);
            } else {
                ARCTICDB_DEBUG(log::storage(), "Failed to find segment for key {}",variant_key_view(k));
                failed_reads.push_back(k);
            }
        }
    });
    if(!failed_reads.empty())
        throw KeyNotFoundException(Composite<VariantKey>(std::move(failed_reads)));
}

inline bool LmdbStorage::do_key_exists(const VariantKey&key) {
    ARCTICDB_SAMPLE(LmdbStorageKeyExists, 0)
    auto txn = ::lmdb::txn::begin(env(), nullptr, MDB_RDONLY);
    ARCTICDB_SUBSAMPLE(LmdbStorageInTransaction, 0)

    auto db_name = fmt::format("{}", variant_key_type(key));
    ARCTICDB_SUBSAMPLE(LmdbStorageOpenDb, 0)

    try {
        ::lmdb::dbi& dbi = dbi_by_key_type_.at(db_name);
        if (unsigned int tmp; ::mdb_dbi_flags(txn, dbi, &tmp) == EINVAL) {
            return false;
        }
        auto stored_key = to_serialized_key(key);
        MDB_val mdb_key{stored_key.size(), stored_key.data()};
        MDB_val mdb_val;
        return ::lmdb::dbi_get(txn, dbi.handle(), &mdb_key, &mdb_val);
    } catch (const ::lmdb::not_found_error &ex) {
        ARCTICDB_DEBUG(log::storage(), "Caught lmdb not found error: {}", ex.what());
        return false;
    }
}

inline std::vector<VariantKey> LmdbStorage::do_remove_internal(Composite<VariantKey>&& ks, ::lmdb::txn& txn, RemoveOpts opts)
{
    auto fmt_db = [](auto &&k) { return variant_key_type(k); };
    std::vector<VariantKey> failed_deletes;

    (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
        auto db_name = fmt::format("{}", group.key());
        ARCTICDB_SUBSAMPLE(LmdbStorageOpenDb, 0)
        try {
            // If no key of this type has been written before, this can fail
            ::lmdb::dbi& dbi = dbi_by_key_type_.at(db_name);
            for (auto &k : group.values()) {
                auto stored_key = to_serialized_key(k);
                MDB_val mdb_key{stored_key.size(), stored_key.data()};
                ARCTICDB_SUBSAMPLE(LmdbStorageDel, 0)

                if (::lmdb::dbi_del(txn, dbi.handle(), &mdb_key)) {
                    ARCTICDB_DEBUG(log::storage(), "Deleted segment for key {}", variant_key_view(k));
                } else {
                    if (!opts.ignores_missing_key_) {
                        log::storage().warn("Failed to delete segment for key {}", variant_key_view(k));
                        failed_deletes.push_back(k);
                    }
                }
            }
        } catch (const std::exception& e) {
            if (!opts.ignores_missing_key_) {
                for (auto &k : group.values()) {
                    log::storage().warn("Failed to delete segment for key {}", variant_key_view(k) );
                                        failed_deletes.push_back(k);
                }
            }
        }
    });
    return failed_deletes;
}

inline void LmdbStorage::do_remove(Composite<VariantKey>&& ks, RemoveOpts opts)
{
    ARCTICDB_SAMPLE(LmdbStorageRemove, 0)
    std::lock_guard<std::mutex> lock{*write_mutex_};
    auto txn = ::lmdb::txn::begin(env());
    ARCTICDB_SUBSAMPLE(LmdbStorageInTransaction, 0)
    auto failed_deletes = do_remove_internal(std::move(ks), txn, opts);
    ARCTICDB_SUBSAMPLE(LmdbStorageCommit, 0)
    txn.commit();

    if(!failed_deletes.empty())
        throw KeyNotFoundException(Composite<VariantKey>(std::move(failed_deletes)));
}

bool LmdbStorage::do_fast_delete() {
    std::lock_guard<std::mutex> lock{*write_mutex_};
    // bool is probably not the best return type here but it does help prevent the insane boilerplate for
    // an additional function that checks whether this is supported (like the prefix matching)
    auto dtxn = ::lmdb::txn::begin(env());
    foreach_key_type([&] (KeyType key_type) {
        if (key_type == KeyType::TOMBSTONE) {
            // TOMBSTONE and LOCK both format to code 'x' - do not try to drop both
            return;
        }
        auto db_name = fmt::format("{}", key_type);
        ARCTICDB_SUBSAMPLE(LmdbStorageOpenDb, 0)
        ARCTICDB_DEBUG(log::storage(), "dropping {}", db_name);
        ::lmdb::dbi& dbi = dbi_by_key_type_.at(db_name);
        ::lmdb::dbi_drop(dtxn, dbi);
    });

    dtxn.commit();
    return true;
}

inline void LmdbStorage::do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string &prefix) {
    ARCTICDB_SAMPLE(LmdbStorageItType, 0);
    auto txn = ::lmdb::txn::begin(env(), nullptr, MDB_RDONLY); // scoped abort on
    std::string type_db = fmt::format("{}", key_type);
    ::lmdb::dbi& dbi = dbi_by_key_type_.at(type_db);

    ARCTICDB_SUBSAMPLE(LmdbStorageOpenCursor, 0)
    auto db_cursor = ::lmdb::cursor::open(txn, dbi);

    MDB_val mdb_db_key;
    ARCTICDB_SUBSAMPLE(LmdbStorageCursorFirst, 0)
    if (!db_cursor.get(&mdb_db_key, nullptr, MDB_cursor_op::MDB_FIRST)) {
        return;
    }
    auto prefix_matcher = stream_id_prefix_matcher(prefix);
    do {
        auto k = variant_key_from_bytes(
            static_cast<uint8_t *>(mdb_db_key.mv_data),
            mdb_db_key.mv_size,
            key_type);

        ARCTICDB_DEBUG(log::storage(), "Iterating key {}: {}", variant_key_type(k), variant_key_view(k));
        if (prefix_matcher(variant_key_id(k))) {
            ARCTICDB_SUBSAMPLE(LmdbStorageVisitKey, 0)
            visitor(std::move(k));
        }
        ARCTICDB_SUBSAMPLE(LmdbStorageCursorNext, 0)
    } while (db_cursor.get(&mdb_db_key, nullptr, MDB_cursor_op::MDB_NEXT));
}

}
