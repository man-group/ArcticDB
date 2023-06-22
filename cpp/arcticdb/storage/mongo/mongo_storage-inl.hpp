/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <folly/gen/Base.h>
#ifndef ARCTICDB_MONGO_STORAGE_H_
#error "This should only be included by mongo_storage.hpp"
#endif

#include <folly/gen/Base.h>
#include <arcticdb/storage/mongo/mongo_client.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/storage/storage.hpp>

namespace arcticdb::storage::mongo {

inline std::string MongoStorage::collection_name(KeyType k) {
    return (fmt::format("{}{}", prefix_, k));
}

inline void MongoStorage::do_write(Composite<KeySegmentPair>&& kvs) {
    namespace fg = folly::gen;
    auto fmt_db = [](auto &&kv) { return kv.key_type(); };

    ARCTICDB_SAMPLE(MongoStorageWrite, 0)

    (fg::from(kvs.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
        for (auto &kv : group.values()) {
            auto collection = collection_name(kv.key_type());
            client_->write_segment(db_, collection, std::move(kv));
        }
    });
}

inline void MongoStorage::do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) {
    namespace fg = folly::gen;
    auto fmt_db = [](auto &&kv) { return kv.key_type(); };

    ARCTICDB_SAMPLE(MongoStorageWrite, 0)

    (fg::from(kvs.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
        for (auto &kv : group.values()) {
            auto collection = collection_name(kv.key_type());
            client_->update_segment(db_, collection, std::move(kv), opts.upsert_);
        }
    });
}

template<class Visitor>
void MongoStorage::do_read(Composite<VariantKey>&& ks, Visitor &&visitor, ReadKeyOpts) {
    namespace fg = folly::gen;
    auto fmt_db = [](auto &&k) { return variant_key_type(k); };
    ARCTICDB_SAMPLE(MongoStorageRead, 0)
    std::vector<VariantKey> failed_reads;

    (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
        for (auto &k : group.values()) {
                auto collection = collection_name(variant_key_type(k));
                auto kv = client_->read_segment(db_, collection, k);
                if(kv.has_segment())
                    visitor(k, kv.segment());
                else
                   failed_reads.push_back(k);
        }
    });

    if(!failed_reads.empty())
        throw KeyNotFoundException(Composite<VariantKey>{std::move(failed_reads)});
}

bool MongoStorage::do_fast_delete() {
    foreach_key_type([&] (KeyType key_type) {
        auto collection = collection_name(key_type);
        client_->drop_collection(db_, collection);
    });
    return true;
}

inline void MongoStorage::do_remove(Composite<VariantKey>&& ks, RemoveOpts) {
    namespace fg = folly::gen;
    auto fmt_db = [](auto &&k) { return variant_key_type(k); };
    ARCTICDB_SAMPLE(MongoStorageRemove, 0)

    Composite<VariantKey> failed_deletes;

    (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([this, &failed_deletes](auto &&group) {
        for (auto &k : group.values()) {
            auto collection = collection_name(variant_key_type(k));
            try {
                client_->remove_keyvalue(db_, collection, k);
            } catch(KeyNotFoundException& e) {
                failed_deletes.push_back(std::move(e.keys()));
            } catch(const std::exception& e) {
                // TODO https://github.com/man-group/ArcticDB/issues/518
                log::storage().warn("Error during remove: {}", e.what());
                failed_deletes.push_back(std::move(k));
            }
        }
    });
    if (!failed_deletes.empty()) {
        throw KeyNotFoundException(std::move(failed_deletes));
    }
}

template<class Visitor>
void MongoStorage::do_iterate_type(KeyType key_type, Visitor &&visitor, const std::string &prefix) {
    auto collection = collection_name(key_type);
    auto func = folly::Function<void(entity::VariantKey&&)>(std::move(visitor));
    ARCTICDB_SAMPLE(MongoStorageItType, 0)
    client_->iterate_type(db_, collection, key_type, std::move(func), prefix);
}

inline bool MongoStorage::do_key_exists(const VariantKey& key) {
    auto collection = collection_name(variant_key_type(key));
    return client_->key_exists(db_, collection, key);
}

} //namespace arcticdb::storage::mongo
