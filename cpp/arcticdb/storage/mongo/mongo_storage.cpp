/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */


#include <arcticdb/storage/mongo/mongo_storage.hpp>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <arcticdb/util/configs_map.hpp>
#include <fmt/format.h>
#include <folly/gen/Base.h>

#include <arcticdb/storage/mongo/mongo_instance.hpp>
#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/storage/mongo/mongo_client.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/storage/storage.hpp>

namespace arcticdb::storage::mongo {

std::string MongoStorage::collection_name(KeyType k) {
    return (fmt::format("{}{}", prefix_, k));
}

void MongoStorage::do_write(Composite<KeySegmentPair>&& kvs) {
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

void MongoStorage::do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) {
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

void MongoStorage::do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, ReadKeyOpts) {
    namespace fg = folly::gen;
    auto fmt_db = [](auto &&k) { return variant_key_type(k); };
    ARCTICDB_SAMPLE(MongoStorageRead, 0)
    std::vector<VariantKey> failed_reads;

    (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
        for (auto &k : group.values()) {
                auto collection = collection_name(variant_key_type(k));
                auto kv = client_->read_segment(db_, collection, k);
                if(kv.has_segment())
                    visitor(k, std::move(kv.segment()));
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

void MongoStorage::do_remove(Composite<VariantKey>&& ks, RemoveOpts) {
    namespace fg = folly::gen;
    auto fmt_db = [](auto &&k) { return variant_key_type(k); };
    ARCTICDB_SAMPLE(MongoStorageRemove, 0)

    (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
        for (auto &k : group.values()) {
            auto collection = collection_name(variant_key_type(k));
            client_->remove_keyvalue(db_, collection, k);
        }
    });
}

void MongoStorage::do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string &prefix) {
    auto collection = collection_name(key_type);
    auto func = folly::Function<void(entity::VariantKey&&)>(visitor);
    ARCTICDB_SAMPLE(MongoStorageItType, 0)
    client_->iterate_type(db_, collection, key_type, std::move(func), prefix);
}

bool MongoStorage::do_key_exists(const VariantKey& key) {
    auto collection = collection_name(variant_key_type(key));
    return client_->key_exists(db_, collection, key);
}


using Config = arcticdb::proto::mongo_storage::Config;

MongoStorage::MongoStorage(
    const LibraryPath &lib,
    OpenMode mode,
    const Config &config) :
    Storage(lib, mode),
    instance_(MongoInstance::instance()),
    client_(std::make_unique<MongoClient>(
        config,
        ConfigsMap::instance()->get_int("MongoClient.MinPoolSize", 100),
        ConfigsMap::instance()->get_int("MongoClient.MaxPoolSize", 1000),
        ConfigsMap::instance()->get_int("MongoClient.SelectionTimeoutMs", 120000))
        ) {
    instance_.reset(); //Just want to ensure singleton here, not hang onto it
    auto key_rg = lib.as_range();
    auto it = key_rg.begin();
    db_ = fmt::format("arcticc_{}", *it++);
    std::ostringstream strm;
    for (; it != key_rg.end(); ++it) {
        strm << *it->get() << "__";
    }
    prefix_ = strm.str();
}

}