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
#include <arcticdb/storage/mock/mongo_mock_client.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>

namespace arcticdb::storage::mongo {

const auto UNSUPPORTED_MONGO_CHARS = std::unordered_set<char>{'/'};

std::string MongoStorage::collection_name(KeyType k) {
    return (fmt::format("{}{}", prefix_, k));
}

/*
 * Mongo error handling notes:
 * All the exceptions thrown by mongocxx are derived from mongocxx::exception. https://mongocxx.org/api/mongocxx-3.5.1/classmongocxx_1_1exception.html
 * - The exceptions that triggered by read, write, delete operations are derived from mongocxx::operation_exception.
 * - mongocxx::operation_exception has an error_code which is returned by the server as documented here: https://www.mongodb.com/docs/manual/reference/error-codes/
 * - some relevant error codes returned by the server are defined in MongoError enum.
 */
void raise_mongo_exception(const mongocxx::operation_exception &e, const std::string &object_name) {
    auto error_code = e.code().value();
    auto mongo_error_suffix = fmt::format("MongoError#{}: {} for object {}", error_code, e.what(), object_name);

    if (error_code == static_cast<int>(MongoError::NoSuchKey)
        || error_code == static_cast<int>(MongoError::KeyNotFound)) {
        throw KeyNotFoundException(fmt::format("Key Not Found Error: {}", mongo_error_suffix));
    }

    if (error_code == static_cast<int>(MongoError::UnAuthorized)
        || error_code == static_cast<int>(MongoError::AuthenticationFailed)) {
        raise<ErrorCode::E_PERMISSION>(fmt::format("Permission error: {}", mongo_error_suffix));
    }

    raise<ErrorCode::E_UNEXPECTED_MONGO_ERROR>(fmt::format("Unexpected Mongo Error: {} {} {}",
                                                           mongo_error_suffix,
                                                           e.code().category().name(),
                                                           e.code().message()));
}

bool is_expected_error_type(int error_code) {
    return error_code == static_cast<int>(MongoError::KeyNotFound)
        || error_code == static_cast<int>(MongoError::NoSuchKey);
}

void raise_if_unexpected_error(const mongocxx::operation_exception &e, const std::string &object_name) {
    auto error_code = e.code().value();

    if (!is_expected_error_type(error_code)) {
        raise_mongo_exception(e, object_name);
    }
}

std::string MongoStorage::name() const {
    return fmt::format("mongo_storage-{}", db_);
}

void MongoStorage::do_write(KeySegmentPair &key_seg) {
    ARCTICDB_SAMPLE(MongoStorageWrite, 0)

    auto collection = collection_name(key_seg.key_type());
    auto key_view = key_seg.key_view();
    try {
        auto success = client_->write_segment(db_, collection, key_seg);
        storage::check<ErrorCode::E_MONGO_BULK_OP_NO_REPLY>(success,
                                                            "Mongo did not acknowledge write for key {}",
                                                            key_view);
    } catch (const mongocxx::operation_exception &ex) {
        std::string object_name = std::string(key_view);
        raise_mongo_exception(ex, object_name);
    }
}

void MongoStorage::do_update(KeySegmentPair &key_seg, UpdateOpts opts) {
    ARCTICDB_SAMPLE(MongoStorageWrite, 0)

    auto collection = collection_name(key_seg.key_type());
    auto key_view = key_seg.key_view();
    try {
        auto result = client_->update_segment(db_, collection, key_seg, opts.upsert_);
        storage::check<ErrorCode::E_MONGO_BULK_OP_NO_REPLY>(result.modified_count.has_value(),
                                                            "Mongo did not acknowledge write for key {}",
                                                            key_view);
        if (!opts.upsert_ && result.modified_count.value() == 0) {
            throw storage::KeyNotFoundException(
                fmt::format("update called with upsert=false but key does not exist: {}", key_view));
        }
    } catch (const mongocxx::operation_exception &ex) {
        std::string object_name = std::string(key_view);
        raise_mongo_exception(ex, object_name);
    }
}

void MongoStorage::do_read(VariantKey &&variant_key, const ReadVisitor &visitor, ReadKeyOpts opts) {
    auto key_seg = do_read(std::move(variant_key), opts);
    visitor(key_seg.variant_key(), std::move(*key_seg.segment_ptr()));
}

KeySegmentPair MongoStorage::do_read(VariantKey&& variant_key, ReadKeyOpts opts) {
    ARCTICDB_SAMPLE(MongoStorageRead, 0)
    boost::container::small_vector<VariantKey, 1> keys_not_found;

    auto collection = collection_name(variant_key_type(variant_key));
    try {
        auto kv = client_->read_segment(db_, collection, variant_key);
        // later we should add the key to failed_reads in this case
        if (!kv.has_value()) {
            throw KeyNotFoundException(variant_key);
        } else {
            return *kv;
        }
    } catch (const mongocxx::operation_exception &ex) {
        std::string object_name = std::string(variant_key_view(variant_key));
        raise_if_unexpected_error(ex, object_name);
        log::storage().log(
            opts.dont_warn_about_missing_key ? spdlog::level::debug : spdlog::level::warn,
            "Failed to find segment for key '{}' {}: {}",
            variant_key_view(variant_key),
            ex.code().value(),
            ex.what());

        throw KeyNotFoundException(keys_not_found);
    }
}

bool MongoStorage::do_fast_delete() {
    foreach_key_type([&](KeyType key_type) {
        auto collection = collection_name(key_type);
        client_->drop_collection(db_, collection);
    });
    return true;
}

void MongoStorage::do_remove(std::span<VariantKey> variant_keys, RemoveOpts opts) {
    namespace fg = folly::gen;
    auto fmt_db = [](auto &&k) { return variant_key_type(k); };
    ARCTICDB_SAMPLE(MongoStorageRemove, 0)
    std::vector<VariantKey> keys_not_found;

    (fg::from(variant_keys) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
        for (auto &k : group.values()) {
            auto collection = collection_name(variant_key_type(k));
            try {
                auto result = client_->remove_keyvalue(db_, collection, k);
                storage::check<ErrorCode::E_MONGO_BULK_OP_NO_REPLY>(result.delete_count.has_value(),
                                                                    "Mongo did not acknowledge deletion for key {}", k);
                util::warn(result.delete_count.value() == 1,
                           "Expected to delete a single document with key {} deleted {} documents",
                           k, result.delete_count.value());
                if (result.delete_count.value() == 0 && !opts.ignores_missing_key_) {
                    keys_not_found.push_back(k);
                }
            } catch (const mongocxx::operation_exception &ex) {
                // mongo delete does not throw exception if key not found, it returns 0 as delete count
                std::string object_name = std::string(variant_key_view(k));
                raise_mongo_exception(ex, object_name);
            }
        }
    });
    if (!keys_not_found.empty()) {
        throw KeyNotFoundException(std::move(keys_not_found));
    }
}

void MongoStorage::do_remove(VariantKey&& variant_key, RemoveOpts opts) {
    std::array<VariantKey, 1> arr{std::move(variant_key)};
    do_remove(std::span(arr), opts);
}

bool MongoStorage::do_iterate_type_until_match(KeyType key_type,
                                               const IterateTypePredicate &visitor,
                                               const std::string &prefix) {
    auto collection = collection_name(key_type);
    ARCTICDB_SAMPLE(MongoStorageItType, 0)
    std::vector<VariantKey> keys;
    try {
        keys = client_->list_keys(db_, collection, key_type, prefix);
    } catch (const mongocxx::operation_exception &ex) {
        // We don't raise when key is not found because we want to return an empty list instead of raising.
        raise_if_unexpected_error(ex, collection);
        log::storage().warn("Failed to iterate key type with key '{}' {}: {}",
                            key_type,
                            ex.code().value(),
                            ex.what());
    }
    for (auto &key : keys) {
        if (visitor(std::move(key))) {
            return true;
        }
    }
    return false;
}

bool MongoStorage::do_is_path_valid(std::string_view path) const {
    return std::none_of(path.cbegin(), path.cend(), [](auto c) {
        return UNSUPPORTED_MONGO_CHARS.contains(c);
    });
}

bool MongoStorage::do_key_exists(const VariantKey &key) {
    auto collection = collection_name(variant_key_type(key));
    try {
        return client_->key_exists(db_, collection, key);
    } catch (const mongocxx::operation_exception &ex) {
        std::string object_name = std::string(variant_key_view(key));
        raise_if_unexpected_error(ex, object_name);
    }

    return false;
}

using Config = arcticdb::proto::mongo_storage::Config;

MongoStorage::MongoStorage(
    const LibraryPath &lib,
    OpenMode mode,
    const Config &config) :
    Storage(lib, mode) {
    if (config.use_mock_storage_for_testing()) {
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using Mock Mongo storage");
        client_ = std::make_unique<MockMongoClient>();
    } else {
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using Real Mongo storage");
        client_ = std::make_unique<MongoClient>(
            config,
            ConfigsMap::instance()->get_int("MongoClient.MinPoolSize", 100),
            ConfigsMap::instance()->get_int("MongoClient.MaxPoolSize", 1000),
            ConfigsMap::instance()->get_int("MongoClient.SelectionTimeoutMs", 120000));
    }
    auto key_rg = lib.as_range();
    auto it = key_rg.begin();
    db_ = fmt::format("arcticc_{}", *it++);
    std::ostringstream strm;
    for (; it != key_rg.end(); ++it) {
        strm << *it << "__";
    }
    prefix_ = strm.str();
}

}
