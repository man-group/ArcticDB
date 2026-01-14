/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/mongo/mongo_client_interface.hpp>
#include <arcticdb/storage/mock/storage_mock_client.hpp>
#include <mongocxx/exception/operation_exception.hpp>

namespace arcticdb::storage::mongo {

struct MongoDocumentKey {
    VariantKey key_;

    ARCTICDB_MOVE_ONLY_DEFAULT(MongoDocumentKey)

    explicit MongoDocumentKey(VariantKey&& key) : key_(std::move(key)) {}

    [[nodiscard]] std::string id_string() const { return fmt::format("{}", variant_key_id(key_)); }
};

struct MongoKey {
    std::string database_name_;
    std::string collection_name_;
    MongoDocumentKey doc_key_;

    ARCTICDB_MOVE_ONLY_DEFAULT(MongoKey)

    MongoKey(std::string database_name, std::string collection_name, VariantKey key) :
        database_name_(std::move(database_name)),
        collection_name_(std::move(collection_name)),
        doc_key_(std::move(key)) {}

    bool operator<(const MongoKey& other) const {
        std::string id_string = doc_key_.id_string();
        std::string other_id_string = other.doc_key_.id_string();

        return std::tie(database_name_, collection_name_, id_string) <
               std::tie(other.database_name_, other.collection_name_, other_id_string);
    }
};

using no_ack_failure = std::monostate;

struct MongoFailure {
    // holds std::monostate if it is a no acknowledgement failure because in case of an acknowledgement from server
    // the mongo apis don't throw an exception
    std::variant<mongocxx::operation_exception, no_ack_failure> failure;

    [[nodiscard]] bool is_no_ack_failure() const { return std::holds_alternative<no_ack_failure>(failure); }

    [[nodiscard]] mongocxx::operation_exception get_exception() const {
        return std::get<mongocxx::operation_exception>(failure);
    }
};

class MockMongoClient : public MongoClientWrapper {
  public:
    MockMongoClient() = default;

    ARCTICDB_MOVE_ONLY_DEFAULT(MockMongoClient)

    static std::string get_failure_trigger(
            const std::string& key, StorageOperation operation_to_fail, MongoError error_code
    );

    bool write_segment(
            const std::string& database_name, const std::string& collection_name, storage::KeySegmentPair& key_seg
    ) override;

    UpdateResult update_segment(
            const std::string& database_name, const std::string& collection_name, storage::KeySegmentPair& key_seg,
            bool upsert
    ) override;

    std::optional<KeySegmentPair> read_segment(
            const std::string& database_name, const std::string& collection_name, const entity::VariantKey& key
    ) override;

    DeleteResult remove_keyvalue(
            const std::string& database_name, const std::string& collection_name, const entity::VariantKey& key
    ) override;

    std::vector<VariantKey> list_keys(
            const std::string& database_name, const std::string& collection_name, KeyType key_type,
            const std::optional<std::string>& prefix
    ) override;

    bool key_exists(const std::string& database_name, const std::string& collection_name, const entity::VariantKey& key)
            override;

    void drop_collection(std::string database_name, std::string collection_name) override;

  private:
    std::map<MongoKey, Segment> mongo_contents;

    bool has_key(const MongoKey& key);
};

} // namespace arcticdb::storage::mongo
