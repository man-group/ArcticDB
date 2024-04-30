/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/mongo/mongo_client_wrapper.hpp>
#include <mongocxx/exception/operation_exception.hpp>

namespace arcticdb::storage {
    enum class StorageOperation;
}

namespace arcticdb::storage::mongo {

struct MongoDocumentKey {
    VariantKey key;

    std::string id_string() const {
        return fmt::format("{}", variant_key_id(key));
    }
};

struct MongoKey {
    std::string database_name;
    std::string collection_name;
    MongoDocumentKey doc_key;

    MongoKey(const std::string& database_name, const std::string& collection_name, const VariantKey& key) :
            database_name(database_name), collection_name(collection_name), doc_key(key) { }

    bool operator<(const MongoKey& other) const {
        std::string id_string = doc_key.id_string();
        std::string other_id_string = other.doc_key.id_string();

        return std::tie(database_name, collection_name, id_string) <
               std::tie(other.database_name, other.collection_name, other_id_string);
    }
};

using no_ack_failure = std::monostate;

struct MongoFailure {
    // holds std::monostate if it is a no acknowledgement failure because in case of an acknowledgement from server
    // the mongo apis don't throw an exception
    std::variant<mongocxx::operation_exception, no_ack_failure> failure;

    [[nodiscard]] bool is_no_ack_failure() const {
        return std::holds_alternative<no_ack_failure>(failure);
    }

    mongocxx::operation_exception get_exception() const {
        return std::get<mongocxx::operation_exception>(failure);
    }

};

class MockMongoClient : public MongoClientWrapper {

public:
    static std::string get_failure_trigger(
            const std::string& key,
            StorageOperation operation_to_fail,
            MongoError error_code);

    bool write_segment(
            const std::string& database_name,
            const std::string& collection_name,
            storage::KeySegmentPair&& kv) override;

    UpdateResult update_segment(
            const std::string& database_name,
            const std::string& collection_name,
            storage::KeySegmentPair&& kv,
            bool upsert) override;

    std::optional<KeySegmentPair> read_segment(
            const std::string& database_name,
            const std::string& collection_name,
            const entity::VariantKey& key) override;

    DeleteResult remove_keyvalue(
            const std::string& database_name,
            const std::string& collection_name,
            const entity::VariantKey& key) override;

    std::vector<VariantKey> list_keys(
            const std::string& database_name,
            const std::string& collection_name,
            KeyType key_type,
            const std::optional<std::string>& prefix) override;

    bool key_exists(
            const std::string& database_name,
            const std::string& collection_name,
            const  entity::VariantKey& key) override;

    void ensure_collection(
            std::string_view database_name,
            std::string_view collection_name) override;

    void drop_collection(
            std::string database_name,
            std::string collection_name) override;
private:
    std::map<MongoKey, Segment> mongo_contents;

    bool has_key(const MongoKey& key);
};

}
