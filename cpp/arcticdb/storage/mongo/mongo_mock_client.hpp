/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/storage/mongo/mongo_client_wrapper.hpp>
#include <arcticdb/storage/storage_mock_client.hpp>
#include <mongocxx/exception/operation_exception.hpp>

namespace arcticdb::storage::mongo {

struct MongoFailure {
    // holds std::monostate if it is a no acknowledgement failure because in case of an acknowledgement from server
    // the mongo apis don't throw an exception
    std::variant<mongocxx::operation_exception, std::monostate> failure;

    [[nodiscard]] bool is_no_ack_failure() const {
        return std::holds_alternative<std::monostate>(failure);
    }

    mongocxx::operation_exception get_exception() const {
        return std::get<mongocxx::operation_exception>(failure);
    }

};

// the key is composed of the database name, collection name and the variant key
using mongo_document_key = std::tuple<std::string, std::string, std::string>;
class MockMongoClient : public MongoClientWrapper,
                        public MockStorageClient<mongo_document_key, MongoFailure> {

public:
    static std::string get_failure_trigger(
            const std::string& key,
            StorageOperation operation_to_fail,
            MongoError error_code);

    std::optional<MongoFailure> has_failure_trigger(
            const mongo_document_key& key,
            StorageOperation op) const override;

    MongoFailure missing_key_failure() const override;

    bool matches_prefix(const mongo_document_key& key, const mongo_document_key& prefix) const override;

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
};

}
