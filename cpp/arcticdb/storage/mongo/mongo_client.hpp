/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <mongocxx/exception/server_error_code.hpp>
#include <mongocxx/exception/operation_exception.hpp>

#include <arcticdb/storage/mongo/mongo_client_interface.hpp>
#include <arcticdb/entity/protobufs.hpp>

namespace arcticdb::storage::mongo {

class MongoClientImpl;

class MongoClient : public MongoClientWrapper {
    using Config = arcticdb::proto::mongo_storage::Config;

  public:
    explicit MongoClient(
            const Config& config, uint64_t min_pool_size, uint64_t max_pool_size, uint64_t selection_timeout_ms
    );

    ~MongoClient() override;

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

    void drop_collection(std::string database_name, std::string collection_name) override;

    bool key_exists(const std::string& database_name, const std::string& collection_name, const entity::VariantKey& key)
            override;

  private:
    MongoClientImpl* client_;
};

std::string get_mongo_error_suffix(const mongocxx::operation_exception& e, std::string_view object_name);

void raise_mongo_server_exception(const mongocxx::operation_exception& e, std::string_view object_name);

} // namespace arcticdb::storage::mongo
