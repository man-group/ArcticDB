/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <fmt/format.h>

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/entity/protobufs.hpp>

namespace arcticdb::storage::mongo {

class MongoClientImpl;

class MongoClient {
    using Config = arcticdb::proto::mongo_storage::Config;
  public:
    explicit MongoClient(
        const Config& config,
        uint64_t min_pool_size,
        uint64_t max_pool_size,
        uint64_t selection_timeout_ms);

    ~MongoClient();

    void write_segment(
        const std::string &database_name,
        const std::string &collection_name,
        storage::KeySegmentPair&& kv);

    void update_segment(
        const std::string &database_name,
        const std::string &collection_name,
        storage::KeySegmentPair&& kv,
        bool upsert);

    storage::KeySegmentPair read_segment(
        const std::string &database_name,
        const std::string &collection_name,
        const entity::VariantKey &key);

    void remove_keyvalue(
        const std::string &database_name,
        const std::string &collection_name,
        const entity::VariantKey &key);

    void iterate_type(
        const std::string &database_name,
        const std::string &collection_name,
        KeyType key_type,
        folly::Function<void(entity::VariantKey &&)>&& visitor,
        const std::optional<std::string> &prefix
        );

    void ensure_collection(
        std::string_view database_name,
        std::string_view collection_name);

    void drop_collection(
            std::string database_name,
            std::string collection_name);

    bool key_exists(
        const std::string &database_name,
        const std::string &collection_name,
        const  entity::VariantKey &key);

private:
    MongoClientImpl* client_;
};

}