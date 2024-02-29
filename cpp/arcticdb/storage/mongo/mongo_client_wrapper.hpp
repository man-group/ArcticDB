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

struct UpdateResult { std::optional<int> modified_count; };

struct DeleteResult { std::optional<int> delete_count; };

class MongoClientWrapper {
public:
    virtual ~MongoClientWrapper() = default;

    virtual bool write_segment(
            const std::string &database_name,
            const std::string &collection_name,
            storage::KeySegmentPair&& kv) = 0;

    virtual UpdateResult update_segment(
            const std::string &database_name,
            const std::string &collection_name,
            storage::KeySegmentPair&& kv,
            bool upsert) = 0;

    virtual std::optional<KeySegmentPair> read_segment(
            const std::string &database_name,
            const std::string &collection_name,
            const entity::VariantKey &key) = 0;

    virtual DeleteResult remove_keyvalue(
            const std::string &database_name,
            const std::string &collection_name,
            const entity::VariantKey &key) = 0;

    virtual std::vector<VariantKey> list_keys(
            const std::string &database_name,
            const std::string &collection_name,
            KeyType key_type,
            const std::optional<std::string> &prefix) = 0;

    virtual void ensure_collection(
            std::string_view database_name,
            std::string_view collection_name) = 0;

    virtual void drop_collection(
            std::string database_name,
            std::string collection_name) = 0;

    virtual bool key_exists(
            const std::string &database_name,
            const std::string &collection_name,
            const  entity::VariantKey &key) = 0;
};

}
