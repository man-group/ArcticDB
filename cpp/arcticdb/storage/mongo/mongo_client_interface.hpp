/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <fmt/format.h>

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/entity/protobufs.hpp>

namespace arcticdb::storage::mongo {

// Some relevant error codes from mongo (not exhaustive). See https://www.mongodb.com/docs/manual/reference/error-codes/
enum class MongoError {
    NoSuchKey = 2,
    HostUnreachable = 6,
    HostNotFound = 7,
    UnknownError = 8,
    UserNotFound = 11,
    UnAuthorized = 13,
    AuthenticationFailed = 18,
    ExceededTimeLimit = 50,
    WriteConflict = 112,
    KeyNotFound = 211,
    DuplicateKey = 11000,
    NoAcknowledge = 50000, // custom error code for simulating no server acknowledgement
};

// modified_count set to null_opt signals update failed. mongocxx returns nullopt if server does not acknowledge the
// operation
struct UpdateResult {
    std::optional<int> modified_count;
};

// delete_count set to null_opt signals delete failed. mongocxx returns nullopt if server does not acknowledge the
// operation
struct DeleteResult {
    std::optional<int> delete_count;
};

class MongoClientWrapper {
  public:
    virtual ~MongoClientWrapper() = default;

    virtual bool write_segment(
            const std::string& database_name, const std::string& collection_name, storage::KeySegmentPair& key_seg
    ) = 0;

    virtual UpdateResult update_segment(
            const std::string& database_name, const std::string& collection_name, storage::KeySegmentPair& key_seg,
            bool upsert
    ) = 0;

    virtual std::optional<KeySegmentPair> read_segment(
            const std::string& database_name, const std::string& collection_name, const entity::VariantKey& key
    ) = 0;

    virtual DeleteResult remove_keyvalue(
            const std::string& database_name, const std::string& collection_name, const entity::VariantKey& key
    ) = 0;

    virtual std::vector<VariantKey> list_keys(
            const std::string& database_name, const std::string& collection_name, KeyType key_type,
            const std::optional<std::string>& prefix
    ) = 0;

    virtual void drop_collection(std::string database_name, std::string collection_name) = 0;

    virtual bool key_exists(
            const std::string& database_name, const std::string& collection_name, const entity::VariantKey& key
    ) = 0;
};

} // namespace arcticdb::storage::mongo
