/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/mock/lmdb_mock_client.hpp>
#include <arcticdb/codec/segment.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/util/string_utils.hpp>
#include <arcticdb/storage/lmdb/lmdb.hpp>


namespace arcticdb::storage::lmdb {

std::string MockLmdbClient::get_failure_trigger(
        const std::string& path,
        StorageOperation operation_to_fail,
        int error_code) {
    return fmt::format("{}#Failure_{}_{}", path, operation_to_string(operation_to_fail), error_code);
}

std::string_view lmdb_operation_string(StorageOperation operation) {
    switch (operation) {
        case StorageOperation::READ:
            return "mdb_get";
        case StorageOperation::WRITE:
            return "mdb_put";
        case StorageOperation::DELETE:
            return "mdb_del";
        case StorageOperation::LIST:
            return "mdb_cursor_get";
        case StorageOperation::EXISTS:
            return "mdb_get";
        default:
            return "unknown";
    }
}

void raise_if_has_failure_trigger(const LmdbKey& key, StorageOperation operation) {
    auto path = key.path_;
    auto failure_string_for_operation = "#Failure_" + operation_to_string(operation) + "_";
    auto position = path.rfind(failure_string_for_operation);

    if (position == std::string::npos) {
        return;
    }

    int error_code = 0;
    try {
        auto start = position + failure_string_for_operation.size();
        error_code = stoi(path.substr(start));
        auto error_message = fmt::format("Simulated Error, message: operation {}, error code {}",
                                         operation_to_string(operation), error_code);
        log::storage().warn("{}", error_message);
    } catch (std::exception&) {
        return;
    }

    if (error_code != 0) {
        ::lmdb::error::raise(lmdb_operation_string(operation).data(), error_code);
    }
}

void raise_key_exists_error(std::string_view lmdb_op) {
    ::lmdb::error::raise(lmdb_op.data(), MDB_KEYEXIST);
}

bool MockLmdbClient::has_key(const LmdbKey& key) const {
    return lmdb_contents_.find(key) != lmdb_contents_.end();
}

bool MockLmdbClient::exists(const std::string& db_name, std::string& path, ::lmdb::txn&, ::lmdb::dbi&) const {
    LmdbKey key = {db_name, path};
    raise_if_has_failure_trigger(key, StorageOperation::EXISTS);

    return has_key(key);
}

std::optional<Segment> MockLmdbClient::read(const std::string& db_name, std::string& path, ::lmdb::txn&, ::lmdb::dbi&) const {
    LmdbKey key = {db_name, path};
    raise_if_has_failure_trigger(key, StorageOperation::READ);

    if (!has_key(key)) {
        return std::nullopt;
    }

    return std::make_optional<Segment>(lmdb_contents_.at(key).clone());
}

void MockLmdbClient::write(const std::string& db_name, std::string& path, arcticdb::Segment& segment,
                           ::lmdb::txn&, ::lmdb::dbi&, int64_t) {
    LmdbKey key = {db_name, path};
            raise_if_has_failure_trigger(key, StorageOperation::WRITE);

    if(has_key(key)) {
        raise_key_exists_error(lmdb_operation_string(StorageOperation::WRITE));
    } else {
        lmdb_contents_.try_emplace(key, segment.clone());
    }
}

bool MockLmdbClient::remove(const std::string& db_name, std::string& path, ::lmdb::txn&, ::lmdb::dbi&) {
    LmdbKey key = {db_name, path};
    raise_if_has_failure_trigger(key, StorageOperation::DELETE);

    if (!has_key(key)) {
        return false;
    }

    lmdb_contents_.erase(key);
    return true;
}

std::vector<VariantKey> MockLmdbClient::list(const std::string& db_name, const std::string& prefix, ::lmdb::txn&,
                                             ::lmdb::dbi&, KeyType key_type) const {
    std::vector<VariantKey> found_keys;

    for (const auto& [key, segment] : lmdb_contents_) {
        if (key.db_name_ == db_name && util::string_starts_with(prefix, key.path_)) {
            raise_if_has_failure_trigger(key, StorageOperation::LIST);

            auto k = variant_key_from_bytes(
                    reinterpret_cast<const uint8_t *>(key.path_.data()),
                    key.path_.size(),
                    key_type);
            found_keys.push_back(k);
        }
    }

    return found_keys;
}

} // namespace arcticdb::storage::lmdb
