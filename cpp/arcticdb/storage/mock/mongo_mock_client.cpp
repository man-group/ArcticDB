/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/mongo/mongo_client_interface.hpp>
#include <arcticdb/storage/mock/mongo_mock_client.hpp>
#include <arcticdb/storage/object_store_utils.hpp>

#include <bsoncxx/builder/basic/document.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/exception/bulk_write_exception.hpp>
#include <mongocxx/exception/query_exception.hpp>

namespace arcticdb::storage::mongo {

std::string MockMongoClient::get_failure_trigger(
        const std::string& key,
        StorageOperation operation_to_fail,
        MongoError error_code) {
    return fmt::format("{}#Failure_{}_{}", key, operation_to_string(operation_to_fail), static_cast<int>(error_code));
}

template <typename exception_type>
MongoFailure create_failure(const std::string& message, MongoError error_code) {
    static_assert(std::is_base_of<mongocxx::operation_exception, exception_type>::value, "exception_type must be a subclass of mongocxx::operation_exception");
    if (error_code == MongoError::NoAcknowledge) {
        return {no_ack_failure()};
    }
    bsoncxx::document::value empty_doc_value = bsoncxx::builder::basic::document{}.extract();
    auto ec = std::error_code(static_cast<int>(error_code), std::generic_category());

    return {exception_type(ec, std::move(empty_doc_value), message)};
}

MongoFailure get_failure(const std::string& message, StorageOperation operation, MongoError error_code) {
    switch(operation) {
        case StorageOperation::READ:
            return create_failure<mongocxx::query_exception>(message, error_code);
        case StorageOperation::WRITE:
            return create_failure<mongocxx::bulk_write_exception>(message, error_code);
        case StorageOperation::EXISTS:
            return create_failure<mongocxx::query_exception>(message, error_code);
        case StorageOperation::DELETE:
            [[fallthrough]];
        case StorageOperation::DELETE_LOCAL:
            return create_failure<mongocxx::bulk_write_exception>(message, error_code);
        case StorageOperation::LIST:
            return create_failure<mongocxx::query_exception>(message, error_code);
        default:
            util::raise_rte("Unknown operation used for error trigger");
    }
}

std::optional<MongoFailure> has_failure_trigger(
        const MongoKey& key,
        StorageOperation operation) {
    auto key_id = key.doc_key_.id_string();
    auto failure_string_for_operation = "#Failure_" + operation_to_string(operation) + "_";
    auto position = key_id.rfind(failure_string_for_operation);
    if (position == std::string::npos) {
        return std::nullopt;
    }

    try {
        auto start = position + failure_string_for_operation.size();
        auto error_code = MongoError(stoi(key_id.substr(start)));
        auto error_message = fmt::format("Simulated Error, message: operation {}, error code {}",
                                         operation_to_string(operation), static_cast<int>(error_code));

        return get_failure(error_message, operation, error_code);
    } catch (std::exception&) {
        return std::nullopt;
    }
}

bool matches_prefix(
        const MongoKey& key,
        const std::string& database_name,
        const std::string& collection_name,
        const std::string& prefix) {

    return key.database_name_ == database_name && key.collection_name_ == collection_name &&
           key.doc_key_.id_string().find(prefix) == 0;
}

void throw_if_exception(MongoFailure& failure) {
    if (!failure.is_no_ack_failure()) {
        throw failure.get_exception();
    }
}

bool MockMongoClient::has_key(const MongoKey& key) {
    return mongo_contents.find(key) != mongo_contents.end();
}

bool MockMongoClient::write_segment(
        const std::string& database_name,
        const std::string& collection_name,
        storage::KeySegmentPair&& key_seg) {
    auto key = MongoKey(database_name, collection_name, key_seg.variant_key());

    auto failure = has_failure_trigger(key, StorageOperation::WRITE);
    if (failure.has_value()) {
        throw_if_exception(*failure);
        return false;
    }

    mongo_contents.insert_or_assign(std::move(key), std::move(key_seg.segment()));
    return true;
}

UpdateResult MockMongoClient::update_segment(
        const std::string& database_name,
        const std::string& collection_name,
        storage::KeySegmentPair&& key_seg,
        bool upsert) {
    auto key = MongoKey(database_name, collection_name, key_seg.variant_key());

    auto failure = has_failure_trigger(key, StorageOperation::WRITE);
    if (failure.has_value()) {
        throw_if_exception(*failure);
        return {std::nullopt};
    }

    auto key_found = has_key(key);
    if (!upsert && !key_found) {
        return {0}; // upsert is false, don't update and return 0 as modified_count
    }

    mongo_contents.insert_or_assign(std::move(key), std::move(key_seg.segment()));
    return {key_found ? 1 : 0};
}

std::optional<KeySegmentPair> MockMongoClient::read_segment(
        const std::string& database_name,
        const std::string& collection_name,
        const entity::VariantKey& key) {
    auto mongo_key = MongoKey(database_name, collection_name, key);
    auto failure = has_failure_trigger(mongo_key, StorageOperation::READ);
    if (failure.has_value()) {
        throw_if_exception(*failure);
        return std::nullopt;
    }

    auto it = mongo_contents.find(mongo_key);
    if (it == mongo_contents.end()) {
        return std::nullopt;
    }

    return KeySegmentPair(std::move(mongo_key.doc_key_.key_), std::move(it->second));
}

DeleteResult MockMongoClient::remove_keyvalue(
        const std::string& database_name,
        const std::string& collection_name,
        const entity::VariantKey& key) {
    auto mongo_key = MongoKey(database_name, collection_name, key);
    auto failure = has_failure_trigger(mongo_key, StorageOperation::DELETE);
    if (failure.has_value()) {
        throw_if_exception(*failure);
        return {std::nullopt};
    }

    auto key_found = has_key(mongo_key);
    if (!key_found) {
        return {0}; // key not found, return 0 as deleted_count
    }

    mongo_contents.erase(mongo_key);
    return {1};
}

bool MockMongoClient::key_exists(
        const std::string& database_name,
        const std::string& collection_name,
        const  entity::VariantKey& key) {
    auto mongo_key = MongoKey(database_name, collection_name, key);
    auto failure = has_failure_trigger(mongo_key, StorageOperation::EXISTS);
    if (failure.has_value()) {
        throw_if_exception(*failure);
        return false;
    }

    return has_key(mongo_key);
}

std::vector<VariantKey> MockMongoClient::list_keys(
        const std::string& database_name,
        const std::string& collection_name,
        KeyType,
        const std::optional<std::string>& prefix) {
    std::string prefix_str = prefix.has_value() ? prefix.value() : "";
    std::vector<VariantKey> output;

    for (auto& key : mongo_contents) {
        if (matches_prefix(key.first, database_name, collection_name, prefix_str)) {
            auto failure = has_failure_trigger(key.first, StorageOperation::LIST);
            if (failure.has_value()) {
                throw_if_exception(*failure);
                return {};
            }
            output.push_back(key.first.doc_key_.key_);
        }
    }

    return output;
}

void MockMongoClient::ensure_collection(std::string_view, std::string_view ) {
    // a database, collection is always guaranteed to be created if not existent
}

void MockMongoClient::drop_collection(std::string database_name, std::string collection_name) {
    for (auto it = mongo_contents.begin(); it != mongo_contents.end(); ) {
        if (it->first.database_name_ == database_name && it->first.collection_name_ == collection_name) {
            it = mongo_contents.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace arcticdb::storage::mongo
