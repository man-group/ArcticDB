/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/storage/mongo/mongo_client_wrapper.hpp>
#include <arcticdb/storage/mongo/mongo_mock_client.hpp>
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
    if(error_code == MongoError::NoAcknowledge) return {std::monostate()};

    bsoncxx::document::value empty_doc_value = bsoncxx::builder::basic::document{}.extract();
    auto ec = std::error_code((int)error_code, std::generic_category());

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
            return create_failure<mongocxx::bulk_write_exception>(message, error_code);
        case StorageOperation::DELETE_LOCAL:
            return create_failure<mongocxx::bulk_write_exception>(message, error_code);
        case StorageOperation::LIST:
            return create_failure<mongocxx::query_exception>(message, error_code);
        default:
            util::raise_rte("Unknown operation used for error trigger");
    }
}

std::optional<MongoFailure> MockMongoClient::has_failure_trigger(
        const mongo_document_key& key,
        StorageOperation operation) const {
    auto key_id = get<2>(key);
    auto failure_string_for_operation = "#Failure_" + operation_to_string(operation) + "_";
    auto position = key_id.rfind(failure_string_for_operation);
    if (position == std::string::npos) return std::nullopt;

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

MongoFailure MockMongoClient::missing_key_failure() const {
    return {std::monostate()};
}

bool MockMongoClient::matches_prefix(const mongo_document_key& key, const mongo_document_key& prefix) const {

    return get<0>(key) == get<0>(prefix) && get<1>(key) == get<1>(prefix) && get<2>(key).find(get<2>(prefix)) == 0;
}

mongo_document_key make_key(
        const std::string& database_name,
        const std::string& collection_name,
        const storage::VariantKey& key) {
    return {database_name, collection_name, fmt::format("{}", variant_key_id(key))};
}

template<typename Output>
bool is_no_ack_failure(StorageResult<Output, MongoFailure>& result) {
    return !result.is_success() && result.get_error().is_no_ack_failure();
}

template<typename Output>
void throw_if_exception(StorageResult<Output, MongoFailure>& result) {
    if(!result.is_success() && !result.get_error().is_no_ack_failure()) {
        throw result.get_error().get_exception();
    }
}

bool MockMongoClient::write_segment(
        const std::string& database_name,
        const std::string& collection_name,
        storage::KeySegmentPair&& kv) {
    auto key = make_key(database_name, collection_name, kv.variant_key());
    auto result = write_internal(key, std::move(kv.segment()));
    throw_if_exception(result);
    return !is_no_ack_failure(result);
}

UpdateResult MockMongoClient::update_segment(
        const std::string& database_name,
        const std::string& collection_name,
        storage::KeySegmentPair&& kv,
        bool upsert) {
    auto key = make_key(database_name, collection_name, kv.variant_key());
    auto key_found = has_key(key);
    if(!upsert && !key_found) return {0}; // upsert is false, don't update and return 0 as modified_count
    if(!write_segment(database_name, collection_name, std::move(kv))) return {std::nullopt};
    return {key_found ? 1 : 0};
}

std::optional<KeySegmentPair> MockMongoClient::read_segment(
        const std::string& database_name,
        const std::string& collection_name,
        const entity::VariantKey& k) {
    auto key = make_key(database_name, collection_name, k);
    auto result = read_internal(key);
    throw_if_exception(result);
    if(is_no_ack_failure(result)) return std::nullopt;

    Segment segment = result.get_output();
    VariantKey variant = k;
    return KeySegmentPair(std::move(variant), std::move(segment));
}

DeleteResult MockMongoClient::remove_keyvalue(
        const std::string& database_name,
        const std::string& collection_name,
        const entity::VariantKey& k) {
    auto key = make_key(database_name, collection_name, k);
    auto key_found = has_key(key);
    auto result = delete_internal({key});
    throw_if_exception(result);

    if(!key_found) return {0}; // key not found, return 0 as deleted_count
    return is_no_ack_failure(result) ? DeleteResult{std::nullopt} : DeleteResult{1};
}

bool MockMongoClient::key_exists(
        const std::string& database_name,
        const std::string& collection_name,
        const  entity::VariantKey& k) {
    auto key = make_key(database_name, collection_name, k);
    auto result = exists_internal(key);
    throw_if_exception(result);
    return result.is_success() && result.get_output();
}

std::vector<VariantKey> MockMongoClient::list_keys(
        const std::string& database_name,
        const std::string& collection_name,
        KeyType,
        const std::optional<std::string>& prefix) {
    mongo_document_key k = {database_name, collection_name, prefix.has_value() ? prefix.value() : ""};
    auto result = list_internal(k);

    throw_if_exception(result);
    if(is_no_ack_failure(result)) return {};

    auto output_list = result.get_output();
    std::vector<VariantKey> keys;
    for(auto&& key : output_list) {
        auto builder = arcticdb::atom_key_builder();
        // as a limitation we only support TABLE_DATA type key this is because entire Variant Key is not saved in the map. This is ok for testing purposes.
        // saving entire key made it problematic to encode errors in the key as the id could be numeric too which wouldn't allow the string #Failure_...
        keys.push_back(builder.build<arcticdb::entity::KeyType::TABLE_DATA>(get<2>(key)));
    }

    return keys;
}

void MockMongoClient::ensure_collection(std::string_view, std::string_view ) {
    // a database, collection is always guaranteed to be created if not existent
}

void MockMongoClient::drop_collection(std::string database_name, std::string collection_name) {
    for (auto it = contents_.begin(); it != contents_.end(); ) {
        if (get<0>(it->first) == database_name && get<1>(it->first) == collection_name) {
            it = contents_.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace arcticdb::storage::mongo
