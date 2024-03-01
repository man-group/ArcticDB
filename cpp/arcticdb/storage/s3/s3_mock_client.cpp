/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/s3/s3_mock_client.hpp>
#include <arcticdb/storage/s3/s3_client_wrapper.hpp>

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/buffer_pool.hpp>

#include <arcticdb/storage/storage_utils.hpp>

#include <aws/s3/S3Errors.h>

namespace arcticdb::storage{

using namespace object_store_utils;

namespace s3 {

std::string MockS3Client::get_failure_trigger(
        const std::string& s3_object_name,
        StorageOperation operation_to_fail,
        Aws::S3::S3Errors error_to_fail_with,
        bool retryable) {
    return fmt::format("{}#Failure_{}_{}_{}", s3_object_name, operation_to_string(operation_to_fail), (int)error_to_fail_with, (int) retryable);
}

std::optional<Aws::S3::S3Error> object_has_failure_trigger(const std::string& s3_object_name, StorageOperation operation) {
    auto failure_string_for_operation = "#Failure_" + operation_to_string(operation) + "_";
    auto position = s3_object_name.rfind(failure_string_for_operation);
    if (position == std::string::npos) return std::nullopt;
    try {
        auto start = position + failure_string_for_operation.size();
        auto failure_code_string = s3_object_name.substr(start, s3_object_name.find_last_of('_') - start);
        auto failure_code = Aws::S3::S3Errors(std::stoi(failure_code_string));
        bool retryable = std::stoi(s3_object_name.substr(s3_object_name.find_last_of('_') + 1));
        return Aws::S3::S3Error(Aws::Client::AWSError<Aws::S3::S3Errors>(failure_code, "Simulated error", "Simulated error message", retryable));
    } catch (std::exception&) {
        return std::nullopt;
    }
}

const auto not_found_error = Aws::S3::S3Error(Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::RESOURCE_NOT_FOUND, false));

std::optional<Aws::S3::S3Error> MockS3Client::has_failure_trigger(const std::pair<std::string, std::string>& key, StorageOperation op) const {
    return object_has_failure_trigger(key.second, op);
}

std::pair<std::string, std::string> get_key(const std::string& bucket_name, const std::string& s3_object_name) {
    return {bucket_name, s3_object_name};
}

std::vector<std::pair<std::string, std::string>> get_keys(const std::string& bucket_name, const std::vector<std::string>& objects) {
    std::vector<std::pair<std::string, std::string>> keys;
    for (auto& object : objects) {
        keys.emplace_back(get_key(bucket_name, object));
    }
    return keys;
}

Aws::S3::S3Error MockS3Client::missing_key_failure() const { return not_found_error; }

bool MockS3Client::matches_prefix(const std::pair<std::string, std::string>& key, const std::string& prefix) const {
    return key.second.rfind(prefix, 0) == 0;
}

S3Result<std::monostate> MockS3Client::head_object(
        const std::string& s3_object_name,
        const std::string &bucket_name) const {
    auto result = exists_internal(get_key(bucket_name, s3_object_name));
    if(!result.is_success()) return {result.get_error()};
    if(!result.get_output()) return {not_found_error};
    return {std::monostate()};
}


S3Result<Segment> MockS3Client::get_object(
        const std::string &s3_object_name,
        const std::string &bucket_name) const {
    return {read_internal(get_key(bucket_name, s3_object_name)).result};
}

S3Result<std::monostate> MockS3Client::put_object(
        const std::string &s3_object_name,
        Segment &&segment,
        const std::string &bucket_name) {
    return {write_internal(get_key(bucket_name, s3_object_name), std::move(segment)).result};
}

S3Result<DeleteOutput> MockS3Client::delete_objects(
        const std::vector<std::string>& s3_object_names,
        const std::string& bucket_name) {
    auto result = delete_internal(get_keys(bucket_name, s3_object_names));
    if (!result.is_success()) return {result.get_error()};

    DeleteOutput output;
    for (auto& key : result.get_output()){
        output.failed_deletes.push_back({key.second, "Sample error message"});
    }
    return {output};
}

// Using a fixed page size since it's only being used for simple tests.
// If we ever need to configure it we should move it to the s3 proto config instead.
constexpr auto page_size = 10;
S3Result<ListObjectsOutput> MockS3Client::list_objects(
        const std::string& name_prefix,
        const std::string& bucket_name,
        const std::optional<std::string> continuation_token) const {
    // Terribly inefficient but fine for tests.
    auto matching_names = std::vector<std::string>();
    for (auto& key : contents_){
        if (key.first.first == bucket_name && key.first.second.rfind(name_prefix, 0) == 0){
            matching_names.emplace_back(key.first.second);
        }
    }

    auto start_from = 0u;
    if (continuation_token.has_value()) {
        start_from = std::stoi(continuation_token.value());
    }

    ListObjectsOutput output;
    auto end_to = matching_names.size();
    if (start_from + page_size < end_to){
        end_to = start_from + page_size;
        output.next_continuation_token = std::to_string(end_to);
    }

    for (auto i=start_from; i < end_to; ++i){
        auto& s3_object_name = matching_names[i];

        auto maybe_error = object_has_failure_trigger(s3_object_name, StorageOperation::LIST);
        if (maybe_error.has_value()) return {maybe_error.value()};

        output.s3_object_names.emplace_back(s3_object_name);
    }

    return {output};
}

}

}
