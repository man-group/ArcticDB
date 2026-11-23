/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/mock/s3_mock_client.hpp>
#include <arcticdb/storage/s3/s3_client_interface.hpp>

#include <aws/s3/S3Errors.h>

namespace arcticdb::storage {

using namespace object_store_utils;

namespace s3 {

std::optional<Aws::S3::S3Error> has_object_failure_trigger(
        const std::string& s3_object_name, StorageOperation operation
) {
    auto failure_string_for_operation = "#Failure_" + operation_to_string(operation) + "_";
    auto position = s3_object_name.rfind(failure_string_for_operation);
    if (position == std::string::npos)
        return std::nullopt;

    try {
        auto start = position + failure_string_for_operation.size();
        auto failure_code_string = s3_object_name.substr(start, s3_object_name.find_last_of('_') - start);
        auto failure_code = Aws::S3::S3Errors(std::stoi(failure_code_string));
        bool retryable = std::stoi(s3_object_name.substr(s3_object_name.find_last_of('_') + 1));
        return Aws::S3::S3Error(Aws::Client::AWSError<Aws::S3::S3Errors>(
                failure_code,
                "Simulated error",
                fmt::format("Simulated error message for object {}", s3_object_name),
                retryable
        ));
    } catch (std::exception&) {
        return std::nullopt;
    }
}

Aws::S3::S3Error create_error(
        Aws::S3::S3Errors error_type, const std::string& exception_name = "", const std::string& exception_message = "",
        bool is_retriable = false, std::optional<Aws::Http::HttpResponseCode> response_code = std::nullopt
) {
    auto error = Aws::S3::S3Error(
            Aws::Client::AWSError<Aws::S3::S3Errors>(error_type, exception_name, exception_message, is_retriable)
    );
    if (response_code.has_value()) {
        error.SetResponseCode(response_code.value());
    }
    return error;
}

const Aws::S3::S3Error not_found_error = create_error(Aws::S3::S3Errors::RESOURCE_NOT_FOUND);
const Aws::S3::S3Error precondition_failed_error = create_error(
        Aws::S3::S3Errors::UNKNOWN, "PreconditionFailed", "Precondition failed", false,
        Aws::Http::HttpResponseCode::PRECONDITION_FAILED
);
const Aws::S3::S3Error not_implemented_error = create_error(
        Aws::S3::S3Errors::UNKNOWN, "NotImplemented",
        "A header you provided implies functionality that is not implemented", false
);

S3Result<std::monostate> MockS3Client::head_object(const std::string& s3_object_name, const std::string& bucket_name)
        const {
    std::scoped_lock<std::mutex> lock(mutex_);
    auto pos = s3_contents_.find({bucket_name, s3_object_name});
    if (pos == s3_contents_.end() || !pos->second.has_value()) {
        return {not_found_error};
    }
    return {std::monostate()};
}

S3Result<ObjectMetadata> MockS3Client::head_object_metadata(
        const std::string& s3_object_name, const std::string& bucket_name
) const {
    std::scoped_lock<std::mutex> lock(mutex_);
    auto pos = s3_contents_.find({bucket_name, s3_object_name});
    if (pos == s3_contents_.end() || !pos->second.has_value()) {
        return {not_found_error};
    }
    return {ObjectMetadata{pos->second->etag, pos->second->size, pos->second->last_modified_ms}};
}

S3Result<Segment> MockS3Client::get_object(const std::string& s3_object_name, const std::string& bucket_name) const {
    auto res = get_object_with_etag(s3_object_name, bucket_name);
    if (!res.is_success()) {
        return {std::move(res.get_error())};
    }
    return {std::move(res.get_output().segment)};
}

S3Result<GetObjectWithEtagOutput> MockS3Client::get_object_with_etag(
        const std::string& s3_object_name, const std::string& bucket_name
) const {
    std::scoped_lock<std::mutex> lock(mutex_);
    auto pos = s3_contents_.find({bucket_name, s3_object_name});
    if (pos == s3_contents_.end() || !pos->second.has_value()) {
        return {not_found_error};
    }
    return {GetObjectWithEtagOutput{pos->second->segment.clone(), pos->second->etag}};
}

folly::Future<S3Result<Segment>> MockS3Client::get_object_async(
        const std::string& s3_object_name, const std::string& bucket_name
) const {
    return folly::makeFuture(get_object(s3_object_name, bucket_name));
}

S3Result<std::monostate> MockS3Client::put_object(
        const std::string& s3_object_name, Segment& segment, const std::string& bucket_name, PutHeader header
) {
    auto res = put_object_with_etag(s3_object_name, segment, bucket_name, header, std::nullopt);
    if (!res.is_success()) {
        return {std::move(res.get_error())};
    }
    return {std::monostate()};
}

S3Result<PutObjectOutput> MockS3Client::put_object_with_etag(
        const std::string& s3_object_name, Segment& segment, const std::string& bucket_name, PutHeader header,
        const std::optional<std::string>& if_match_etag
) {
    std::scoped_lock<std::mutex> lock(mutex_);
    auto pos = s3_contents_.find({bucket_name, s3_object_name});
    const bool exists = pos != s3_contents_.end() && pos->second.has_value();
    if (header == PutHeader::IF_NONE_MATCH && exists) {
        return {precondition_failed_error};
    }
    if (header == PutHeader::IF_MATCH) {
        if (!exists || !if_match_etag.has_value() || pos->second->etag != *if_match_etag) {
            return {precondition_failed_error};
        }
    }

    // When we write Segments we occasionally don't fill in the size_. This is fine as it's not needed for writing.
    // However, it is required when reading so we need to calculate it. It's easier to do on write.
    const uint64_t size = segment.calculate_size();
    // Synthetic per-write quoted ETag (matching AWS S3) and monotonic timestamp (ms) for metadata listing tests.
    const uint64_t seq = ++write_seq_;
    std::string etag = fmt::format("\"mock-{}-{}\"", bucket_name, seq);
    MockObject obj{segment.clone(), size, etag, static_cast<int64_t>(seq * 1000)};
    s3_contents_.insert_or_assign({bucket_name, s3_object_name}, std::move(obj));

    return {PutObjectOutput{std::move(etag)}};
}

S3Result<DeleteObjectsOutput> MockS3Client::delete_objects(
        const std::vector<std::string>& s3_object_names, const std::string& bucket_name
) {
    std::scoped_lock<std::mutex> lock(mutex_);
    DeleteObjectsOutput output;
    for (const auto& s3_object_name : s3_object_names) {
        auto pos = s3_contents_.find({bucket_name, s3_object_name});
        if (pos == s3_contents_.end() || !pos->second.has_value()) {
            output.failed_deletes.emplace_back(s3_object_name, "KeyNotFound");
        } else {
            pos->second = std::nullopt;
        }
    }
    return {output};
}

folly::Future<S3Result<std::monostate>> MockS3Client::delete_object(
        const std::string& s3_object_name, const std::string& bucket_name
) {
    std::scoped_lock<std::mutex> lock(mutex_);
    auto pos = s3_contents_.find({bucket_name, s3_object_name});
    if (pos == s3_contents_.end() || !pos->second.has_value()) {
        S3Result<std::monostate> res{not_found_error};
        return folly::makeFuture(res);
    } else {
        pos->second = std::nullopt;
        S3Result<std::monostate> res{};
        return folly::makeFuture(res);
    }
}

// Using a fixed page size since it's only being used for simple tests.
// If we ever need to configure it we should move it to the s3 proto config instead.
constexpr auto page_size = 10;
S3Result<ListObjectsOutput> MockS3Client::list_objects(
        const std::string& name_prefix, const std::string& bucket_name,
        const std::optional<std::string>& continuation_token
) const {
    std::scoped_lock<std::mutex> lock(mutex_);
    ListObjectsOutput output;
    auto it = s3_contents_.begin();
    if (continuation_token.has_value()) {
        // We use the next s3_object_name as a continuation token. This way if new s3 objects get added to the
        // s3_contents map in between list_objects calls we won't return duplicate entries.
        it = s3_contents_.find({bucket_name, continuation_token.value()});
        util::check(it != s3_contents_.end(), "Invalid mock continuation_token");
    }
    for (auto i = 0u; it != s3_contents_.end() && i < page_size; ++it, ++i) {
        if (it->first.bucket_name == bucket_name && it->first.s3_object_name.rfind(name_prefix, 0) == 0 &&
            it->second.has_value()) {
            output.s3_object_names.emplace_back(it->first.s3_object_name);
            output.s3_object_sizes.emplace_back(it->second->size);
            output.s3_object_etags.emplace_back(it->second->etag);
            output.s3_object_last_modified_ms.emplace_back(it->second->last_modified_ms);
        }
    }
    if (it != s3_contents_.end()) {
        output.next_continuation_token = it->first.s3_object_name;
    }
    return {output};
}

} // namespace s3

} // namespace arcticdb::storage
