/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <aws/s3/S3Errors.h>

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/buffer_pool.hpp>

#include <arcticdb/storage/object_store_utils.hpp>
#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/storage/mock/storage_mock_client.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/util/exponential_backoff.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/util/composite.hpp>

namespace arcticdb::storage {

using namespace object_store_utils;

namespace s3 {

template<typename Output, typename Error>
struct StorageResult {
    std::variant<Output, Error> result;

    [[nodiscard]] bool is_success() const { return std::holds_alternative<Output>(result); }

    Error& get_error() { return std::get<Error>(result); }
    Output& get_output() { return std::get<Output>(result); }
};

template<class Output>
using S3Result = StorageResult<Output, Aws::S3::S3Error>;

struct ListObjectsOutput {
    std::vector<std::string> s3_object_names;
    std::vector<uint64_t> s3_object_sizes;
    // next_continuation_token indicates there are more s3_objects to be listed because they didn't fit in one response.
    // If set can be used to get the remaining s3_objects.
    std::optional<std::string> next_continuation_token;
};

struct FailedDelete {
    std::string s3_object_name;
    std::string error_message;

#ifdef __apple_build_version__
    // Required because Apple Clang doesn't support aggregate initialization.
    FailedDelete(const std::string& name, const std::string& message) : s3_object_name(name), error_message(message) {}
#endif
};

struct DeleteObjectsOutput {
    std::vector<FailedDelete> failed_deletes;
};

enum class PutHeader { NONE, IF_NONE_MATCH };

// An abstract class, which is responsible for sending the requests and parsing the responses from S3.
// It can be derived as either a real connection to S3 or a mock used for unit tests.
class S3ClientInterface {
  public:
    [[nodiscard]] virtual S3Result<std::monostate> head_object(
            const std::string& s3_object_name, const std::string& bucket_name
    ) const = 0;

    [[nodiscard]] virtual S3Result<Segment> get_object(
            const std::string& s3_object_name, const std::string& bucket_name
    ) const = 0;

    [[nodiscard]] virtual folly::Future<S3Result<Segment>> get_object_async(
            const std::string& s3_object_name, const std::string& bucket_name
    ) const = 0;

    virtual S3Result<std::monostate> put_object(
            const std::string& s3_object_name, Segment& segment, const std::string& bucket_name,
            PutHeader header = PutHeader::NONE
    ) = 0;

    virtual S3Result<DeleteObjectsOutput> delete_objects(
            const std::vector<std::string>& s3_object_names, const std::string& bucket_name
    ) = 0;

    [[nodiscard]] virtual folly::Future<S3Result<std::monostate>> delete_object(
            const std::string& s3_object_name, const std::string& bucket_name
    ) = 0;

    [[nodiscard]] virtual S3Result<ListObjectsOutput> list_objects(
            const std::string& prefix, const std::string& bucket_name,
            const std::optional<std::string>& continuation_token
    ) const = 0;

    virtual ~S3ClientInterface() = default;
};

} // namespace s3

} // namespace arcticdb::storage
