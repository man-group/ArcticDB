/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <string>

#include <arcticdb/pipeline/query.hpp>

namespace arcticdb::entity {

enum class VersionRequestType: uint32_t {
    SNAPSHOT,
    TIMESTAMP,
    SPECIFIC,
    LATEST
};

class DataError {
public:
    DataError(StreamId symbol,
              std::string&& exception_string,
              const std::optional<pipelines::VersionQueryType>& version_query_type=std::nullopt,
              std::optional<ErrorCode> error_code=std::nullopt);

    DataError() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(DataError);

    void set_error_code(ErrorCode error_code);

    std::string symbol() const;

    std::optional<VersionRequestType>  version_request_type() const;

    std::optional<std::variant<std::monostate, int64_t, SnapshotId>> version_request_data() const;

    std::optional<ErrorCode> error_code() const;

    std::optional<ErrorCategory> error_category() const;

    std::string exception_string() const;

    std::string to_string() const;
private:
    StreamId symbol_;
    std::optional<VersionRequestType> version_request_type_;
    // int64_t for timestamp and SignedVersionId
    std::optional<std::variant<std::monostate, int64_t, SnapshotId>> version_request_data_;
    std::string exception_string_;
    std::optional<ErrorCode> error_code_;
};

}  // namespace arcticdb::entity
