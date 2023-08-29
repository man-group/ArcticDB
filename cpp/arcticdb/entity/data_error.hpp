/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <string>

#include <fmt/format.h>

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
    DataError(const StreamId& symbol,
              std::string&& exception_string,
              const std::optional<pipelines::VersionQueryType>& version_query_type=std::nullopt,
              std::optional<ErrorCode> error_code=std::nullopt) :
        symbol_(symbol),
        exception_string_(exception_string),
        error_code_(error_code){
        if (version_query_type.has_value()){
            util::variant_match(
                    *version_query_type,
                    [this] (const pipelines::SnapshotVersionQuery& query) {
                        version_request_type_ = VersionRequestType::SNAPSHOT;
                        version_request_data_ = query.name_;
                    },
                    [this] (const pipelines::TimestampVersionQuery& query) {
                        version_request_type_ = VersionRequestType::TIMESTAMP;
                        version_request_data_ = query.timestamp_;
                    },
                    [this] (const pipelines::SpecificVersionQuery& query) {
                        version_request_type_ = VersionRequestType::SPECIFIC;
                        version_request_data_ = query.version_id_;
                    },
                    [this] (const std::monostate&) {
                        version_request_type_ = VersionRequestType::LATEST;
                    }
            );
        }
    }

    DataError() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(DataError);

    void set_error_code(ErrorCode error_code) {
        error_code_ = error_code;
    }

    std::string symbol() const {
        return fmt::format("{}", symbol_);
    }

    std::optional<VersionRequestType>  version_request_type() const {
        return version_request_type_;
    }

    std::optional<std::variant<std::monostate, int64_t, std::string>> version_request_data() const {
        return version_request_data_;
    }

    std::optional<ErrorCode> error_code() const {
        return error_code_;
    }

    std::optional<ErrorCategory> error_category() const {
        return error_code_.has_value() ? std::make_optional<ErrorCategory>(get_error_category(*error_code_)) : std::nullopt;
    }

    std::string exception_string() const {
        return exception_string_;
    }

    std::string to_string() const {
        std::string version_request_explanation = "UNKNOWN";
        if (version_request_type_.has_value()) {
            switch (*version_request_type_) {
                case VersionRequestType::SNAPSHOT:
                    version_request_explanation = fmt::format("in snapshot '{}'", std::get<std::string>(*version_request_data_));
                    break;
                case VersionRequestType::TIMESTAMP:
                    version_request_explanation = fmt::format("at time '{}'", std::get<int64_t>(*version_request_data_));
                    break;
                case VersionRequestType::SPECIFIC:
                    version_request_explanation = fmt::format("with specified version '{}'", std::get<int64_t>(*version_request_data_));
                    break;
                case VersionRequestType::LATEST:
                    version_request_explanation = fmt::format("with specified version 'latest'");
                    break;
                default:
                    internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                            "Unexpected enum value in DataError::to_string: {}",
                            static_cast<uint32_t>(*version_request_type_));
            }
        }
        return fmt::format(
                "Version {} of symbol '{}' unable to be retrieved: Error Category: '{}' Exception String: '{}'",
                version_request_explanation,
                symbol_,
                error_category().has_value() ? get_error_category_names().at(*error_category()) : "UNKNOWN",
                exception_string_);
    }
private:
    StreamId symbol_;
    std::optional<VersionRequestType> version_request_type_;
    // int64_t for timestamp and SignedVersionId
    std::optional<std::variant<std::monostate, int64_t, std::string>> version_request_data_;
    std::string exception_string_;
    std::optional<ErrorCode> error_code_;
};
}