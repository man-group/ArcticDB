/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/data_error.hpp>

namespace arcticdb::entity {

DataError::DataError(
        StreamId symbol, std::string&& exception_string,
        const std::optional<pipelines::VersionQueryType>& version_query_type, std::optional<ErrorCode> error_code
) :
    symbol_(std::move(symbol)),
    exception_string_(std::move(exception_string)),
    error_code_(error_code) {
    if (version_query_type.has_value()) {
        util::variant_match(
                *version_query_type,
                [this](const pipelines::SnapshotVersionQuery& query) {
                    version_request_type_ = VersionRequestType::SNAPSHOT;
                    version_request_data_ = query.name_;
                },
                [this](const pipelines::TimestampVersionQuery& query) {
                    version_request_type_ = VersionRequestType::TIMESTAMP;
                    version_request_data_ = query.timestamp_;
                },
                [this](const pipelines::SpecificVersionQuery& query) {
                    version_request_type_ = VersionRequestType::SPECIFIC;
                    version_request_data_ = query.version_id_;
                },
                [](const std::shared_ptr<SchemaItem>&) {
                    util::raise_rte("collect_schema() not supported with batch methods");
                },
                [this](const std::monostate&) { version_request_type_ = VersionRequestType::LATEST; }
        );
    }
}

void DataError::set_error_code(ErrorCode error_code) { error_code_ = error_code; }

std::string DataError::symbol() const { return fmt::format("{}", symbol_); }

std::optional<VersionRequestType> DataError::version_request_type() const { return version_request_type_; }

std::optional<std::variant<std::monostate, int64_t, SnapshotId>> DataError::version_request_data() const {
    return version_request_data_;
}

std::optional<ErrorCode> DataError::error_code() const { return error_code_; }

std::optional<ErrorCategory> DataError::error_category() const {
    return error_code_.has_value() ? std::make_optional<ErrorCategory>(get_error_category(*error_code_)) : std::nullopt;
}

std::string DataError::exception_string() const { return exception_string_; }

std::string DataError::to_string() const {
    std::string version_request_explanation = "UNKNOWN";
    if (version_request_type_.has_value()) {
        switch (*version_request_type_) {
        case VersionRequestType::SNAPSHOT:
            version_request_explanation = fmt::format("in snapshot '{}'", std::get<SnapshotId>(*version_request_data_));
            break;
        case VersionRequestType::TIMESTAMP:
            version_request_explanation = fmt::format("at time '{}'", std::get<int64_t>(*version_request_data_));
            break;
        case VersionRequestType::SPECIFIC:
            version_request_explanation =
                    fmt::format("with specified version '{}'", std::get<int64_t>(*version_request_data_));
            break;
        case VersionRequestType::LATEST:
            version_request_explanation = fmt::format("with specified version 'latest'");
            break;
        default:
            internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                    "Unexpected enum value in DataError::to_string: {}", static_cast<uint32_t>(*version_request_type_)
            );
        }
    }
    auto category = error_category();
    auto category_name = category ? get_error_category_names().at(*category) : "UNKNOWN";
    return fmt::format(
            "Version {} of symbol '{}' unable to be retrieved: Error Category: '{}' Exception String: '{}'",
            version_request_explanation,
            symbol_,
            category_name,
            exception_string_
    );
}
} // namespace arcticdb::entity