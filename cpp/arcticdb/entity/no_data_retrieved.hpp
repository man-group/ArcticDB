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

class NoDataRetrieved {
public:
    NoDataRetrieved(const StreamId& symbol,
                    const pipelines::VersionQueryType& version_query_type) :
        symbol_(symbol) {
        util::variant_match(
                version_query_type,
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

    NoDataRetrieved() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(NoDataRetrieved);

    std::string symbol() const {
        return fmt::format("{}", symbol_);
    }

    VersionRequestType version_request_type() const {
        return version_request_type_;
    }

    std::variant<std::monostate, int64_t, std::string> version_request_data() const {
        return version_request_data_;
    }

    std::string to_string() const {
        switch (version_request_type_) {
            case VersionRequestType::SNAPSHOT:
                return fmt::format("Symbol '{}' not found in snapshot '{}'", symbol_, std::get<std::string>(version_request_data_));
            case VersionRequestType::TIMESTAMP:
                return fmt::format("Symbol '{}' not found at time '{}'", symbol_, std::get<int64_t>(version_request_data_));
            case VersionRequestType::SPECIFIC:
                return fmt::format("Symbol '{}' not found with specified version '{}'", symbol_, std::get<int64_t>(version_request_data_));
            case VersionRequestType::LATEST:
                return fmt::format("Symbol '{}' not found with specified version 'latest'", symbol_);
            default:
                internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                        "Unexpected enum value in NoDataRetrieved::to_string: {}",
                        static_cast<uint32_t>(version_request_type_));
        }
    }
private:
    StreamId symbol_;
    VersionRequestType version_request_type_{VersionRequestType::LATEST};
    // int64_t for timestamp and SignedVersionId
    std::variant<std::monostate, int64_t, std::string> version_request_data_;
};
}