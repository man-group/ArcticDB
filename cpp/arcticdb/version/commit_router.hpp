/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/error_code.hpp>
#include <arcticdb/util/hash.hpp>

#include <fmt/format.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace arcticdb::version {

inline constexpr uint32_t kDefaultVirtualShards = 256;
inline constexpr uint32_t kSequencerProtocolVersion = 2;

[[nodiscard]] inline uint32_t compute_virtual_shard(
        std::string_view library_name, std::string_view symbol, uint32_t num_virtual_shards = kDefaultVirtualShards
) noexcept {
    if (num_virtual_shards == 0) {
        return 0;
    }
    HashAccum h;
    if (!library_name.empty()) {
        h(library_name.data(), library_name.size());
    }
    const char sep = '\0';
    h(&sep, 1);
    if (!symbol.empty()) {
        h(symbol.data(), symbol.size());
    }
    return static_cast<uint32_t>(h.digest() % static_cast<HashedValue>(num_virtual_shards));
}

[[nodiscard]] inline proto::sequencer::ConflictPolicy conflict_policy_from_string(std::string_view s) {
    std::string lower;
    lower.reserve(s.size());
    for (unsigned char c : s) {
        lower.push_back(static_cast<char>(std::tolower(c)));
    }
    if (lower.empty() || lower == "unspecified" || lower == "default") {
        return proto::sequencer::CONFLICT_POLICY_UNSPECIFIED;
    }
    if (lower == "strict_append" || lower == "strict") {
        return proto::sequencer::STRICT_APPEND;
    }
    if (lower == "reject") {
        return proto::sequencer::REJECT;
    }
    if (lower == "overwrite_lww" || lower == "lww" || lower == "overwrite") {
        return proto::sequencer::OVERWRITE_LWW;
    }
    if (lower == "keep_both") {
        return proto::sequencer::KEEP_BOTH;
    }
    throw UserInputException(fmt::format("Unknown sequencer conflict_policy '{}'", s));
}

struct SequencerClientConfig {
    std::string endpoint; // Single endpoint or comma-separated HA seed list
    uint32_t inline_threshold_bytes{65536};
    proto::sequencer::ConflictPolicy conflict_policy{proto::sequencer::CONFLICT_POLICY_UNSPECIFIED};
    std::vector<std::string> conflict_key_columns;
    uint32_t request_timeout_ms{30000};
    uint32_t connect_timeout_ms{5000};
    uint32_t max_retries{64};
    uint32_t total_retry_budget_ms{25000};

    // TLS / mTLS & Auth (vendor-agnostic: TLS 1.3, cert-manager mTLS, OIDC/K8s SA JWT, static bearer token)
    std::string ca_cert_path;
    std::string ca_cert_dir;
    std::string client_cert_path;
    std::string client_key_path;
    bool verify_tls{true};
    std::string bearer_token;
    std::string bearer_token_file;
    std::string auth_token_header{"Authorization"};
    std::vector<std::string> allowed_redirect_hosts;

    // Transport / HTTP/2 pooling
    bool enable_http2{true};
    bool use_multiplexing{true};

    bool operator==(const SequencerClientConfig&) const = default;
};

// Generic client-side commit routing interface implemented by HTTP/Protobuf transport or in-process test stubs.
// Keeps ArcticDB Core completely free of enterprise server, fencing, or watchdog logic.
class CommitRouter {
  public:
    virtual ~CommitRouter() = default;

    virtual proto::sequencer::CommitResponse submit(const proto::sequencer::CommitRequest& request) = 0;

    virtual proto::sequencer::HealthResponse health() = 0;

    [[nodiscard]] virtual std::string endpoint() const = 0;
};

[[noreturn]] inline void throw_from_commit_response(const proto::sequencer::CommitResponse& response) {
    const std::string msg = response.error_message().empty()
                                    ? fmt::format("Sequencer commit failed with status {}", static_cast<int>(response.status()))
                                    : response.error_message();
    switch (response.status()) {
        case proto::sequencer::SCHEMA_MISMATCH:
            if (response.arctic_error_code() == static_cast<int32_t>(ErrorCode::E_DESCRIPTOR_MISMATCH)) {
                throw StreamDescriptorMismatch(msg);
            }
            throw SchemaException(msg);
        case proto::sequencer::UNSORTED_INDEX:
            throw UnsortedDataException(msg);
        case proto::sequencer::NO_SUCH_VERSION:
            throw NoSuchVersionException(msg);
        case proto::sequencer::DATA_CONFLICT:
            throw DataConflictException(msg);
        case proto::sequencer::SLICE_CONFLICT:
            throw SliceConflictException(msg);
        case proto::sequencer::INVALID_ARGUMENT:
        case proto::sequencer::PAYLOAD_TOO_LARGE:
            throw UserInputException(msg);
        case proto::sequencer::UNAUTHENTICATED:
        case proto::sequencer::PERMISSION_DENIED:
            throw PermissionException(msg);
        case proto::sequencer::QUOTA_EXCEEDED:
            throw QuotaExceededException(msg);
        case proto::sequencer::FENCED_REDIRECT:
        case proto::sequencer::NOT_LEADER:
        case proto::sequencer::SHARD_DRAINING:
        case proto::sequencer::OVERLOADED:
            throw StorageException(msg);
        case proto::sequencer::INTERNAL_ERROR:
        case proto::sequencer::COMMIT_STATUS_UNSPECIFIED:
        case proto::sequencer::OK:
        default:
            throw InternalException(msg);
    }
}

} // namespace arcticdb::version
