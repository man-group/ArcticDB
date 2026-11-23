/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/version/commit_router.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace arcticdb::version {

// Thread-safe libcurl CommitRouter client supporting:
//   - TLS 1.3 + mTLS (with automatic client-cert rotation detection via mtime)
//   - Vendor-agnostic Bearer/OIDC/K8s SA token file hot-reload
//   - Shared CURLSH* (DNS + SSL session + connection cache) and persistent PooledHandle reuse
//   - CURLM HTTP/2 multiplexing (CURL_HTTP_VERSION_2TLS + CURLPIPE_MULTIPLEX) for HTTPS endpoints
//   - Multi-seed HA topology caching (GET /v1/topology, Phase 2 404 fallback), virtual-shard routing,
//     validated 0ms leader_hint redirects (no scheme downgrade), and full-jitter retry budgets
class HttpCommitRouter final : public CommitRouter {
  public:
    explicit HttpCommitRouter(SequencerClientConfig config);
    explicit HttpCommitRouter(std::string endpoint, uint32_t request_timeout_ms = 30000);
    ~HttpCommitRouter() override;

    HttpCommitRouter(const HttpCommitRouter&) = delete;
    HttpCommitRouter& operator=(const HttpCommitRouter&) = delete;

    proto::sequencer::CommitResponse submit(const proto::sequencer::CommitRequest& request) override;
    proto::sequencer::HealthResponse health() override;
    [[nodiscard]] std::string endpoint() const override;

    // Fetch and cache cluster topology from any reachable seed; returns true on update.
    bool refresh_topology(
            bool force = false, const std::string& deprioritize_endpoint = "", uint32_t max_timeout_ms = 2000
    );

    [[nodiscard]] uint64_t cached_topology_version() const noexcept;
    [[nodiscard]] std::string route_endpoint_for(std::string_view library_name, std::string_view symbol) const;

    // Validates a server-supplied redirect/leader_hint URL against scheme-downgrade and host allowlist rules.
    [[nodiscard]] bool is_valid_redirect_endpoint(const std::string& candidate) const;
    [[nodiscard]] bool is_statically_allowed_endpoint(const std::string& candidate) const;

  private:
    struct HttpResult {
        long http_status{0};
        std::string body;
        std::string transport_error;
        uint32_t retry_after_ms{0};
        long http_version{0};
    };

    struct PooledHandle {
        void* curl{nullptr};
        void* headers{nullptr};
        uint64_t cert_generation{0};
        std::string auth_header_value;
        bool is_post{true};
    };

    struct MultiRequest {
        std::string url;
        std::string post_body;
        bool is_post{true};
        std::string auth_header;
        uint64_t cert_generation{0};
        uint32_t timeout_override_ms{0};
        HttpResult result;
        std::mutex mu;
        std::condition_variable cv;
        bool done{false};
    };

    HttpResult perform_request(
            const std::string& base_endpoint, const std::string& path, const std::string* post_body,
            uint32_t timeout_override_ms = 0
    );
    HttpResult perform_request_easy(
            const std::string& base_endpoint, const std::string& url, const std::string* post_body,
            const std::string& auth_header, uint64_t cert_gen, uint32_t timeout_override_ms
    );
    HttpResult perform_request_multi(
            const std::string& url, const std::string* post_body, const std::string& auth_header, uint64_t cert_gen,
            uint32_t timeout_override_ms
    );

    void configure_handle_common(void* curl, const std::string& url, const std::string& auth_header, bool is_https);
    PooledHandle acquire_handle(
            const std::string& base_endpoint, bool is_post, const std::string& auth_header, uint64_t cert_gen
    );
    void release_handle(const std::string& base_endpoint, PooledHandle&& handle, bool keep_alive);
    void clear_handle_pool();

    void start_multi_loop();
    void stop_multi_loop();
    void multi_loop_fn();

    std::string resolve_auth_header_value();
    uint64_t check_client_cert_generation();
    void update_endpoint(const std::string& new_endpoint);
    void update_shard_hint(uint32_t shard_id, const std::string& leader_endpoint);
    std::vector<std::string> candidate_seed_endpoints() const;

    static std::string normalize_endpoint(std::string ep);
    static std::vector<std::string> parse_seed_endpoints(const std::string& raw);
    static bool parse_url_parts(
            const std::string& url, std::string& scheme, std::string& host, uint16_t& port, bool& has_path_or_query
    );

    SequencerClientConfig config_;
    std::vector<std::string> seed_endpoints_;
    bool require_https_{false};
    std::unordered_set<std::string> static_allowed_hosts_;
    std::vector<std::string> static_allowed_suffixes_;

    mutable std::mutex endpoint_mutex_;
    std::string active_endpoint_;

    // HA Topology state
    mutable std::mutex topology_mutex_;
    proto::sequencer::ClusterTopology topology_;
    bool has_topology_{false};
    std::atomic<bool> topology_supported_{true};
    uint32_t num_virtual_shards_{kDefaultVirtualShards};
    std::vector<std::string> shard_owner_endpoints_; // indexed by shard_id
    std::unordered_set<std::string> topology_allowed_hosts_;
    std::chrono::steady_clock::time_point last_topology_refresh_{};
    bool topology_refresh_in_flight_{false};
    std::condition_variable topology_cv_;

    // Shared CURLSH* (DNS / SSL session / connect cache across handles)
    void* curl_share_{nullptr};
    void* curl_share_ctx_{nullptr};
    std::mutex share_dns_mutex_;
    std::mutex share_ssl_mutex_;
    std::mutex share_connect_mutex_;
    std::mutex share_generic_mutex_;

    // Per-endpoint persistent PooledHandle pool
    std::mutex pool_mutex_;
    std::unordered_map<std::string, std::vector<PooledHandle>> idle_handles_;

    // Optional CURLM HTTP/2 multiplexing loop (for HTTPS endpoints when use_multiplexing=true)
    std::mutex multi_mutex_;
    std::deque<std::shared_ptr<MultiRequest>> multi_queue_;
    void* curl_multi_{nullptr};
    std::thread multi_thread_;
    std::atomic<bool> multi_running_{false};
    std::atomic<bool> multi_stop_{false};

    // Bearer token file reload cache & mTLS client cert rotation tracker
    std::mutex auth_mutex_;
    std::string cached_file_token_;
    std::filesystem::file_time_type cached_token_mtime_{};
    bool has_cached_token_mtime_{false};
    std::chrono::steady_clock::time_point last_token_check_{};

    std::mutex cert_mutex_;
    std::filesystem::file_time_type cached_cert_mtime_{};
    std::uintmax_t cached_cert_size_{0};
    std::filesystem::file_time_type cached_key_mtime_{};
    std::uintmax_t cached_key_size_{0};
    bool has_cached_cert_mtime_{false};
    std::chrono::steady_clock::time_point last_cert_check_{};
    std::atomic<uint64_t> cert_generation_{1};
};

} // namespace arcticdb::version
