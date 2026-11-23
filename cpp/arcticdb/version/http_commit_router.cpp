/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/http_commit_router.hpp>
#include <arcticdb/log/log.hpp>

#include <curl/curl.h>
#include <fmt/format.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstring>
#include <fstream>
#include <limits>
#include <random>
#include <sstream>
#include <thread>
#include <utility>

namespace arcticdb::version {

namespace {

void ensure_curl_global_init() {
    static std::once_flag init_flag;
    std::call_once(init_flag, []() { curl_global_init(CURL_GLOBAL_DEFAULT); });
    // Intentionally never call curl_global_cleanup() to match S3ApiInstance lifecycle (s3_api.cpp:539).
}

constexpr size_t kMaxHttpResponseBytes = 64ULL * 1024ULL * 1024ULL;

size_t curl_write_callback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    if (size != 0 && nmemb > std::numeric_limits<size_t>::max() / size) {
        return 0;
    }
    const size_t total = size * nmemb;
    auto* out = static_cast<std::string*>(userdata);
    if (out == nullptr || total > kMaxHttpResponseBytes || out->size() > kMaxHttpResponseBytes - total) {
        return 0;
    }
    out->append(ptr, total);
    return total;
}

struct HeaderParseState {
    uint32_t retry_after_ms{0};
};

bool ascii_iequals_prefix(std::string_view line, std::string_view prefix) {
    if (line.size() < prefix.size()) {
        return false;
    }
    for (size_t i = 0; i < prefix.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(line[i])) !=
            std::tolower(static_cast<unsigned char>(prefix[i]))) {
            return false;
        }
    }
    return true;
}

std::string_view trim_sv(std::string_view s) {
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) {
        s.remove_prefix(1);
    }
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) {
        s.remove_suffix(1);
    }
    return s;
}

size_t curl_header_callback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    const size_t total = size * nmemb;
    auto* state = static_cast<HeaderParseState*>(userdata);
    if (state == nullptr || total == 0) {
        return total;
    }
    std::string_view line(ptr, total);
    if (ascii_iequals_prefix(line, "x-arctic-retry-after-ms:")) {
        const auto val = trim_sv(line.substr(24));
        try {
            const unsigned long ms = std::stoul(std::string(val));
            state->retry_after_ms = std::max<uint32_t>(state->retry_after_ms, static_cast<uint32_t>(std::min<unsigned long>(ms, 60000UL)));
        } catch (...) {
        }
    } else if (ascii_iequals_prefix(line, "retry-after:")) {
        const auto val = trim_sv(line.substr(12));
        try {
            const unsigned long sec = std::stoul(std::string(val));
            const uint32_t ms = static_cast<uint32_t>(std::min<unsigned long>(sec * 1000UL, 60000UL));
            if (state->retry_after_ms == 0) {
                state->retry_after_ms = ms;
            }
        } catch (...) {
        }
    }
    return total;
}

std::string to_lower_ascii(std::string s) {
    for (char& c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return s;
}

struct ShareLockContext {
    std::mutex* dns_mu;
    std::mutex* ssl_mu;
    std::mutex* conn_mu;
    std::mutex* generic_mu;
};

void curl_share_lock_cb(CURL*, curl_lock_data data, curl_lock_access, void* userptr) {
    auto* ctx = static_cast<ShareLockContext*>(userptr);
    switch (data) {
        case CURL_LOCK_DATA_DNS:
            ctx->dns_mu->lock();
            break;
        case CURL_LOCK_DATA_SSL_SESSION:
            ctx->ssl_mu->lock();
            break;
        case CURL_LOCK_DATA_CONNECT:
            ctx->conn_mu->lock();
            break;
        default:
            ctx->generic_mu->lock();
            break;
    }
}

void curl_share_unlock_cb(CURL*, curl_lock_data data, void* userptr) {
    auto* ctx = static_cast<ShareLockContext*>(userptr);
    switch (data) {
        case CURL_LOCK_DATA_DNS:
            ctx->dns_mu->unlock();
            break;
        case CURL_LOCK_DATA_SSL_SESSION:
            ctx->ssl_mu->unlock();
            break;
        case CURL_LOCK_DATA_CONNECT:
            ctx->conn_mu->unlock();
            break;
        default:
            ctx->generic_mu->unlock();
            break;
    }
}

bool contains_crlf(std::string_view s) noexcept {
    for (unsigned char c : s) {
        if (c == '\r' || c == '\n' || c == '\0') {
            return true;
        }
    }
    return false;
}

bool is_https_url(std::string_view url) noexcept {
    return ascii_iequals_prefix(url, "https://");
}

struct curl_slist* build_request_headers(bool is_post, const std::string& auth_header_name, const std::string& auth_val) {
    struct curl_slist* headers = nullptr;
    if (is_post) {
        // Clear Expect: header to avoid libcurl's 1-second 100-continue stall on POSTs > 1 KiB.
        headers = curl_slist_append(headers, "Expect:");
        headers = curl_slist_append(headers, "Content-Type: application/x-protobuf");
    }
    headers = curl_slist_append(headers, "Accept: application/x-protobuf");
    headers = curl_slist_append(headers, "X-ArcticDB-Sequencer-Protocol: 2");
    if (!auth_val.empty()) {
        const std::string hdr_name = auth_header_name.empty() ? "Authorization" : auth_header_name;
        if (!contains_crlf(hdr_name) && !contains_crlf(auth_val)) {
            const std::string line = hdr_name + ": " + auth_val;
            headers = curl_slist_append(headers, line.c_str());
        }
    }
    return headers;
}

uint32_t jittered_backoff_ms(uint32_t base_ms) {
    if (base_ms <= 1) {
        return base_ms;
    }
    thread_local std::mt19937 rng(std::random_device{}());
    const uint32_t lo = base_ms / 2;
    std::uniform_int_distribution<uint32_t> dist(lo, base_ms);
    return dist(rng);
}

bool parse_strict_port(std::string_view port_sv, uint16_t& out_port) noexcept {
    if (port_sv.empty() || port_sv.size() > 5) {
        return false;
    }
    uint32_t val = 0;
    for (unsigned char c : port_sv) {
        if (!std::isdigit(c)) {
            return false;
        }
        val = val * 10u + static_cast<uint32_t>(c - '0');
    }
    if (val == 0 || val > 65535u) {
        return false;
    }
    out_port = static_cast<uint16_t>(val);
    return true;
}

bool is_valid_hostname_ascii(std::string_view host, bool is_ipv6) noexcept {
    if (host.empty()) {
        return false;
    }
    if (is_ipv6) {
        bool has_colon = false;
        for (unsigned char c : host) {
            if (c == ':') {
                has_colon = true;
            } else if (!std::isxdigit(c) && c != '.') {
                return false;
            }
        }
        return has_colon;
    }
    if (host.front() == '.' || host.front() == '-' || host.back() == '-') {
        return false;
    }
    char prev = 0;
    for (unsigned char c : host) {
        if (!std::isalnum(c) && c != '.' && c != '-') {
            return false;
        }
        if (c == '.' && prev == '.') {
            return false;
        }
        prev = static_cast<char>(c);
    }
    return true;
}

} // namespace

std::string HttpCommitRouter::normalize_endpoint(std::string ep) {
    while (!ep.empty() && std::isspace(static_cast<unsigned char>(ep.front()))) {
        ep.erase(ep.begin());
    }
    while (!ep.empty() && (std::isspace(static_cast<unsigned char>(ep.back())) || ep.back() == '/')) {
        ep.pop_back();
    }
    const size_t sep = ep.find("://");
    if (sep != std::string::npos) {
        for (size_t i = 0; i < sep; ++i) {
            ep[i] = static_cast<char>(std::tolower(static_cast<unsigned char>(ep[i])));
        }
    }
    return ep;
}

std::vector<std::string> HttpCommitRouter::parse_seed_endpoints(const std::string& raw) {
    std::vector<std::string> out;
    std::stringstream ss(raw);
    std::string item;
    while (std::getline(ss, item, ',')) {
        std::string norm = normalize_endpoint(std::move(item));
        if (!norm.empty()) {
            out.push_back(std::move(norm));
        }
    }
    return out;
}

bool HttpCommitRouter::parse_url_parts(
        const std::string& url, std::string& scheme, std::string& host, uint16_t& port, bool& has_path_or_query
) {
    has_path_or_query = false;
    if (url.empty()) {
        return false;
    }
    for (unsigned char c : url) {
        if (c < 0x20 || c == 0x7f || std::isspace(c) || c == '\\' || c == '%') {
            return false;
        }
    }
    const size_t sep = url.find("://");
    if (sep == std::string::npos) {
        return false;
    }
    scheme = to_lower_ascii(url.substr(0, sep));
    if (scheme != "http" && scheme != "https") {
        return false;
    }
    std::string rest = url.substr(sep + 3);
    if (rest.empty()) {
        return false;
    }
    // Reject embedded credentials (user:pass@host)
    if (rest.find('@') != std::string::npos) {
        return false;
    }
    const size_t path_pos = rest.find_first_of("/?#");
    if (path_pos != std::string::npos) {
        has_path_or_query = true;
        rest = rest.substr(0, path_pos);
    }
    if (rest.empty()) {
        return false;
    }
    port = (scheme == "https") ? 443 : 80;
    bool is_ipv6 = false;
    if (rest.front() == '[') {
        is_ipv6 = true;
        const size_t close = rest.find(']');
        if (close == std::string::npos) {
            return false;
        }
        host = to_lower_ascii(rest.substr(1, close - 1));
        if (close + 1 < rest.size()) {
            if (rest[close + 1] != ':') {
                return false;
            }
            if (!parse_strict_port(std::string_view(rest).substr(close + 2), port)) {
                return false;
            }
        }
    } else {
        const size_t colon = rest.rfind(':');
        if (colon != std::string::npos) {
            host = to_lower_ascii(rest.substr(0, colon));
            if (!parse_strict_port(std::string_view(rest).substr(colon + 1), port)) {
                return false;
            }
        } else {
            host = to_lower_ascii(rest);
        }
    }
    return is_valid_hostname_ascii(host, is_ipv6);
}

HttpCommitRouter::HttpCommitRouter(SequencerClientConfig config) : config_(std::move(config)) {
    ensure_curl_global_init();

    auto parsed_seeds = parse_seed_endpoints(config_.endpoint);
    if (parsed_seeds.empty() && !config_.endpoint.empty()) {
        parsed_seeds.push_back(normalize_endpoint(config_.endpoint));
    }

    for (const auto& ep : parsed_seeds) {
        std::string scheme, host;
        uint16_t port = 0;
        bool has_pq = false;
        if (parse_url_parts(ep, scheme, host, port, has_pq)) {
            if (scheme == "https") {
                require_https_ = true;
            }
        }
    }
    for (const auto& ep : parsed_seeds) {
        std::string scheme, host;
        uint16_t port = 0;
        bool has_pq = false;
        if (parse_url_parts(ep, scheme, host, port, has_pq)) {
            if (require_https_ && scheme != "https") {
                log::version().warn("HttpCommitRouter dropping plaintext seed '{}' because HTTPS is required", ep);
                continue;
            }
            seed_endpoints_.push_back(ep);
            static_allowed_hosts_.insert(host);
        } else {
            seed_endpoints_.push_back(ep);
        }
    }
    active_endpoint_ = seed_endpoints_.empty() ? "" : seed_endpoints_.front();

    for (const auto& pattern : config_.allowed_redirect_hosts) {
        std::string p = to_lower_ascii(pattern);
        if (p.size() >= 2 && p.front() == '[' && p.back() == ']') {
            p = p.substr(1, p.size() - 2);
        }
        if (p.size() > 2 && p[0] == '*' && p[1] == '.') {
            static_allowed_suffixes_.push_back(p.substr(1)); // keep leading '.'
        } else if (p.size() > 1 && p[0] == '.') {
            static_allowed_suffixes_.push_back(std::move(p));
        } else if (!p.empty()) {
            static_allowed_hosts_.insert(std::move(p));
        }
    }

    // Initialize shared CURLSH* for DNS + SSL session + connection caching across handles.
    auto* share = curl_share_init();
    if (share != nullptr) {
        auto* lock_ctx = new ShareLockContext{
                &share_dns_mutex_, &share_ssl_mutex_, &share_connect_mutex_, &share_generic_mutex_
        };
        curl_share_setopt(share, CURLSHOPT_LOCKFUNC, curl_share_lock_cb);
        curl_share_setopt(share, CURLSHOPT_UNLOCKFUNC, curl_share_unlock_cb);
        curl_share_setopt(share, CURLSHOPT_USERDATA, lock_ctx);
        curl_share_setopt(share, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS);
        curl_share_setopt(share, CURLSHOPT_SHARE, CURL_LOCK_DATA_SSL_SESSION);
        curl_share_setopt(share, CURLSHOPT_SHARE, CURL_LOCK_DATA_CONNECT);
        curl_share_ = share;
        curl_share_ctx_ = lock_ctx;
    }

    if (config_.use_multiplexing && config_.enable_http2 && require_https_) {
        start_multi_loop();
    }
}

HttpCommitRouter::HttpCommitRouter(std::string endpoint, uint32_t request_timeout_ms) :
    HttpCommitRouter(SequencerClientConfig{
            .endpoint = std::move(endpoint),
            .request_timeout_ms = request_timeout_ms,
    }) {}

HttpCommitRouter::~HttpCommitRouter() {
    stop_multi_loop();
    clear_handle_pool();
    if (curl_share_ != nullptr) {
        auto* share = static_cast<CURLSH*>(curl_share_);
        curl_share_setopt(share, CURLSHOPT_LOCKFUNC, nullptr);
        curl_share_setopt(share, CURLSHOPT_UNLOCKFUNC, nullptr);
        curl_share_cleanup(share);
        curl_share_ = nullptr;
    }
    if (curl_share_ctx_ != nullptr) {
        delete static_cast<ShareLockContext*>(curl_share_ctx_);
        curl_share_ctx_ = nullptr;
    }
}

void HttpCommitRouter::clear_handle_pool() {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    for (auto& [ep, handles] : idle_handles_) {
        for (auto& h : handles) {
            if (h.headers != nullptr) {
                curl_slist_free_all(static_cast<struct curl_slist*>(h.headers));
            }
            if (h.curl != nullptr) {
                curl_easy_cleanup(static_cast<CURL*>(h.curl));
            }
        }
    }
    idle_handles_.clear();
}

std::string HttpCommitRouter::endpoint() const {
    std::lock_guard<std::mutex> lock(endpoint_mutex_);
    return active_endpoint_;
}

void HttpCommitRouter::update_endpoint(const std::string& new_endpoint) {
    const std::string norm = normalize_endpoint(new_endpoint);
    if (norm.empty() || !is_valid_redirect_endpoint(norm)) {
        return;
    }
    std::lock_guard<std::mutex> lock(endpoint_mutex_);
    active_endpoint_ = norm;
}

void HttpCommitRouter::update_shard_hint(uint32_t shard_id, const std::string& leader_endpoint) {
    const std::string norm = normalize_endpoint(leader_endpoint);
    if (norm.empty() || !is_valid_redirect_endpoint(norm)) {
        return;
    }
    std::lock_guard<std::mutex> lock(topology_mutex_);
    if (shard_id < shard_owner_endpoints_.size()) {
        shard_owner_endpoints_[shard_id] = norm;
    }
}

bool HttpCommitRouter::is_statically_allowed_endpoint(const std::string& candidate) const {
    std::string scheme, host;
    uint16_t port = 0;
    bool has_pq = false;
    if (!parse_url_parts(candidate, scheme, host, port, has_pq) || has_pq) {
        return false;
    }
    if (require_https_ && scheme != "https") {
        log::version().warn("HttpCommitRouter rejected TLS-downgrade endpoint '{}'", candidate);
        return false;
    }
    if (static_allowed_hosts_.contains(host)) {
        return true;
    }
    for (const auto& suffix : static_allowed_suffixes_) {
        if (host.size() > suffix.size() && host.ends_with(suffix)) {
            return true;
        }
    }
    log::version().warn("HttpCommitRouter rejected untrusted endpoint host '{}' ({})", host, candidate);
    return false;
}

bool HttpCommitRouter::is_valid_redirect_endpoint(const std::string& candidate) const {
    return is_statically_allowed_endpoint(candidate);
}

uint64_t HttpCommitRouter::cached_topology_version() const noexcept {
    std::lock_guard<std::mutex> lock(topology_mutex_);
    return has_topology_ ? topology_.topology_version() : 0;
}

std::string HttpCommitRouter::route_endpoint_for(std::string_view library_name, std::string_view symbol) const {
    {
        std::lock_guard<std::mutex> lock(topology_mutex_);
        if (has_topology_ && !shard_owner_endpoints_.empty()) {
            const uint32_t shard_id = compute_virtual_shard(library_name, symbol, num_virtual_shards_);
            if (shard_id < shard_owner_endpoints_.size() && !shard_owner_endpoints_[shard_id].empty()) {
                return shard_owner_endpoints_[shard_id];
            }
        }
    }
    return endpoint();
}

std::vector<std::string> HttpCommitRouter::candidate_seed_endpoints() const {
    std::vector<std::string> eps;
    std::unordered_set<std::string> seen;
    auto push = [&](const std::string& ep) {
        if (!ep.empty() && seen.insert(ep).second) {
            eps.push_back(ep);
        }
    };
    push(endpoint());
    {
        std::lock_guard<std::mutex> lock(topology_mutex_);
        if (has_topology_) {
            for (const auto& m : topology_.members()) {
                if (m.state() == proto::sequencer::MEMBER_ALIVE && !m.endpoint().empty()) {
                    const std::string norm = normalize_endpoint(m.endpoint());
                    if (is_statically_allowed_endpoint(norm)) {
                        push(norm);
                    }
                }
            }
        }
    }
    for (const auto& s : seed_endpoints_) {
        push(s);
    }
    return eps;
}

std::string HttpCommitRouter::resolve_auth_header_value() {
    if (!config_.bearer_token_file.empty()) {
        std::lock_guard<std::mutex> lock(auth_mutex_);
        const auto now = std::chrono::steady_clock::now();
        if (!has_cached_token_mtime_ ||
            std::chrono::duration_cast<std::chrono::seconds>(now - last_token_check_).count() >= 5) {
            last_token_check_ = now;
            std::error_code ec;
            const auto mtime = std::filesystem::last_write_time(config_.bearer_token_file, ec);
            if (!ec && (!has_cached_token_mtime_ || mtime != cached_token_mtime_)) {
                std::ifstream ifs(config_.bearer_token_file);
                if (ifs) {
                    std::string tok((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
                    while (!tok.empty() && (tok.back() == '\n' || tok.back() == '\r' || tok.back() == ' ')) {
                        tok.pop_back();
                    }
                    // Retain previous non-empty cached token across transient 0-byte file truncation during atomic rotation.
                    if (!tok.empty() && !contains_crlf(tok)) {
                        cached_file_token_ = std::move(tok);
                        cached_token_mtime_ = mtime;
                        has_cached_token_mtime_ = true;
                    }
                }
            }
        }
        if (!cached_file_token_.empty()) {
            const std::string hdr = config_.auth_token_header.empty() ? "Authorization" : config_.auth_token_header;
            if (to_lower_ascii(hdr) == "authorization" && cached_file_token_.find(' ') == std::string::npos) {
                return "Bearer " + cached_file_token_;
            }
            return cached_file_token_;
        }
    }
    if (!config_.bearer_token.empty() && !contains_crlf(config_.bearer_token)) {
        const std::string hdr = config_.auth_token_header.empty() ? "Authorization" : config_.auth_token_header;
        if (to_lower_ascii(hdr) == "authorization" && config_.bearer_token.find(' ') == std::string::npos) {
            return "Bearer " + config_.bearer_token;
        }
        return config_.bearer_token;
    }
    return {};
}

uint64_t HttpCommitRouter::check_client_cert_generation() {
    if (config_.client_cert_path.empty() && config_.client_key_path.empty()) {
        return cert_generation_.load(std::memory_order_relaxed);
    }
    std::lock_guard<std::mutex> lock(cert_mutex_);
    const auto now = std::chrono::steady_clock::now();
    if (!has_cached_cert_mtime_ ||
        std::chrono::duration_cast<std::chrono::seconds>(now - last_cert_check_).count() >= 5) {
        last_cert_check_ = now;
        std::error_code ec_cert, ec_csz, ec_key, ec_ksz;
        const auto cert_mt = !config_.client_cert_path.empty()
                                     ? std::filesystem::last_write_time(config_.client_cert_path, ec_cert)
                                     : std::filesystem::file_time_type{};
        const auto cert_sz = !config_.client_cert_path.empty()
                                     ? std::filesystem::file_size(config_.client_cert_path, ec_csz)
                                     : std::uintmax_t{0};
        const auto key_mt = !config_.client_key_path.empty()
                                    ? std::filesystem::last_write_time(config_.client_key_path, ec_key)
                                    : std::filesystem::file_time_type{};
        const auto key_sz = !config_.client_key_path.empty()
                                    ? std::filesystem::file_size(config_.client_key_path, ec_ksz)
                                    : std::uintmax_t{0};
        if (!ec_cert && !ec_csz && !ec_key && !ec_ksz) {
            const bool changed = has_cached_cert_mtime_ &&
                                 (cert_mt != cached_cert_mtime_ || cert_sz != cached_cert_size_ ||
                                  key_mt != cached_key_mtime_ || key_sz != cached_key_size_);
            cached_cert_mtime_ = cert_mt;
            cached_cert_size_ = cert_sz;
            cached_key_mtime_ = key_mt;
            cached_key_size_ = key_sz;
            has_cached_cert_mtime_ = true;
            if (changed) {
                const uint64_t gen = cert_generation_.fetch_add(1, std::memory_order_acq_rel) + 1;
                clear_handle_pool();
                return gen;
            }
        }
    }
    return cert_generation_.load(std::memory_order_relaxed);
}

void HttpCommitRouter::configure_handle_common(
        void* raw_curl, const std::string& url, const std::string& /*auth_header*/, bool is_https
) {
    CURL* curl = static_cast<CURL*>(raw_curl);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 0L);
#if defined(CURLOPT_PROTOCOLS_STR)
    curl_easy_setopt(curl, CURLOPT_PROTOCOLS_STR, require_https_ ? "https" : "http,https");
#else
    curl_easy_setopt(curl, CURLOPT_PROTOCOLS_STR, require_https_ ? "https" : "http,https");
#endif
    curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 60L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 30L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, static_cast<long>(config_.connect_timeout_ms));
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, static_cast<long>(config_.request_timeout_ms));
    if (curl_share_ != nullptr && config_.client_cert_path.empty()) {
        curl_easy_setopt(curl, CURLOPT_SHARE, static_cast<CURLSH*>(curl_share_));
    }
    if (is_https) {
        curl_easy_setopt(
                curl,
                CURLOPT_HTTP_VERSION,
                config_.enable_http2 ? CURL_HTTP_VERSION_2TLS : CURL_HTTP_VERSION_1_1
        );
        curl_easy_setopt(curl, CURLOPT_SSLVERSION, CURL_SSLVERSION_TLSv1_3);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, config_.verify_tls ? 1L : 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, config_.verify_tls ? 2L : 0L);
        if (!config_.ca_cert_path.empty()) {
            curl_easy_setopt(curl, CURLOPT_CAINFO, config_.ca_cert_path.c_str());
        }
        if (!config_.ca_cert_dir.empty()) {
            curl_easy_setopt(curl, CURLOPT_CAPATH, config_.ca_cert_dir.c_str());
        }
        if (!config_.client_cert_path.empty()) {
            curl_easy_setopt(curl, CURLOPT_SSLCERT, config_.client_cert_path.c_str());
        }
        if (!config_.client_key_path.empty()) {
            curl_easy_setopt(curl, CURLOPT_SSLKEY, config_.client_key_path.c_str());
        }
    } else {
        curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
    }
}

HttpCommitRouter::PooledHandle HttpCommitRouter::acquire_handle(
        const std::string& base_endpoint, bool is_post, const std::string& auth_header, uint64_t cert_gen
) {
    const std::string pool_key = base_endpoint + (is_post ? "|P" : "|G");
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        auto it = idle_handles_.find(pool_key);
        if (it != idle_handles_.end() && !it->second.empty()) {
            PooledHandle h = std::move(it->second.back());
            it->second.pop_back();
            if (h.cert_generation == cert_gen) {
                if (h.auth_header_value != auth_header) {
                    if (h.headers != nullptr) {
                        curl_slist_free_all(static_cast<struct curl_slist*>(h.headers));
                    }
                    h.headers = build_request_headers(is_post, config_.auth_token_header, auth_header);
                    h.auth_header_value = auth_header;
                    curl_easy_setopt(static_cast<CURL*>(h.curl), CURLOPT_HTTPHEADER, h.headers);
                }
                return h;
            }
            // Stale cert generation -> destroy and recreate cleanly.
            if (h.headers != nullptr) {
                curl_slist_free_all(static_cast<struct curl_slist*>(h.headers));
            }
            if (h.curl != nullptr) {
                curl_easy_cleanup(static_cast<CURL*>(h.curl));
            }
        }
    }

    PooledHandle h;
    h.curl = curl_easy_init();
    h.cert_generation = cert_gen;
    h.is_post = is_post;
    h.auth_header_value = auth_header;
    if (h.curl != nullptr) {
        const bool is_https = is_https_url(base_endpoint);
        configure_handle_common(h.curl, base_endpoint, auth_header, is_https);
        h.headers = build_request_headers(is_post, config_.auth_token_header, auth_header);
        curl_easy_setopt(static_cast<CURL*>(h.curl), CURLOPT_HTTPHEADER, h.headers);
        if (is_post) {
            curl_easy_setopt(static_cast<CURL*>(h.curl), CURLOPT_POST, 1L);
        } else {
            curl_easy_setopt(static_cast<CURL*>(h.curl), CURLOPT_HTTPGET, 1L);
        }
    }
    return h;
}

void HttpCommitRouter::release_handle(const std::string& base_endpoint, PooledHandle&& handle, bool keep_alive) {
    if (handle.curl == nullptr) {
        return;
    }
    if (!keep_alive || handle.cert_generation != cert_generation_.load(std::memory_order_acquire)) {
        if (handle.headers != nullptr) {
            curl_slist_free_all(static_cast<struct curl_slist*>(handle.headers));
        }
        curl_easy_cleanup(static_cast<CURL*>(handle.curl));
        return;
    }
    const std::string pool_key = base_endpoint + (handle.is_post ? "|P" : "|G");
    std::lock_guard<std::mutex> lock(pool_mutex_);
    auto& vec = idle_handles_[pool_key];
    if (vec.size() < 64) {
        vec.push_back(std::move(handle));
    } else {
        if (handle.headers != nullptr) {
            curl_slist_free_all(static_cast<struct curl_slist*>(handle.headers));
        }
        curl_easy_cleanup(static_cast<CURL*>(handle.curl));
    }
}

void HttpCommitRouter::start_multi_loop() {
    if (multi_running_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    multi_stop_.store(false, std::memory_order_release);
    CURLM* mh = curl_multi_init();
    if (mh == nullptr) {
        multi_running_.store(false, std::memory_order_release);
        return;
    }
    curl_multi_setopt(mh, CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);
    curl_multi_setopt(mh, CURLMOPT_MAX_HOST_CONNECTIONS, 64L);
    curl_multi_setopt(mh, CURLMOPT_MAX_CONCURRENT_STREAMS, 100L);
    curl_multi_ = mh;
    multi_thread_ = std::thread([this]() { multi_loop_fn(); });
}

void HttpCommitRouter::stop_multi_loop() {
    if (!multi_running_.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(multi_mutex_);
        multi_stop_.store(true, std::memory_order_release);
        if (curl_multi_ != nullptr) {
            curl_multi_wakeup(static_cast<CURLM*>(curl_multi_));
        }
    }
    if (multi_thread_.joinable()) {
        multi_thread_.join();
    }
    {
        std::lock_guard<std::mutex> lock(multi_mutex_);
        while (!multi_queue_.empty()) {
            auto req = std::move(multi_queue_.front());
            multi_queue_.pop_front();
            {
                std::lock_guard<std::mutex> lk(req->mu);
                req->result.transport_error = "HttpCommitRouter shutting down";
                req->done = true;
            }
            req->cv.notify_one();
        }
        if (curl_multi_ != nullptr) {
            curl_multi_cleanup(static_cast<CURLM*>(curl_multi_));
            curl_multi_ = nullptr;
        }
    }
}

void HttpCommitRouter::multi_loop_fn() {
    CURLM* mh = static_cast<CURLM*>(curl_multi_);
    const uint64_t initial_cert_gen = cert_generation_.load(std::memory_order_acquire);
    std::unordered_map<std::string, uint64_t> active_cert_gen_by_origin;
    auto url_origin = [](const std::string& url) -> std::string {
        const size_t scheme_end = url.find("://");
        if (scheme_end == std::string::npos) {
            return url;
        }
        const size_t path_pos = url.find('/', scheme_end + 3);
        return path_pos == std::string::npos ? url : url.substr(0, path_pos);
    };
    struct ActiveEntry {
        std::shared_ptr<MultiRequest> req;
        struct curl_slist* headers{nullptr};
        HeaderParseState hdr_state;
    };
    std::unordered_map<CURL*, std::unique_ptr<ActiveEntry>> active;

    while (!multi_stop_.load(std::memory_order_acquire) || !active.empty()) {
        // Drain newly queued requests
        std::deque<std::shared_ptr<MultiRequest>> incoming;
        {
            std::lock_guard<std::mutex> lock(multi_mutex_);
            incoming.swap(multi_queue_);
        }
        for (auto& req : incoming) {
            if (multi_stop_.load(std::memory_order_acquire)) {
                std::lock_guard<std::mutex> lk(req->mu);
                req->result.transport_error = "HttpCommitRouter shutting down";
                req->done = true;
                req->cv.notify_one();
                continue;
            }
            CURL* easy = curl_easy_init();
            if (easy == nullptr) {
                std::lock_guard<std::mutex> lk(req->mu);
                req->result.transport_error = "curl_easy_init failed";
                req->done = true;
                req->cv.notify_one();
                continue;
            }
            auto entry = std::make_unique<ActiveEntry>();
            entry->req = req;
            entry->headers = build_request_headers(req->is_post, config_.auth_token_header, req->auth_header);
            const bool is_https = is_https_url(req->url);
            configure_handle_common(easy, req->url, req->auth_header, is_https);
            const std::string origin = url_origin(req->url);
            auto [gen_it, inserted] = active_cert_gen_by_origin.emplace(origin, initial_cert_gen);
            if (req->cert_generation > gen_it->second) {
                curl_easy_setopt(easy, CURLOPT_FRESH_CONNECT, 1L);
                curl_easy_setopt(easy, CURLOPT_SSL_SESSIONID_CACHE, 0L);
                gen_it->second = req->cert_generation;
            }
            const uint32_t eff_timeout_ms =
                    req->timeout_override_ms > 0 ? req->timeout_override_ms : config_.request_timeout_ms;
            curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS, static_cast<long>(eff_timeout_ms));
            curl_easy_setopt(easy, CURLOPT_HTTPHEADER, entry->headers);
            curl_easy_setopt(easy, CURLOPT_PIPEWAIT, 1L);
            curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, curl_write_callback);
            curl_easy_setopt(easy, CURLOPT_WRITEDATA, &req->result.body);
            curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION, curl_header_callback);
            curl_easy_setopt(easy, CURLOPT_HEADERDATA, &entry->hdr_state);
            if (req->is_post) {
                curl_easy_setopt(easy, CURLOPT_POST, 1L);
                curl_easy_setopt(easy, CURLOPT_POSTFIELDS, req->post_body.data());
                curl_easy_setopt(easy, CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(req->post_body.size()));
            } else {
                curl_easy_setopt(easy, CURLOPT_HTTPGET, 1L);
            }
            curl_multi_add_handle(mh, easy);
            active.emplace(easy, std::move(entry));
        }

        int still_running = 0;
        curl_multi_perform(mh, &still_running);

        int msgs_left = 0;
        while (CURLMsg* msg = curl_multi_info_read(mh, &msgs_left)) {
            if (msg->msg == CURLMSG_DONE) {
                CURL* easy = msg->easy_handle;
                auto it = active.find(easy);
                if (it != active.end()) {
                    auto entry = std::move(it->second);
                    active.erase(it);
                    if (msg->data.result != CURLE_OK) {
                        entry->req->result.transport_error = curl_easy_strerror(msg->data.result);
                    } else {
                        curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &entry->req->result.http_status);
                        curl_easy_getinfo(easy, CURLINFO_HTTP_VERSION, &entry->req->result.http_version);
                        entry->req->result.retry_after_ms = entry->hdr_state.retry_after_ms;
                    }
                    curl_multi_remove_handle(mh, easy);
                    if (entry->headers != nullptr) {
                        curl_slist_free_all(entry->headers);
                    }
                    curl_easy_cleanup(easy);
                    {
                        std::lock_guard<std::mutex> lk(entry->req->mu);
                        entry->req->done = true;
                    }
                    entry->req->cv.notify_one();
                }
            }
        }

        if (!multi_stop_.load(std::memory_order_acquire)) {
            curl_multi_poll(mh, nullptr, 0, 50, nullptr);
        } else if (!active.empty()) {
            curl_multi_poll(mh, nullptr, 0, 10, nullptr);
        }
    }

    for (auto& [easy, entry] : active) {
        curl_multi_remove_handle(mh, easy);
        if (entry->headers != nullptr) {
            curl_slist_free_all(entry->headers);
        }
        curl_easy_cleanup(easy);
        {
            std::lock_guard<std::mutex> lk(entry->req->mu);
            entry->req->result.transport_error = "HttpCommitRouter stopped";
            entry->req->done = true;
        }
        entry->req->cv.notify_one();
    }
}

HttpCommitRouter::HttpResult HttpCommitRouter::perform_request_multi(
        const std::string& url, const std::string* post_body, const std::string& auth_header, uint64_t cert_gen,
        uint32_t timeout_override_ms
) {
    auto req = std::make_shared<MultiRequest>();
    req->url = url;
    req->is_post = (post_body != nullptr);
    if (post_body != nullptr) {
        req->post_body = *post_body;
    }
    req->auth_header = auth_header;
    req->cert_generation = cert_gen;
    req->timeout_override_ms = timeout_override_ms;

    {
        std::lock_guard<std::mutex> lock(multi_mutex_);
        if (multi_stop_.load(std::memory_order_acquire) || curl_multi_ == nullptr) {
            HttpResult res;
            res.transport_error = "HttpCommitRouter shutting down";
            return res;
        }
        multi_queue_.push_back(req);
        curl_multi_wakeup(static_cast<CURLM*>(curl_multi_));
    }

    std::unique_lock<std::mutex> lk(req->mu);
    req->cv.wait(lk, [&req]() { return req->done; });
    return std::move(req->result);
}

HttpCommitRouter::HttpResult HttpCommitRouter::perform_request_easy(
        const std::string& base_endpoint, const std::string& url, const std::string* post_body,
        const std::string& auth_header, uint64_t cert_gen, uint32_t timeout_override_ms
) {
    HttpResult result;
    const bool is_post = (post_body != nullptr);
    PooledHandle handle = acquire_handle(base_endpoint, is_post, auth_header, cert_gen);
    CURL* curl = static_cast<CURL*>(handle.curl);
    if (curl == nullptr) {
        result.transport_error = "curl_easy_init failed";
        return result;
    }

    HeaderParseState hdr_state;
    const uint32_t eff_timeout_ms =
            timeout_override_ms > 0 ? timeout_override_ms : config_.request_timeout_ms;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, static_cast<long>(eff_timeout_ms));
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result.body);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, curl_header_callback);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &hdr_state);

    if (is_post) {
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_body->data());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(post_body->size()));
    }

    const CURLcode rc = curl_easy_perform(curl);
    bool keep_alive = false;
    if (rc != CURLE_OK) {
        result.transport_error = curl_easy_strerror(rc);
    } else {
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &result.http_status);
        curl_easy_getinfo(curl, CURLINFO_HTTP_VERSION, &result.http_version);
        result.retry_after_ms = hdr_state.retry_after_ms;
        keep_alive = true;
    }

    release_handle(base_endpoint, std::move(handle), keep_alive);
    return result;
}

HttpCommitRouter::HttpResult HttpCommitRouter::perform_request(
        const std::string& base_endpoint, const std::string& path, const std::string* post_body,
        uint32_t timeout_override_ms
) {
    const std::string norm_ep = normalize_endpoint(base_endpoint);
    const std::string auth_header = resolve_auth_header_value();
    const uint64_t cert_gen = check_client_cert_generation();
    const std::string url = norm_ep + path;
    if (multi_running_.load(std::memory_order_acquire) && is_https_url(norm_ep)) {
        return perform_request_multi(url, post_body, auth_header, cert_gen, timeout_override_ms);
    }
    return perform_request_easy(norm_ep, url, post_body, auth_header, cert_gen, timeout_override_ms);
}

bool HttpCommitRouter::refresh_topology(
        bool force, const std::string& deprioritize_endpoint, uint32_t max_timeout_ms
) {
    if (!topology_supported_.load(std::memory_order_acquire) && !force) {
        return false;
    }

    // Singleflight guard + 250ms rate limit so 64 concurrent threads never stampede /v1/topology.
    {
        std::unique_lock<std::mutex> lock(topology_mutex_);
        const auto now = std::chrono::steady_clock::now();
        if (!force && has_topology_ &&
            std::chrono::duration_cast<std::chrono::milliseconds>(now - last_topology_refresh_).count() < 250) {
            return false;
        }
        if (topology_refresh_in_flight_) {
            topology_cv_.wait(lock, [this]() { return !topology_refresh_in_flight_; });
            return has_topology_;
        }
        topology_refresh_in_flight_ = true;
    }

    bool updated = false;
    bool saw_404_only = true;
    auto candidates = candidate_seed_endpoints();
    if (!deprioritize_endpoint.empty() && candidates.size() > 1) {
        const std::string norm_dep = normalize_endpoint(deprioritize_endpoint);
        auto it = std::find(candidates.begin(), candidates.end(), norm_dep);
        if (it != candidates.end()) {
            std::rotate(it, it + 1, candidates.end());
        }
    }
    const uint32_t topo_timeout_ms =
            std::max<uint32_t>(250u, std::min<uint32_t>(config_.request_timeout_ms, max_timeout_ms));
    const auto topo_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(topo_timeout_ms);

    for (const auto& ep : candidates) {
        const auto rem_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    topo_deadline - std::chrono::steady_clock::now()
                            )
                                    .count();
        if (rem_ms <= 0) {
            break;
        }
        const uint32_t attempt_timeout_ms =
                static_cast<uint32_t>(std::max<int64_t>(100, std::min<int64_t>(rem_ms, topo_timeout_ms)));
        HttpResult res = perform_request(ep, "/v1/topology", nullptr, attempt_timeout_ms);
        if (!res.transport_error.empty()) {
            saw_404_only = false;
            continue;
        }
        if (res.http_status == 404) {
            continue;
        }
        saw_404_only = false;
        if (res.http_status == 200) {
            proto::sequencer::TopologyResponse topo_resp;
            if (topo_resp.ParseFromString(res.body) && topo_resp.has_topology()) {
                const auto& topo = topo_resp.topology();
                // P0-18: Build a sanitized ClusterTopology keeping ONLY statically allowed member endpoints
                // so an untrusted/hijacked peer can never smuggle an SSRF host into topology_.members()!
                proto::sequencer::ClusterTopology sanitized_topo;
                sanitized_topo.set_topology_version(topo.topology_version());
                sanitized_topo.set_num_virtual_shards(topo.num_virtual_shards());
                sanitized_topo.set_ha_mode(topo.ha_mode());
                sanitized_topo.set_active_leader_node_id(topo.active_leader_node_id());
                sanitized_topo.set_cluster_epoch(topo.cluster_epoch());
                std::unordered_map<std::string, std::string> node_to_ep;
                std::unordered_set<std::string> hosts;
                for (const auto& m : topo.members()) {
                    const std::string norm = normalize_endpoint(m.endpoint());
                    if (!norm.empty() && is_statically_allowed_endpoint(norm)) {
                        std::string scheme, host;
                        uint16_t port = 0;
                        bool has_pq = false;
                        if (parse_url_parts(norm, scheme, host, port, has_pq)) {
                            node_to_ep[m.node_id()] = norm;
                            hosts.insert(host);
                            auto* dst_m = sanitized_topo.add_members();
                            dst_m->CopyFrom(m);
                            dst_m->set_endpoint(norm);
                        }
                    }
                }
                for (const auto& a : topo.assignments()) {
                    if (node_to_ep.count(a.owner_node_id()) > 0) {
                        sanitized_topo.add_assignments()->CopyFrom(a);
                    }
                }

                const uint32_t v =
                        topo.num_virtual_shards() > 0 ? topo.num_virtual_shards() : kDefaultVirtualShards;
                std::vector<std::string> owners(v);
                if (topo.ha_mode() == "active_standby" && !topo.active_leader_node_id().empty()) {
                    auto it = node_to_ep.find(topo.active_leader_node_id());
                    if (it != node_to_ep.end()) {
                        std::fill(owners.begin(), owners.end(), it->second);
                    }
                } else {
                    for (const auto& a : sanitized_topo.assignments()) {
                        if (a.shard_id() < v) {
                            auto it = node_to_ep.find(a.owner_node_id());
                            if (it != node_to_ep.end()) {
                                owners[a.shard_id()] = it->second;
                            }
                        }
                    }
                }

                std::lock_guard<std::mutex> lock(topology_mutex_);
                if (!has_topology_ || sanitized_topo.topology_version() > topology_.topology_version()) {
                    topology_.CopyFrom(sanitized_topo);
                    has_topology_ = true;
                    num_virtual_shards_ = v;
                    shard_owner_endpoints_ = std::move(owners);
                    topology_allowed_hosts_ = std::move(hosts);
                    last_topology_refresh_ = std::chrono::steady_clock::now();
                    updated = true;
                }
                break;
            }
        }
    }

    if (!updated && saw_404_only && !candidates.empty()) {
        topology_supported_.store(false, std::memory_order_release);
    }

    {
        std::lock_guard<std::mutex> lock(topology_mutex_);
        if (!updated) {
            last_topology_refresh_ = std::chrono::steady_clock::now();
        }
        topology_refresh_in_flight_ = false;
    }
    topology_cv_.notify_all();
    return updated;
}

proto::sequencer::CommitResponse HttpCommitRouter::submit(const proto::sequencer::CommitRequest& request) {
    // Opportunistically fetch initial HA topology on first call when multiple seeds exist.
    if (seed_endpoints_.size() > 1 && topology_supported_.load(std::memory_order_acquire) &&
        cached_topology_version() == 0) {
        (void)refresh_topology(false);
    }

    proto::sequencer::CommitRequest wire_req(request);
    if (wire_req.client_protocol_version() == 0) {
        wire_req.set_client_protocol_version(kSequencerProtocolVersion);
    }

    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(std::max<uint32_t>(1000u, config_.total_retry_budget_ms));

    std::string target_ep = route_endpoint_for(wire_req.library_name(), wire_req.symbol());
    std::string last_error;
    proto::sequencer::CommitResponse last_retryable_resp;
    bool has_last_retryable_resp = false;
    uint32_t seed_cursor = 0;
    uint32_t consecutive_redirects = 0;
    std::unordered_set<std::string> visited_redirect_eps;

    for (uint32_t attempt = 0;; ++attempt) {
        wire_req.set_client_topology_version(cached_topology_version());
        std::string serialized;
        if (!wire_req.SerializeToString(&serialized)) {
            proto::sequencer::CommitResponse err;
            err.set_status(proto::sequencer::INTERNAL_ERROR);
            err.set_error_message("Failed to serialize CommitRequest protobuf");
            return err;
        }

        if (target_ep.empty()) {
            target_ep = endpoint();
        }

        const auto pre_now = std::chrono::steady_clock::now();
        const int64_t pre_rem_ms = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - pre_now).count();
        const bool ha_failover_possible = (seed_endpoints_.size() > 1) || (cached_topology_version() > 0);
        // Always clamp attempt timeout to remaining retry budget (and <= 4s in HA clusters so a SIGSTOPed pod
        // cannot consume the entire retry budget on Attempt #1).
        const uint32_t base_cap_ms =
                ha_failover_possible ? std::min<uint32_t>(config_.request_timeout_ms, 4000u) : config_.request_timeout_ms;
        const uint32_t attempt_timeout_ms = static_cast<uint32_t>(
                std::max<int64_t>(500, std::min<int64_t>(static_cast<int64_t>(base_cap_ms), std::max<int64_t>(500, pre_rem_ms)))
        );
        HttpResult res = perform_request(target_ep, "/v1/commit", &serialized, attempt_timeout_ms);

        const auto now = std::chrono::steady_clock::now();
        const bool within_retry_count = (config_.max_retries == 0) || (attempt < config_.max_retries);
        const bool can_retry = (now < deadline) && within_retry_count;
        const auto remaining_topo_budget_ms = [&]() -> uint32_t {
            const int64_t rem = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        deadline - std::chrono::steady_clock::now()
                                )
                                        .count();
            return static_cast<uint32_t>(std::max<int64_t>(250, std::min<int64_t>(2000, rem)));
        };

        if (res.transport_error.empty()) {
            proto::sequencer::CommitResponse resp;
            if (!res.body.empty() && resp.ParseFromString(res.body)) {
                const auto st = resp.status();

                if (st == proto::sequencer::OK) {
                    if (resp.topology_stale()) {
                        (void)refresh_topology(false);
                    }
                    return resp;
                }

                // Non-retryable client/auth/semantic errors return immediately.
                if (st == proto::sequencer::SCHEMA_MISMATCH || st == proto::sequencer::UNSORTED_INDEX ||
                    st == proto::sequencer::NO_SUCH_VERSION || st == proto::sequencer::INVALID_ARGUMENT ||
                    st == proto::sequencer::DATA_CONFLICT || st == proto::sequencer::SLICE_CONFLICT ||
                    st == proto::sequencer::UNAUTHENTICATED || st == proto::sequencer::PERMISSION_DENIED ||
                    st == proto::sequencer::PAYLOAD_TOO_LARGE) {
                    return resp;
                }

                // Immediate 0ms redirect on first unvisited hop for NOT_LEADER / FENCED_REDIRECT;
                // if peers temporarily disagree (ping-pong during rolling upgrade), back off + refresh topology.
                if (st == proto::sequencer::NOT_LEADER || st == proto::sequencer::FENCED_REDIRECT) {
                    const std::string hint =
                            !resp.leader_hint().empty() ? resp.leader_hint() : resp.redirect_endpoint();
                    last_retryable_resp = resp;
                    has_last_retryable_resp = true;
                    last_error = resp.error_message().empty() ? "Sequencer redirected (NOT_LEADER)" : resp.error_message();

                    if (can_retry) {
                        visited_redirect_eps.insert(target_ep);
                        bool fast_hop = false;
                        if (!hint.empty() && is_valid_redirect_endpoint(hint)) {
                            const std::string norm_hint = normalize_endpoint(hint);
                            if (seed_endpoints_.size() <= 1 && cached_topology_version() == 0) {
                                update_endpoint(norm_hint);
                            } else {
                                update_shard_hint(resp.shard_id(), norm_hint);
                            }
                            if (norm_hint != target_ep) {
                                const bool already_visited = visited_redirect_eps.contains(norm_hint);
                                target_ep = norm_hint;
                                if (consecutive_redirects == 0 && !already_visited) {
                                    ++consecutive_redirects;
                                    fast_hop = true;
                                }
                            }
                        }
                        if (fast_hop) {
                            continue; // 0ms fast redirect on first unvisited hop (no blocking topology RTT)!
                        }
                        ++consecutive_redirects;
                        (void)refresh_topology(true, target_ep, remaining_topo_budget_ms());
                        const std::string topo_ep = route_endpoint_for(wire_req.library_name(), wire_req.symbol());
                        if (!topo_ep.empty()) {
                            target_ep = topo_ep;
                        }
                        const uint32_t base_ms = resp.retry_after_ms() > 0
                                                         ? resp.retry_after_ms()
                                                         : std::min<uint32_t>(500u, 50u << std::min<uint32_t>(consecutive_redirects, 4u));
                        const uint32_t wait_ms = jittered_backoff_ms(base_ms);
                        const auto rem = std::chrono::duration_cast<std::chrono::milliseconds>(
                                                 deadline - std::chrono::steady_clock::now()
                                         )
                                                 .count();
                        if (rem > 0) {
                            std::this_thread::sleep_for(
                                    std::chrono::milliseconds(std::min<int64_t>(rem, static_cast<int64_t>(wait_ms)))
                            );
                        }
                        continue;
                    }
                    return resp;
                }

                // Retryable overload / draining / rate-limit responses (HTTP 429 / 503).
                if (st == proto::sequencer::SHARD_DRAINING || st == proto::sequencer::OVERLOADED ||
                    st == proto::sequencer::QUOTA_EXCEEDED || res.http_status == 503 || res.http_status == 429) {
                    consecutive_redirects = 0;
                    visited_redirect_eps.clear();
                    last_retryable_resp = resp;
                    has_last_retryable_resp = true;
                    last_error = resp.error_message().empty()
                                         ? fmt::format("Sequencer transient status {} (HTTP {})", static_cast<int>(st), res.http_status)
                                         : resp.error_message();
                    if (can_retry) {
                        if (st == proto::sequencer::SHARD_DRAINING || resp.topology_stale()) {
                            const std::string hint =
                                    !resp.leader_hint().empty() ? resp.leader_hint() : resp.redirect_endpoint();
                            if (!hint.empty() && is_valid_redirect_endpoint(hint)) {
                                const std::string norm_hint = normalize_endpoint(hint);
                                update_shard_hint(resp.shard_id(), norm_hint);
                                if (norm_hint != target_ep) {
                                    target_ep = norm_hint;
                                }
                            }
                            (void)refresh_topology(true, target_ep, remaining_topo_budget_ms());
                            std::string new_ep = route_endpoint_for(wire_req.library_name(), wire_req.symbol());
                            if (!new_ep.empty()) {
                                target_ep = std::move(new_ep);
                            }
                        }
                        const uint32_t server_hint_ms = std::max(resp.retry_after_ms(), res.retry_after_ms);
                        const uint32_t exp_ms =
                                std::min<uint32_t>(1000u, 50u << std::min<uint32_t>(attempt, 5u));
                        const uint32_t wait_ms = jittered_backoff_ms(std::max(server_hint_ms, exp_ms));
                        const auto rem = std::chrono::duration_cast<std::chrono::milliseconds>(
                                                 deadline - std::chrono::steady_clock::now()
                                         )
                                                 .count();
                        if (rem > 0) {
                            std::this_thread::sleep_for(
                                    std::chrono::milliseconds(std::min<int64_t>(rem, static_cast<int64_t>(wait_ms)))
                            );
                        }
                        continue;
                    }
                    return resp;
                }

                return resp;
            }

            // Unparseable HTTP body: map well-known HTTP status codes cleanly.
            if (res.http_status == 401) {
                proto::sequencer::CommitResponse err;
                err.set_status(proto::sequencer::UNAUTHENTICATED);
                err.set_error_message("Sequencer authentication failed (HTTP 401)");
                return err;
            }
            if (res.http_status == 403) {
                proto::sequencer::CommitResponse err;
                err.set_status(proto::sequencer::PERMISSION_DENIED);
                err.set_error_message("Sequencer permission denied (HTTP 403)");
                return err;
            }
            if (res.http_status == 413) {
                proto::sequencer::CommitResponse err;
                err.set_status(proto::sequencer::PAYLOAD_TOO_LARGE);
                err.set_error_message("CommitRequest payload too large (HTTP 413)");
                return err;
            }
            if ((res.http_status == 503 || res.http_status == 429) && can_retry) {
                last_error = fmt::format("Sequencer returned HTTP {} (body={})", res.http_status, res.body);
                const uint32_t exp_ms = std::min<uint32_t>(2000u, 50u << std::min<uint32_t>(attempt, 6u));
                const uint32_t wait_ms = jittered_backoff_ms(std::max(res.retry_after_ms, exp_ms));
                const auto rem = std::chrono::duration_cast<std::chrono::milliseconds>(
                                         deadline - std::chrono::steady_clock::now()
                                 )
                                         .count();
                if (rem > 0) {
                    std::this_thread::sleep_for(
                            std::chrono::milliseconds(std::min<int64_t>(rem, static_cast<int64_t>(wait_ms)))
                    );
                }
                continue;
            }
            proto::sequencer::CommitResponse err;
            err.set_status(
                    res.http_status == 429   ? proto::sequencer::QUOTA_EXCEEDED
                    : res.http_status == 503 ? proto::sequencer::OVERLOADED
                                             : proto::sequencer::INTERNAL_ERROR
            );
            err.set_error_message(
                    fmt::format("Sequencer HTTP {} with unparseable response ({} bytes)", res.http_status, res.body.size())
            );
            return err;
        }

        // Transport error (pod crashed/SIGSTOPed, TCP reset, DNS timeout): failover to another seed/topology member.
        consecutive_redirects = 0;
        visited_redirect_eps.clear();
        const std::string failed_ep = target_ep;
        last_error = fmt::format("Sequencer transport error ({}): {}", failed_ep, res.transport_error);
        if (!can_retry) {
            break;
        }

        const auto candidates = candidate_seed_endpoints();
        if (candidates.size() > 1) {
            for (size_t k = 0; k < candidates.size(); ++k) {
                const auto& cand = candidates[(seed_cursor++) % candidates.size()];
                if (cand != failed_ep) {
                    target_ep = cand;
                    break;
                }
            }
            if (refresh_topology(true, failed_ep, remaining_topo_budget_ms())) {
                const std::string refreshed = route_endpoint_for(wire_req.library_name(), wire_req.symbol());
                if (!refreshed.empty() && refreshed != failed_ep) {
                    target_ep = refreshed;
                }
            }
        }
        const uint32_t exp_ms = std::min<uint32_t>(1000u, 100u << std::min<uint32_t>(attempt, 4u));
        const uint32_t wait_ms = jittered_backoff_ms(exp_ms);
        const auto rem = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 deadline - std::chrono::steady_clock::now()
                         )
                                 .count();
        if (rem > 0) {
            std::this_thread::sleep_for(
                    std::chrono::milliseconds(std::min<int64_t>(rem, static_cast<int64_t>(wait_ms)))
            );
        }
    }

    if (has_last_retryable_resp) {
        return last_retryable_resp;
    }
    proto::sequencer::CommitResponse err;
    err.set_status(proto::sequencer::OVERLOADED);
    err.set_error_message(last_error.empty() ? "Sequencer unavailable after retry budget" : last_error);
    return err;
}

proto::sequencer::HealthResponse HttpCommitRouter::health() {
    HttpResult res = perform_request(endpoint(), "/v1/health", nullptr);
    proto::sequencer::HealthResponse resp;
    if (res.transport_error.empty() && res.http_status == 200 && resp.ParseFromString(res.body)) {
        return resp;
    }
    resp.set_healthy(false);
    return resp;
}

} // namespace arcticdb::version
