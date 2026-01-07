/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <aws/sts/STSClient.h>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/storage/s3/s3_api.hpp>
#include <arcticdb/storage/s3/s3_settings.hpp>
#include <arcticdb/storage/s3/s3_client_interface.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>
#include <aws/core/auth/AWSCredentials.h>

namespace arcticdb::storage::s3 {

const std::string USE_AWS_CRED_PROVIDERS_TOKEN = "_RBAC_";

class S3Storage : public Storage, AsyncStorage {
  public:
    S3Storage(const LibraryPath& lib, OpenMode mode, const S3Settings& conf);

    std::string get_key_path(const VariantKey& key) const;

    std::string name() const final;

    bool has_async_api() const final { return ConfigsMap::instance()->get_int("S3.Async", 0) == 1; }

    AsyncStorage* async_api() override { return this; }

    bool supports_object_size_calculation() const final;

  protected:
    void do_write(KeySegmentPair& key_seg) final;

    void do_write_if_none(KeySegmentPair& kv) final;

    void do_update(KeySegmentPair& key_seg, UpdateOpts opts) final;

    void do_read(VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts) final;

    KeySegmentPair do_read(VariantKey&& variant_key, ReadKeyOpts opts) final;

    folly::Future<folly::Unit> do_async_read(
            entity::VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts
    ) final;

    folly::Future<KeySegmentPair> do_async_read(entity::VariantKey&& variant_key, ReadKeyOpts opts) final;

    void do_remove(VariantKey&& variant_key, RemoveOpts opts) override;

    void do_remove(std::span<VariantKey> variant_keys, RemoveOpts opts) override;

    void do_visit_object_sizes(KeyType key_type, const std::string& prefix, const ObjectSizesVisitor& visitor) final;

    bool do_iterate_type_until_match(KeyType key_type, const IterateTypePredicate& visitor, const std::string& prefix)
            final;

    bool do_key_exists(const VariantKey& key) final;

    bool do_supports_prefix_matching() const final { return true; }

    SupportsAtomicWrites do_supports_atomic_writes() const final { return SupportsAtomicWrites::NEEDS_TEST; };

    bool do_fast_delete() final { return false; }

    void create_s3_client(const S3Settings& conf, const Aws::Auth::AWSCredentials& creds);

    std::string do_key_path(const VariantKey& key) const final { return get_key_path(key); };

    S3ClientInterface& client() { return *s3_client_; }
    const std::string& bucket_name() const { return bucket_name_; }
    const std::string& root_folder() const { return root_folder_; }

    std::shared_ptr<S3ApiInstance> s3_api_;
    std::unique_ptr<S3ClientInterface> s3_client_;
    // aws sdk annoyingly requires raw pointer being passed in the sts client factory to the s3 client
    // thus sts_client_ should have same life span as s3_client_
    std::unique_ptr<Aws::STS::STSClient> sts_client_;
    std::string root_folder_;
    std::string bucket_name_;
    std::string region_;
    // Directory (aka express) buckets in AWS only support ListObjectsV2 requests with prefixes ending in a '/'
    // delimiter. Until we have proper feature detection, just cache if this is the case after the first failed listing
    // operation with a prefix that does not end in a '/'
    bool directory_bucket_;
};

class GCPXMLStorage : public S3Storage {
  public:
    GCPXMLStorage(const LibraryPath& lib, OpenMode mode, const GCPXMLSettings& conf);

  protected:
    void do_remove(std::span<VariantKey> variant_keys, RemoveOpts opts) override;
    void do_remove(VariantKey&& variant_key, RemoveOpts opts) override;
};

inline arcticdb::proto::storage::VariantStorage pack_config(const std::string& bucket_name) {
    arcticdb::proto::storage::VariantStorage output;
    arcticdb::proto::s3_storage::Config cfg;
    cfg.set_bucket_name(bucket_name);
    util::pack_to_any(cfg, *output.mutable_config());
    return output;
}

inline arcticdb::proto::storage::VariantStorage pack_config(
        const std::string& bucket_name, const std::string& credential_name, const std::string& credential_key,
        const std::string& endpoint
) {
    arcticdb::proto::storage::VariantStorage output;
    arcticdb::proto::s3_storage::Config cfg;
    cfg.set_bucket_name(bucket_name);
    cfg.set_credential_name(credential_name);
    cfg.set_credential_key(credential_key);
    cfg.set_endpoint(endpoint);
    util::pack_to_any(cfg, *output.mutable_config());
    return output;
}

inline std::optional<Aws::Client::ClientConfiguration> parse_proxy_env_var(
        Aws::Http::Scheme endpoint_scheme, const char* opt_env_var
) {
    if (opt_env_var == nullptr) {
        return std::nullopt;
    }
    auto env_var = std::string_view(opt_env_var);
    // env_var format: [http[s]://][username[:password]@]hostname[:port]
    Aws::Client::ClientConfiguration client_configuration;
    if (env_var.find("https://") == 0) {
        client_configuration.proxyScheme = Aws::Http::Scheme::HTTPS;
        env_var = env_var.substr(8);
    } else if (env_var.find("http://") == 0) {
        env_var = env_var.substr(7);
    }
    // env_var format: [username[:password]@]hostname[:port]
    auto creds_end_index = env_var.rfind('@');
    if (creds_end_index != std::string::npos) {
        auto auth = env_var.substr(0, creds_end_index);

        auto user_pass_divider_idx = auth.find(':');
        if (user_pass_divider_idx != std::string::npos) {
            client_configuration.proxyUserName = auth.substr(0, user_pass_divider_idx);
            client_configuration.proxyPassword = auth.substr(user_pass_divider_idx + 1);
        } else {
            client_configuration.proxyUserName = auth;
        }
        env_var = env_var.substr(creds_end_index + 1);
    }
    // env_var format: hostname[:port]
    auto port_start_idx = env_var.rfind(':');
    uint64_t port;
    if (port_start_idx == std::string::npos) {
        port = endpoint_scheme == Aws::Http::Scheme::HTTPS ? 443 : 80;
    } else {
        try {
            port = std::stoul(std::string(env_var.substr(port_start_idx + 1, env_var.length())));
        } catch (const std::logic_error& e) {
            log::storage().warn("Failed to parse port in '{}': {}", env_var, e.what());
            return std::nullopt;
        }
        if (port > std::numeric_limits<uint16_t>::max()) {
            log::storage().warn(
                    "Failed to parse '{}': port {} > {}", env_var, port, std::numeric_limits<uint16_t>::max()
            );
            return std::nullopt;
        }
        env_var = env_var.substr(0, port_start_idx);
    }
    client_configuration.proxyPort = port;
    // env_var format: hostname
    client_configuration.proxyHost = env_var;
    log::storage().info("S3 proxy set from env var '{}'", env_var);
    return client_configuration;
}

inline std::optional<Aws::Utils::Array<Aws::String>> parse_no_proxy_env_var(const char* opt_env_var) {
    if (opt_env_var == nullptr) {
        return std::nullopt;
    }
    auto env_var = std::stringstream(opt_env_var);
    std::string host;
    std::vector<std::string> hosts;
    while (std::getline(env_var, host, ',')) {
        hosts.push_back(host);
    }
    Aws::Utils::Array<Aws::String> non_proxy_hosts{hosts.size()};
    for (const auto& tmp : folly::enumerate(hosts)) {
        non_proxy_hosts[tmp.index] = *tmp;
    }
    return non_proxy_hosts;
}

/* Use the environment variables http_proxy, https_proxy, no_proxy, and upper-case versions thereof, to configure the
 * proxy. Lower-case environment variables take precedence over upper-case in keeping with curl etc:
 * https://about.gitlab.com/blog/2021/01/27/we-need-to-talk-no-proxy/
 * Protocol, username, password, and port are all optional, and will default to http, empty, empty and 80/443
 * (for http and https) respectively. Maximal and minimal example valid environment variables:
 * http://username:password@my-proxy.com:8080
 * my-proxy.com => http://my-proxy.com:80 (no authentication)
 * no_proxy and it's uppercase equivalent should be a comma-separated list of hosts not to apply other proxy settings to
 * e.g. no_proxy="host-1.com,host-2.com"
 * */
inline Aws::Client::ClientConfiguration get_proxy_config(Aws::Http::Scheme endpoint_scheme) {
    // The ordering in the vectors matter, lowercase should be checked in preference of upper case.
    const std::unordered_map<Aws::Http::Scheme, std::vector<std::string>> scheme_env_var_names{
            {Aws::Http::Scheme::HTTP, {"http_proxy", "HTTP_PROXY"}},
            {Aws::Http::Scheme::HTTPS, {"https_proxy", "HTTPS_PROXY"}}
    };
    std::optional<Aws::Client::ClientConfiguration> client_configuration;
    for (const auto& env_var_name : scheme_env_var_names.at(endpoint_scheme)) {
        char* opt_env_var = std::getenv(env_var_name.c_str());
        client_configuration = parse_proxy_env_var(endpoint_scheme, opt_env_var);
        if (client_configuration.has_value()) {
            break;
        }
    }
    if (client_configuration.has_value()) {
        for (const auto& env_var_name : {"no_proxy", "NO_PROXY"}) {
            char* opt_env_var = std::getenv(env_var_name);
            auto non_proxy_hosts = parse_no_proxy_env_var(opt_env_var);
            if (non_proxy_hosts) {
                client_configuration->nonProxyHosts = *non_proxy_hosts;
                log::storage().info("S3 proxy exclusion list set from env var '{}'", opt_env_var);
                break;
            }
        }
        return *client_configuration;
    } else {
        return Aws::Client::ClientConfiguration();
    }
}
// Since aws-sdk-cpp >= 1.11.486, it has turned on checksum integrity check `x-amz-checksum-mode: enabled`
// This feature is not supported in many s3 implementations. SDK with this feature ON will reject responses from
// s3 implementations that do not provide checksum, which leads to storage exception in arcticdb
// Update environment variable before import arcticdb_ext to disable checksum validation
inline void configure_s3_checksum_validation() {
    const char* response_checksum = std::getenv("AWS_RESPONSE_CHECKSUM_VALIDATION");
    const char* request_checksum = std::getenv("AWS_REQUEST_CHECKSUM_CALCULATION");

    if ((response_checksum && std::string(response_checksum) == "when_supported") ||
        (request_checksum && std::string(request_checksum) == "when_supported")) {
        log::storage().warn("S3 Checksum validation has been specifically enabled by user. "
                            "If endpoint doesn't support it, 1. incorrect objects could be silently written "
                            "2. Endpoint response will be rejected by SDK and lead to storage exception in arcticdb");
    } else {
#ifdef _WIN32
        _putenv_s("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required");
        _putenv_s("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required");
#else
        setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required", 1);
        setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required", 1);
#endif
    }
}

template<typename ConfigType>
auto get_s3_config_and_set_env_var(const ConfigType& conf) {
    configure_s3_checksum_validation();

    auto endpoint_scheme = conf.https() ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;
    Aws::Client::ClientConfiguration client_configuration = get_proxy_config(endpoint_scheme);
    client_configuration.scheme = endpoint_scheme;
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Endpoint Scheme: {}", conf.https() ? "https" : "http");

    if (!conf.region().empty()) {
        client_configuration.region = conf.region();
    }

    if (!conf.endpoint().empty()) {
        client_configuration.endpointOverride = conf.endpoint();
    }

    const bool verify_ssl = ConfigsMap::instance()->get_int("S3Storage.VerifySSL", conf.ssl());
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Verify ssl: {}", verify_ssl);
    client_configuration.verifySSL = verify_ssl;
    if (client_configuration.verifySSL && (!conf.ca_cert_path().empty() || !conf.ca_cert_dir().empty())) {
        client_configuration.caFile = conf.ca_cert_path();
        client_configuration.caPath = conf.ca_cert_dir();
    }

    client_configuration.maxConnections = ConfigsMap::instance()->get_int(
            "S3Storage.MaxConnections", async::TaskScheduler::instance()->io_thread_count()
    );
    client_configuration.connectTimeoutMs = ConfigsMap::instance()->get_int(
            "S3Storage.ConnectTimeoutMs", conf.connect_timeout() == 0 ? 30000 : conf.connect_timeout()
    );
    client_configuration.httpRequestTimeoutMs = ConfigsMap::instance()->get_int("S3Storage.HttpRequestTimeoutMs", 0);
    client_configuration.requestTimeoutMs = ConfigsMap::instance()->get_int(
            "S3Storage.RequestTimeoutMs", conf.request_timeout() == 0 ? 200000 : conf.request_timeout()
    );
    client_configuration.lowSpeedLimit = ConfigsMap::instance()->get_int("S3Storage.LowSpeedLimit", 1);

    const bool use_win_inet = ConfigsMap::instance()->get_int("S3Storage.UseWinINet", 0);
    if (use_win_inet) {
        client_configuration.httpLibOverride = Aws::Http::TransferLibType::WIN_INET_CLIENT;
    }

    return client_configuration;
}

template<typename ConfigType>
Aws::Auth::AWSCredentials get_aws_credentials(const ConfigType& conf) {
    return Aws::Auth::AWSCredentials(conf.credential_name().c_str(), conf.credential_key().c_str());
}

} // namespace arcticdb::storage::s3
