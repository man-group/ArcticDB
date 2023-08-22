/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_factory.hpp>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <arcticdb/log/log.hpp>
#include <arcticdb/storage/s3/s3_api.hpp>
#include <arcticdb/storage/object_store_utils.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/storage/s3/s3_client_accessor.hpp>
#include <arcticdb/util/composite.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>

namespace arcticdb::storage::s3 {

const std::string USE_AWS_CRED_PROVIDERS_TOKEN = "_RBAC_";

class S3Storage final : public Storage {
  public:
    friend class S3TestClientAccessor<S3Storage>;
    using Config = arcticdb::proto::s3_storage::Config;

    S3Storage(const LibraryPath &lib, OpenMode mode, const Config &conf);

    /**
     * Full object path in S3 bucket.
     */
    std::string get_key_path(const VariantKey& key) const;

  private:
    void do_write(Composite<KeySegmentPair>&& kvs) final;

    void do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) final;

    void do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, ReadKeyOpts opts) final;

    void do_remove(Composite<VariantKey>&& ks, RemoveOpts opts) final;

    void do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string &prefix) final;

    bool do_key_exists(const VariantKey& key) final;

    bool do_supports_prefix_matching() const final {
        return true;
    }

    bool do_fast_delete() final {
        return false;
    }

    std::string do_key_path(const VariantKey& key) const final { return get_key_path(key); };

    auto& client() { return s3_client_; }
    const std::string& bucket_name() const { return bucket_name_; }
    const std::string& root_folder() const { return root_folder_; }

    std::shared_ptr<S3ApiInstance> s3_api_;
    Aws::S3::S3Client s3_client_;
    std::string root_folder_;
    std::string bucket_name_;
};

inline arcticdb::proto::storage::VariantStorage pack_config(const std::string &bucket_name) {
    arcticdb::proto::storage::VariantStorage output;
    arcticdb::proto::s3_storage::Config cfg;
    cfg.set_bucket_name(bucket_name);
    util::pack_to_any(cfg, *output.mutable_config());
    return output;
}

inline arcticdb::proto::storage::VariantStorage pack_config(
        const std::string &bucket_name,
        const std::string &credential_name,
        const std::string &credential_key,
        const std::string &endpoint
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

inline std::optional<Aws::Client::ClientConfiguration> parse_proxy_env_var(Aws::Http::Scheme endpoint_scheme,
                                                                           const char* opt_env_var) {
    if(opt_env_var == nullptr) {
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
    if (creds_end_index != std::string::npos){
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
    if (port_start_idx == std::string::npos){
        port = endpoint_scheme == Aws::Http::Scheme::HTTPS ? 443 : 80;
    } else {
        try {
            port = std::stoul(std::string(env_var.substr(port_start_idx + 1, env_var.length())));
        } catch (const std::logic_error& e) {
            log::storage().warn("Failed to parse port in '{}': {}", env_var, e.what());
            return std::nullopt;
        }
        if (port > std::numeric_limits<uint16_t>::max()) {
            log::storage().warn("Failed to parse '{}': port {} > {}", env_var, port, std::numeric_limits<uint16_t>::max());
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
    while(std::getline(env_var, host, ','))
    {
        hosts.push_back(host);
    }
    Aws::Utils::Array<Aws::String> non_proxy_hosts{hosts.size()};
    for (const auto& tmp: folly::enumerate(hosts)) {
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
    const std::unordered_map<Aws::Http::Scheme, std::vector<std::string>> scheme_env_var_names {
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
        for (const auto& env_var_name: {"no_proxy", "NO_PROXY"}) {
            char* opt_env_var = std::getenv(env_var_name);
            auto non_proxy_hosts = parse_no_proxy_env_var(opt_env_var);
            if (non_proxy_hosts.has_value()) {
                client_configuration->nonProxyHosts = non_proxy_hosts.value();
                log::storage().info("S3 proxy exclusion list set from env var '{}'", opt_env_var);
                break;
            }
        }
        return client_configuration.value();
    } else {
        return Aws::Client::ClientConfiguration();
    }
}

template<typename ConfigType>
auto get_s3_config(const ConfigType& conf) {
    auto endpoint_scheme = conf.https() ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;
    Aws::Client::ClientConfiguration client_configuration = get_proxy_config(endpoint_scheme);
    client_configuration.scheme = endpoint_scheme;

    if (!conf.region().empty()) {
        client_configuration.region = conf.region();
    }

    if (!conf.endpoint().empty()) {
        client_configuration.endpointOverride = conf.endpoint();
    }

    const bool verify_ssl = ConfigsMap::instance()->get_int("S3Storage.VerifySSL", conf.ssl());
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Verify ssl: {}", verify_ssl);
    client_configuration.verifySSL = verify_ssl;
    client_configuration.maxConnections = conf.max_connections() == 0 ?
            ConfigsMap::instance()->get_int("VersionStore.NumIOThreads", 16) :
            conf.max_connections();
    client_configuration.connectTimeoutMs = conf.connect_timeout() == 0 ? 30000 : conf.connect_timeout();
    client_configuration.requestTimeoutMs = conf.request_timeout() == 0 ? 200000 : conf.request_timeout();
    return client_configuration;
}

template<typename ConfigType>
Aws::Auth::AWSCredentials get_aws_credentials(const ConfigType& conf) {
    return Aws::Auth::AWSCredentials(conf.credential_name().c_str(), conf.credential_key().c_str());
}

} //namespace arcticdb::storage::s3
