/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/storage/s3/s3_api.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>

#include <gtest/gtest.h>
#include <arcticdb/util/test/gtest_utils.hpp>

#include <aws/core/Aws.h>

struct EnvFunctionShim : ::testing::Test {
    std::unordered_set<const char*> env_vars_to_unset{};

    void setenv(const char* envname, const char* envval, bool)
    {
        env_vars_to_unset.insert(envname);
#if (WIN32)
        _putenv_s(envname, envval);
#else
        ::setenv(envname, envval, false);
#endif
    }

    virtual ~EnvFunctionShim()
    {
        for (const char* envname : env_vars_to_unset) {
#if (WIN32)
            _putenv_s(envname, "");
#else
            ::unsetenv(envname);
#endif
        }
    }
};

class ProxyEnvVarSetHttpProxyForHttpsEndpointFixture : public EnvFunctionShim {
protected:
    ProxyEnvVarSetHttpProxyForHttpsEndpointFixture()
    {
        arcticdb::storage::s3::S3ApiInstance::instance();
        setenv("HTTPS_PROXY", "http://http-proxy.com", false);
    }
};

class ProxyEnvVarUpperCaseFixture : public EnvFunctionShim {
protected:
    ProxyEnvVarUpperCaseFixture()
    {
        arcticdb::storage::s3::S3ApiInstance::instance();
        setenv("HTTP_PROXY", "http://http-proxy-2.com:2222", false);
        setenv("HTTPS_PROXY", "https://https-proxy-2.com:2222", false);
    }
};

class ProxyEnvVarLowerCasePrecedenceFixture : public EnvFunctionShim {
protected:
    ProxyEnvVarLowerCasePrecedenceFixture()
    {
        arcticdb::storage::s3::S3ApiInstance::instance();
        setenv("http_proxy", "http://http-proxy-1.com:2222", false);
        setenv("HTTP_PROXY", "http://http-proxy-2.com:2222", false);
        setenv("https_proxy", "https://https-proxy-1.com:2222", false);
        setenv("HTTPS_PROXY", "https://https-proxy-2.com:2222", false);
    }
};

class NoProxyEnvVarUpperCaseFixture : public EnvFunctionShim {
protected:
    NoProxyEnvVarUpperCaseFixture()
    {
        arcticdb::storage::s3::S3ApiInstance::instance();
        setenv("HTTP_PROXY", "http://http-proxy-2.com:2222", false);
        setenv("NO_PROXY", "http://test-1.endpoint.com", false);
    }
};

class NoProxyEnvVarLowerCasePrecedenceFixture : public EnvFunctionShim {
protected:
    NoProxyEnvVarLowerCasePrecedenceFixture()
    {
        arcticdb::storage::s3::S3ApiInstance::instance();
        setenv("http_proxy", "http://http-proxy-2.com:2222", false);
        setenv("no_proxy", "http://test-1.endpoint.com,http://test-2.endpoint.com", false);
        setenv("NO_PROXY", "http://test-3.endpoint.com", false);
    }
};

TEST(TestS3Storage, proxy_env_var_parsing)
{
    using namespace arcticdb::storage::s3;
    using namespace Aws::Http;
    auto instance = arcticdb::storage::s3::S3ApiInstance::instance();
    struct ProxyConfig {
        Scheme endpoint_scheme_;
        Scheme proxy_scheme_;
        std::string host_;
        uint16_t port_;
        std::string username_;
        std::string password_;
        bool operator==(const Aws::Client::ClientConfiguration& client_config) const
        {
            return proxy_scheme_ == client_config.proxyScheme && host_ == client_config.proxyHost &&
                   port_ == client_config.proxyPort && username_ == client_config.proxyUserName &&
                   password_ == client_config.proxyPassword;
        };
    };
    std::unordered_map<std::string, ProxyConfig> passing_test_cases{
        {"http-proxy.com:2222", {Scheme::HTTP, Scheme::HTTP, "http-proxy.com", 2222, "", ""}},
        {"https://https-proxy.com", {Scheme::HTTPS, Scheme::HTTPS, "https-proxy.com", 443, "", ""}},
        // Test setting http proxy for https endpoint
        {"http://http-proxy.com", {Scheme::HTTPS, Scheme::HTTP, "http-proxy.com", 443, "", ""}},
        {"http://username@proxy.com", {Scheme::HTTP, Scheme::HTTP, "proxy.com", 80, "username", ""}},
        {"http://username:pass@proxy.com:2222", {Scheme::HTTP, Scheme::HTTP, "proxy.com", 2222, "username", "pass"}},
        {"http://username:p@ss@proxy.com:2222", {Scheme::HTTP, Scheme::HTTP, "proxy.com", 2222, "username", "p@ss"}}};
    for (const auto& [env_var, expected_proxy_config] : passing_test_cases) {
        auto client_config = parse_proxy_env_var(expected_proxy_config.endpoint_scheme_, env_var.c_str());
        ASSERT_TRUE(client_config.has_value());
        ASSERT_TRUE(expected_proxy_config == client_config);
    }

    std::unordered_map<std::string, ProxyConfig> failing_test_cases{
        {"http-proxy.com:not-a-valid-port", {Scheme::HTTP, Scheme::HTTP, "", 0, "", ""}},
        {"https://username:pass@proxy.com:99999", {Scheme::HTTPS, Scheme::HTTP, "", 0, "", ""}}};
    for (const auto& [env_var, expected_proxy_config] : failing_test_cases) {
        auto client_config = parse_proxy_env_var(expected_proxy_config.endpoint_scheme_, env_var.c_str());
        ASSERT_FALSE(client_config.has_value());
    }
}

TEST_F(ProxyEnvVarSetHttpProxyForHttpsEndpointFixture, test_config_resolution_proxy)
{
    arcticdb::storage::s3::S3Storage::Config s3_config;
    s3_config.set_endpoint("https://test.endpoint.com");
    s3_config.set_https(true);
    auto ret_cfg = arcticdb::storage::s3::get_s3_config(s3_config);
    ASSERT_EQ(ret_cfg.proxyHost, "http-proxy.com");
    ASSERT_EQ(ret_cfg.proxyPort, 443);
    ASSERT_EQ(ret_cfg.proxyScheme, Aws::Http::Scheme::HTTP);
}

TEST_F(ProxyEnvVarUpperCaseFixture, test_config_resolution_proxy)
{
    arcticdb::storage::s3::S3Storage::Config s3_config_http;
    s3_config_http.set_endpoint("http://test.endpoint.com");
    auto ret_cfg = arcticdb::storage::s3::get_s3_config(s3_config_http);
    ASSERT_EQ(ret_cfg.proxyHost, "http-proxy-2.com");
    ASSERT_EQ(ret_cfg.proxyPort, 2222);
    ASSERT_EQ(ret_cfg.proxyScheme, Aws::Http::Scheme::HTTP);

    arcticdb::storage::s3::S3Storage::Config s3_config_https;
    s3_config_https.set_endpoint("https://test.endpoint.com");
    s3_config_https.set_https(true);
    ret_cfg = arcticdb::storage::s3::get_s3_config(s3_config_https);
    ASSERT_EQ(ret_cfg.proxyHost, "https-proxy-2.com");
    ASSERT_EQ(ret_cfg.proxyPort, 2222);
    ASSERT_EQ(ret_cfg.proxyScheme, Aws::Http::Scheme::HTTPS);
}

TEST_F(ProxyEnvVarLowerCasePrecedenceFixture, test_config_resolution_proxy)
{
    SKIP_WIN("Env vars are not case-sensitive on Windows");
    arcticdb::storage::s3::S3Storage::Config s3_config_http;
    s3_config_http.set_endpoint("http://test.endpoint.com");
    auto ret_cfg = arcticdb::storage::s3::get_s3_config(s3_config_http);
    ASSERT_EQ(ret_cfg.proxyHost, "http-proxy-1.com");
    ASSERT_EQ(ret_cfg.proxyPort, 2222);
    ASSERT_EQ(ret_cfg.proxyScheme, Aws::Http::Scheme::HTTP);

    arcticdb::storage::s3::S3Storage::Config s3_config_https;
    s3_config_https.set_endpoint("https://test.endpoint.com");
    s3_config_https.set_https(true);
    ret_cfg = arcticdb::storage::s3::get_s3_config(s3_config_https);
    ASSERT_EQ(ret_cfg.proxyHost, "https-proxy-1.com");
    ASSERT_EQ(ret_cfg.proxyPort, 2222);
    ASSERT_EQ(ret_cfg.proxyScheme, Aws::Http::Scheme::HTTPS);
}

TEST_F(NoProxyEnvVarUpperCaseFixture, test_config_resolution_proxy)
{
    arcticdb::storage::s3::S3Storage::Config s3_config;
    s3_config.set_endpoint("http://test.endpoint.com");
    auto ret_cfg = arcticdb::storage::s3::get_s3_config(s3_config);

    Aws::Utils::Array<Aws::String> expected_non_proxy_hosts{1};
    expected_non_proxy_hosts[0] = "http://test-1.endpoint.com";
    ASSERT_EQ(ret_cfg.nonProxyHosts, expected_non_proxy_hosts);
}

TEST_F(NoProxyEnvVarLowerCasePrecedenceFixture, test_config_resolution_proxy)
{
    SKIP_WIN("Env vars are not case-sensitive on Windows");
    arcticdb::storage::s3::S3Storage::Config s3_config;
    s3_config.set_endpoint("http://test.endpoint.com");
    auto ret_cfg = arcticdb::storage::s3::get_s3_config(s3_config);

    Aws::Utils::Array<Aws::String> expected_non_proxy_hosts{2};
    expected_non_proxy_hosts[0] = "http://test-1.endpoint.com";
    expected_non_proxy_hosts[1] = "http://test-2.endpoint.com";
    ASSERT_EQ(ret_cfg.nonProxyHosts, expected_non_proxy_hosts);
}