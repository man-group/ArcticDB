/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/s3/s3_api.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/s3/s3_client_wrapper.hpp>
#include <arcticdb/storage/s3/nfs_backed_storage.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/test/common.hpp>
#include <arcticdb/util/test/gtest_utils.hpp>

#include <aws/core/Aws.h>

struct EnvFunctionShim : ::testing::Test {
    std::unordered_set<const char*> env_vars_to_unset{};

    void setenv(const char* envname, const char* envval, bool) {
        env_vars_to_unset.insert(envname);
#if (WIN32)
        _putenv_s(envname, envval);
#else
        ::setenv(envname, envval, false);
#endif
    }

    ~EnvFunctionShim() override {
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
    ProxyEnvVarSetHttpProxyForHttpsEndpointFixture() {
        arcticdb::storage::s3::S3ApiInstance::instance();
        setenv("HTTPS_PROXY", "http://http-proxy.com", false);
    }
};

class ProxyEnvVarUpperCaseFixture : public EnvFunctionShim {
  protected:
    ProxyEnvVarUpperCaseFixture() {
        arcticdb::storage::s3::S3ApiInstance::instance();
        setenv("HTTP_PROXY", "http://http-proxy-2.com:2222", false);
        setenv("HTTPS_PROXY", "https://https-proxy-2.com:2222", false);
    }
};

class ProxyEnvVarLowerCasePrecedenceFixture : public EnvFunctionShim {
  protected:
    ProxyEnvVarLowerCasePrecedenceFixture() {
        arcticdb::storage::s3::S3ApiInstance::instance();
        setenv("http_proxy", "http://http-proxy-1.com:2222", false);
        setenv("HTTP_PROXY", "http://http-proxy-2.com:2222", false);
        setenv("https_proxy", "https://https-proxy-1.com:2222", false);
        setenv("HTTPS_PROXY", "https://https-proxy-2.com:2222", false);
    }
};

class NoProxyEnvVarUpperCaseFixture : public EnvFunctionShim {
  protected:
    NoProxyEnvVarUpperCaseFixture() {
        arcticdb::storage::s3::S3ApiInstance::instance();
        setenv("HTTP_PROXY", "http://http-proxy-2.com:2222", false);
        setenv("NO_PROXY", "http://test-1.endpoint.com", false);
    }
};

class NoProxyEnvVarLowerCasePrecedenceFixture : public EnvFunctionShim {
  protected:
    NoProxyEnvVarLowerCasePrecedenceFixture() {
        arcticdb::storage::s3::S3ApiInstance::instance();
        setenv("http_proxy", "http://http-proxy-2.com:2222", false);
        setenv("no_proxy", "http://test-1.endpoint.com,http://test-2.endpoint.com", false);
        setenv("NO_PROXY", "http://test-3.endpoint.com", false);
    }
};

TEST(TestS3Storage, proxy_env_var_parsing) {
    using namespace arcticdb::storage::s3;
    using namespace Aws::Http;
    auto api = S3ApiInstance::instance();
    struct ProxyConfig {
        Scheme endpoint_scheme_;
        Scheme proxy_scheme_;
        std::string host_;
        uint16_t port_;
        std::string username_;
        std::string password_;
        bool operator==(const Aws::Client::ClientConfiguration& client_config) const {
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
            {"http://username:pass@proxy.com:2222", {Scheme::HTTP, Scheme::HTTP, "proxy.com", 2222, "username", "pass"}
            },
            {"http://username:p@ss@proxy.com:2222", {Scheme::HTTP, Scheme::HTTP, "proxy.com", 2222, "username", "p@ss"}}
    };
    for (const auto& [env_var, expected_proxy_config] : passing_test_cases) {
        auto client_config = parse_proxy_env_var(expected_proxy_config.endpoint_scheme_, env_var.c_str());
        ASSERT_TRUE(client_config.has_value());
        ASSERT_TRUE(expected_proxy_config == client_config);
    }

    std::unordered_map<std::string, ProxyConfig> failing_test_cases{
            {"http-proxy.com:not-a-valid-port", {Scheme::HTTP, Scheme::HTTP, "", 0, "", ""}},
            {"https://username:pass@proxy.com:99999", {Scheme::HTTPS, Scheme::HTTP, "", 0, "", ""}}
    };
    for (const auto& [env_var, expected_proxy_config] : failing_test_cases) {
        auto client_config = parse_proxy_env_var(expected_proxy_config.endpoint_scheme_, env_var.c_str());
        ASSERT_FALSE(client_config.has_value());
    }
}

TEST(TestS3Storage, test_s3_settings_default_value) {
    arcticdb::proto::s3_storage::Config s3_config;
    arcticdb::storage::s3::S3Settings s3_settings(s3_config);
    ASSERT_EQ(s3_settings.aws_auth(), arcticdb::storage::s3::AWSAuthMethod::DISABLED);
}

TEST_F(ProxyEnvVarSetHttpProxyForHttpsEndpointFixture, test_config_resolution_proxy) {
    arcticdb::proto::s3_storage::Config s3_config;
    s3_config.set_endpoint("https://test.endpoint.com");
    s3_config.set_https(true);
    auto ret_cfg = arcticdb::storage::s3::get_s3_config_and_set_env_var(s3_config);
    ASSERT_EQ(ret_cfg.proxyHost, "http-proxy.com");
    ASSERT_EQ(ret_cfg.proxyPort, 443);
    ASSERT_EQ(ret_cfg.proxyScheme, Aws::Http::Scheme::HTTP);
}

TEST_F(ProxyEnvVarUpperCaseFixture, test_config_resolution_proxy) {
    arcticdb::proto::s3_storage::Config s3_config_http;
    s3_config_http.set_endpoint("http://test.endpoint.com");
    auto ret_cfg = arcticdb::storage::s3::get_s3_config_and_set_env_var(s3_config_http);
    ASSERT_EQ(ret_cfg.proxyHost, "http-proxy-2.com");
    ASSERT_EQ(ret_cfg.proxyPort, 2222);
    ASSERT_EQ(ret_cfg.proxyScheme, Aws::Http::Scheme::HTTP);

    arcticdb::proto::s3_storage::Config s3_config_https;
    s3_config_https.set_endpoint("https://test.endpoint.com");
    s3_config_https.set_https(true);
    ret_cfg = arcticdb::storage::s3::get_s3_config_and_set_env_var(s3_config_https);
    ASSERT_EQ(ret_cfg.proxyHost, "https-proxy-2.com");
    ASSERT_EQ(ret_cfg.proxyPort, 2222);
    ASSERT_EQ(ret_cfg.proxyScheme, Aws::Http::Scheme::HTTPS);
}

TEST_F(ProxyEnvVarLowerCasePrecedenceFixture, test_config_resolution_proxy) {
    SKIP_WIN("Env vars are not case-sensitive on Windows");
    arcticdb::proto::s3_storage::Config s3_config_http;
    s3_config_http.set_endpoint("http://test.endpoint.com");
    auto ret_cfg = arcticdb::storage::s3::get_s3_config_and_set_env_var(s3_config_http);
    ASSERT_EQ(ret_cfg.proxyHost, "http-proxy-1.com");
    ASSERT_EQ(ret_cfg.proxyPort, 2222);
    ASSERT_EQ(ret_cfg.proxyScheme, Aws::Http::Scheme::HTTP);

    arcticdb::proto::s3_storage::Config s3_config_https;
    s3_config_https.set_endpoint("https://test.endpoint.com");
    s3_config_https.set_https(true);
    ret_cfg = arcticdb::storage::s3::get_s3_config_and_set_env_var(s3_config_https);
    ASSERT_EQ(ret_cfg.proxyHost, "https-proxy-1.com");
    ASSERT_EQ(ret_cfg.proxyPort, 2222);
    ASSERT_EQ(ret_cfg.proxyScheme, Aws::Http::Scheme::HTTPS);
}

TEST_F(NoProxyEnvVarUpperCaseFixture, test_config_resolution_proxy) {
    arcticdb::proto::s3_storage::Config s3_config;
    s3_config.set_endpoint("http://test.endpoint.com");
    auto ret_cfg = arcticdb::storage::s3::get_s3_config_and_set_env_var(s3_config);

    Aws::Utils::Array<Aws::String> expected_non_proxy_hosts{1};
    expected_non_proxy_hosts[0] = "http://test-1.endpoint.com";
    ASSERT_EQ(ret_cfg.nonProxyHosts, expected_non_proxy_hosts);
}

TEST_F(NoProxyEnvVarLowerCasePrecedenceFixture, test_config_resolution_proxy) {
    SKIP_WIN("Env vars are not case-sensitive on Windows");
    arcticdb::proto::s3_storage::Config s3_config;
    s3_config.set_endpoint("http://test.endpoint.com");
    auto ret_cfg = arcticdb::storage::s3::get_s3_config_and_set_env_var(s3_config);

    Aws::Utils::Array<Aws::String> expected_non_proxy_hosts{2};
    expected_non_proxy_hosts[0] = "http://test-1.endpoint.com";
    expected_non_proxy_hosts[1] = "http://test-2.endpoint.com";
    ASSERT_EQ(ret_cfg.nonProxyHosts, expected_non_proxy_hosts);
}

using namespace arcticdb;
using namespace storage;
using namespace s3;

proto::s3_storage::Config get_test_s3_config() {
    auto config = proto::s3_storage::Config();
    config.set_use_mock_storage_for_testing(true);
    return config;
}

S3Settings get_test_s3_settings() {
    auto config = proto::s3_storage::Config();
    auto s3_settings = S3Settings(arcticdb::storage::s3::AWSAuthMethod::DISABLED, "", true);
    s3_settings.update(config);
    return s3_settings;
}

class S3StorageFixture : public testing::Test {
  protected:
    S3StorageFixture() : store(LibraryPath("lib", '.'), OpenMode::DELETE, S3Settings(get_test_s3_config())) {}

    S3Storage store;
};

class WrapperS3StorageFixture : public testing::Test {
  protected:
    WrapperS3StorageFixture() : store(LibraryPath("lib", '.'), OpenMode::DELETE, get_test_s3_settings()) {}

    S3Storage store;

    void SetUp() override { ConfigsMap::instance()->set_int("S3ClientTestWrapper.EnableFailures", 1); }

    void TearDown() override { ConfigsMap::instance()->unset_int("S3ClientTestWrapper.EnableFailures"); }
};
arcticdb::storage::nfs_backed::NfsBackedStorage::Config get_test_nfs_config() {
    arcticdb::storage::nfs_backed::NfsBackedStorage::Config cfg;
    cfg.set_use_mock_storage_for_testing(true);
    return cfg;
}

class NfsStorageFixture : public testing::Test {
  protected:
    NfsStorageFixture() : store(LibraryPath("lib", '.'), OpenMode::DELETE, get_test_nfs_config()) {}

    arcticdb::storage::nfs_backed::NfsBackedStorage store;
};

class S3AndNfsStorageFixture : public testing::TestWithParam<std::string> {
  public:
    std::unique_ptr<Storage> get_storage() {
        LibraryPath lp{"lib"};
        if (GetParam() == "nfs") {
            return std::make_unique<arcticdb::storage::nfs_backed::NfsBackedStorage>(
                    lp, OpenMode::DELETE, get_test_nfs_config()
            );
        } else if (GetParam() == "s3") {
            return std::make_unique<S3Storage>(lp, OpenMode::DELETE, S3Settings(get_test_s3_config()));
        } else {
            util::raise_rte("Unexpected fixture type {}", GetParam());
        }
    }
};

TEST_F(WrapperS3StorageFixture, test_write) {
    ASSERT_THROW(write_in_store(store, "symbol"), UnexpectedS3ErrorException);
}

TEST_F(S3StorageFixture, test_key_exists) {
    write_in_store(store, "symbol");

    ASSERT_TRUE(exists_in_store(store, "symbol"));
    ASSERT_FALSE(exists_in_store(store, "symbol-not-present"));
    ASSERT_THROW(
            exists_in_store(
                    store,
                    S3ClientTestWrapper::get_failure_trigger(
                            "symbol", StorageOperation::EXISTS, Aws::S3::S3Errors::NETWORK_CONNECTION, false
                    )
            ),
            UnexpectedS3ErrorException
    );
}

TEST_P(S3AndNfsStorageFixture, test_key_path) {
    std::vector<VariantKey> res;

    auto store = get_storage();
    store->iterate_type(KeyType::TABLE_DATA, [&](VariantKey&& found_key) { res.emplace_back(found_key); }, "");

    for (auto vk : res) {
        auto key_path = store->key_path(vk);
        ASSERT_TRUE(key_path.size() > 0);
        ASSERT_TRUE(key_path.starts_with(get_root_folder(store->library_path())));
    }
}

TEST_F(S3StorageFixture, test_read) {
    write_in_store(store, "symbol");

    ASSERT_EQ(read_in_store(store, "symbol"), "symbol");
    ASSERT_THROW(read_in_store(store, "symbol-not-present"), KeyNotFoundException);
    ASSERT_THROW(
            read_in_store(
                    store,
                    S3ClientTestWrapper::get_failure_trigger(
                            "symbol", StorageOperation::READ, Aws::S3::S3Errors::THROTTLING, false
                    )
            ),
            UnexpectedS3ErrorException
    );
}

TEST_P(S3AndNfsStorageFixture, test_read_missing_key_in_exception) {
    auto s = get_storage();
    auto& store = *s;

    try {
        read_in_store(store, "snap-not-present", KeyType::SNAPSHOT_REF);
        FAIL();
    } catch (KeyNotFoundException& e) {
        auto keys = e.keys();
        ASSERT_EQ(keys.size(), 1);
        const auto& key = keys.at(0);
        ASSERT_EQ(variant_key_id(key), StreamId{"snap-not-present"});
    }
}

INSTANTIATE_TEST_SUITE_P(S3AndNfs, S3AndNfsStorageFixture, testing::Values("s3", "nfs"));

TEST_F(S3StorageFixture, test_write) {
    write_in_store(store, "symbol");
    ASSERT_THROW(
            write_in_store(
                    store,
                    S3ClientTestWrapper::get_failure_trigger(
                            "symbol", StorageOperation::WRITE, Aws::S3::S3Errors::NETWORK_CONNECTION, false
                    )
            ),
            UnexpectedS3ErrorException
    );
}

TEST_F(S3StorageFixture, test_remove) {
    for (int i = 0; i < 5; ++i) {
        write_in_store(store, fmt::format("symbol_{}", i));
    }

    // Remove 0 and 1
    remove_in_store(store, {"symbol_0", "symbol_1"});
    auto remaining = std::set<std::string>{"symbol_2", "symbol_3", "symbol_4"};
    ASSERT_EQ(list_in_store(store), remaining);

    // Remove 2 and local fail on 3
    ASSERT_THROW(
            remove_in_store(
                    store,
                    {"symbol_2",
                     S3ClientTestWrapper::get_failure_trigger(
                             "symbol_3", StorageOperation::DELETE_LOCAL, Aws::S3::S3Errors::NETWORK_CONNECTION
                     )}
            ),
            UnexpectedS3ErrorException
    );
    remaining = std::set<std::string>{"symbol_3", "symbol_4"};
    ASSERT_EQ(list_in_store(store), remaining);

    // Attempt to remove 3 and 4, should fail entirely
    ASSERT_THROW(
            remove_in_store(
                    store,
                    {"symbol_3",
                     S3ClientTestWrapper::get_failure_trigger(
                             "symbol_4", StorageOperation::DELETE, Aws::S3::S3Errors::NETWORK_CONNECTION, false
                     )}
            ),
            UnexpectedS3ErrorException
    );
    ASSERT_EQ(list_in_store(store), remaining);
}

TEST_F(S3StorageFixture, test_list) {
    auto symbols = std::set<std::string>();
    for (int i = 10; i < 25; ++i) {
        auto symbol = fmt::format("symbol_{}", i);
        write_in_store(store, symbol);
        symbols.emplace(symbol);
    }
    ASSERT_EQ(list_in_store(store), symbols);

    write_in_store(
            store,
            S3ClientTestWrapper::get_failure_trigger(
                    "symbol_99", StorageOperation::LIST, Aws::S3::S3Errors::NETWORK_CONNECTION, false
            )
    );

    ASSERT_THROW(list_in_store(store), UnexpectedS3ErrorException);
}

TEST_F(S3StorageFixture, test_matching_key_type_prefix_list) {
    auto log_symbols = populate_store(store, "symbol_log", 0, 5, entity::KeyType::LOG);
    ASSERT_EQ(list_in_store(store, entity::KeyType::LOG), log_symbols);

    auto log_compacted_symbols = populate_store(store, "symbol_logc", 0, 5, entity::KeyType::LOG_COMPACTED);
    ASSERT_EQ(list_in_store(store, entity::KeyType::LOG_COMPACTED), log_compacted_symbols);

    ASSERT_EQ(list_in_store(store, entity::KeyType::LOG), log_symbols);
}

// The first call to ListObjectsV2 with a directory bucket and a prefix not ending in the '/' delimiter returns this
// error. We then retry effectively without the prefix, which should succeed
TEST_F(S3StorageFixture, test_list_directory_bucket_success) {
    auto* mock_s3_client = dynamic_cast<S3ClientTestWrapper*>(&store.client());
    mock_s3_client->add_list_objects_failure_unretryable(Aws::S3::S3Errors::INVALID_REQUEST);
    std::string prefix("symbol_");
    auto symbols = std::set<std::string>();
    for (int i = 10; i < 25; ++i) {
        auto symbol_matching_prefix = fmt::format("{}_{}", prefix, i);
        write_in_store(store, symbol_matching_prefix);
        symbols.emplace(symbol_matching_prefix);
        auto symbol_not_matching_prefix = fmt::format("blah_{}_{}", prefix, i);
        write_in_store(store, symbol_matching_prefix);
    }
    ASSERT_EQ(list_in_store(store, KeyType::TABLE_DATA, prefix), symbols);
    ASSERT_TRUE(store.directory_bucket());
}

TEST_F(S3StorageFixture, test_list_directory_bucket_failure) {
    auto* mock_s3_client = dynamic_cast<S3ClientTestWrapper*>(&store.client());
    mock_s3_client->add_list_objects_failure_unretryable(Aws::S3::S3Errors::INVALID_REQUEST);
    // This simulates the case where the invalid request response was not due to a directory bucket, but because of
    // something else. In this case we should raise
    mock_s3_client->add_list_objects_failure_unretryable(Aws::S3::S3Errors::INVALID_REQUEST);
    std::string prefix("symbol_");
    auto symbols = std::set<std::string>();
    for (int i = 10; i < 25; ++i) {
        auto symbol_matching_prefix = fmt::format("{}_{}", prefix, i);
        write_in_store(store, symbol_matching_prefix);
        symbols.emplace(symbol_matching_prefix);
        auto symbol_not_matching_prefix = fmt::format("blah_{}_{}", prefix, i);
        write_in_store(store, symbol_matching_prefix);
    }
    ASSERT_THROW(list_in_store(store, KeyType::TABLE_DATA, prefix), UnexpectedS3ErrorException);
    ASSERT_FALSE(store.directory_bucket());
}
