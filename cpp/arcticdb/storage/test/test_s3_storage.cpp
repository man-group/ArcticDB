/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/async/async_store.hpp>
#include <arcticdb/storage/memory/memory_storage.hpp>
#include <arcticdb/storage/mock/s3_mock_client.hpp>
#include <arcticdb/storage/s3/s3_api.hpp>
#include <arcticdb/storage/s3/s3_client_impl.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/s3/s3_client_wrapper.hpp>
#include <arcticdb/storage/s3/detail-inl.hpp>
#include <arcticdb/storage/s3/nfs_backed_storage.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/toolbox/query_stats.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/test/common.hpp>
#include <arcticdb/util/test/gtest_utils.hpp>
#include <arcticdb/util/configs_map.hpp>

#include <arcticdb/log/log.hpp>

#include <aws/core/Aws.h>
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/Object.h>
#ifndef WIN32
#include <aws/core/http/HttpClientFactory.h>
#endif

#include <spdlog/sinks/ostream_sink.h>

#include <set>
#include <sstream>
#include <type_traits>

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

namespace {

S3ClientTestWrapper* get_test_wrapper(Storage& store) {
    if (auto* s3_storage = dynamic_cast<S3Storage*>(&store)) {
        return dynamic_cast<S3ClientTestWrapper*>(&s3_storage->client());
    }
    if (auto* nfs_storage = dynamic_cast<nfs_backed::NfsBackedStorage*>(&store)) {
        return dynamic_cast<S3ClientTestWrapper*>(&nfs_storage->client());
    }
    return nullptr;
}

} // namespace

// S3 can accept a DeleteObjects request but reject some of the objects in it
class S3AndNfsRemoveRetryFixture : public S3AndNfsStorageFixture {
  protected:
    void SetUp() override {
        storage = get_storage();
        client = get_test_wrapper(*storage);
        ASSERT_NE(client, nullptr);
        for (int i = 0; i < 3; ++i) {
            write_in_store(*storage, fmt::format("symbol_{}", i));
        }
    }

    std::unique_ptr<Storage> storage;
    S3ClientTestWrapper* client = nullptr;
};

TEST_P(S3AndNfsRemoveRetryFixture, retries_objects_rejected_with_retryable_error) {
    client->add_delete_objects_failure("symbol_1", "InternalError", 1);

    remove_in_store(*storage, {"symbol_0", "symbol_1", "symbol_2"});

    ASSERT_TRUE(list_in_store(*storage).empty());
}

TEST_P(S3AndNfsRemoveRetryFixture, deletes_object_rejected_on_every_attempt_but_the_last) {
    client->add_delete_objects_failure("symbol_1", "InternalError", s3::detail::DELETE_OBJECTS_MAX_ATTEMPTS - 1);

    remove_in_store(*storage, {"symbol_0", "symbol_1", "symbol_2"});

    ASSERT_TRUE(list_in_store(*storage).empty());
}

TEST_P(S3AndNfsRemoveRetryFixture, raises_if_object_is_rejected_on_every_attempt) {
    // Would succeed on one more attempt, so this also checks we stop after DELETE_OBJECTS_MAX_ATTEMPTS
    client->add_delete_objects_failure("symbol_1", "InternalError", s3::detail::DELETE_OBJECTS_MAX_ATTEMPTS);

    ASSERT_THROW(remove_in_store(*storage, {"symbol_0", "symbol_1", "symbol_2"}), UnexpectedS3ErrorException);

    ASSERT_EQ(list_in_store(*storage), std::set<std::string>{"symbol_1"});
}

TEST_P(S3AndNfsRemoveRetryFixture, does_not_retry_objects_rejected_with_non_retryable_error) {
    // Fails only once, so if we retried the delete would succeed and nothing would be raised
    client->add_delete_objects_failure("symbol_1", "AccessDenied", 1);

    ASSERT_THROW(remove_in_store(*storage, {"symbol_0", "symbol_1", "symbol_2"}), UnexpectedS3ErrorException);

    ASSERT_EQ(list_in_store(*storage), std::set<std::string>{"symbol_1"});
}

INSTANTIATE_TEST_SUITE_P(S3AndNfs, S3AndNfsRemoveRetryFixture, testing::Values("s3", "nfs"));

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

namespace {

template<typename StoreT>
std::map<std::string, ObjectMetadata> list_metadata(
        StoreT& store, entity::KeyType key_type = entity::KeyType::TABLE_DATA, const std::string& prefix = ""
) {
    std::map<std::string, ObjectMetadata> result;
    auto collector = [&result](const VariantKey& key, const ObjectMetadata& meta) {
        result.emplace(std::get<StringId>(variant_key_id(key)), meta);
    };
    if constexpr (std::is_base_of_v<Storage, StoreT>) {
        store.visit_object_metadata(key_type, collector, prefix);
    } else {
        store.visit_object_metadata(key_type, collector, prefix).get();
    }
    return result;
}

std::shared_ptr<async::AsyncStore<>> make_async_store(std::shared_ptr<Storage> storage) {
    const auto lp = storage->library_path();
    auto storages = std::make_shared<Storages>(Storages::StorageVector{std::move(storage)}, OpenMode::DELETE);
    auto library = std::make_shared<Library>(lp, std::move(storages));
    return std::make_shared<async::AsyncStore<>>(
            std::move(library), proto::encoding::VariantCodec{}, EncodingVersion::V2
    );
}

std::map<std::string, CompressedSize> list_sizes_in_async_store(async::AsyncStore<>& async_store) {
    std::map<std::string, CompressedSize> result;
    async_store
            .visit_object_sizes(
                    KeyType::TABLE_DATA,
                    std::nullopt,
                    [&result](const VariantKey& key, CompressedSize size) {
                        result.emplace(std::get<StringId>(variant_key_id(key)), size);
                    }
            )
            .get();
    return result;
}

uint64_t list_objects_v2_count() {
    auto stats_output = query_stats::QueryStats::instance()->get_stats();
    return stats_output["storage_operations"]["S3_ListObjectsV2"]["TABLE_DATA"]["count"];
}

void assert_valid_mock_metadata(const ObjectMetadata& meta, CompressedSize expected_size) {
    const bool quoted_etag = meta.etag.size() >= 2 && meta.etag.front() == '"' && meta.etag.back() == '"';
    ASSERT_TRUE(quoted_etag) << meta.etag;
    ASSERT_EQ(meta.size, expected_size);
    ASSERT_GT(meta.last_modified_ms, 0);
}

struct ScopedQueryStats {
    ScopedQueryStats() {
        auto stats = query_stats::QueryStats::instance();
        was_enabled_ = stats->is_enabled();
        stats->enable();
        stats->reset_stats();
    }
    ~ScopedQueryStats() {
        auto stats = query_stats::QueryStats::instance();
        stats->reset_stats();
        if (!was_enabled_) {
            stats->disable();
        }
    }
    bool was_enabled_{false};
};

} // namespace

TEST_P(S3AndNfsStorageFixture, test_visit_object_metadata_paginates_and_matches_sizes) {
    std::shared_ptr<Storage> store = get_storage();
    ASSERT_TRUE(store->supports_object_metadata_listing());

    // Write 25 keys to span multiple pages (MockS3Client page_size == 10).
    const auto expected_symbols = populate_store(*store, "sym", 0, 25);
    const auto expected_size = get_test_segment().calculate_size();

    ScopedQueryStats scoped_stats;
    auto metadata = list_metadata(*store);
    ASSERT_EQ(list_objects_v2_count(), 3u);
    ASSERT_EQ(metadata.size(), expected_symbols.size());

    auto async_store = make_async_store(store);
    auto sizes = list_sizes_in_async_store(*async_store);
    ASSERT_EQ(list_objects_v2_count(), 6u);
    ASSERT_EQ(sizes.size(), expected_symbols.size());

    auto totals = async_store->get_object_sizes(KeyType::TABLE_DATA, std::nullopt).get();
    ASSERT_EQ(list_objects_v2_count(), 9u);
    ASSERT_EQ(totals->count_.load(), expected_symbols.size());

    std::set<std::string> seen_etags;
    CompressedSize total_bytes = 0;
    for (const auto& symbol : expected_symbols) {
        ASSERT_TRUE(metadata.contains(symbol));
        ASSERT_TRUE(sizes.contains(symbol));
        const auto& meta = metadata.at(symbol);
        assert_valid_mock_metadata(meta, expected_size);
        ASSERT_EQ(sizes.at(symbol), expected_size);
        total_bytes += meta.size;
        seen_etags.insert(meta.etag);
    }
    ASSERT_EQ(totals->compressed_size_.load(), total_bytes);
    ASSERT_EQ(seen_etags.size(), expected_symbols.size());
}

TEST_P(S3AndNfsStorageFixture, test_visit_object_metadata_prefix_filter) {
    auto store = get_storage();
    const auto expected_alpha = populate_store(*store, "alpha", 0, 15);
    populate_store(*store, "beta", 0, 5);

    auto metadata = list_metadata(*store, KeyType::TABLE_DATA, "alpha_");
    ASSERT_EQ(metadata.size(), expected_alpha.size());
    for (const auto& [sym, meta] : metadata) {
        ASSERT_TRUE(expected_alpha.contains(sym));
        ASSERT_FALSE(meta.etag.empty());
        ASSERT_GT(meta.size, 0u);
    }
}

TEST_P(S3AndNfsStorageFixture, test_visit_object_metadata_overwrite_and_delete) {
    auto store = get_storage();
    write_in_store(*store, "sym_keep");
    write_in_store(*store, "sym_change");

    auto before = list_metadata(*store);
    ASSERT_EQ(before.size(), 2u);
    const auto original_meta = before.at("sym_change");

    update_in_store(*store, "sym_change");
    auto after_update = list_metadata(*store);
    ASSERT_EQ(after_update.size(), 2u);
    ASSERT_EQ(after_update.at("sym_keep"), before.at("sym_keep"));
    const auto& updated_meta = after_update.at("sym_change");
    ASSERT_NE(updated_meta.etag, original_meta.etag);
    ASSERT_GT(updated_meta.last_modified_ms, original_meta.last_modified_ms);

    remove_in_store(*store, {"sym_change"});
    auto after_delete = list_metadata(*store);
    ASSERT_EQ(after_delete.size(), 1u);
    ASSERT_TRUE(after_delete.contains("sym_keep"));
    ASSERT_FALSE(after_delete.contains("sym_change"));
}

TEST_F(S3StorageFixture, test_visit_object_metadata_directory_bucket_fallback) {
    auto* mock_s3_client = dynamic_cast<S3ClientTestWrapper*>(&store.client());
    ASSERT_NE(mock_s3_client, nullptr);
    mock_s3_client->add_list_objects_failure_unretryable(Aws::S3::S3Errors::INVALID_REQUEST);

    const auto expected = populate_store(store, "symbol", 10, 25);
    populate_store(store, "other_symbol", 10, 25);

    ScopedQueryStats scoped_stats;
    auto result = list_metadata(store, KeyType::TABLE_DATA, "symbol_");
    ASSERT_TRUE(store.directory_bucket());
    // 1 initial INVALID_REQUEST + 3 pages for 30 objects (MockS3Client page_size == 10).
    ASSERT_EQ(list_objects_v2_count(), 4u);
    ASSERT_EQ(result.size(), expected.size());
    for (const auto& sym : expected) {
        ASSERT_TRUE(result.contains(sym));
        ASSERT_FALSE(result.at(sym).etag.empty());
    }
}

TEST_F(S3StorageFixture, test_visit_object_metadata_directory_bucket_failure) {
    auto* mock_s3_client = dynamic_cast<S3ClientTestWrapper*>(&store.client());
    ASSERT_NE(mock_s3_client, nullptr);
    mock_s3_client->add_list_objects_failure_unretryable(Aws::S3::S3Errors::INVALID_REQUEST);
    mock_s3_client->add_list_objects_failure_unretryable(Aws::S3::S3Errors::INVALID_REQUEST);

    write_in_store(store, "symbol_1");
    ScopedQueryStats scoped_stats;
    ASSERT_THROW(list_metadata(store, KeyType::TABLE_DATA, "symbol_"), UnexpectedS3ErrorException);
    ASSERT_FALSE(store.directory_bucket());
    ASSERT_EQ(list_objects_v2_count(), 2u);
}

TEST_F(S3StorageFixture, test_visit_object_metadata_expected_error_page_1_vs_page_2) {
    auto* mock_s3_client = dynamic_cast<S3ClientTestWrapper*>(&store.client());
    ASSERT_NE(mock_s3_client, nullptr);

    // On page 1, an expected NoSuchBucket returns empty rather than raising.
    mock_s3_client->add_list_objects_failure_unretryable(Aws::S3::S3Errors::NO_SUCH_BUCKET);
    {
        ScopedQueryStats scoped_stats;
        ASSERT_TRUE(list_metadata(store).empty());
        ASSERT_EQ(list_objects_v2_count(), 1u);
    }

    // Write 12 zero-padded keys so page 1 returns 10 keys and a continuation token, then inject
    // NoSuchBucket on page 2. Listing must raise rather than looping or silently truncating.
    for (int i = 0; i < 12; ++i) {
        write_in_store(store, fmt::format("symbol_{:02}", i));
    }
    const auto trigger_no_such_bucket = S3ClientTestWrapper::get_failure_trigger(
            "symbol_99", StorageOperation::LIST, Aws::S3::S3Errors::NO_SUCH_BUCKET, false
    );
    write_in_store(store, trigger_no_such_bucket);

    {
        ScopedQueryStats scoped_stats;
        ASSERT_THROW(list_metadata(store), UnexpectedS3ErrorException);
        ASSERT_EQ(list_objects_v2_count(), 2u);
    }
    {
        ScopedQueryStats scoped_stats;
        ASSERT_THROW(list_in_store(store), UnexpectedS3ErrorException);
        ASSERT_EQ(list_objects_v2_count(), 2u);
    }

    // Page-2 NoSuchKey maps through raise_s3_exception() to KeyNotFoundException.
    remove_in_store(store, {trigger_no_such_bucket});
    write_in_store(
            store,
            S3ClientTestWrapper::get_failure_trigger(
                    "symbol_99", StorageOperation::LIST, Aws::S3::S3Errors::NO_SUCH_KEY, false
            )
    );
    {
        ScopedQueryStats scoped_stats;
        ASSERT_THROW(list_metadata(store), KeyNotFoundException);
        ASSERT_EQ(list_objects_v2_count(), 2u);
    }
}

TEST_F(S3StorageFixture, test_visit_object_metadata_invalid_request_on_page_2_does_not_retry_from_page_1) {
    for (int i = 0; i < 12; ++i) {
        write_in_store(store, fmt::format("symbol_{:02}", i));
    }
    write_in_store(
            store,
            S3ClientTestWrapper::get_failure_trigger(
                    "symbol_99", StorageOperation::LIST, Aws::S3::S3Errors::INVALID_REQUEST, false
            )
    );

    ScopedQueryStats scoped_stats;
    ASSERT_THROW(list_metadata(store, KeyType::TABLE_DATA, "symbol_"), UnexpectedS3ErrorException);
    ASSERT_FALSE(store.directory_bucket());
    ASSERT_EQ(list_objects_v2_count(), 2u);
}

TEST_P(S3AndNfsStorageFixture, test_visit_object_metadata_unexpected_error_throws) {
    auto store = get_storage();
    populate_store(*store, "symbol", 0, 5);

    auto* wrapper = get_test_wrapper(*store);
    ASSERT_NE(wrapper, nullptr);
    wrapper->add_list_objects_failure_unretryable(Aws::S3::S3Errors::NETWORK_CONNECTION);
    ASSERT_THROW(list_metadata(*store), UnexpectedS3ErrorException);
}

TEST(TestObjectMetadataListing, unsupported_storage_and_stream_source) {
    LibraryPath lp{"lib"};
    auto mem_storage = std::make_shared<memory::MemoryStorage>(lp, OpenMode::DELETE, memory::MemoryStorage::Config{});
    ASSERT_FALSE(mem_storage->supports_object_metadata_listing());
    ASSERT_THROW(
            mem_storage->visit_object_metadata(KeyType::TABLE_DATA, [](const VariantKey&, const ObjectMetadata&) {}),
            ArcticException
    );

    Storages empty_storages(Storages::StorageVector{}, OpenMode::DELETE);
    ASSERT_FALSE(empty_storages.supports_object_metadata_listing());

    auto s3_storage = std::make_shared<S3Storage>(lp, OpenMode::DELETE, S3Settings(get_test_s3_config()));
    write_in_store(*s3_storage, "sym_1");
    Storages mixed_storages(Storages::StorageVector{s3_storage, mem_storage}, OpenMode::DELETE);
    ASSERT_FALSE(mixed_storages.supports_object_metadata_listing());
    size_t visited = 0;
    auto count_visits = [&visited](const VariantKey&, const ObjectMetadata&) { ++visited; };
    ASSERT_THROW(mixed_storages.visit_object_metadata(KeyType::TABLE_DATA, count_visits, "", true), ArcticException);
    ASSERT_THROW(mixed_storages.visit_object_metadata(KeyType::TABLE_DATA, count_visits, "", false), ArcticException);
    ASSERT_EQ(visited, 0u);

    auto mem_async_store = make_async_store(mem_storage);
    ASSERT_FALSE(mem_async_store->supports_object_metadata_listing());
    auto mem_fut =
            mem_async_store->visit_object_metadata(KeyType::TABLE_DATA, [](const VariantKey&, const ObjectMetadata&) {
            });
    ASSERT_THROW(std::move(mem_fut).get(), ArcticException);

    // Default StreamSource::visit_object_metadata returns an exceptional future rather than throwing synchronously.
    InMemoryStore in_memory_store;
    ASSERT_FALSE(in_memory_store.supports_object_metadata_listing());
    auto default_fut =
            in_memory_store.visit_object_metadata(KeyType::TABLE_DATA, [](const VariantKey&, const ObjectMetadata&) {});
    ASSERT_TRUE(default_fut.hasException());
    ASSERT_THROW(std::move(default_fut).get(), ArcticException);
}

TEST(TestObjectMetadataListing, async_store_lists_s3_metadata_prefix_and_refs) {
    LibraryPath lp{"lib"};
    auto s3_storage = std::make_shared<S3Storage>(lp, OpenMode::DELETE, S3Settings(get_test_s3_config()));
    write_in_store(*s3_storage, "alpha_1");
    write_in_store(*s3_storage, "alpha_2");
    write_in_store(*s3_storage, "beta_1");
    s3_storage->write(KeySegmentPair{RefKey{SnapshotId{"snap_a"}, KeyType::SNAPSHOT_REF}, get_test_segment()});
    s3_storage->write(KeySegmentPair{RefKey{SnapshotId{"snap_b"}, KeyType::SNAPSHOT_REF}, get_test_segment()});

    auto s3_async_store = make_async_store(s3_storage);
    ASSERT_TRUE(s3_async_store->supports_object_metadata_listing());
    ASSERT_EQ(list_metadata(*s3_async_store).size(), 3u);

    auto alpha_only = list_metadata(*s3_async_store, KeyType::TABLE_DATA, "alpha_");
    ASSERT_EQ(alpha_only.size(), 2u);
    ASSERT_TRUE(alpha_only.contains("alpha_1") && alpha_only.contains("alpha_2"));

    auto snap_refs = list_metadata(*s3_async_store, KeyType::SNAPSHOT_REF);
    ASSERT_EQ(snap_refs.size(), 2u);
    ASSERT_TRUE(snap_refs.contains("snap_a") && snap_refs.contains("snap_b"));
    assert_valid_mock_metadata(snap_refs.at("snap_a"), get_test_segment().calculate_size());
}

TEST(TestObjectMetadataListing, repeated_continuation_token_throws) {
    struct StuckTokenS3Client : MockS3Client {
        mutable size_t calls{0};
        S3Result<ListObjectsOutput>
        list_objects(const std::string&, const std::string&, const std::optional<std::string>&) const override {
            ++calls;
            return {ListObjectsOutput{{}, std::optional<std::string>{"stuck-token"}}};
        }
    };

    StuckTokenS3Client client;
    auto path_info = s3::detail::calculate_path_info("lib", KeyType::TABLE_DATA, true, "", 0);
    s3::detail::Visitor visitor{[](VariantKey&&, const ObjectMetadata&) { return false; }};
    ASSERT_THROW(
            s3::detail::do_list_objects_impl(KeyType::TABLE_DATA, "bucket", client, path_info, visitor),
            UnexpectedS3ErrorException
    );
    ASSERT_EQ(client.calls, 2u);
}

TEST(TestObjectMetadataListing, s3_client_impl_parses_list_objects_v2_metadata) {
    Aws::S3::Model::Object obj1;
    obj1.WithKey("lib/sr/snap_a")
            .WithSize(1234)
            .WithETag("\"d41d8cd98f00b204e9800998ecf8427e\"")
            .WithLastModified(Aws::Utils::DateTime(int64_t{1700000000000}));

    Aws::S3::Model::Object obj2;
    obj2.WithKey("lib/sr/snap_b")
            .WithSize(5678)
            .WithETag("\"098f6bcd4621d373cade4e832627b4f6-2\"")
            .WithLastModified(Aws::Utils::DateTime(int64_t{1700000060000}));

    Aws::S3::Model::Object obj_neg;
    obj_neg.WithKey("lib/sr/snap_neg")
            .WithSize(-5)
            .WithETag("\"e\"")
            .WithLastModified(Aws::Utils::DateTime(int64_t{0}));

    Aws::S3::Model::ListObjectsV2Result aws_result;
    aws_result.AddContents(obj1);
    aws_result.AddContents(obj2);
    aws_result.AddContents(obj_neg);
    aws_result.SetIsTruncated(true);
    aws_result.SetNextContinuationToken("page-2-token");

    const auto parsed = arcticdb::storage::s3::parse_list_objects_v2_result(aws_result);
    ASSERT_EQ(
            parsed.objects,
            (std::vector<ListedObject>{
                    ListedObject{
                            "lib/sr/snap_a",
                            ObjectMetadata{"\"d41d8cd98f00b204e9800998ecf8427e\"", 1234u, 1700000000000}
                    },
                    ListedObject{
                            "lib/sr/snap_b",
                            ObjectMetadata{"\"098f6bcd4621d373cade4e832627b4f6-2\"", 5678u, 1700000060000}
                    },
                    ListedObject{"lib/sr/snap_neg", ObjectMetadata{"\"e\"", 0u, 0}}
            })
    );
    ASSERT_EQ(parsed.next_continuation_token, std::optional<std::string>{"page-2-token"});

    // IsTruncated=true with an empty NextContinuationToken raises rather than silently truncating or looping.
    Aws::S3::Model::ListObjectsV2Result malformed_truncated;
    malformed_truncated.SetIsTruncated(true);
    ASSERT_THROW(
            (void)arcticdb::storage::s3::parse_list_objects_v2_result(malformed_truncated), UnexpectedS3ErrorException
    );
}

TEST(S3LogSystem, RoutesToSpdlogWithLevelAndTag) {
    using namespace arcticdb::storage::s3;
    using Aws::Utils::Logging::LogLevel;

    auto& logger = arcticdb::log::s3();
    const auto saved_level = logger.level();

    std::ostringstream captured;
    auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(captured);
    sink->set_pattern("%l|%v");
    logger.sinks().push_back(sink);
    logger.set_level(spdlog::level::trace);

    SpdlogLogSystem log_system(LogLevel::Trace);
    for (auto [aws_level, tag] :
         {std::pair{LogLevel::Fatal, "FatalTag"},
          {LogLevel::Error, "ErrorTag"},
          {LogLevel::Warn, "WarnTag"},
          {LogLevel::Info, "InfoTag"},
          {LogLevel::Debug, "DebugTag"},
          {LogLevel::Trace, "TraceTag"}}) {
        Aws::OStringStream message;
        message << "msg-" << tag;
        log_system.LogStream(aws_level, tag, message);
    }
    // The printf-style Log path must keep the per-message level too.
    log_system.Log(LogLevel::Error, "FmtTag", "value=%d", 42);
    log_system.Flush();

    logger.sinks().pop_back();
    logger.set_level(saved_level);

    const auto out = captured.str();
    ASSERT_NE(out.find("critical|[FatalTag] msg-FatalTag"), std::string::npos);
    ASSERT_NE(out.find("error|[ErrorTag] msg-ErrorTag"), std::string::npos);
    ASSERT_NE(out.find("warning|[WarnTag] msg-WarnTag"), std::string::npos);
    ASSERT_NE(out.find("info|[InfoTag] msg-InfoTag"), std::string::npos);
    ASSERT_NE(out.find("debug|[DebugTag] msg-DebugTag"), std::string::npos);
    ASSERT_NE(out.find("trace|[TraceTag] msg-TraceTag"), std::string::npos);
    ASSERT_NE(out.find("error|[FmtTag] value=42"), std::string::npos);
}

#ifndef WIN32
TEST(TestS3Storage, curl_http_client_factory_is_registered) {
    arcticdb::storage::s3::S3ApiInstance::instance();
    Aws::Client::ClientConfiguration client_configuration;
    auto http_client = Aws::Http::CreateHttpClient(client_configuration);
    ASSERT_NE(dynamic_cast<arcticdb::storage::s3::ArcticCurlHttpClient*>(http_client.get()), nullptr);
}

TEST(TestS3Storage, dns_shuffle_addresses_disabled_by_default) {
    using namespace arcticdb::storage::s3;
    ScopedConfig::unset_int("S3Storage.DnsShuffleAddresses");
    ASSERT_FALSE(dns_shuffle_addresses_enabled());
}

TEST(TestS3Storage, dns_shuffle_addresses_can_be_disabled_via_config) {
    using namespace arcticdb::storage::s3;
    ScopedConfig scoped_config("S3Storage.DnsShuffleAddresses", 0);
    ASSERT_FALSE(dns_shuffle_addresses_enabled());
}

TEST(TestS3Storage, curl_http_client_captures_dns_shuffle_config_at_construction) {
    using namespace arcticdb::storage::s3;
    S3ApiInstance::instance();

    Aws::Client::ClientConfiguration client_configuration;
    client_configuration.endpointOverride = "s3.us-east-1.amazonaws.com";

    {
        ScopedConfig scoped_config("S3Storage.DnsShuffleAddresses", 1);
        auto http_client = Aws::Http::CreateHttpClient(client_configuration);
        auto* arctic_curl_http_client = dynamic_cast<ArcticCurlHttpClient*>(http_client.get());
        ASSERT_NE(arctic_curl_http_client, nullptr);

        // Flipping the config after construction must not affect a client that already exists, since the value
        // is captured once in the constructor rather than read live on every request.
        ConfigsMap::instance()->set_int("S3Storage.DnsShuffleAddresses", 0);
        ASSERT_TRUE(arctic_curl_http_client->should_shuffle_dns_addresses());
    }

    {
        ScopedConfig scoped_config("S3Storage.DnsShuffleAddresses", 0);
        auto http_client = Aws::Http::CreateHttpClient(client_configuration);
        auto* arctic_curl_http_client = dynamic_cast<ArcticCurlHttpClient*>(http_client.get());
        ASSERT_NE(arctic_curl_http_client, nullptr);

        ConfigsMap::instance()->set_int("S3Storage.DnsShuffleAddresses", 1);
        ASSERT_FALSE(arctic_curl_http_client->should_shuffle_dns_addresses());
    }
}
#endif // WIN32
