/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>

#include <arcticdb/storage/test/test_s3_storage_common.hpp>
#include <aws/core/Aws.h>
#include <arcticdb/util/composite.hpp>

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

    virtual ~EnvFunctionShim() {
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
        as3::S3ApiInstance::instance();
        setenv("HTTPS_PROXY", "http://http-proxy.com", false);
    }
};

class ProxyEnvVarUpperCaseFixture : public EnvFunctionShim {
protected:
    ProxyEnvVarUpperCaseFixture()
    {
        as3::S3ApiInstance::instance();
        setenv("HTTP_PROXY", "http://http-proxy-2.com:2222", false);
        setenv("HTTPS_PROXY", "https://https-proxy-2.com:2222", false);
    }
};

class ProxyEnvVarLowerCasePrecedenceFixture : public EnvFunctionShim {
protected:
    ProxyEnvVarLowerCasePrecedenceFixture()
    {
        as3::S3ApiInstance::instance();
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
        as3::S3ApiInstance::instance();
        setenv("HTTP_PROXY", "http://http-proxy-2.com:2222", false);
        setenv("NO_PROXY", "http://test-1.endpoint.com", false);
    }
};

class NoProxyEnvVarLowerCasePrecedenceFixture : public EnvFunctionShim {
protected:
    NoProxyEnvVarLowerCasePrecedenceFixture()
    {
        as3::S3ApiInstance::instance();
        setenv("http_proxy", "http://http-proxy-2.com:2222", false);
        setenv("no_proxy", "http://test-1.endpoint.com,http://test-2.endpoint.com", false);
        setenv("NO_PROXY", "http://test-3.endpoint.com", false);
    }
};

TEST(TestS3Storage, basic) {
    auto[cfg, lib_path] = test_s3_path_and_config();
    auto [root_folder, bucket_name, client] = GET_S3;

    ac::entity::AtomKey
            k = ac::entity::atom_key_builder().gen_id(1).build<ac::entity::KeyType::TABLE_DATA>("test_symbol");

    as::KeySegmentPair kv(k);
    kv.segment().header().set_start_ts(1234);
    kv.segment().set_buffer(std::make_shared<ac::Buffer>());

    std::vector<as::KeySegmentPair> ka{std::move(kv)};
    arcticdb::storage::s3::detail::do_write_impl(arcticdb::Composite<as::KeySegmentPair>{std::move(ka)}, root_folder, bucket_name, client, as::s3::detail::FlatBucketizer{});
    std::vector<arcticdb::entity::VariantKey> a{k};
    as::KeySegmentPair res;

    arcticdb::storage::s3::detail::do_read_impl(
            arcticdb::Composite<ae::VariantKey>(std::move(a)),
            [&](auto&& k, auto&& seg) {
                res.atom_key() = std::get<as::AtomKey>(k);
                res.segment() = std::move(seg);
                res.segment().force_own_buffer();
            },
            root_folder,
            bucket_name,
            client,
            as::s3::detail::FlatBucketizer{}, as::ReadKeyOpts{});

    ASSERT_EQ(res.segment().header().start_ts(), 1234);

    bool executed = false;
    arcticdb::storage::s3::detail::do_iterate_type_impl(
            arcticdb::entity::KeyType::TABLE_DATA,
            [&](auto&& found_key) {
                ASSERT_EQ(to_atom(found_key), k);
                executed = true;
            },
            root_folder,
            bucket_name,
            client,
            as::s3::detail::FlatBucketizer{},
            as::s3::detail::default_prefix_handler());

    ASSERT_TRUE(executed);

    as::KeySegmentPair update_kv(k);
    update_kv.segment().header().set_start_ts(4321);
    update_kv.segment().set_buffer(std::make_shared<ac::Buffer>());

    std::vector<as::KeySegmentPair> kb{std::move(update_kv)};
    arcticdb::storage::s3::detail::do_update_impl(arcticdb::Composite<as::KeySegmentPair>{std::move(kb)}, root_folder, bucket_name, client, as::s3::detail::FlatBucketizer{});
    std::vector<arcticdb::entity::VariantKey> b{k};
    as::KeySegmentPair update_res;

    arcticdb::storage::s3::detail::do_read_impl(
            arcticdb::Composite<ae::VariantKey>(std::move(b)),
            [&](auto&& k, auto&& seg) {
                update_res.atom_key() = std::get<as::AtomKey>(k);
                update_res.segment() = std::move(seg);
                update_res.segment().force_own_buffer();
            },
            root_folder,
            bucket_name,
            client,
            as::s3::detail::FlatBucketizer{}, as::ReadKeyOpts{});

    ASSERT_EQ(update_res.segment().header().start_ts(), 4321);

    executed = false;
    arcticdb::storage::s3::detail::do_iterate_type_impl(
            arcticdb::entity::KeyType::TABLE_DATA,
            [&](auto&& found_key) {
                ASSERT_EQ(to_atom(found_key), k);
                executed = true;
            },
            root_folder,
            bucket_name,
            client,
            as::s3::detail::FlatBucketizer{},
            as::s3::detail::default_prefix_handler());

    ASSERT_TRUE(executed);

}

TEST(TestS3Storage, refkey) {
    auto[cfg, lib_path] = test_s3_path_and_config();
    auto [root_folder, bucket_name, client] = GET_S3;
    ae::RefKey k{"test_ref_key", ae::KeyType::VERSION_REF};
    write_test_ref_key(k, root_folder, bucket_name, client);
    std::vector<arcticdb::entity::VariantKey> a{k};
    as::KeySegmentPair res;

    arcticdb::storage::s3::detail::do_read_impl(
            arcticdb::Composite<ae::VariantKey>(std::move(a)),
            [&](auto&& k, auto&& seg) {
                res.variant_key() = k;
                res.segment() = std::move(seg);
                res.segment().force_own_buffer();
            },
            root_folder,
            bucket_name,
            client,
            as::s3::detail::FlatBucketizer{}, as::ReadKeyOpts{});

    ASSERT_EQ(res.segment().header().start_ts(), 1234);
}

void stress_test_s3_storage() {
 //   auto[cfg, lib_path] = test_s3_path_and_config();
 //   auto [root_folder, bucket_name, client] = real_s3_storage(lib_path, cfg);

    const int NumThreads = 100;
    const int Repeats = 1000;

    ae::RefKey key{"test_ref_key", ae::KeyType::VERSION_REF};

    as::KeySegmentPair kv(key);
    kv.segment().header().set_start_ts(1234);
    kv.segment().set_buffer(std::make_shared<ac::Buffer>());
//    std::array<as::KeySegmentPair, 1> ka{std::move(kv)};
//    arcticdb::storage::s3::detail::do_write_impl(folly::range(ka), root_folder, bucket_name, client);
    const bool TestRead = false;
    std::atomic<int> write_count{0};

    auto thread_func = [&write_count, &key] () {
        auto[cfg, lib_path] = test_s3_path_and_config();
        auto [root_folder, bucket_name, client] = real_s3_storage(lib_path, cfg);

        for (auto i = 0; i < Repeats; ++i) {
            bool written = false;
            if(!written && write_count == i) {
                arcticdb::log::storage().info("Thread {} writing", thread_id());
                written = true;
                ++write_count;
                write_test_ref_key(key, root_folder, bucket_name, client);
            }
            if(i % 100 == 1)
                arcticdb::log::storage().info("thread {} done {} repeats", thread_id(), i);
            if(TestRead) {
                as::KeySegmentPair res;

                arcticdb::storage::s3::detail::do_read_impl(
                        arcticdb::Composite<ae::VariantKey>(std::move(key)),
                        [&](auto&& k, auto&& seg) {
                            res.variant_key() = k;
                            res.segment() = std::move(seg);
                            res.segment().force_own_buffer();
                        },
                        root_folder,
                        bucket_name,
                        client,
                        as::s3::detail::FlatBucketizer{}, as::ReadKeyOpts{});
            }
            else {
                bool res = arcticdb::storage::s3::detail::do_key_exists_impl(
                        key,
                        root_folder,
                        bucket_name,
                        client,
                        as::s3::detail::FlatBucketizer{});

                if (!res)
                    throw std::runtime_error("failed");
            }
        }
    };

    std::vector<std::unique_ptr<std::thread>> threads;

    for(auto i = 0; i < NumThreads; ++i) {
        threads.push_back(std::make_unique<std::thread>(thread_func));
    }

    for(auto& thread : threads)
        thread->join();
}


/* This test exposes an open issue with Pure S3 where if a file exists and a new one is
 * written to the same key, the file can return KeyNotFound as it is being updated.
 */
TEST(TestS3Storage, ReadStress) {
#ifndef USE_FAKE_CLIENT
    //stress_test_s3_storage();
#endif
}

TEST(TestS3Storage, remove) {
    auto[cfg, lib_path] = test_s3_path_and_config();
    auto [root_folder, bucket_name, client] = GET_S3;
    std::vector<as::KeySegmentPair> writes;
    std::vector<ac::entity::VariantKey> keys ;
    for (size_t i = 0; i < 100; i++) {
        ac::entity::AtomKey
                k = ac::entity::atom_key_builder().gen_id(1).build<ac::entity::KeyType::TABLE_INDEX>("test_symbol" + std::to_string(i));

        as::KeySegmentPair kv(k);
        kv.segment().header().set_start_ts(i);
        kv.segment().set_buffer(std::make_shared<ac::Buffer>());

        writes.emplace_back(std::move(kv));
        keys.emplace_back(std::move(k));
    }
    arcticdb::storage::s3::detail::do_write_impl(arcticdb::Composite<as::KeySegmentPair>(std::move(writes)), root_folder, bucket_name, client, as::s3::detail::FlatBucketizer{});

    arcticdb::storage::s3::detail::do_remove_impl(arcticdb::Composite<ae::VariantKey>(std::move(keys)), root_folder, bucket_name, client, as::s3::detail::FlatBucketizer{});

    arcticdb::storage::s3::detail::do_iterate_type_impl(
            arcticdb::entity::KeyType::TABLE_INDEX,
            [&](auto && found_key) {
                ASSERT_TRUE(std::find(keys.begin(), keys.end(), found_key) == keys.end());
            },
            root_folder,
            bucket_name,
            client,
            as::s3::detail::FlatBucketizer{},
            as::s3::detail::default_prefix_handler());
}

TEST(TestS3Storage, remove_ignores_missing) {
    auto [cfg, lib_path] = test_s3_path_and_config();
    auto [root_folder, bucket_name, client] = real_s3_storage(lib_path, cfg);

    ae::RefKey k{"should_not_exist", ae::KeyType::VERSION_REF};
    arcticdb::storage::s3::detail::do_remove_impl(arcticdb::Composite<ae::VariantKey>(k),
                                                 root_folder, bucket_name, client, as::s3::detail::FlatBucketizer{});
}

TEST(TestS3Storage, proxy_env_var_parsing) {
    using namespace arcticdb::storage::s3;
    using namespace Aws::Http;
    struct ProxyConfig {
        Scheme endpoint_scheme_;
        Scheme proxy_scheme_;
        std::string host_;
        uint16_t port_;
        std::string username_;
        std::string password_;
        bool operator==(const Aws::Client::ClientConfiguration& client_config) const {
            return
            proxy_scheme_ == client_config.proxyScheme &&
            host_ == client_config.proxyHost &&
            port_ == client_config.proxyPort &&
            username_ == client_config.proxyUserName &&
            password_ == client_config.proxyPassword;
        };
    };
    std::unordered_map<std::string, ProxyConfig> passing_test_cases {
        {"http-proxy.com:2222", {Scheme::HTTP, Scheme::HTTP, "http-proxy.com", 2222, "", ""}},
        {"https://https-proxy.com", {Scheme::HTTPS, Scheme::HTTPS, "https-proxy.com", 443, "", ""}},
        // Test setting http proxy for https endpoint
        {"http://http-proxy.com", {Scheme::HTTPS, Scheme::HTTP, "http-proxy.com", 443, "", ""}},
        {"http://username@proxy.com", {Scheme::HTTP, Scheme::HTTP, "proxy.com", 80, "username", ""}},
        {"http://username:pass@proxy.com:2222", {Scheme::HTTP, Scheme::HTTP, "proxy.com", 2222, "username", "pass"}},
        {"http://username:p@ss@proxy.com:2222", {Scheme::HTTP, Scheme::HTTP, "proxy.com", 2222, "username", "p@ss"}}
    };
    for (const auto& [env_var, expected_proxy_config]: passing_test_cases) {
        auto client_config = parse_proxy_env_var(expected_proxy_config.endpoint_scheme_, env_var.c_str());
        ASSERT_TRUE(client_config.has_value());
        ASSERT_TRUE(expected_proxy_config == client_config);
    }

    std::unordered_map<std::string, ProxyConfig> failing_test_cases {
        {"http-proxy.com:not-a-valid-port", {Scheme::HTTP, Scheme::HTTP, "", 0, "", ""}},
        {"https://username:pass@proxy.com:99999", {Scheme::HTTPS, Scheme::HTTP, "", 0, "", ""}}
    };
    for (const auto& [env_var, expected_proxy_config]: failing_test_cases) {
        auto client_config = parse_proxy_env_var(expected_proxy_config.endpoint_scheme_, env_var.c_str());
        ASSERT_FALSE(client_config.has_value());
    }
}

TEST_F(ProxyEnvVarSetHttpProxyForHttpsEndpointFixture, test_config_resolution_proxy) {
    as3::S3Storage::Config s3_config;
    s3_config.set_endpoint("https://test.endpoint.com");
    s3_config.set_https(true);
    auto ret_cfg = arcticdb::storage::s3::get_s3_config(s3_config);
    ASSERT_EQ(ret_cfg.proxyHost, "http-proxy.com");
    ASSERT_EQ(ret_cfg.proxyPort, 443);
    ASSERT_EQ(ret_cfg.proxyScheme, Aws::Http::Scheme::HTTP);
}

TEST_F(ProxyEnvVarUpperCaseFixture, test_config_resolution_proxy) {
    as3::S3Storage::Config s3_config_http;
    s3_config_http.set_endpoint("http://test.endpoint.com");
    auto ret_cfg = arcticdb::storage::s3::get_s3_config(s3_config_http);
    ASSERT_EQ(ret_cfg.proxyHost, "http-proxy-2.com");
    ASSERT_EQ(ret_cfg.proxyPort, 2222);
    ASSERT_EQ(ret_cfg.proxyScheme, Aws::Http::Scheme::HTTP);

    as3::S3Storage::Config s3_config_https;
    s3_config_https.set_endpoint("https://test.endpoint.com");
    s3_config_https.set_https(true);
    ret_cfg = arcticdb::storage::s3::get_s3_config(s3_config_https);
    ASSERT_EQ(ret_cfg.proxyHost, "https-proxy-2.com");
    ASSERT_EQ(ret_cfg.proxyPort, 2222);
    ASSERT_EQ(ret_cfg.proxyScheme, Aws::Http::Scheme::HTTPS);
}

TEST_F(ProxyEnvVarLowerCasePrecedenceFixture, test_config_resolution_proxy) {
    as3::S3Storage::Config s3_config_http;
    s3_config_http.set_endpoint("http://test.endpoint.com");
    auto ret_cfg = arcticdb::storage::s3::get_s3_config(s3_config_http);
    ASSERT_EQ(ret_cfg.proxyHost, "http-proxy-1.com");
    ASSERT_EQ(ret_cfg.proxyPort, 2222);
    ASSERT_EQ(ret_cfg.proxyScheme, Aws::Http::Scheme::HTTP);

    as3::S3Storage::Config s3_config_https;
    s3_config_https.set_endpoint("https://test.endpoint.com");
    s3_config_https.set_https(true);
    ret_cfg = arcticdb::storage::s3::get_s3_config(s3_config_https);
    ASSERT_EQ(ret_cfg.proxyHost, "https-proxy-1.com");
    ASSERT_EQ(ret_cfg.proxyPort, 2222);
    ASSERT_EQ(ret_cfg.proxyScheme, Aws::Http::Scheme::HTTPS);
}

TEST_F(NoProxyEnvVarUpperCaseFixture, test_config_resolution_proxy) {
    as3::S3Storage::Config s3_config;
    s3_config.set_endpoint("http://test.endpoint.com");
    auto ret_cfg = arcticdb::storage::s3::get_s3_config(s3_config);

    Aws::Utils::Array<Aws::String> expected_non_proxy_hosts{1};
    expected_non_proxy_hosts[0] = "http://test-1.endpoint.com";
    ASSERT_EQ(ret_cfg.nonProxyHosts, expected_non_proxy_hosts);
}

TEST_F(NoProxyEnvVarLowerCasePrecedenceFixture, test_config_resolution_proxy) {
    as3::S3Storage::Config s3_config;
    s3_config.set_endpoint("http://test.endpoint.com");
    auto ret_cfg = arcticdb::storage::s3::get_s3_config(s3_config);

    Aws::Utils::Array<Aws::String> expected_non_proxy_hosts{2};
    expected_non_proxy_hosts[0] = "http://test-1.endpoint.com";
    expected_non_proxy_hosts[1] = "http://test-2.endpoint.com";
    ASSERT_EQ(ret_cfg.nonProxyHosts, expected_non_proxy_hosts);
}