/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
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
#include <arcticdb/storage/s3/s3_utils.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/storage/s3/s3_client_accessor.hpp>
#include <arcticdb/util/composite.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <cstdlib>

namespace arcticdb::storage::s3 {

class S3Storage final : public Storage<S3Storage> {

    using Parent = Storage<S3Storage>;
    friend Parent;

  public:
    friend class S3TestClientAccessor<S3Storage>;
    using Config = arcticdb::proto::s3_storage::Config;

    S3Storage(const LibraryPath &lib, OpenMode mode, const Config &conf);

    /**
     * Full object path in S3 bucket.
     */
    std::string get_key_path(const VariantKey& key) const;

  protected:
    void do_write(Composite<KeySegmentPair>&& kvs);

    void do_update(Composite<KeySegmentPair>&& kvs);

    template<class Visitor>
    void do_read(Composite<VariantKey>&& ks, Visitor &&visitor);

    void do_remove(Composite<VariantKey>&& ks);

    template<class Visitor>
    void do_iterate_type(KeyType key_type, Visitor &&visitor, const std::string &prefix);

    bool do_key_exists(const VariantKey& key);

    bool do_supports_prefix_matching() {
        return true;
    }

    bool do_fast_delete() {
        return false;
    }

  private:
    auto& client() { return s3_client_; }
    const std::string& bucket_name() const { return bucket_name_; }
    const std::string& root_folder() const { return root_folder_; }

    std::shared_ptr<S3ApiInstance> s3_api_;
    Aws::S3::S3Client s3_client_;
    std::string root_folder_;
    std::string bucket_name_;
};


class S3StorageFactory final : public StorageFactory<S3StorageFactory> {
    using Parent = StorageFactory<S3StorageFactory>;
    friend Parent;

  public:
    using Config = arcticdb::proto::s3_storage::Config;
    using StorageType = S3Storage;

    S3StorageFactory(const Config &conf) :
        conf_(conf) {
    }
  private:
    auto do_create_storage(const LibraryPath &lib, OpenMode mode) {
            return S3Storage(lib, mode, conf_);
    }

    Config conf_;
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

inline std::vector<std::pair<std::string, Aws::Http::Scheme>> get_proxy_env_config(){
    return {
        std::make_pair("http_proxy", Aws::Http::Scheme::HTTP),
        std::make_pair("https_proxy", Aws::Http::Scheme::HTTPS),
        std::make_pair("HTTP_PROXY", Aws::Http::Scheme::HTTP),
        std::make_pair("HTTPS_PROXY", Aws::Http::Scheme::HTTPS),
    };
}

/* This function is incomplete (AN-264/AN-18)
 * In the event of an authenticating proxy URL of the form 'http://user:password@euro-webproxy.drama.man.com:8080'
 * an stoi exception is thrown when trying to extract the port.
 * In addition, to work in mkd.core tests, the NO_PROXY env variable must also be respected, which is */
//inline void set_proxy(Aws::Client::ClientConfiguration& clientConfig) {
//    for (const auto& env_var : get_proxy_env_config()) {
//        char* _val = std::getenv(env_var.first.c_str());
//        if(_val != nullptr) {
//            auto val = std::string(_val);
//            auto endpoint_start_idx = val.find("://");
//            endpoint_start_idx = endpoint_start_idx == std::string::npos ? 0 : endpoint_start_idx + 3;
//
//            auto port_start_idx = val .find(':', endpoint_start_idx + 1);
//            int port;
//            if (port_start_idx == std::string::npos){
//                port = env_var.second == Aws::Http::Scheme::HTTPS ? 443 : 80;
//            } else {
//                port = std::stoi(val.substr(port_start_idx + 1, val.length()));
//            }
//
//            auto endpoint_end_idx = port_start_idx == std::string::npos ? val.length() : port_start_idx;
//            auto endpoint = val.substr(endpoint_start_idx, endpoint_end_idx - endpoint_start_idx);
//
//            clientConfig.proxyHost = endpoint;
//            clientConfig.proxyPort = port;
//            clientConfig.proxyScheme = env_var.second;
//
//            break;
//        }
//    }
//}

template<typename ConfigType>
auto get_s3_config(const ConfigType& conf) {
    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.scheme = conf.https() ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;

    if (!conf.region().empty())
        clientConfig.region = conf.region();

//    set_proxy(clientConfig);

    auto endpoint = conf.endpoint();
    util::check_arg(!endpoint.empty(), "S3 Endpoint must be specified");
    clientConfig.endpointOverride = endpoint;
    clientConfig.verifySSL = false;
    clientConfig.maxConnections = conf.max_connections() == 0 ?
            ConfigsMap::instance()->get_int("VersionStore.NumIOThreads", 16) :
            conf.max_connections();
    clientConfig.connectTimeoutMs = conf.connect_timeout() == 0 ? 30000 : conf.connect_timeout();
    clientConfig.requestTimeoutMs = conf.request_timeout() == 0 ? 200000 : conf.request_timeout();
    return clientConfig;
}

template<typename ConfigType>
Aws::Auth::AWSCredentials get_aws_credentials(const ConfigType& conf) {
    return Aws::Auth::AWSCredentials(conf.credential_name().c_str(), conf.credential_key().c_str());
}

} //namespace arcticdb::s3

#define ARCTICDB_S3_STORAGE_H_
#include <arcticdb/storage/s3/s3_storage-inl.hpp>