#pragma once

#include <string>
#include <arcticdb/entity/protobufs.hpp>

class AzureSettings {
    std::string container_name_;
    std::string endpoint_;
    std::string ca_cert_path_;
    std::string ca_cert_dir_;
    uint32_t max_connections_;
    uint32_t request_timeout_;
    std::string prefix_;
    bool use_mock_storage_for_testing_;

public:
    AzureSettings() = default;
    AzureSettings(const arcticc::pb2::azure_storage_pb2::Config& config) {
        container_name_ = config.container_name();
        endpoint_ = config.endpoint();
        ca_cert_path_ = config.ca_cert_path();
        ca_cert_dir_ = config.ca_cert_dir();
        max_connections_ = config.max_connections();
        request_timeout_ = config.request_timeout();
        prefix_ = config.prefix();
        use_mock_storage_for_testing_ = config.use_mock_storage_for_testing();
    }
    // export to protobuf
    arcticc::pb2::azure_storage_pb2::Config to_protobuf() const {
        arcticc::pb2::azure_storage_pb2::Config config;
        config.set_container_name(container_name_);
        config.set_endpoint(endpoint_);
        config.set_ca_cert_path(ca_cert_path_);
        config.set_ca_cert_dir(ca_cert_dir_);
        config.set_max_connections(max_connections_);
        config.set_request_timeout(request_timeout_);
        config.set_prefix(prefix_);
        config.set_use_mock_storage_for_testing(use_mock_storage_for_testing_);
        return config;
    }
    std::string container_name() const {
        return container_name_;
    }

    void set_container_name(std::string_view container_name) {
        container_name_ = container_name;
    }

    std::string endpoint() const {
        return endpoint_;
    }

    void set_endpoint(std::string_view endpoint) {
        endpoint_ = endpoint;
    }

    std::string ca_cert_path() const {
        return ca_cert_path_;
    }

    void set_ca_cert_path(std::string_view ca_cert_path){
        ca_cert_path_ = ca_cert_path;
    }

    std::string ca_cert_dir() const {
        return ca_cert_dir_;
    }

    void set_ca_cert_dir(std::string_view ca_cert_dir){
        ca_cert_dir_ = ca_cert_dir;
    }

    uint32_t max_connections() const {
        return max_connections_;
    }

    void set_max_connections(uint32_t max_connections){
        max_connections_ = max_connections;
    }

    uint32_t request_timeout() const {
        return request_timeout_;
    }

    void set_request_timeout(uint32_t request_timeout){
        request_timeout_ = request_timeout;
    }

    std::string prefix() const {
        return prefix_;
    }

    void set_prefix(std::string_view prefix){
        prefix_ = prefix;
    }

    bool use_mock_storage_for_testing() const {
        return use_mock_storage_for_testing_;
    }

    void set_use_mock_storage_for_testing(bool use_mock_storage_for_testing){
        use_mock_storage_for_testing_ = use_mock_storage_for_testing;
    }
};