/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <string>
#include <arcticdb/entity/protobufs.hpp>

namespace arcticdb::storage::s3 {

enum class AWSAuthMethod : uint32_t {
    DISABLED = 0,
    DEFAULT_CREDENTIALS_PROVIDER_CHAIN = 1,
    STS_PROFILE_CREDENTIALS_PROVIDER = 2
};

class GCPXMLSettings {
    std::string bucket_;
    std::string endpoint_;
    std::string access_;
    std::string secret_;
    std::string prefix_;
    AWSAuthMethod aws_auth_;
    std::string aws_profile_;

public:
    GCPXMLSettings() { }

    explicit GCPXMLSettings(AWSAuthMethod aws_auth, const std::string& aws_profile) : aws_auth_(aws_auth), aws_profile_(aws_profile) {

    }

    GCPXMLSettings update(const arcticc::pb2::gcp_storage_pb2::Config& config){
        prefix_ = config.prefix();
        return *this;
    }

    [[nodiscard]] std::string endpoint() const {
        return endpoint_;
    }

    void set_endpoint(std::string_view endpoint) {
        endpoint_ = endpoint;
    }

    [[nodiscard]] std::string access() const {
        return access_;
    }

    void set_access(const std::string_view access) {
        access_ = access;
    }

    [[nodiscard]] std::string secret() const {
        return secret_;
    }

    void set_secret(const std::string_view secret) {
        secret_ = secret;
    }

    [[nodiscard]] AWSAuthMethod aws_auth() const {
        return aws_auth_;
    }

    void set_aws_auth(const AWSAuthMethod aws_auth) {
        aws_auth_ = aws_auth;
    }

    [[nodiscard]] std::string aws_profile() const {
        return aws_profile_;
    }

    void set_aws_profile(const std::string_view aws_profile) {
        aws_profile_ = aws_profile;
    }

    [[nodiscard]] std::string bucket() const {
        return bucket_;
    }

    void set_bucket(const std::string_view bucket) {
        bucket_ = bucket;
    };

    void set_prefix(const std::string_view prefix) {
        prefix_ = prefix;
    }

    [[nodiscard]] std::string prefix() const {
        return prefix_;
    }
};

class S3Settings {
private:
    std::string bucket_name_;
    std::string credential_name_;
    std::string credential_key_;
    std::string endpoint_;
    uint32_t max_connections_;
    uint32_t connect_timeout_;
    uint32_t request_timeout_;
    bool ssl_;
    std::string prefix_;
    bool https_;
    std::string region_;
    bool use_virtual_addressing_;
    bool use_mock_storage_for_testing_;
    std::string ca_cert_path_;
    std::string ca_cert_dir_;
    bool use_raw_prefix_;
    AWSAuthMethod aws_auth_;
    std::string aws_profile_;
    bool use_internal_client_wrapper_for_testing_;

public:
    explicit S3Settings(AWSAuthMethod aws_auth,
                        const std::string& aws_profile,
                        bool use_internal_client_wrapper_for_testing) :
        aws_auth_(aws_auth),
        aws_profile_(aws_profile),
        use_internal_client_wrapper_for_testing_(use_internal_client_wrapper_for_testing) {
    }

    explicit S3Settings(const arcticc::pb2::s3_storage_pb2::Config& config) : 
        S3Settings(AWSAuthMethod::DISABLED, "", false)
    {
        update(config);
    }
    
    S3Settings update(const arcticc::pb2::s3_storage_pb2::Config& config){
        bucket_name_ = config.bucket_name();
        credential_name_ = config.credential_name();
        credential_key_ = config.credential_key();
        endpoint_ = config.endpoint();
        max_connections_ = config.max_connections();
        connect_timeout_ = config.connect_timeout();
        request_timeout_ = config.request_timeout();
        ssl_ = config.ssl();
        prefix_ = config.prefix();
        https_ = config.https();
        region_ = config.region();
        use_virtual_addressing_ = config.use_virtual_addressing();
        use_mock_storage_for_testing_ = config.use_mock_storage_for_testing();
        ca_cert_path_ = config.ca_cert_path();
        ca_cert_dir_ = config.ca_cert_dir();
        use_raw_prefix_ = config.use_raw_prefix();
        return *this;
    }

    S3Settings update(const GCPXMLSettings& gcp_xml_settings) {
        bucket_name_ = gcp_xml_settings.bucket();
        credential_name_ = gcp_xml_settings.access();
        credential_key_ = gcp_xml_settings.secret();
        endpoint_ = gcp_xml_settings.endpoint();
        prefix_ = gcp_xml_settings.prefix();
        aws_auth_ = gcp_xml_settings.aws_auth();
        aws_profile_ = gcp_xml_settings.aws_profile();
        use_virtual_addressing_ = false;
        use_mock_storage_for_testing_ = false;
        use_internal_client_wrapper_for_testing_ = false;
        use_raw_prefix_ = false;
        max_connections_ = 0;
        connect_timeout_ = 0;
        request_timeout_ = 0;
        ssl_ = false;
        https_ = false; // TODO aseaton sort this out
        return *this;
    }

    std::string bucket_name() const {
        return bucket_name_;
    }

    std::string credential_name() const {
        return credential_name_;
    }

    std::string credential_key() const {
        return credential_key_;
    }

    std::string endpoint() const {
        return endpoint_;
    }

    uint32_t max_connections() const {
        return max_connections_;
    }

    uint32_t connect_timeout() const {
        return connect_timeout_;
    }

    uint32_t request_timeout() const {
        return request_timeout_;
    }

    bool ssl() const {
        return ssl_;
    }

    std::string prefix() const {
        return prefix_;
    }

    bool https() const {
        return https_;
    }

    std::string region() const {
        return region_;
    }

    bool use_virtual_addressing() const {
        return use_virtual_addressing_;
    }

    bool use_mock_storage_for_testing() const {
        return use_mock_storage_for_testing_;
    }

    std::string ca_cert_path() const {
        return ca_cert_path_;
    }

    std::string ca_cert_dir() const {
        return ca_cert_dir_;
    }

    AWSAuthMethod aws_auth() const {
        return aws_auth_;
    }

    bool use_internal_client_wrapper_for_testing() const {
        return use_internal_client_wrapper_for_testing_;
    }

    std::string aws_profile() const {
        return aws_profile_;
    }

    bool use_raw_prefix() const {
        return use_raw_prefix_;
    }
};

}