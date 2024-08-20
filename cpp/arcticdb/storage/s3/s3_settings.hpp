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

public:
    explicit S3Settings(AWSAuthMethod aws_auth, const std::string& aws_profile) :
        aws_auth_(aws_auth),
        aws_profile_(aws_profile) {
    }
    explicit S3Settings(const arcticc::pb2::s3_storage_pb2::Config& config) {
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

    std::string aws_profile() const {
        return aws_profile_;
    }

    bool use_raw_prefix() const {
        return use_raw_prefix_;
    }
};
}