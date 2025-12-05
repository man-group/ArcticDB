/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/test/gtest_utils.hpp>

#include <arcticdb/storage/azure/azure_client_impl.hpp>
#include <arcticdb/storage/azure/azure_client_interface.hpp>
#include <arcticdb/util/error_code.hpp>

using namespace arcticdb;
using namespace storage;
using namespace azure;

class AzureTransportTest : public testing::Test {
  protected:
    arcticdb::proto::azure_storage::Config config;

    void SetUp() override {
        config.set_endpoint("https://testaccount.blob.core.windows.net");
        config.set_container_name("testcontainer");
    }
};

TEST_F(AzureTransportTest, TestWindowsTransportSelection) {
#if defined(_WIN32)
    // Test that WinHTTP is used when no CA cert settings are provided
    auto options = RealAzureClient::get_client_options(config);
    ASSERT_NE(options.Transport.Transport, nullptr);

    // Test that an exception is thrown when CA cert settings are provided
    config.set_ca_cert_path("/path/to/cert.pem");
    ASSERT_THROW(RealAzureClient::get_client_options(config), ArcticSpecificException<ErrorCode::E_INVALID_USER_ARGUMENT>);

    config.set_ca_cert_path("");
    config.set_ca_cert_dir("/path/to/certs");
    ASSERT_THROW(RealAzureClient::get_client_options(config), ArcticSpecificException<ErrorCode::E_INVALID_USER_ARGUMENT>);
#endif
}

TEST_F(AzureTransportTest, TestMacOSTransportSelection) {
#if defined(__APPLE__)
    // Test that libcurl is used when no CA cert settings are provided
    auto options = RealAzureClient::get_client_options(config);
    ASSERT_NE(options.Transport.Transport, nullptr);

    // Test that an exception is thrown when CA cert settings are provided
    config.set_ca_cert_path("/path/to/cert.pem");
    ASSERT_THROW(RealAzureClient::get_client_options(config), ArcticSpecificException<ErrorCode::E_INVALID_USER_ARGUMENT>);

    config.set_ca_cert_path("");
    config.set_ca_cert_dir("/path/to/certs");
    ASSERT_THROW(RealAzureClient::get_client_options(config), ArcticSpecificException<ErrorCode::E_INVALID_USER_ARGUMENT>);
#endif
}

TEST_F(AzureTransportTest, TestLinuxTransportSelection) {
#if !defined(_WIN32) && !defined(__APPLE__)
    // Test that libcurl is used with default settings
    auto options = RealAzureClient::get_client_options(config);
    ASSERT_NE(options.Transport.Transport, nullptr);

    // Test that libcurl is used with CA cert path
    config.set_ca_cert_path("/path/to/cert.pem");
    options = RealAzureClient::get_client_options(config);
    ASSERT_NE(options.Transport.Transport, nullptr);

    // Test that libcurl is used with CA cert directory
    config.set_ca_cert_path("");
    config.set_ca_cert_dir("/path/to/certs");
    options = RealAzureClient::get_client_options(config);
    ASSERT_NE(options.Transport.Transport, nullptr);

    // Test that libcurl is used with both CA cert path and directory
    config.set_ca_cert_path("/path/to/cert.pem");
    options = RealAzureClient::get_client_options(config);
    ASSERT_NE(options.Transport.Transport, nullptr);
#endif
}