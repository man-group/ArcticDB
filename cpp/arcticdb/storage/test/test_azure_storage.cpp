/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/storage/azure/azure_storage.hpp>
#include <arcticdb/storage/azure/azure_mock_client.hpp>
#include <arcticdb/storage/storage_mock_client.hpp>
#include <arcticdb/storage/test/common.hpp>

using namespace arcticdb;
using namespace storage;
using namespace azure;

arcticdb::proto::azure_storage::Config get_mock_azure_config() {
    arcticdb::proto::azure_storage::Config cfg;
    cfg.set_use_mock_storage_for_testing(true);

    return cfg;
}

class AzureMockStorageFixture : public testing::Test {
protected:
    AzureStorage store;
    AzureMockStorageFixture() : store(LibraryPath("_arctic_cfg", '.'), OpenMode::DELETE, get_mock_azure_config()) {}
};

TEST_F(AzureMockStorageFixture, test_key_exists) {
    write_in_store(store, "symbol");

    ASSERT_TRUE(exists_in_store(store, "symbol"));
    ASSERT_FALSE(exists_in_store(store, "symbol-not-present"));
}

TEST_F(AzureMockStorageFixture, test_read){
    write_in_store(store, "symbol");

    ASSERT_EQ(read_in_store(store, "symbol"), "symbol");
    ASSERT_THROW(read_in_store(store, "symbol-not-present"), arcticdb::ArcticException);
}

TEST_F(AzureMockStorageFixture, test_write){
    write_in_store(store, "symbol");
    ASSERT_THROW(
            write_in_store(store, MockAzureClient::get_failure_trigger("symbol",
               StorageOperation::WRITE, AzureErrorCode_to_string(AzureErrorCode::UnauthorizedBlobOverwrite),
               Azure::Core::Http::HttpStatusCode::Unauthorized)),arcticdb::ArcticException);
}

TEST_F(AzureMockStorageFixture, test_remove) {
    for (int i = 0; i < 5; ++i) {
        write_in_store(store, fmt::format("symbol_{}", i));
    }

    // Remove 0 and 1
    remove_in_store(store, {"symbol_0", "symbol_1"});
    auto remaining = std::set<std::string>{"symbol_2", "symbol_3", "symbol_4"};
    ASSERT_EQ(list_in_store(store), remaining);
}

TEST_F(AzureMockStorageFixture, test_list) {
    auto symbols = std::set<std::string>();
    for (int i = 10; i < 25; ++i) {
        auto symbol = fmt::format("symbol_{}", i);
        write_in_store(store, symbol);
        symbols.emplace(symbol);
    }

    ASSERT_EQ(list_in_store(store), symbols);
}
