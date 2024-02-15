/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/test/gtest_utils.hpp>

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/azure/azure_storage.hpp>
#include <arcticdb/storage/azure/azure_mock_client.hpp>
#include <arcticdb/util/buffer.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>

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

VariantKey get_variant_key(std::string name) {
    auto builder = atom_key_builder();
    return builder.build<KeyType::TABLE_DATA>(name);
}

Segment get_example_segment(){
    auto segment_in_memory = get_test_frame<arcticdb::stream::TimeseriesIndex>("symbol", {}, 10, 0).segment_;
    auto codec_opts = proto::encoding::VariantCodec();
    return encode_dispatch(std::move(segment_in_memory), codec_opts, EncodingVersion::V2);
}

void write_in_store(AzureStorage& store, std::string symbol) {
    auto variant_key = get_variant_key(symbol);
    store.write(KeySegmentPair(std::move(variant_key), get_example_segment()));
}

std::string read_in_store(AzureStorage& store, std::string symbol) {
    auto variant_key = get_variant_key(symbol);
    auto opts = ReadKeyOpts{};
    auto result = store.read(std::move(variant_key), opts);
    return std::get<StringId>(result.atom_key().id());
}

bool exists_in_store(AzureStorage& store, std::string symbol){
    auto variant_key = get_variant_key(symbol);
    return store.key_exists(variant_key);
}

void remove_in_store(AzureStorage& store, std::vector<std::string> symbols){
    auto to_remove = std::vector<VariantKey>();
    for (auto& symbol : symbols){
        to_remove.emplace_back(get_variant_key(symbol));
    }
    auto opts = RemoveOpts();
    store.remove(Composite(std::move(to_remove)), opts);
}

std::set<std::string> list_in_store(AzureStorage& store){
    auto keys = std::set<std::string>();
    store.iterate_type(KeyType::TABLE_DATA, [&keys](VariantKey&& key){
        auto atom_key = std::get<AtomKey>(key);
        keys.emplace(std::get<StringId>(atom_key.id()));
    });
    return keys;
}

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
            write_in_store(store, MockAzureClient::get_failure_trigger("symbol", AzureOperation::WRITE, Azure::Core::Http::HttpStatusCode::Unauthorized)),
            arcticdb::ArcticException);
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
