/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/codec/codec.hpp>

#include <filesystem>
#include <stdexcept>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/util/random.h>
#include <arcticdb/storage/test/common.hpp>
#include <arcticdb/util/test/test_utils.hpp>

namespace ac = arcticdb;
namespace as = arcticdb::storage;
namespace ae = arcticdb::entity;

namespace {

class StorageTestSuite : public testing::TestWithParam<StorageGenerator> {
    void SetUp() override { GetParam().delete_any_test_databases(); }

    void TearDown() override { GetParam().delete_any_test_databases(); }
};

TEST_P(StorageTestSuite, test_list) {
    auto store = GetParam().new_storage();
    auto symbols = std::set<std::string>();
    for (int i = 10; i < 25; ++i) {
        auto symbol = fmt::format("symbol_{}", i);
        write_in_store(*store, symbol);
        symbols.emplace(symbol);
    }

    ASSERT_EQ(list_in_store(*store), symbols);
}

TEST_P(StorageTestSuite, test_exists_matching) {
    auto store = GetParam().new_storage();
    auto s = fmt::format("symbol_1");
    write_in_store(*store, s);
    auto res = store->scan_for_matching_key(ae::KeyType::TABLE_DATA, [](ae::VariantKey&& k) {
        return variant_key_id(k) == ac::StreamId{"symbol_1"};
    });
    ASSERT_TRUE(res);
}

TEST_P(StorageTestSuite, test_exists_not_matching) {
    auto store = GetParam().new_storage();
    auto s = fmt::format("symbol_1");
    write_in_store(*store, s);
    auto res = store->scan_for_matching_key(ae::KeyType::TABLE_DATA, [](ae::VariantKey&& k) {
        return variant_key_id(k) == ac::StreamId{"symbol_2"};
    });
    ASSERT_FALSE(res);
}

TEST_P(StorageTestSuite, test_exists_checks_everything) {
    auto store = GetParam().new_storage();
    for (size_t i = 0; i < 10; i++) {
        write_in_store(*store, fmt::format("symbol_{}", i));
    }
    size_t visited = 0;
    auto res = store->scan_for_matching_key(ae::KeyType::TABLE_DATA, [&visited](ae::VariantKey&& k) {
        visited++;
        return variant_key_id(k) == ac::StreamId{"symbol_10"};
    });
    ASSERT_FALSE(res);
    ASSERT_EQ(visited, 10);
}

using namespace std::string_literals;

std::vector<StorageGenerator> get_storage_generators() { return {"lmdb"s, "mem"s, "mongo"s, "azure"s, "s3"s}; }

INSTANTIATE_TEST_SUITE_P(
        TestStorageOperations, StorageTestSuite, testing::ValuesIn(get_storage_generators()),
        [](const testing::TestParamInfo<StorageTestSuite::ParamType>& info) { return info.param.get_name(); }
);

} // namespace
