/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/azure/azure_storage.hpp>
#include <arcticdb/storage/mongo/mongo_storage.hpp>
#include <arcticdb/storage/memory/memory_storage.hpp>

using namespace arcticdb;
using namespace arcticdb::storage;

namespace {
std::unique_ptr<Storage> make_azure() {
    arcticdb::proto::azure_storage::Config cfg;
    cfg.set_use_mock_storage_for_testing(true);
    return std::make_unique<azure::AzureStorage>(LibraryPath("lib", '/'), OpenMode::DELETE, cfg);
}

std::unique_ptr<Storage> make_mongo() {
    arcticdb::proto::mongo_storage::Config cfg;
    cfg.set_use_mock_storage_for_testing(true);
    return std::make_unique<mongo::MongoStorage>(LibraryPath("lib", '/'), OpenMode::DELETE, cfg);
}

std::unique_ptr<Storage> make_memory() {
    arcticdb::proto::memory_storage::Config cfg;
    return std::make_unique<memory::MemoryStorage>(LibraryPath("lib", '.'), OpenMode::DELETE, cfg);
}
} // namespace

TEST(PathValidation, AzureRejectsBackslashEverywhere) {
    auto store = make_azure();
    EXPECT_EQ(store->is_path_valid("a\\b"), '\\');
    EXPECT_EQ(store->is_library_path_valid("a\\b"), '\\');
    EXPECT_FALSE(store->is_path_valid("a/b").has_value());
    EXPECT_FALSE(store->is_library_path_valid("a/b").has_value());
}

TEST(PathValidation, MongoForwardSlashIsLibraryOnly) {
    auto store = make_mongo();
    EXPECT_FALSE(store->is_path_valid("a/b").has_value());
    EXPECT_EQ(store->is_library_path_valid("a/b"), '/');
}

TEST(PathValidation, BackendsWithoutRestrictionsAcceptEverything) {
    auto store = make_memory();
    EXPECT_FALSE(store->is_path_valid("a/b").has_value());
    EXPECT_FALSE(store->is_path_valid("a\\b").has_value());
    EXPECT_FALSE(store->is_library_path_valid("a/b").has_value());
    EXPECT_FALSE(store->is_library_path_valid("a\\b").has_value());
}
