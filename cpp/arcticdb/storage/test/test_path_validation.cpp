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
    // '\' is banned in both symbol keys and library names, on top of the globally unsupported S3 characters.
    EXPECT_TRUE(store->unsupported_symbol_chars().contains('\\'));
    EXPECT_TRUE(store->unsupported_library_chars().contains('\\'));
    EXPECT_TRUE(store->unsupported_symbol_chars().contains('*'));
    EXPECT_FALSE(store->unsupported_symbol_chars().contains('/'));
}

TEST(PathValidation, MongoForwardSlashIsLibraryOnly) {
    auto store = make_mongo();
    // '/' is only disallowed in library names (it forms the Mongo database name), not in symbol keys.
    EXPECT_FALSE(store->unsupported_symbol_chars().contains('/'));
    EXPECT_TRUE(store->unsupported_library_chars().contains('/'));
}

TEST(PathValidation, BackendsWithoutRestrictionsHaveNoExtraChars) {
    auto store = make_memory();
    // Only the globally unsupported S3 characters apply; no backend-specific extras.
    EXPECT_FALSE(store->unsupported_symbol_chars().contains('/'));
    EXPECT_FALSE(store->unsupported_symbol_chars().contains('\\'));
    EXPECT_FALSE(store->unsupported_library_chars().contains('/'));
    EXPECT_FALSE(store->unsupported_library_chars().contains('\\'));
    EXPECT_TRUE(store->unsupported_symbol_chars().contains('<'));
    EXPECT_FALSE(store->verify_library_suffix("a\\b").has_value());
}
