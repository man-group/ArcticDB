/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/storage/memory/memory_storage.hpp>
#include <arcticdb/storage/azure/azure_storage.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/mongo/mongo_storage.hpp>
#include <arcticdb/util/test/test_utils.hpp>

std::unique_ptr<storage::Storage> StorageGenerator::new_storage() const {
    storage::LibraryPath library_path{"a", "b"};
    if (storage_ == "lmdb") {
        if (!fs::exists(TEST_DATABASES_PATH)) {
            fs::create_directories(TEST_DATABASES_PATH);
        }
        arcticdb::proto::lmdb_storage::Config cfg;
        fs::path db_name = "test_lmdb";
        cfg.set_path((TEST_DATABASES_PATH / db_name).generic_string());
        cfg.set_map_size(128ULL * (1ULL << 20));
        cfg.set_recreate_if_exists(true);

        return std::make_unique<storage::lmdb::LmdbStorage>(library_path, storage::OpenMode::WRITE, cfg);
    } else if (storage_ == "mem") {
        arcticdb::proto::memory_storage::Config cfg;
        return std::make_unique<storage::memory::MemoryStorage>(library_path, storage::OpenMode::WRITE, cfg);
    } else if (storage_ == "azure") {
        arcticdb::proto::azure_storage::Config cfg;
        cfg.set_use_mock_storage_for_testing(true);
        return std::make_unique<storage::azure::AzureStorage>(library_path, storage::OpenMode::WRITE, cfg);
    } else if (storage_ == "s3") {
        arcticdb::proto::s3_storage::Config cfg;
        cfg.set_use_mock_storage_for_testing(true);
        return std::make_unique<storage::s3::S3Storage>(
                                                        library_path, storage::OpenMode::WRITE, storage::s3::S3Settings(cfg)
                                                        );
    } else if (storage_ == "mongo") {
        arcticdb::proto::mongo_storage::Config cfg;
        cfg.set_use_mock_storage_for_testing(true);
        return std::make_unique<storage::mongo::MongoStorage>(library_path, storage::OpenMode::WRITE, cfg);
    } else {
        throw std::runtime_error(fmt::format("Unknown backend generator type {}.", storage_));
    }
}
