/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/storage/memory/memory_storage.hpp>
#ifdef ARCTICDB_INCLUDE_ROCKSDB
#include <arcticdb/storage/rocksdb/rocksdb_storage.hpp>
#endif
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/s3/s3_mock_client.hpp>
#include <arcticdb/storage/azure/azure_storage.hpp>
#include <arcticdb/storage/azure/azure_mock_client.hpp>
#include <arcticdb/storage/mongo/mongo_storage.hpp>
#include <arcticdb/storage/mongo/mongo_mock_client.hpp>
#include <arcticdb/storage/test/common.hpp>
#include <arcticdb/util/buffer.hpp>

#include <filesystem>
#include <memory>

using namespace arcticdb;
using namespace storage;

inline const fs::path TEST_DATABASES_PATH = "./test_databases";

class StorageFactory {
public:
    virtual ~StorageFactory() = default;
    virtual std::unique_ptr<arcticdb::storage::Storage> create() = 0;

    virtual void setup() { }
    virtual void clear_setup() { }
};

class LMDBStorageFactory : public StorageFactory {
private:
    uint64_t map_size;

public:
    LMDBStorageFactory() : map_size(128ULL * (1ULL << 20) /* 128MB */) { }

    explicit LMDBStorageFactory(uint64_t map_size) : map_size(map_size) { }

    std::unique_ptr<arcticdb::storage::Storage> create() override {
        arcticdb::proto::lmdb_storage::Config cfg;

        fs::path db_name = "test_lmdb";
        cfg.set_path((TEST_DATABASES_PATH / db_name).generic_string());
        cfg.set_map_size(map_size);
        cfg.set_recreate_if_exists(true);

        arcticdb::storage::LibraryPath library_path{"a", "b"};

        return std::make_unique<arcticdb::storage::lmdb::LmdbStorage>(library_path, arcticdb::storage::OpenMode::DELETE, cfg);
    }

    void setup() override {
        if (!fs::exists(TEST_DATABASES_PATH)) {
            fs::create_directories(TEST_DATABASES_PATH);
        }
    }

    void clear_setup() override {
        if (fs::exists(TEST_DATABASES_PATH)) {
            fs::remove_all(TEST_DATABASES_PATH);
        }
    }
};

class MemoryStorageFactory : public StorageFactory {
public:
    std::unique_ptr<arcticdb::storage::Storage> create() override {
        arcticdb::proto::memory_storage::Config cfg;
        arcticdb::storage::LibraryPath library_path{"a", "b"};

        return std::make_unique<arcticdb::storage::memory::MemoryStorage>(library_path, arcticdb::storage::OpenMode::DELETE, cfg);
    }
};

#ifdef ARCTICDB_INCLUDE_ROCKSDB
class RocksDBStorageFactory : public StorageFactory {

public:
    std::unique_ptr<arcticdb::storage::Storage> create() override {
        arcticdb::proto::rocksdb_storage::Config cfg;

        fs::path db_name = "test_rocksdb";
        cfg.set_path((TEST_DATABASES_PATH / db_name).generic_string());

        arcticdb::storage::LibraryPath library_path("lib", '/');

        return std::make_unique<arcticdb::storage::rocksdb::RocksDBStorage>(library_path, arcticdb::storage::OpenMode::DELETE, cfg);
    }

    void setup() override {
        if (!fs::exists(TEST_DATABASES_PATH)) {
            fs::create_directories(TEST_DATABASES_PATH);
        }
    }

    void clear_setup() override {
        if (fs::exists(TEST_DATABASES_PATH)) {
            fs::remove_all(TEST_DATABASES_PATH);
        }
    }
};
#endif

class S3MockStorageFactory : public StorageFactory {
public:
    std::unique_ptr<arcticdb::storage::Storage> create() override {
        arcticdb::proto::s3_storage::Config cfg;
        cfg.set_use_mock_storage_for_testing(true);
        arcticdb::storage::LibraryPath library_path("lib", '.');

        return std::make_unique<arcticdb::storage::s3::S3Storage>(library_path, arcticdb::storage::OpenMode::DELETE, cfg);
    }
};

class AzureMockStorageFactory : public StorageFactory {
public:
    std::unique_ptr<arcticdb::storage::Storage> create() override {
        arcticdb::proto::azure_storage::Config cfg;
        cfg.set_use_mock_storage_for_testing(true);
        arcticdb::storage::LibraryPath library_path("lib", '/');

        return std::make_unique<arcticdb::storage::azure::AzureStorage>(library_path,arcticdb::storage::OpenMode::DELETE, cfg);
    }
};

class MongoMockStorageFactory : public StorageFactory {
public:
    std::unique_ptr<arcticdb::storage::Storage> create() override {
        arcticdb::proto::mongo_storage::Config cfg;
        cfg.set_use_mock_storage_for_testing(true);
        arcticdb::storage::LibraryPath library_path("lib", '/');

        return std::make_unique<arcticdb::storage::mongo::MongoStorage>(library_path,arcticdb::storage::OpenMode::DELETE, cfg);
    }
};

// Generic tests that run with all types of storages

class GenericStorageTest : public ::testing::TestWithParam<std::shared_ptr<StorageFactory>> {
protected:
    std::unique_ptr<arcticdb::storage::Storage> storage;

    void SetUp() override {
        GetParam()->setup();
        storage = GetParam()->create();
    }

    void TearDown() override {
        storage.reset();
        GetParam()->clear_setup();
    }
};

TEST_P(GenericStorageTest, WriteDuplicateKeyException) {
    write_in_store(*storage, "sym");

    ASSERT_TRUE(exists_in_store(*storage, "sym"));
    ASSERT_THROW({
        write_in_store(*storage, "sym");
    },  arcticdb::storage::DuplicateKeyException);

}

TEST_P(GenericStorageTest, ReadKeyNotFoundException) {
    ASSERT_FALSE(exists_in_store(*storage, "sym"));
    ASSERT_THROW({
        read_in_store(*storage, "sym");
    },  arcticdb::storage::KeyNotFoundException);

}

TEST_P(GenericStorageTest, UpdateKeyNotFoundException) {
    ASSERT_FALSE(exists_in_store(*storage, "sym"));
    ASSERT_THROW({
        update_in_store(*storage, "sym");
    },  arcticdb::storage::KeyNotFoundException);

}

TEST_P(GenericStorageTest, RemoveKeyNotFoundException) {
    ASSERT_FALSE(exists_in_store(*storage, "sym"));
    ASSERT_THROW({
        remove_in_store(*storage, {"sym"});
    },  arcticdb::storage::KeyNotFoundException);

}

INSTANTIATE_TEST_SUITE_P(
        AllStoragesCommonTests,
        GenericStorageTest,
        ::testing::Values(
                std::make_shared<LMDBStorageFactory>(),
                std::make_shared<MemoryStorageFactory>()
        )
);

#ifdef ARCTICDB_INCLUDE_ROCKSDB

INSTANTIATE_TEST_SUITE_P(
        RocksDBStoragesCommonTests,
        GenericStorageTest,
        ::testing::Values(
                std::make_shared<RocksDBStorageFactory>()
        )
);

#endif

// LMDB Storage specific tests

class LMDBStorageTestBase : public ::testing::Test {
protected:
    void SetUp() override {
        if (!fs::exists(TEST_DATABASES_PATH)) {
            fs::create_directories(TEST_DATABASES_PATH);
        }
    }

    void TearDown() override {
        if (fs::exists(TEST_DATABASES_PATH)) {
            fs::remove_all(TEST_DATABASES_PATH);
        }
    }
};

TEST_F(LMDBStorageTestBase, WriteMapFullError) {
    // Create a Storage with 32KB map size
    LMDBStorageFactory factory(32ULL * (1ULL << 10));
    auto storage = factory.create();

    arcticdb::entity::AtomKey k = arcticdb::entity::atom_key_builder().gen_id(0).build<arcticdb::entity::KeyType::VERSION>("sym");
    arcticdb::storage::KeySegmentPair kv(k);
    kv.segment().header().set_start_ts(1234);
    kv.segment().set_buffer(std::make_shared<arcticdb::Buffer>(40000));

    ASSERT_THROW({
        storage->write(std::move(kv));
    },  ::lmdb::map_full_error);

}

// S3 error handling with mock client
// Note: Exception handling is different for S3 as compared to other storages.
// S3 does not return an error if you rewrite an existing key. It overwrites it.
// S3 does not return an error if you update a key that doesn't exist. It creates it.

TEST(S3MockStorageTest, TestReadKeyNotFoundException) {
    S3MockStorageFactory factory;
    auto storage = factory.create();

    ASSERT_FALSE(exists_in_store(*storage, "sym"));
    ASSERT_THROW({
        read_in_store(*storage, "sym");
    },  arcticdb::storage::KeyNotFoundException);

}

// Check that Permission exception is thrown when Access denied or invalid access key error occurs on various calls
TEST(S3MockStorageTest, TestPermissionErrorException) {
    S3MockStorageFactory factory;
    auto storage = factory.create();

    std::string failureSymbol = s3::MockS3Client::get_failure_trigger("sym1", StorageOperation::READ, Aws::S3::S3Errors::ACCESS_DENIED);

    ASSERT_THROW({
        read_in_store(*storage, failureSymbol);
    },  StoragePermissionException);

    failureSymbol = s3::MockS3Client::get_failure_trigger("sym2", StorageOperation::DELETE, Aws::S3::S3Errors::ACCESS_DENIED);

    ASSERT_THROW({
        remove_in_store(*storage, {failureSymbol});
    },  StoragePermissionException);

    failureSymbol = s3::MockS3Client::get_failure_trigger("sym3", StorageOperation::WRITE, Aws::S3::S3Errors::INVALID_ACCESS_KEY_ID);

    ASSERT_THROW({
        update_in_store(*storage, failureSymbol);
    },  StoragePermissionException);

}

TEST(S3MockStorageTest, TestS3RetryableException) {
    S3MockStorageFactory factory;
    auto storage = factory.create();

    std::string failureSymbol = s3::MockS3Client::get_failure_trigger("sym1", StorageOperation::READ, Aws::S3::S3Errors::NETWORK_CONNECTION);

    ASSERT_THROW({
        read_in_store(*storage, failureSymbol);
    },  S3RetryableException);
}

TEST(S3MockStorageTest, TestUnexpectedS3ErrorException ) {
    S3MockStorageFactory factory;
    auto storage = factory.create();

    std::string failureSymbol = s3::MockS3Client::get_failure_trigger("sym1", StorageOperation::READ, Aws::S3::S3Errors::NETWORK_CONNECTION, false);

    ASSERT_THROW({
        read_in_store(*storage, failureSymbol);
    },  UnexpectedS3ErrorException);
}

// Azure error testing with mock client
TEST(AzureMockStorageTest, TestReadKeyNotFoundException) {
    AzureMockStorageFactory factory;
    auto storage = factory.create();

    ASSERT_FALSE(exists_in_store(*storage, "sym"));
    ASSERT_THROW({
        read_in_store(*storage, "sym");
    },  arcticdb::storage::KeyNotFoundException);

}

// Check that Permission exception is thrown when http Forbidden status code is returned
TEST(AzureMockStorageTest, TestPermissionErrorException) {
    AzureMockStorageFactory factory;
    auto storage = factory.create();
    write_in_store(*storage, "sym1");

    std::string failureSymbol = azure::MockAzureClient::get_failure_trigger("sym1", StorageOperation::WRITE,
                                                                            azure::AzureErrorCode_to_string(azure::AzureErrorCode::UnauthorizedBlobOverwrite),
                                                                            Azure::Core::Http::HttpStatusCode::Forbidden);
    ASSERT_THROW({
        update_in_store(*storage, failureSymbol);
    },  StoragePermissionException);

    failureSymbol = azure::MockAzureClient::get_failure_trigger("sym1", StorageOperation::DELETE,
                                                                azure::AzureErrorCode_to_string(azure::AzureErrorCode::UnauthorizedBlobOverwrite),
                                                                Azure::Core::Http::HttpStatusCode::Forbidden);
    ASSERT_THROW({
        remove_in_store(*storage, {failureSymbol});
    },  StoragePermissionException);

}

TEST(AzureMockStorageTest, TestUnexpectedAzureErrorException ) {
    AzureMockStorageFactory factory;
    auto storage = factory.create();

    std::string failureSymbol = azure::MockAzureClient::get_failure_trigger("sym1@#~?.&$", StorageOperation::READ,
                                                                            azure::AzureErrorCode_to_string(azure::AzureErrorCode::InvalidBlobOrBlock),
                                                                            Azure::Core::Http::HttpStatusCode::BadRequest);

    ASSERT_THROW({
        read_in_store(*storage, failureSymbol);
    },  UnexpectedAzureException);

    failureSymbol = azure::MockAzureClient::get_failure_trigger("sym1", StorageOperation::READ,
                                                                "",
                                                                Azure::Core::Http::HttpStatusCode::InternalServerError);

    ASSERT_THROW({
        read_in_store(*storage, failureSymbol);
    },  UnexpectedAzureException);
}

TEST(MongoMockStorageTest, TestReadKeyNotFoundException) {
    MongoMockStorageFactory factory;
    auto storage = factory.create();

    ASSERT_FALSE(exists_in_store(*storage, "sym"));
    ASSERT_THROW({
                     read_in_store(*storage, "sym");
                 },  arcticdb::storage::KeyNotFoundException);

}

// Check that Permission exception is thrown when Access denied or invalid access key error occurs on various calls
TEST(MongoMockStorageTest, TestPermissionErrorException) {
    MongoMockStorageFactory factory;
    auto storage = factory.create();

    std::string failureSymbol = mongo::MockMongoClient::get_failure_trigger("sym1", StorageOperation::READ, mongo::MongoError::UnAuthorized);

    ASSERT_THROW({
                     read_in_store(*storage, failureSymbol);
                 },  KeyNotFoundException);  // should throw permission error after exception normalization

    failureSymbol = mongo::MockMongoClient::get_failure_trigger("sym2", StorageOperation::DELETE, mongo::MongoError::UserNotFound);
    write_in_store(*storage, failureSymbol);
    ASSERT_THROW({
                     remove_in_store(*storage, {failureSymbol});
                 },  std::runtime_error);  // should throw permission error after exception normalization

    failureSymbol = mongo::MockMongoClient::get_failure_trigger("sym3", StorageOperation::WRITE, mongo::MongoError::UnAuthorized);

    ASSERT_THROW({
                     update_in_store(*storage, failureSymbol);
                 },  std::runtime_error); // should throw permission error after exception normalization

}

TEST(MongoMockStorageTest, MongoUnexpectedException) {
    MongoMockStorageFactory factory;
    auto storage = factory.create();

    std::string failureSymbol = mongo::MockMongoClient::get_failure_trigger("sym1", StorageOperation::READ, mongo::MongoError::HostNotFound);

    ASSERT_THROW({
                     read_in_store(*storage, failureSymbol);
                 },  std::runtime_error); // should throw MongoUnexpectedException after exception normalization
}

TEST(MongoMockStorageTest, test_remove) {
    MongoMockStorageFactory factory;
    auto store = factory.create();
    for (int i = 0; i < 5; ++i) {
        write_in_store(*store, fmt::format("symbol_{}", i));
    }

    // Remove 0 and 1
    remove_in_store(*store, {"symbol_0", "symbol_1"});
    auto remaining = std::set<std::string>{"symbol_2", "symbol_3", "symbol_4"};
    ASSERT_EQ(list_in_store(*store), remaining);

    // Attempt to remove 2, 3 and 4, should succeed till 3.
    ASSERT_THROW(
            remove_in_store(*store, {"symbol_2", "symbol_3", mongo::MockMongoClient::get_failure_trigger("symbol_4", StorageOperation::DELETE, mongo::MongoError::HostUnreachable)}),
            std::runtime_error);    // Should throw MongoUnexpectedException after exception normalization
    remaining = std::set<std::string>{"symbol_4"};
    ASSERT_EQ(list_in_store(*store), remaining);
}

TEST(MongoMockStorageTest, test_list) {
    MongoMockStorageFactory factory;
    auto store = factory.create();
    auto symbols = std::set<std::string>();
    for (int i = 10; i < 25; ++i) {
        auto symbol = fmt::format("symbol_{}", i);
        write_in_store(*store, symbol);
        symbols.emplace(symbol);
    }
    ASSERT_EQ(list_in_store(*store), symbols);

    write_in_store(*store, mongo::MockMongoClient::get_failure_trigger("symbol_99", StorageOperation::LIST, mongo::MongoError::HostNotFound));

    ASSERT_THROW(list_in_store(*store), std::runtime_error); // should throw MongoUnexpectedException after exception normalization
}

TEST(MongoMockStorageTest, drop_collection) {
    MongoMockStorageFactory factory;
    auto store = factory.create();
    auto symbols = std::set<std::string>();
    for (int i = 10; i < 25; ++i) {
        auto symbol = fmt::format("symbol_{}", i);
        write_in_store(*store, symbol);
        symbols.emplace(symbol);
    }
    ASSERT_EQ(list_in_store(*store), symbols);

    store->fast_delete();   // calls drop_collection
    ASSERT_EQ(list_in_store(*store), std::set<std::string>{});
}
