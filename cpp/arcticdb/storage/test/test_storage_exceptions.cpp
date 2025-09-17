/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/storage/mock/lmdb_mock_client.hpp>
#include <arcticdb/storage/memory/memory_storage.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/mock/s3_mock_client.hpp>
#include <arcticdb/storage/azure/azure_storage.hpp>
#include <arcticdb/storage/mock/azure_mock_client.hpp>
#include <arcticdb/storage/mongo/mongo_storage.hpp>
#include <arcticdb/storage/mock/mongo_mock_client.hpp>
#include <arcticdb/storage/test/common.hpp>

#include <filesystem>
#include <memory>

using namespace arcticdb;
using namespace storage;

inline const fs::path TEST_DATABASES_PATH = "./test_databases";

class StorageFactory {
  public:
    virtual ~StorageFactory() = default;
    virtual std::unique_ptr<arcticdb::storage::Storage> create() = 0;

    virtual void setup() {}
    virtual void clear_setup() {}
};

class LMDBStorageFactory : public StorageFactory {
  private:
    uint64_t map_size;
    bool use_mock;
    fs::path db_path;
    std::string lib_name;

  public:
    explicit LMDBStorageFactory(uint64_t map_size, bool use_mock = false) :
        map_size(map_size),
        use_mock(use_mock),
        db_path(TEST_DATABASES_PATH / "test_lmdb"),
        lib_name("test_lib") {}

    explicit LMDBStorageFactory(bool use_mock = false) :
        LMDBStorageFactory(128ULL * (1ULL << 20) /* 128MB */, use_mock) {}

    std::unique_ptr<arcticdb::storage::Storage> create() override {
        arcticdb::proto::lmdb_storage::Config cfg;

        cfg.set_path((db_path).generic_string());
        cfg.set_map_size(map_size);
        cfg.set_recreate_if_exists(true);
        cfg.set_use_mock_storage_for_testing(use_mock);

        arcticdb::storage::LibraryPath library_path(lib_name, '/');

        return std::make_unique<arcticdb::storage::lmdb::LmdbStorage>(
                library_path, arcticdb::storage::OpenMode::DELETE, cfg
        );
    }

    fs::path get_lib_path() const { return db_path / lib_name; }

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

        return std::make_unique<arcticdb::storage::memory::MemoryStorage>(
                library_path, arcticdb::storage::OpenMode::DELETE, cfg
        );
    }
};

class S3MockStorageFactory : public StorageFactory {
  public:
    std::unique_ptr<arcticdb::storage::Storage> create() override {
        arcticdb::proto::s3_storage::Config cfg;
        cfg.set_use_mock_storage_for_testing(true);
        arcticdb::storage::LibraryPath library_path("lib", '.');

        return std::make_unique<arcticdb::storage::s3::S3Storage>(
                library_path, arcticdb::storage::OpenMode::DELETE, arcticdb::storage::s3::S3Settings(cfg)
        );
    }
};

class AzureMockStorageFactory : public StorageFactory {
  public:
    std::unique_ptr<arcticdb::storage::Storage> create() override {
        arcticdb::proto::azure_storage::Config cfg;
        cfg.set_use_mock_storage_for_testing(true);
        arcticdb::storage::LibraryPath library_path("lib", '/');

        return std::make_unique<arcticdb::storage::azure::AzureStorage>(
                library_path, arcticdb::storage::OpenMode::DELETE, cfg
        );
    }
};

class MongoMockStorageFactory : public StorageFactory {
  public:
    std::unique_ptr<arcticdb::storage::Storage> create() override {
        arcticdb::proto::mongo_storage::Config cfg;
        cfg.set_use_mock_storage_for_testing(true);
        arcticdb::storage::LibraryPath library_path("lib", '/');

        return std::make_unique<arcticdb::storage::mongo::MongoStorage>(
                library_path, arcticdb::storage::OpenMode::DELETE, cfg
        );
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
    ASSERT_THROW({ write_in_store(*storage, "sym"); }, arcticdb::storage::DuplicateKeyException);
}

TEST_P(GenericStorageTest, ReadKeyNotFoundException) {
    ASSERT_FALSE(exists_in_store(*storage, "sym"));
    ASSERT_THROW({ read_in_store(*storage, "sym"); }, arcticdb::storage::KeyNotFoundException);
}

TEST_P(GenericStorageTest, UpdateKeyNotFoundException) {
    ASSERT_FALSE(exists_in_store(*storage, "sym"));
    ASSERT_THROW({ update_in_store(*storage, "sym"); }, arcticdb::storage::KeyNotFoundException);
}

TEST_P(GenericStorageTest, RemoveKeyNotFoundException) {
    ASSERT_FALSE(exists_in_store(*storage, "sym"));
    ASSERT_THROW({ remove_in_store(*storage, {"sym"}); }, arcticdb::storage::KeyNotFoundException);
}

INSTANTIATE_TEST_SUITE_P(
        AllStoragesCommonTests, GenericStorageTest,
        ::testing::Values(
                std::make_shared<LMDBStorageFactory>(), std::make_shared<LMDBStorageFactory>(true),
                std::make_shared<MemoryStorageFactory>()
        )
);

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
    // Create a Storage with 64KB map size
    LMDBStorageFactory factory(64ULL * (1ULL << 10), false);
    auto storage = factory.create();

    arcticdb::entity::AtomKey k =
            arcticdb::entity::atom_key_builder().gen_id(0).build<arcticdb::entity::KeyType::VERSION>("sym");

    auto segment_in_memory = get_test_frame<arcticdb::stream::TimeseriesIndex>("symbol", {}, 40000, 0).segment_;
    auto codec_opts = proto::encoding::VariantCodec();
    auto segment = encode_dispatch(std::move(segment_in_memory), codec_opts, arcticdb::EncodingVersion::V2);
    arcticdb::storage::KeySegmentPair kv(k, std::move(segment));

    ASSERT_THROW({ storage->write(std::move(kv)); }, LMDBMapFullException);
}

TEST_F(LMDBStorageTestBase, MockMapFullError) {
    LMDBStorageFactory factory(true);
    auto storage = factory.create();

    std::string failureSymbol =
            storage::lmdb::MockLmdbClient::get_failure_trigger("sym", StorageOperation::WRITE, MDB_MAP_FULL);

    ASSERT_THROW({ write_in_store(*storage, failureSymbol); }, LMDBMapFullException);

    write_in_store(*storage, "sym1");
}

TEST_F(LMDBStorageTestBase, MockUnexpectedLMDBErrorException) {
    LMDBStorageFactory factory(true);
    auto storage = factory.create();

    write_in_store(*storage, "sym1");
    write_in_store(*storage, "sym2");

    std::set<std::string> symbols = {"sym1", "sym2"};
    ASSERT_EQ(list_in_store(*storage), symbols);

    std::string failureSymbol =
            storage::lmdb::MockLmdbClient::get_failure_trigger("sym3", StorageOperation::WRITE, MDB_INVALID);
    ASSERT_THROW({ write_in_store(*storage, failureSymbol); }, UnexpectedLMDBErrorException);

    failureSymbol = storage::lmdb::MockLmdbClient::get_failure_trigger("symx", StorageOperation::READ, MDB_CORRUPTED);
    ASSERT_THROW({ read_in_store(*storage, failureSymbol); }, UnexpectedLMDBErrorException);

    failureSymbol =
            storage::lmdb::MockLmdbClient::get_failure_trigger("sym1", StorageOperation::EXISTS, MDB_PAGE_NOTFOUND);
    ASSERT_THROW({ exists_in_store(*storage, failureSymbol); }, UnexpectedLMDBErrorException);

    failureSymbol = storage::lmdb::MockLmdbClient::get_failure_trigger("sym1", StorageOperation::DELETE, MDB_PANIC);
    ASSERT_THROW({ remove_in_store(*storage, {failureSymbol}); }, UnexpectedLMDBErrorException);

    ASSERT_EQ(list_in_store(*storage), symbols);

    failureSymbol = storage::lmdb::MockLmdbClient::get_failure_trigger("sym3", StorageOperation::LIST, MDB_CURSOR_FULL);
    write_in_store(*storage, failureSymbol);

    ASSERT_THROW({ list_in_store(*storage); }, UnexpectedLMDBErrorException);

    remove_in_store(*storage, {failureSymbol});
    ASSERT_EQ(list_in_store(*storage), symbols);
}

TEST_F(LMDBStorageTestBase, RemoveLibPath) {
    LMDBStorageFactory factory;
    auto storage = factory.create();
    const auto path = factory.get_lib_path();

    storage->cleanup();
    ASSERT_FALSE(fs::exists(path));
    // Once we call close, any other operations should throw UnexpectedLMDBErrorException as lmdb env is closed
    ASSERT_THROW({ write_in_store(*storage, "sym1"); }, UnexpectedLMDBErrorException);

    ASSERT_THROW({ update_in_store(*storage, "sym1"); }, UnexpectedLMDBErrorException);

    ASSERT_THROW({ remove_in_store(*storage, {"sym1"}); }, UnexpectedLMDBErrorException);

    ASSERT_THROW({ read_in_store(*storage, "sym1"); }, UnexpectedLMDBErrorException);

    ASSERT_THROW({ exists_in_store(*storage, "sym1"); }, UnexpectedLMDBErrorException);

    ASSERT_THROW({ list_in_store(*storage); }, UnexpectedLMDBErrorException);
}

// S3 error handling with mock client
// Note: Exception handling is different for S3 as compared to other storages.
// S3 does not return an error if you rewrite an existing key. It overwrites it.
// S3 does not return an error if you update a key that doesn't exist. It creates it.

TEST(S3MockStorageTest, TestReadKeyNotFoundException) {
    S3MockStorageFactory factory;
    auto storage = factory.create();

    ASSERT_FALSE(exists_in_store(*storage, "sym"));
    ASSERT_THROW({ read_in_store(*storage, "sym"); }, arcticdb::storage::KeyNotFoundException);
}

// Check that Permission exception is thrown when Access denied or invalid access key error occurs on various calls
TEST(S3MockStorageTest, TestPermissionErrorException) {
    S3MockStorageFactory factory;
    auto storage = factory.create();

    std::string failureSymbol =
            s3::MockS3Client::get_failure_trigger("sym1", StorageOperation::READ, Aws::S3::S3Errors::ACCESS_DENIED);

    ASSERT_THROW({ read_in_store(*storage, failureSymbol); }, PermissionException);

    failureSymbol =
            s3::MockS3Client::get_failure_trigger("sym2", StorageOperation::DELETE, Aws::S3::S3Errors::ACCESS_DENIED);

    ASSERT_THROW({ remove_in_store(*storage, {failureSymbol}); }, PermissionException);

    failureSymbol = s3::MockS3Client::get_failure_trigger(
            "sym3", StorageOperation::WRITE, Aws::S3::S3Errors::INVALID_ACCESS_KEY_ID
    );

    ASSERT_THROW({ update_in_store(*storage, failureSymbol); }, PermissionException);
}

TEST(S3MockStorageTest, TestS3RetryableException) {
    S3MockStorageFactory factory;
    auto storage = factory.create();

    std::string failureSymbol = s3::MockS3Client::get_failure_trigger(
            "sym1", StorageOperation::READ, Aws::S3::S3Errors::NETWORK_CONNECTION
    );

    ASSERT_THROW({ read_in_store(*storage, failureSymbol); }, S3RetryableException);
}

TEST(S3MockStorageTest, TestUnexpectedS3ErrorException) {
    S3MockStorageFactory factory;
    auto storage = factory.create();

    std::string failureSymbol = s3::MockS3Client::get_failure_trigger(
            "sym{1}", StorageOperation::READ, Aws::S3::S3Errors::NETWORK_CONNECTION, false
    );

    ASSERT_THROW({ read_in_store(*storage, failureSymbol); }, UnexpectedS3ErrorException);
}

// Azure error testing with mock client
TEST(AzureMockStorageTest, TestReadKeyNotFoundException) {
    AzureMockStorageFactory factory;
    auto storage = factory.create();

    ASSERT_FALSE(exists_in_store(*storage, "sym"));
    ASSERT_THROW({ read_in_store(*storage, "sym"); }, arcticdb::storage::KeyNotFoundException);
}

// Check that Permission exception is thrown when http Forbidden status code is returned
TEST(AzureMockStorageTest, TestPermissionErrorException) {
    AzureMockStorageFactory factory;
    auto storage = factory.create();
    write_in_store(*storage, "sym1");

    std::string failureSymbol = azure::MockAzureClient::get_failure_trigger(
            "sym1",
            StorageOperation::WRITE,
            azure::AzureErrorCode_to_string(azure::AzureErrorCode::UnauthorizedBlobOverwrite),
            Azure::Core::Http::HttpStatusCode::Forbidden
    );
    ASSERT_THROW({ update_in_store(*storage, failureSymbol); }, PermissionException);

    failureSymbol = azure::MockAzureClient::get_failure_trigger(
            "sym1",
            StorageOperation::DELETE,
            azure::AzureErrorCode_to_string(azure::AzureErrorCode::UnauthorizedBlobOverwrite),
            Azure::Core::Http::HttpStatusCode::Forbidden
    );
    ASSERT_THROW({ remove_in_store(*storage, {failureSymbol}); }, PermissionException);
}

TEST(AzureMockStorageTest, TestUnexpectedAzureErrorException) {
    AzureMockStorageFactory factory;
    auto storage = factory.create();

    std::string failureSymbol = azure::MockAzureClient::get_failure_trigger(
            "sym1@#~?.&$",
            StorageOperation::READ,
            azure::AzureErrorCode_to_string(azure::AzureErrorCode::InvalidBlobOrBlock),
            Azure::Core::Http::HttpStatusCode::BadRequest
    );

    ASSERT_THROW({ read_in_store(*storage, failureSymbol); }, UnexpectedAzureException);

    failureSymbol = azure::MockAzureClient::get_failure_trigger(
            "sym{1}", StorageOperation::READ, "", Azure::Core::Http::HttpStatusCode::InternalServerError
    );

    ASSERT_THROW({ read_in_store(*storage, failureSymbol); }, UnexpectedAzureException);
}

TEST(MongoMockStorageTest, TestReadKeyNotFoundException) {
    MongoMockStorageFactory factory;
    auto storage = factory.create();

    ASSERT_FALSE(exists_in_store(*storage, "sym"));
    ASSERT_THROW({ read_in_store(*storage, "sym"); }, arcticdb::storage::KeyNotFoundException);
}

// Check that Permission exception is thrown when Access denied or invalid access key error occurs on various calls
TEST(MongoMockStorageTest, TestPermissionErrorException) {
    MongoMockStorageFactory factory;
    auto storage = factory.create();

    std::string failureSymbol = mongo::MockMongoClient::get_failure_trigger(
            "sym1", StorageOperation::READ, mongo::MongoError::UnAuthorized
    );

    ASSERT_THROW({ read_in_store(*storage, failureSymbol); }, PermissionException);

    failureSymbol = mongo::MockMongoClient::get_failure_trigger(
            "sym2", StorageOperation::DELETE, mongo::MongoError::AuthenticationFailed
    );
    write_in_store(*storage, failureSymbol);
    ASSERT_THROW({ remove_in_store(*storage, {failureSymbol}); }, PermissionException);

    failureSymbol = mongo::MockMongoClient::get_failure_trigger(
            "sym3", StorageOperation::WRITE, mongo::MongoError::UnAuthorized
    );

    ASSERT_THROW({ update_in_store(*storage, failureSymbol); }, PermissionException);
}

TEST(MongoMockStorageTest, MongoUnexpectedException) {
    MongoMockStorageFactory factory;
    auto storage = factory.create();

    std::string failureSymbol = mongo::MockMongoClient::get_failure_trigger(
            "sym1", StorageOperation::READ, mongo::MongoError::HostNotFound
    );

    ASSERT_THROW({ read_in_store(*storage, failureSymbol); }, UnexpectedMongoException);
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
            remove_in_store(
                    *store,
                    {"symbol_2",
                     "symbol_3",
                     mongo::MockMongoClient::get_failure_trigger(
                             "symbol_4", StorageOperation::DELETE, mongo::MongoError::HostUnreachable
                     )}
            ),
            UnexpectedMongoException
    );
    remaining = std::set<std::string>{"symbol_4"};
    ASSERT_EQ(list_in_store(*store), remaining);

    ASSERT_THROW(
            remove_in_store(*store, {"symbol_non_existent"}),
            KeyNotFoundException
    ); // removing non-existent keys should throw KeyNotFoundException in Mongo storage
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

    write_in_store(
            *store,
            mongo::MockMongoClient::get_failure_trigger(
                    "symbol_{99}", StorageOperation::LIST, mongo::MongoError::HostNotFound
            )
    );

    ASSERT_THROW(list_in_store(*store), UnexpectedMongoException);
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

    store->fast_delete(); // calls drop_collection
    ASSERT_EQ(list_in_store(*store), std::set<std::string>{});
}
