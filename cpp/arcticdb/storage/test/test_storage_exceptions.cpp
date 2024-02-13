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
#include <arcticdb/util/buffer.hpp>

#include <filesystem>
#include <memory>

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

        return std::make_unique<arcticdb::storage::lmdb::LmdbStorage>(library_path, arcticdb::storage::OpenMode::WRITE, cfg);
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
    std::unique_ptr<arcticdb::storage::Storage> create() override {
        arcticdb::proto::memory_storage::Config cfg;
        arcticdb::storage::LibraryPath library_path{"a", "b"};

        return std::make_unique<arcticdb::storage::memory::MemoryStorage>(library_path, arcticdb::storage::OpenMode::WRITE, cfg);
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
    arcticdb::entity::AtomKey k = arcticdb::entity::atom_key_builder().gen_id(0).build<arcticdb::entity::KeyType::VERSION>("sym");

    arcticdb::storage::KeySegmentPair kv(k);
    kv.segment().set_buffer(std::make_shared<arcticdb::Buffer>());

    storage->write(std::move(kv));

    ASSERT_TRUE(storage->key_exists(k));

    arcticdb::storage::KeySegmentPair kv1(k);
    kv1.segment().set_buffer(std::make_shared<arcticdb::Buffer>());

    ASSERT_THROW({
        storage->write(std::move(kv1));
    },  arcticdb::storage::DuplicateKeyException);

}

TEST_P(GenericStorageTest, ReadKeyNotFoundException) {
    arcticdb::entity::AtomKey k = arcticdb::entity::atom_key_builder().gen_id(0).build<arcticdb::entity::KeyType::VERSION>("sym");

    ASSERT_TRUE(!storage->key_exists(k));
    ASSERT_THROW({
        storage->read(k, arcticdb::storage::ReadKeyOpts{});
    },  arcticdb::storage::KeyNotFoundException);

}

TEST_P(GenericStorageTest, UpdateKeyNotFoundException) {
    arcticdb::entity::AtomKey k = arcticdb::entity::atom_key_builder().gen_id(0).build<arcticdb::entity::KeyType::VERSION>("sym");

    arcticdb::storage::KeySegmentPair kv(k);
    kv.segment().header().set_start_ts(1234);
    kv.segment().set_buffer(std::make_shared<arcticdb::Buffer>());

    ASSERT_TRUE(!storage->key_exists(k));
    ASSERT_THROW({
        storage->update(std::move(kv), arcticdb::storage::UpdateOpts{});
    },  arcticdb::storage::KeyNotFoundException);

}

TEST_P(GenericStorageTest, RemoveKeyNotFoundException) {
    arcticdb::entity::AtomKey k = arcticdb::entity::atom_key_builder().gen_id(0).build<arcticdb::entity::KeyType::VERSION>("sym");

    ASSERT_TRUE(!storage->key_exists(k));
    ASSERT_THROW({
        storage->remove(k, arcticdb::storage::RemoveOpts{});
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
