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
#include <arcticdb/util/buffer.hpp>

#include <filesystem>
#include <memory>

namespace ac = arcticdb;
namespace acs = arcticdb::storage;

inline const fs::path TEST_DATABASES_PATH = "./test_databases";

class StorageFactory {
public:
    virtual ~StorageFactory() = default;
    virtual std::unique_ptr<acs::Storage> Create() = 0;
};

class LMDBStorageFactory : public StorageFactory {
private:
    uint64_t map_size;

public:
    LMDBStorageFactory() : map_size(128ULL * (1ULL << 20)) { }

    explicit LMDBStorageFactory(uint64_t map_size) : map_size(map_size) { }

    std::unique_ptr<acs::Storage> Create() override {
        arcticdb::proto::lmdb_storage::Config cfg;

        fs::path db_name = "test_lmdb";
        cfg.set_path((TEST_DATABASES_PATH / db_name).generic_string());
        cfg.set_map_size(map_size);
        cfg.set_recreate_if_exists(true);
        cfg.set_recreate_if_exists(true);

        acs::LibraryPath library_path{"a", "b"};

        return std::make_unique<acs::lmdb::LmdbStorage>(library_path, acs::OpenMode::WRITE, cfg);
    }
};

// Generic tests that run with all types of storages

class GenericStorageTest : public ::testing::TestWithParam<std::shared_ptr<StorageFactory>> {
protected:
    std::unique_ptr<acs::Storage> storage;

    void SetUp() override {
        if (!fs::exists(TEST_DATABASES_PATH)) {
            fs::create_directories(TEST_DATABASES_PATH);
        }
        storage = GetParam()->Create();
    }

    void TearDown() override {
        if (fs::exists(TEST_DATABASES_PATH)) {
            fs::remove_all(TEST_DATABASES_PATH);
        }

        storage = nullptr;
    }
};

TEST_P(GenericStorageTest, WriteDuplicateKeyException) {
    ac::entity::AtomKey k = ac::entity::atom_key_builder().gen_id(0).build<ac::entity::KeyType::VERSION>("sym");

    acs::KeySegmentPair kv(k);
    kv.segment().set_buffer(std::make_shared<ac::Buffer>());

    storage->write(std::move(kv));

    ASSERT_TRUE(storage->key_exists(k));

    acs::KeySegmentPair kv1(k);
    kv1.segment().set_buffer(std::make_shared<ac::Buffer>());


    ASSERT_THROW({
        storage->write(std::move(kv1));
    },  acs::DuplicateKeyException);

}

TEST_P(GenericStorageTest, ReadKeyNotFoundException) {
    ac::entity::AtomKey k = ac::entity::atom_key_builder().gen_id(0).build<ac::entity::KeyType::VERSION>("sym");

    ASSERT_TRUE(!storage->key_exists(k));
    ASSERT_THROW({
                    storage->read(k, acs::ReadKeyOpts{});
                 }, acs::KeyNotFoundException);

}

TEST_P(GenericStorageTest, UpdateKeyNotFoundException) {
    ac::entity::AtomKey k = ac::entity::atom_key_builder().gen_id(0).build<ac::entity::KeyType::VERSION>("sym");

    acs::KeySegmentPair kv(k);
    kv.segment().header().set_start_ts(1234);
    kv.segment().set_buffer(std::make_shared<ac::Buffer>());

    ASSERT_TRUE(!storage->key_exists(k));
    ASSERT_THROW({
                    storage->update(std::move(kv), acs::UpdateOpts{});
                 }, acs::KeyNotFoundException);

}

TEST_P(GenericStorageTest, RemoveKeyNotFoundException) {
    ac::entity::AtomKey k = ac::entity::atom_key_builder().gen_id(0).build<ac::entity::KeyType::VERSION>("sym");

    ASSERT_TRUE(!storage->key_exists(k));
    ASSERT_THROW({
                     storage->remove(k, acs::RemoveOpts{});
                 }, acs::KeyNotFoundException);

}

INSTANTIATE_TEST_SUITE_P(
        AllStoragesCommonTests,
        GenericStorageTest,
        ::testing::Values(std::make_shared<LMDBStorageFactory>())
);

// LMDB Storage specific tests

class LMDBStorageTestBase : public ::testing::TestWithParam<std::shared_ptr<LMDBStorageFactory>> {
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
    LMDBStorageFactory factory(32ULL * (1ULL << 10));
    auto storage = std::unique_ptr<acs::lmdb::LmdbStorage>(dynamic_cast<acs::lmdb::LmdbStorage*>(factory.Create().release()));

    ac::entity::AtomKey k = ac::entity::atom_key_builder().gen_id(0).build<ac::entity::KeyType::VERSION>("sym");
    acs::KeySegmentPair kv(k);
    kv.segment().header().set_start_ts(1234);
    kv.segment().set_buffer(std::make_shared<ac::Buffer>(40000));

    ASSERT_THROW({
                    storage->write(std::move(kv));
                 }, ::lmdb::map_full_error);

}
