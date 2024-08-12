/* Copyright 2024 Man Group Operations Limited
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
#include <arcticdb/storage/azure/azure_storage.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/mongo/mongo_storage.hpp>

#include <filesystem>
#include <stdexcept>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/util/random.h>
#include <arcticdb/stream/row_builder.hpp>
#include <arcticdb/storage/test/common.hpp>

namespace ac = arcticdb;
namespace as = arcticdb::storage;
namespace ae = arcticdb::entity;

namespace test_storage_operations {

class BackendGenerator {
 public:
  BackendGenerator(std::string backend) : backend_(std::move(backend)) {}

  [[nodiscard]] std::unique_ptr<as::Storage> new_backend() const {
    as::LibraryPath library_path{"a", "b"};
    if (backend_ == "lmdb") {
      if (!fs::exists(TEST_DATABASES_PATH)) {
        fs::create_directories(TEST_DATABASES_PATH);
      }
      arcticdb::proto::lmdb_storage::Config cfg;
      fs::path db_name = "test_lmdb";
      cfg.set_path((TEST_DATABASES_PATH / db_name).generic_string());
      cfg.set_map_size(128ULL * (1ULL << 20) );
      cfg.set_recreate_if_exists(true);

      return std::make_unique<as::lmdb::LmdbStorage>(library_path, as::OpenMode::WRITE, cfg);
    } else if (backend_ == "mem") {
      arcticdb::proto::memory_storage::Config cfg;
      return std::make_unique<as::memory::MemoryStorage>(library_path, as::OpenMode::WRITE, cfg);
    } else if (backend_ == "azure") {
      arcticdb::proto::azure_storage::Config cfg;
      cfg.set_use_mock_storage_for_testing(true);
      return std::make_unique<as::azure::AzureStorage>(library_path, as::OpenMode::WRITE, cfg);
    } else if (backend_ == "s3") {
      arcticdb::proto::s3_storage::Config cfg;
      cfg.set_use_mock_storage_for_testing(true);
      return std::make_unique<as::s3::S3Storage>(library_path, as::OpenMode::WRITE, cfg);
    } else if (backend_ == "mongo") {
      arcticdb::proto::mongo_storage::Config cfg;
      cfg.set_use_mock_storage_for_testing(true);
      return std::make_unique<as::mongo::MongoStorage>(library_path, as::OpenMode::WRITE, cfg);
    } else {
      throw std::runtime_error(fmt::format("Unknown backend generator type {}.", backend_));
    }
  }

  void delete_any_test_databases() const {
    if (fs::exists(TEST_DATABASES_PATH)) {
      fs::remove_all(TEST_DATABASES_PATH);
    }
  }

  [[nodiscard]] std::string get_name() const {return backend_;}
 private:
  const std::string backend_;
  inline static const fs::path TEST_DATABASES_PATH = "./test_databases";
};

class StorageTestSuite : public testing::TestWithParam<BackendGenerator> {
  void SetUp() override {
    GetParam().delete_any_test_databases();
  }

  void TearDown() override {
    GetParam().delete_any_test_databases();
  }
};

TEST_P(StorageTestSuite, test_list) {
  auto store = GetParam().new_backend();
  auto symbols = std::set<std::string>();
  for (int i = 10; i < 25; ++i) {
    auto symbol = fmt::format("symbol_{}", i);
    write_in_store(*store, symbol);
    symbols.emplace(symbol);
  }

  ASSERT_EQ(list_in_store(*store), symbols);
}

TEST_P(StorageTestSuite, test_exists_matching) {
  auto store = GetParam().new_backend();
  auto s = fmt::format("symbol_1");
  write_in_store(*store, s);
  auto res = store->key_exists(ae::KeyType::TABLE_DATA, [](ae::VariantKey&& k) {
    return variant_key_id(k) == ac::StreamId{"symbol_1"};
  });
  ASSERT_TRUE(res);
}

TEST_P(StorageTestSuite, test_exists_not_matching) {
  auto store = GetParam().new_backend();
  auto s = fmt::format("symbol_1");
  write_in_store(*store, s);
  auto res = store->key_exists(ae::KeyType::TABLE_DATA, [](ae::VariantKey&& k) {
    return variant_key_id(k) == ac::StreamId{"symbol_2"};
  });
  ASSERT_FALSE(res);
}

TEST_P(StorageTestSuite, test_exists_checks_everything) {
  auto store = GetParam().new_backend();
  for (size_t i = 0; i < 10; i++) {
    write_in_store(*store, fmt::format("symbol_{}", i));
  }
  size_t visited = 0;
  auto res = store->key_exists(ae::KeyType::TABLE_DATA, [&visited](ae::VariantKey&& k) {
    visited++;
    return variant_key_id(k) == ac::StreamId{"symbol_10"};
  });
  ASSERT_FALSE(res);
  ASSERT_EQ(visited, 10);
}

using namespace std::string_literals;

std::vector<BackendGenerator> get_backend_generators() {
  return {
      "lmdb"s,
      "mem"s,
      "mongo"s,
      "azure"s,
      "s3"s
  };
}

INSTANTIATE_TEST_SUITE_P(TestStorageOperations,
                         StorageTestSuite,
                         testing::ValuesIn(get_backend_generators()),
                         [](const testing::TestParamInfo<StorageTestSuite::ParamType>& info) { return info.param.get_name(); });

}
