/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/library_index.hpp>
#include <arcticdb/storage/config_resolvers.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/util/test/config_common.hpp>
#include "folly/Range.h"

#include <unordered_map>
#include <vector>
#include <type_traits>

namespace ac = arcticdb;
namespace as = arcticdb::storage;
namespace asl = arcticdb::storage::lmdb;

TEST(TestStorageFactory, LmdbLookup) {
    namespace pbs = arcticdb::proto::storage;
    as::EnvironmentName environment_name{"research"};
    as::StorageName storage_name("lmdb_local");
    as::LibraryPath library_path{"a", "b"};

    auto env_config = arcticdb::get_test_environment_config(library_path, storage_name, environment_name);
    std::shared_ptr<as::ConfigResolver> config_resolver = as::create_in_memory_resolver(env_config);

    ASSERT_EQ(config_resolver->resolver_type(), "in_mem");

    auto library_conf = config_resolver->get_libraries(as::EnvironmentName{"prod"});
    ASSERT_TRUE(library_conf.empty());
    library_conf = config_resolver->get_libraries(as::EnvironmentName{"research"});
    ASSERT_FALSE(library_conf.empty());
    auto& library = library_conf[0];
    ASSERT_EQ(library.first.to_delim_path(), "a.b");
    ASSERT_EQ(library.second.storage_ids(0), "lmdb_local");

    auto storages = config_resolver->get_storages(environment_name);
    auto& storage = storages[0];
    ASSERT_EQ(storage.first.value, "lmdb_local");
    arcticdb::proto::lmdb_storage::Config config;
    storage.second.config().UnpackTo(&config);
    ASSERT_EQ(config.path(), "./"); //bit non-standard
}

TEST(TestStorageFactory, LibraryIndex) {
    as::EnvironmentName environment_name{"research"};
    as::StorageName storage_name("lmdb_local");
    as::LibraryPath library_path{"a", "b"};

    auto env_config = arcticdb::get_test_environment_config(library_path, storage_name, environment_name);
    std::shared_ptr<as::ConfigResolver> config_resolver = as::create_in_memory_resolver(env_config);

    ASSERT_EQ(config_resolver->resolver_type(), "in_mem");
    as::LibraryIndex library_index{environment_name, config_resolver};

    ac::storage::LibraryPath l{"a", "b"};
    std::vector<ac::storage::LibraryPath> expected{l};
    ASSERT_EQ(expected, library_index.list_libraries("a"));
    as::UserAuth au{"abc"};
    auto lib = library_index.get_library(l, as::OpenMode::WRITE, au);
    ASSERT_EQ(l, lib->library_path());
    ASSERT_EQ(as::OpenMode::WRITE, lib->open_mode());
}

