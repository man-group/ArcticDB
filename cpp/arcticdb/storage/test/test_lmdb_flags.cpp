/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/util/configs_map.hpp>

#include <filesystem>

namespace {

namespace ac = arcticdb;
namespace as = arcticdb::storage;
using LmdbStorage = as::lmdb::LmdbStorage;

// MDB_WRITEMAP can only be given to mdb_env_open, so it is the interesting case for this hook
constexpr int64_t WRITEMAP_NOSYNC = MDB_WRITEMAP | MDB_NOSYNC;

TEST(LmdbFlags, DefaultsToNoExtraFlags) { ASSERT_EQ(as::lmdb::lmdb_extra_env_flags(), 0U); }

TEST(LmdbFlags, ExtraFlagsComeFromConfigsMap) {
    ac::ScopedConfig extra("LMDBStorage.ExtraFlags", WRITEMAP_NOSYNC);
    ASSERT_EQ(as::lmdb::lmdb_extra_env_flags(), static_cast<unsigned int>(WRITEMAP_NOSYNC));
}

TEST(LmdbFlags, StorageOpensAndWritesWithExtraFlags) {
    ac::ScopedConfig extra("LMDBStorage.ExtraFlags", WRITEMAP_NOSYNC);
    const auto db_path = std::filesystem::temp_directory_path() / "arcticdb_test_lmdb_flags";
    std::filesystem::remove_all(db_path);
    std::filesystem::create_directories(db_path);

    LmdbStorage::Config cfg;
    cfg.set_path(db_path.generic_string());
    cfg.set_map_size(128ULL * (1ULL << 20));
    cfg.set_recreate_if_exists(true);
    as::LibraryPath library_path{"a", "b"};
    {
        // Scoped so the env is closed before remove_all: Windows refuses to delete open files
        LmdbStorage lmdb_storage(library_path, as::OpenMode::WRITE, cfg);

        ac::entity::AtomKey k =
                ac::entity::atom_key_builder().gen_id(1).build<ac::entity::KeyType::TABLE_DATA>(ac::NumericId{999});
        auto segment_in_memory = ac::get_test_frame<ac::stream::TimeseriesIndex>("symbol", {}, 10, 0).segment_;
        auto codec_opts = ac::proto::encoding::VariantCodec();
        auto segment = ac::encode_dispatch(std::move(segment_in_memory), codec_opts, ac::EncodingVersion::V2);
        as::KeySegmentPair kv(k, std::move(segment));
        lmdb_storage.write(std::move(kv));
        ASSERT_TRUE(lmdb_storage.key_exists(k));
    }

    std::filesystem::remove_all(db_path);
}

} // namespace
