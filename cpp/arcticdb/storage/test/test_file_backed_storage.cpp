/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include "storage/storage_exceptions.hpp"
#include <gtest/gtest.h>

#include <arcticdb/storage/file_backed/file_backed_storage.hpp>
#include <arcticdb/storage/test/common.hpp>
#include <arcticdb/util/clock.hpp>

#include <fmt/format.h>

#include <algorithm>
#include <filesystem>

namespace fs = std::filesystem;
using namespace arcticdb;

namespace {

// TODO: This really needs a good home.
class TempDir {
  public:
    TempDir(const ::testing::TestInfo& info) {
        temp_path_ =
                fs::temp_directory_path() /
                fmt::format(
                        "arcticdb_{}_{}_{}", info.test_suite_name(), info.name(), util::SysClock::nanos_since_epoch()
                );
        fs::remove_all(temp_path_);
        fs::create_directories(temp_path_);
    }

    ~TempDir() { fs::remove_all(temp_path_); }

    const fs::path& get() const noexcept { return temp_path_; }

  private:
    fs::path temp_path_;
};

class FileBackedStorage : public ::testing::Test {
  public:
    FileBackedStorage() : temp_dir_(*::testing::UnitTest::GetInstance()->current_test_info()) {
        arcticdb::proto::file_storage::Config cfg;
        cfg.set_dir_path(temp_dir_.get().generic_string());
        // TODO: Rename the variable as use_mock_os_apis. It's okay to use actual storage, we just inject failures.
        cfg.set_use_mock_storage_for_testing(true);

        // I copied the delim from elsewhere. I don't get it. The default is '.'.
        arcticdb::storage::LibraryPath library_path("test_lib", '/');
        storage_.emplace(library_path, storage::OpenMode::DELETE, cfg);
    }

    storage::file_backed::FileBackedStorage& storage() { return *storage_; }

  private:
    TempDir temp_dir_;
    std::optional<storage::file_backed::FileBackedStorage> storage_;
};

bool segments_are_identical(const Segment& x, const Segment& y) {
    BufferView x_buffer = x.buffer();
    BufferView y_buffer = y.buffer();
    return x_buffer.bytes() == y_buffer.bytes() && std::ranges::equal(
                                                           std::span<uint8_t>(x_buffer.data(), x_buffer.bytes()),
                                                           std::span<uint8_t>(y_buffer.data(), y_buffer.bytes())
                                                   );
}

TEST_F(FileBackedStorage, WritesAreVisibleAndReadable) {
    Segment segment{storage::get_test_segment()};
    const std::string symbol{"symbol"};
    auto key = storage::get_test_key(symbol, entity::KeyType::VERSION);
    storage().write(storage::KeySegmentPair(entity::VariantKey{key}, segment.clone()));
    ASSERT_TRUE(exists_in_store(storage(), symbol));

    auto result = storage().read(entity::VariantKey{key}, {});
    ASSERT_EQ(std::get<StringId>(result.atom_key().id()), symbol);
    ASSERT_TRUE(segments_are_identical(segment, result.segment()));
}

TEST_F(FileBackedStorage, KeyNotFoundException) {
    auto key = storage::get_test_key("symbol", entity::KeyType::VERSION);
    ASSERT_THROW(storage().read(entity::VariantKey{key}, {}), storage::KeyNotFoundException);
}

TEST_F(FileBackedStorage, KeysDoNotCollide) {
    // An early bug in file path generation led to files being overwritten. Write a few different segments
    // to four files and then try to reread them and match them to original data.
    const std::array<Segment, 4> segments{
            storage::get_test_segment(10),
            storage::get_test_segment(20),
            storage::get_test_segment(30),
            storage::get_test_segment(40)
    };
    const std::array<entity::KeyType, 4> key_types = {
            entity::KeyType::TABLE_DATA,
            entity::KeyType::TABLE_INDEX,
            entity::KeyType::VERSION,
            entity::KeyType::VERSION_REF
    };
    const std::string symbol{"symbol"};
    for (size_t i = 0; i < key_types.size(); ++i) {
        auto key = arcticdb::atom_key_builder().content_hash(i).build(symbol, key_types[i]);
        storage().write(storage::KeySegmentPair(entity::VariantKey{key}, segments[i].clone()));
    }

    for (size_t i = 0; i < key_types.size(); ++i) {
        auto key = arcticdb::atom_key_builder().content_hash(i).build(symbol, key_types[i]);
        ASSERT_TRUE(exists_in_store(storage(), symbol));

        auto result = storage().read(entity::VariantKey{key}, {});
        ASSERT_TRUE(segments_are_identical(segments[i], result.segment()));
    }
}

TEST_F(FileBackedStorage, ListSymbols) {
    Segment segment{storage::get_test_segment()};
    const std::string symbol{"symbol"};
    auto key = storage::get_test_key(symbol, entity::KeyType::VERSION);
    storage().write(storage::KeySegmentPair(entity::VariantKey{key}, segment.clone()));
    ASSERT_TRUE(exists_in_store(storage(), symbol));

    bool executed_on_found = false;
    storage().iterate_type(arcticdb::entity::KeyType::VERSION, [&](entity::VariantKey&& found_key) {
        ASSERT_EQ(found_key, key);
        executed_on_found = true;
    });
    ASSERT_TRUE(executed_on_found);

    bool executed_on_missing = false;
    storage().iterate_type(arcticdb::entity::KeyType::TABLE_DATA, [&](entity::VariantKey&& found_key) {
        ASSERT_EQ(found_key, key);
        executed_on_missing = true;
    });
    ASSERT_FALSE(executed_on_missing);
}

} // namespace
