/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/storage/file_backed/file_backed_storage.hpp>
#include <arcticdb/storage/test/common.hpp>
#include <arcticdb/util/clock.hpp>

#include <filesystem>

namespace fs = std::filesystem;
using namespace arcticdb;

namespace {

// TODO: This really needs a good home.
class TempDirFixture : public ::testing::Test {
  protected:
    void SetUp() override {
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        tmp_dir_ =
                fs::temp_directory_path() /
                fmt::format(
                        "arcticdb_{}_{}_{}", info->test_suite_name(), info->name(), util::SysClock::nanos_since_epoch()
                );
        fs::remove_all(tmp_dir_);
        fs::create_directories(tmp_dir_);
    }

    void TearDown() override { fs::remove_all(tmp_dir_); }

    const fs::path& dir() const noexcept { return tmp_dir_; }

    fs::path tmp_dir_;
};

class FileBackedStorageF : public TempDirFixture {
  public:
    void SetUp() override {
        TempDirFixture::SetUp();

        arcticdb::proto::file_storage::Config cfg;

        cfg.set_dir_path(dir().generic_string());
        // TODO: Rename the variable as use_mock_os_apis. It's okay to use actual storage, we just inject failures.
        cfg.set_use_mock_storage_for_testing(true);

        // I copied the delim from elsewhere. Seems odd. The default is '.'.
        arcticdb::storage::LibraryPath library_path("test_lib", '/');
        storage_.emplace(library_path, storage::OpenMode::DELETE, cfg);
    }
    void TearDown() override {
        storage_.reset();
        TempDirFixture::TearDown();
    }

    storage::file_backed::FileBackedStorage& storage() { return *storage_; }

  private:
    std::optional<storage::file_backed::FileBackedStorage> storage_;
};

TEST_F(FileBackedStorageF, WritesAreVisibleAndReadable) {
    // storage_->write(storage::KeySegmentPair(VariantKey{"hi"}, storage::get_test_segment()));
    write_in_store(storage(), "sym");
    ASSERT_TRUE(exists_in_store(storage(), "sym"));
    ASSERT_EQ(read_in_store(storage(), "sym"), "sym");
}

} // namespace
