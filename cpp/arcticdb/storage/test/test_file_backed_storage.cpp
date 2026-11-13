/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <cerrno>
#include <gtest/gtest.h>

#include <arcticdb/storage/file_backed/file_backed_storage.hpp>
#include <arcticdb/storage/test/common.hpp>
#include <arcticdb/storage/test/faulty_posix.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/util/clock.hpp>
#include <arcticdb/util/error_code.hpp>

#include <fmt/format.h>

#include <algorithm>
#include <filesystem>

namespace fs = std::filesystem;
using namespace arcticdb;
using namespace storage::file_backed;
using namespace std::literals;

namespace {

static const std::string STREAM_ID{"symbol"};

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

class FileBackedStorageFixture : public ::testing::Test {
  public:
    FileBackedStorageFixture() :
        temp_dir_(*::testing::UnitTest::GetInstance()->current_test_info()),
        faulty_posix_(std::make_shared<FaultyPosix>()) {
        arcticdb::proto::file_storage::Config cfg;
        cfg.set_dir_path(temp_dir_.get().generic_string());

        // I copied the delim from elsewhere. I don't get it. The default is '.'.
        arcticdb::storage::LibraryPath library_path("test_lib", '/');
        storage_.emplace(library_path, storage::OpenMode::DELETE, cfg, faulty_posix_);
    }

    void TearDown() override {
        ASSERT_EQ(storage().get_num_temporary_files(), 0) << "Temporary files were not cleaned up.";

        // TODO: This is really difficult to ensure.
        ASSERT_EQ(faulty_posix_->num_open_file_descriptors(), 0);
    }

    FileBackedStorage& storage() { return *storage_; }
    FaultyPosix& posix() { return *faulty_posix_; }

  private:
    TempDir temp_dir_;
    std::shared_ptr<FaultyPosix> faulty_posix_;
    std::optional<FileBackedStorage> storage_;
};

bool segments_are_identical(const Segment& x, const Segment& y) {
    BufferView x_buffer = x.buffer();
    BufferView y_buffer = y.buffer();
    return x_buffer.bytes() == y_buffer.bytes() && std::ranges::equal(
                                                           std::span<uint8_t>(x_buffer.data(), x_buffer.bytes()),
                                                           std::span<uint8_t>(y_buffer.data(), y_buffer.bytes())
                                                   );
}

using FileStorageApi = FileBackedStorageFixture;

TEST_F(FileStorageApi, WritesAreVisibleAndReadable) {
    Segment segment{storage::get_test_segment()};
    auto key = storage::get_test_key(STREAM_ID, entity::KeyType::VERSION);
    storage().write(storage::KeySegmentPair(entity::VariantKey{key}, segment.clone()));
    ASSERT_TRUE(exists_in_store(storage(), STREAM_ID, entity::KeyType::VERSION));

    auto result = storage().read(entity::VariantKey{key}, {});
    ASSERT_EQ(std::get<StringId>(result.atom_key().id()), STREAM_ID);
    ASSERT_TRUE(segments_are_identical(segment, result.segment()));
}

TEST_F(FileStorageApi, KeyNotFoundException) {
    auto key = storage::get_test_key("symbol", entity::KeyType::VERSION);
    ASSERT_THROW(storage().read(entity::VariantKey{key}, {}), storage::KeyNotFoundException);
}

TEST_F(FileStorageApi, KeyDeletion) {
    auto key = storage::get_test_key(STREAM_ID, entity::KeyType::VERSION);
    storage().write(storage::KeySegmentPair(entity::VariantKey{key}, storage::get_test_segment()));
    ASSERT_TRUE(exists_in_store(storage(), STREAM_ID, entity::KeyType::VERSION));

    storage().remove(entity::VariantKey{key}, {});
    ASSERT_FALSE(exists_in_store(storage(), STREAM_ID, entity::KeyType::VERSION));
    ASSERT_THROW(storage().remove(entity::VariantKey{key}, {}), storage::KeyNotFoundException);
}

TEST_F(FileStorageApi, MultiKeyDeletion) {
    auto nonexistent1 = storage::get_test_key("nonexistent1", entity::KeyType::VERSION);
    auto nonexistent2 = storage::get_test_key("nonexistent2", entity::KeyType::VERSION);
    std::vector<entity::VariantKey> to_remove{nonexistent1, nonexistent2};
    for (size_t i = 0; i < 3; ++i) {
        const std::string stream_id = fmt::format("symbol{}", i);
        auto key = storage::get_test_key(stream_id, entity::KeyType::VERSION);
        to_remove.emplace_back(key);
        storage().write(storage::KeySegmentPair(entity::VariantKey{key}, storage::get_test_segment()));
        ASSERT_TRUE(exists_in_store(storage(), stream_id, entity::KeyType::VERSION));
    }

    ASSERT_THROW(storage().remove(to_remove, {}), storage::KeyNotFoundException);
    for (const entity::VariantKey& key : to_remove)
        ASSERT_FALSE(storage().key_exists(key));
}

// An early bug in file path generation led to files being overwritten. Write a few different segments
// to four files and then try to reread them and match them to original data.
TEST_F(FileStorageApi, KeysDoNotCollide) {
    static const std::array<Segment, 4> segments{
            storage::get_test_segment(10),
            storage::get_test_segment(20),
            storage::get_test_segment(30),
            storage::get_test_segment(40)
    };
    static const std::array<entity::AtomKey, 4> keys = {
            arcticdb::atom_key_builder().build(STREAM_ID, entity::KeyType::TABLE_DATA),
            arcticdb::atom_key_builder().build(STREAM_ID, entity::KeyType::TABLE_INDEX),
            arcticdb::atom_key_builder().build(STREAM_ID, entity::KeyType::VERSION),
            arcticdb::atom_key_builder().build(STREAM_ID, entity::KeyType::VERSION_REF)
    };
    for (size_t i = 0; i < segments.size(); ++i)
        storage().write(storage::KeySegmentPair(entity::VariantKey{keys[i]}, segments[i].clone()));

    for (size_t i = 0; i < segments.size(); ++i) {
        ASSERT_TRUE(storage().key_exists(keys[i]));
        storage::KeySegmentPair result = storage().read(entity::VariantKey{keys[i]}, {});
        ASSERT_TRUE(segments_are_identical(segments[i], result.segment()));
    }
}

TEST_F(FileStorageApi, ListSymbols) {
    Segment segment{storage::get_test_segment()};
    auto key = storage::get_test_key(STREAM_ID, entity::KeyType::VERSION);
    storage().write(storage::KeySegmentPair(entity::VariantKey{key}, segment.clone()));
    ASSERT_TRUE(exists_in_store(storage(), STREAM_ID, entity::KeyType::VERSION));

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

// Exercise buffered I/O. Both read() and write() needn't succeed to consume all input/output in one call.
// If we use a 1 MB data key, might reveal a problem.
TEST_F(FileStorageApi, LargeWrites) {
    // TODO: fully copy pasted from WritesAreVisibleAndReadable
    Segment segment{storage::get_test_segment(4096)};
    auto key = storage::get_test_key(STREAM_ID, entity::KeyType::VERSION);
    storage().write(storage::KeySegmentPair(entity::VariantKey{key}, segment.clone()));
    ASSERT_TRUE(exists_in_store(storage(), STREAM_ID, entity::KeyType::VERSION));

    auto result = storage().read(entity::VariantKey{key}, {});
    ASSERT_EQ(std::get<StringId>(result.atom_key().id()), STREAM_ID);
    ASSERT_TRUE(segments_are_identical(segment, result.segment()));
}

using FaultParam = std::tuple<SyscallId, std::optional<PosixError>, std::string_view>;

class FaultInjection : public FileBackedStorageFixture, public ::testing::WithParamInterface<FaultParam> {
    void SetUp() override {
        const FaultParam& params = GetParam();
        posix().inject_fault(std::get<0>(params), std::get<1>(params));
    }

    void TearDown() override {
        FileBackedStorageFixture::TearDown();
        ASSERT_FALSE(posix().uncosumed_faults())
                << "Tests were written with the expectation that all faults would be consumed, but they were not.";
    }
};

using QuotaExceeded = FaultInjection;

TEST_P(QuotaExceeded, NoSideEffect) {
    Segment segment{storage::get_test_segment()};
    ASSERT_THROW(
            storage().write(storage::KeySegmentPair(storage::get_test_key(STREAM_ID), segment.clone())),
            QuotaExceededException
    );
    ASSERT_FALSE(exists_in_store(storage(), STREAM_ID));
}

INSTANTIATE_TEST_SUITE_P(
        FileStorageResilience, QuotaExceeded,
        testing::Values(
                FaultParam(SyscallId::WRITE, ENOSPC, "WriteNoSpace"sv),
                FaultParam(SyscallId::WRITE, EDQUOT, "WriteNoQuota"sv),
                FaultParam(SyscallId::SYNC, ENOSPC, "SyncNoSpace"sv),
                FaultParam(SyscallId::SYNC, EDQUOT, "SyncNoQuota"sv),
                FaultParam(SyscallId::CLOSE, ENOSPC, "CloseNoSpace"sv),
                FaultParam(SyscallId::CLOSE, EDQUOT, "CloseNoQuota"sv)
        ),
        [](const testing::TestParamInfo<FaultInjection::ParamType>& info) {
            return std::string(std::get<2>(info.param));
        }
);

using RetryableFault = FaultInjection;

TEST_P(RetryableFault, Succeeds) {
    // TODO: This test implementation is identical to WritesAreVisibleAndReadable. Straighten out?
    Segment segment{storage::get_test_segment()};
    auto key = storage::get_test_key(STREAM_ID, entity::KeyType::VERSION);
    storage().write(storage::KeySegmentPair(entity::VariantKey{key}, segment.clone()));
    ASSERT_TRUE(exists_in_store(storage(), STREAM_ID, entity::KeyType::VERSION));

    auto result = storage().read(entity::VariantKey{key}, {});
    ASSERT_EQ(std::get<StringId>(result.atom_key().id()), STREAM_ID);
    ASSERT_TRUE(segments_are_identical(segment, result.segment()));
}

INSTANTIATE_TEST_SUITE_P(
        FileStorageResilience, RetryableFault,
        testing::Values(
                FaultParam(SyscallId::READ, ESTALE, "StaleFdOnRead"sv),
                FaultParam(SyscallId::WRITE, ESTALE, "StaleFdOnWrite"sv)
        ),
        [](const testing::TestParamInfo<FaultInjection::ParamType>& info) {
            return std::string(std::get<2>(info.param));
        }
);

} // namespace
