/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#ifdef __linux__

#include <cerrno>
#include <fcntl.h>
#include <gtest/gtest.h>

#include <arcticdb/storage/file_backed/file_backed_storage.hpp>
#include <arcticdb/storage/file_backed/real_posix.hpp>
#include <arcticdb/storage/key_segment_pair.hpp>
#include <arcticdb/storage/test/common.hpp>
#include <arcticdb/storage/test/faulty_posix.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/util/clock.hpp>
#include <arcticdb/util/error_code.hpp>

#include <boost/algorithm/string/replace.hpp>
#include <boost/scope_exit.hpp>
#include <fmt/format.h>

#include <algorithm>
#include <filesystem>
#include <functional>
#include <map>
#include <set>
#include <sys/stat.h>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace arcticdb;
using namespace storage::file_backed;
using namespace std::literals;

namespace {

static const std::string STREAM_ID{"symbol"};
static const std::string TEST_LIBRARY_NAME{"test_lib"};

// In GitHub actions, the test suite is run as root, which means all expectations about file permissions
// are overruled. Root is allowed to do anything to files.
bool permissions_are_enforced() { return ::geteuid() != 0; }

// TODO: This really needs a good home.
class TempDir {
  public:
    TempDir(const ::testing::TestInfo& info) {
        std::string subdir = fmt::format(
                "arcticdb_{}_{}_{}", info.test_suite_name(), info.name(), util::SysClock::nanos_since_epoch()
        );
        // Instantiated test suites contain slashes.
        boost::algorithm::replace_all(subdir, "/", "_");
        temp_path_ = fs::temp_directory_path() / subdir;
        fs::remove_all(temp_path_);
        fs::create_directories(temp_path_);
    }

    ~TempDir() { fs::remove_all(temp_path_); }

    const fs::path& get() const noexcept { return temp_path_; }

  private:
    fs::path temp_path_;
};

FileBackedStorage get_storage(
        const fs::path& dir, storage::OpenMode open_mode = storage::OpenMode::DELETE,
        const std::shared_ptr<PosixInterface>& posix = std::make_shared<RealPosix>()
) {
    proto::file_storage::Config cfg;
    cfg.set_dir_path(dir.generic_string());
    storage::LibraryPath library_path(TEST_LIBRARY_NAME, '.');
    return FileBackedStorage(library_path, open_mode, cfg, posix);
}

TEST(FileStorageInit, FileExistsWhereLibraryWouldGo) {
    TempDir temp_dir(*::testing::UnitTest::GetInstance()->current_test_info());

    // What if a file exists with the same name as the directory which the library needs?
    int fd = ::open((temp_dir.get() / TEST_LIBRARY_NAME).c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    ASSERT_GE(fd, 0);
    int res = ::close(fd);
    ASSERT_EQ(res, 0);

    ASSERT_THROW(get_storage(temp_dir.get()), UnexpectedFileStorageException);
    ASSERT_THROW(get_storage(temp_dir.get(), storage::OpenMode::READ), UnexpectedFileStorageException);
}

TEST(FileStorageInit, NoPermissionToCreateLibrary) {
    if (!permissions_are_enforced())
        GTEST_SKIP() << "Running as root, so no PermissionException can be raised.";
    TempDir temp_dir(*::testing::UnitTest::GetInstance()->current_test_info());
    // Make sure the temp directory deletion succeeds.
    BOOST_SCOPE_EXIT(&temp_dir) { ::chmod(temp_dir.get().c_str(), 07777); }
    BOOST_SCOPE_EXIT_END

    // Can't even check if the directory exists.
    ASSERT_EQ(::chmod(temp_dir.get().c_str(), 0), 0);
    ASSERT_THROW(get_storage(temp_dir.get()), PermissionException);

    // Existence check succeeds, but then can't create the library directory.
    ASSERT_EQ(::chmod(temp_dir.get().c_str(), 0500), 0);
    ASSERT_THROW(get_storage(temp_dir.get()), PermissionException);
}

TEST(FileStorageInit, ReadOnlyMode) {
    if (!permissions_are_enforced())
        GTEST_SKIP() << "Running as root makes this test a no-op.";
    TempDir temp_dir(*::testing::UnitTest::GetInstance()->current_test_info());
    // Make sure the temp directory deletion succeeds.
    BOOST_SCOPE_EXIT(&temp_dir) { ::chmod(temp_dir.get().c_str(), 07777); }
    BOOST_SCOPE_EXIT_END

    auto library_dir = temp_dir.get() / TEST_LIBRARY_NAME;
    int res = ::mkdir(library_dir.c_str(), 0500);
    ASSERT_EQ(res, 0);

    // Writing no longer possible, but if the library directory exists, should succeed.
    ASSERT_EQ(::chmod(temp_dir.get().c_str(), 0500), 0);
    get_storage(temp_dir.get(), storage::OpenMode::READ);
}

class FileBackedStorageFixture : public ::testing::Test {
  public:
    FileBackedStorageFixture() :
        temp_dir_(*::testing::UnitTest::GetInstance()->current_test_info()),
        faulty_posix_(std::make_shared<FaultyPosix>()) {
        proto::file_storage::Config cfg;
        cfg.set_dir_path(temp_dir_.get().generic_string());

        storage::LibraryPath library_path(TEST_LIBRARY_NAME, '.');
        storage_dir_ = fs::absolute(temp_dir_.get() / library_path.to_delim_path());
        storage_.emplace(library_path, storage::OpenMode::DELETE, cfg, faulty_posix_);
    }

    void TearDown() override {
        ASSERT_EQ(storage().get_num_temporary_files(), 0) << "Temporary files were not cleaned up.";
        ASSERT_EQ(faulty_posix_->num_open_file_descriptors(), 0);
    }

    FileBackedStorage& storage() { return *storage_; }
    FaultyPosix& posix() { return *faulty_posix_; }
    const fs::path& temp_dir() const { return temp_dir_.get(); }
    const fs::path& storage_dir() const { return storage_dir_; }

  private:
    TempDir temp_dir_;
    fs::path storage_dir_;
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

TEST_F(FileStorageApi, ForwardSlashAllowedInSymbol) {
    GTEST_SKIP() << "Known shortcoming. Need to escape forward slashes.";
    const std::string symbol("market/price");
    Segment segment{storage::get_test_segment()};
    auto key = storage::get_test_key(symbol, entity::KeyType::VERSION);
    storage().write(storage::KeySegmentPair(entity::VariantKey{key}, segment.clone()));
    ASSERT_TRUE(exists_in_store(storage(), STREAM_ID, entity::KeyType::VERSION));

    auto result = storage().read(entity::VariantKey{key}, {});
    ASSERT_EQ(std::get<StringId>(result.atom_key().id()), STREAM_ID);
    ASSERT_TRUE(segments_are_identical(segment, result.segment()));
}

TEST_F(FileStorageApi, KeyNotFoundException) {
    auto key = storage::get_test_key(STREAM_ID, entity::KeyType::VERSION);
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
            atom_key_builder().build(STREAM_ID, entity::KeyType::TABLE_DATA),
            atom_key_builder().build(STREAM_ID, entity::KeyType::TABLE_INDEX),
            atom_key_builder().build(STREAM_ID, entity::KeyType::VERSION),
            atom_key_builder().build(STREAM_ID, entity::KeyType::VERSION_REF)
    };
    for (size_t i = 0; i < segments.size(); ++i)
        storage().write(storage::KeySegmentPair(entity::VariantKey{keys[i]}, segments[i].clone()));

    for (size_t i = 0; i < segments.size(); ++i) {
        ASSERT_TRUE(storage().key_exists(keys[i]));
        storage::KeySegmentPair result = storage().read(entity::VariantKey{keys[i]}, {});
        ASSERT_TRUE(segments_are_identical(segments[i], result.segment()));
    }
}

TEST_F(FileStorageApi, IterateType) {
    Segment segment{storage::get_test_segment()};
    auto key = storage::get_test_key(STREAM_ID, entity::KeyType::VERSION);
    storage().write(storage::KeySegmentPair(entity::VariantKey{key}, segment.clone()));
    ASSERT_TRUE(exists_in_store(storage(), STREAM_ID, entity::KeyType::VERSION));

    bool executed_on_found = false;
    storage().iterate_type(entity::KeyType::VERSION, [&](entity::VariantKey&& found_key) {
        ASSERT_EQ(found_key, key);
        executed_on_found = true;
    });
    ASSERT_TRUE(executed_on_found);

    bool executed_on_missing = false;
    storage().iterate_type(entity::KeyType::TABLE_DATA, [&](entity::VariantKey&& found_key) {
        ASSERT_EQ(found_key, key);
        executed_on_missing = true;
    });
    ASSERT_FALSE(executed_on_missing);
}

// Both traversal APIs filter the same directory with the same rules, so the tests that differ only in which API they
// drive are written once and parameterised over the lister.

using StreamIdLister = std::function<std::set<std::string>(storage::Storage&, entity::KeyType, const std::string&)>;

std::set<std::string> keys_listed(storage::Storage& store, entity::KeyType key_type, const std::string& prefix) {
    return storage::list_in_store(store, key_type, prefix);
}

std::set<std::string> keys_sized(storage::Storage& store, entity::KeyType key_type, const std::string& prefix) {
    std::set<std::string> keys;
    for (const auto& [stream_id, size] : storage::sizes_in_store(store, key_type, prefix))
        keys.insert(stream_id);
    return keys;
}

class FileStorageTraversal : public FileBackedStorageFixture, public ::testing::WithParamInterface<StreamIdLister> {
  public:
    StreamIdLister lister() const { return GetParam(); }
};

TEST_P(FileStorageTraversal, FiltersByPrefix) {
    for (const std::string stream_id : {"symbol_a", "symbol_b", "other"})
        write_in_store(storage(), stream_id, entity::KeyType::VERSION);

    ASSERT_EQ(lister()(storage(), entity::KeyType::VERSION, "symbol"), (std::set<std::string>{"symbol_a", "symbol_b"}));
}

TEST_P(FileStorageTraversal, DoesNotSkipSpecialNames) {
    // If the file encoding scheme is careless, users can use symbol names, which look processed to the traversal's
    // name filters and would be invisible to it.
    const std::set<std::string> symbols{".tmp_symbol", ".nfs_symbol"};
    for (const std::string& stream_id : symbols)
        write_in_store(storage(), stream_id, entity::KeyType::VERSION);

    ASSERT_EQ(lister()(storage(), entity::KeyType::VERSION, ""), symbols);
}

INSTANTIATE_TEST_SUITE_P(
        FileStorageTraversalMethods, FileStorageTraversal, ::testing::Values(keys_listed, keys_sized),
        [](const testing::TestParamInfo<StreamIdLister>& info) {
            return info.index == 0 ? "IterateType" : "VisitObjectSizes";
        }
);

TEST_F(FileStorageApi, VisitObjectSizes) {
    const std::array<std::string, 3> symbols{"symbol_a", "symbol_b", "other"};
    std::map<std::string, size_t> expected;
    for (const std::string& stream_id : symbols) {
        Segment segment{storage::get_test_segment()};
        expected[stream_id] = segment.calculate_size();
        storage().write(
                storage::KeySegmentPair(storage::get_test_key(stream_id, entity::KeyType::VERSION), std::move(segment))
        );
    }

    ASSERT_EQ(storage::sizes_in_store(storage(), entity::KeyType::VERSION), expected);
}

TEST_F(FileStorageApi, IterateTypeResilience) {
    // Dropping a strange unrecognized kind of file in the directory shouldn't stymie the code.
    const fs::path keys_dir = storage_dir() / "ver";
    int res = ::mkdir(keys_dir.c_str(), 0755);
    ASSERT_EQ(res, 0);
    // TODO: This file name triggers exception in particular kind of key tokenization scheme we use today, but
    //       that will change. This test will become useless if it doesn't exercise the exception handling inside
    //       iterate_type.
    const fs::path file_path = storage_dir() / "ver" / "*invalid_file_name";
    int fd = ::open(file_path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    ASSERT_GE(fd, 0);
    BOOST_SCOPE_EXIT(&fd) { ::close(fd); }
    BOOST_SCOPE_EXIT_END

    ASSERT_EQ(storage::list_in_store(storage(), entity::KeyType::VERSION), std::set<std::string>{});
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
                FaultParam(SyscallId::WRITE, ESTALE, "StaleFdOnWrite"sv),
                // mkdir doesn't even need to be retried, we just need resilience.
                FaultParam(SyscallId::MKDIR, EEXIST, "KeyDirExistsOnWrite"sv)
                // FaultParam(SyscallId::OPEN, EEXIST, "TmpAlreadyExistsOnWrite"sv)
        ),
        [](const testing::TestParamInfo<FaultInjection::ParamType>& info) {
            return std::string(std::get<2>(info.param));
        }
);

} // namespace

#endif
