/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/storage/test/common.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace arcticdb;
using namespace arcticdb::storage;

namespace {

const fs::path CONCURRENCY_TEST_PATH = "./test_databases_concurrency";

// Returned as the base type on purpose: cleanup() is private on LmdbStorage and reachable only through a Storage
// handle, which is also how production reaches it (Library::cleanup() -> Storages::cleanup()).
std::unique_ptr<Storage> make_lmdb_storage(const std::string& lib_name) {
    arcticdb::proto::lmdb_storage::Config cfg;
    cfg.set_path(CONCURRENCY_TEST_PATH.generic_string());
    cfg.set_map_size(64ULL * (1ULL << 20));
    cfg.set_recreate_if_exists(true);
    LibraryPath library_path(lib_name, '/');
    return std::make_unique<arcticdb::storage::lmdb::LmdbStorage>(library_path, OpenMode::DELETE, cfg);
}

class LmdbStorageConcurrencyTest : public ::testing::Test {
  protected:
    void SetUp() override {
        if (!fs::exists(CONCURRENCY_TEST_PATH)) {
            fs::create_directories(CONCURRENCY_TEST_PATH);
        }
    }

    void TearDown() override {
        if (fs::exists(CONCURRENCY_TEST_PATH)) {
            fs::remove_all(CONCURRENCY_TEST_PATH);
        }
    }
};

} // namespace

/*
 * Reproduction for the LmdbStorage environment-lifetime defect.
 *
 * Storage::write runs on folly IO pool threads (see async::WriteSegmentTask), while cleanup() runs on whichever thread
 * called Arctic.delete_library. Before the fix, LmdbStorage::env() and LmdbStorage::get_dbi() returned raw references
 * into lmdb_instance_ and kept no ownership for the duration of the call, so cleanup() could drop the last reference
 * and close the MDB_env - unmapping lock.mdb - while a writer was inside mdb_txn_begin, which is exactly the CI
 * segfault (pthread_mutex_lock on env->me_txns->mti_wmutex). Reading and resetting the shared_ptr member from
 * different threads with no synchronisation is separately a data race.
 *
 * This test does not fail deterministically on a non-instrumented build: a use-after-free of a just-freed MDB_env
 * usually reads plausible garbage rather than crashing. Run it under AddressSanitizer (heap-use-after-free inside
 * mdb_txn_begin / mdb_txn_commit) or ThreadSanitizer (data race on lmdb_instance_) to see it fail before the fix.
 */
TEST_F(LmdbStorageConcurrencyTest, WriteDuringCleanup) {
#if defined(_WIN32)
    // Windows refuses to delete a mapped file, so cleanup() raises rather than racing. Both the defect and the POSIX
    // unlink-while-mapped semantics the fix relies on are unreproducible here.
    GTEST_SKIP() << "relies on POSIX unlink-while-mapped semantics";
#else
    constexpr int num_rounds = 20;
    constexpr int num_writers = 4;

    for (int round = 0; round < num_rounds; ++round) {
        auto storage = make_lmdb_storage(fmt::format("cleanup_race_{}", round));

        std::atomic<bool> stop{false};
        std::atomic<int> writes_started{0};
        std::vector<std::thread> writers;
        writers.reserve(num_writers);

        // cleanup() can throw, and destroying a joinable std::thread calls std::terminate. Declared after the
        // threads so it runs before they are destroyed.
        struct StopWriters {
            std::atomic<bool>& stop;
            std::vector<std::thread>& writers;
            ~StopWriters() {
                stop.store(true, std::memory_order_relaxed);
                for (auto& writer : writers) {
                    if (writer.joinable()) {
                        writer.join();
                    }
                }
            }
        } stop_writers{stop, writers};

        for (int t = 0; t < num_writers; ++t) {
            writers.emplace_back([&, t]() {
                for (int n = 0; !stop.load(std::memory_order_relaxed); ++n) {
                    try {
                        write_in_store(*storage, fmt::format("sym_{}_{}", t, n));
                        writes_started.fetch_add(1, std::memory_order_relaxed);
                    } catch (const std::exception&) {
                        // Expected once cleanup() has removed the environment; escaping would std::terminate.
                    }
                }
            });
        }

        // Let the writers get going so that cleanup lands in the middle of an in-flight transaction.
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        storage->cleanup();
        stop.store(true, std::memory_order_relaxed);
        for (auto& writer : writers) {
            writer.join();
        }

        ASSERT_GT(writes_started.load(), 0) << "writers never got going, the race window was never opened";
        // After cleanup every subsequent call must report the environment is gone rather than touch freed memory.
        ASSERT_THROW({ write_in_store(*storage, "after_cleanup"); }, UnexpectedLMDBErrorException);
    }
#endif
}

/*
 * LmdbStorage::times_path_opened is a process-wide static std::unordered_map mutated from the constructor
 * (warn_if_lmdb_already_open), the destructor and reset_warning_counter. Storages over different paths are constructed
 * and destroyed from arbitrary threads (pybind11 holders are released on whichever thread drops the last Python
 * reference), so before the fix this was unsynchronised mutation of a std::unordered_map - a plausible source of the
 * heap corruption seen in the second CI signature.
 *
 * Like the test above this needs ThreadSanitizer (or a lot of luck) to fail on a pre-fix build.
 */
TEST_F(LmdbStorageConcurrencyTest, ConcurrentOpenAndClose) {
    constexpr int num_threads = 8;
    constexpr int num_iterations = 25;

    arcticdb::storage::lmdb::LmdbStorage::reset_warning_counter();

    struct Failures {
        std::mutex mutex;
        std::vector<std::string> messages;
    } failures;

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([t, &failures]() {
            for (int i = 0; i < num_iterations; ++i) {
                // Escaping a std::thread callable calls std::terminate; record and assert in the test body instead.
                try {
                    auto storage = make_lmdb_storage(fmt::format("open_close_{}_{}", t, i));
                    write_in_store(*storage, "sym");
                } catch (const std::exception& e) {
                    std::lock_guard<std::mutex> lock{failures.mutex};
                    failures.messages.emplace_back(e.what());
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    {
        std::lock_guard<std::mutex> lock{failures.mutex};
        EXPECT_TRUE(failures.messages.empty())
                << failures.messages.size() << " of " << (num_threads * num_iterations)
                << " open/write/close cycles failed, first: " << failures.messages.front();
    }

    arcticdb::storage::lmdb::LmdbStorage::reset_warning_counter();
}
