/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/test/common.hpp>
#include <arcticdb/util/error_code.hpp>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

using namespace arcticdb;
using namespace arcticdb::storage;

namespace {

constexpr uint64_t MAP_SIZE = 64ULL << 20;
constexpr int NUM_ROUNDS = 20;
constexpr int NUM_WORKERS = 4;

// Runs op on NUM_WORKERS threads until stopped. Destroying a joinable std::thread terminates, so the destructor stops
// and joins on every exit path, including a failed ASSERT.
struct Workers {
    std::atomic<bool> stop{false};
    std::atomic<int> completed{0};
    std::vector<std::thread> threads;

    template<typename Op>
    explicit Workers(Op op) {
        for (int t = 0; t < NUM_WORKERS; ++t) {
            threads.emplace_back([this, op, t] {
                for (int n = 0; !stop; ++n) {
                    try {
                        op(t, n);
                        ++completed;
                    } catch (const UnexpectedLMDBErrorException&) {
                        return; // the environment has been removed
                    } catch (const ArcticException& e) {
                        ADD_FAILURE() << e.what();
                        return;
                    }
                }
            });
        }
    }

    bool wait_for_first_completion() const {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (completed == 0 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return completed > 0;
    }

    ~Workers() {
        stop = true;
        for (auto& thread : threads) {
            thread.join();
        }
    }
};

} // namespace

// cleanup() while other threads write. Under ASan this fails if cleanup() can close the environment under an
// in-flight transaction; on Windows it fails if a write still maps the files when cleanup() deletes them.
TEST_F(LMDBStorageTestBase, WriteDuringCleanup) {
    for (int round = 0; round < NUM_ROUNDS; ++round) {
        auto storage = make_test_lmdb_storage(fmt::format("write_cleanup_{}", round), MAP_SIZE);
        {
            Workers writers([&storage](int t, int n) { write_in_store(*storage, fmt::format("sym_{}_{}", t, n)); });
            ASSERT_TRUE(writers.wait_for_first_completion());
            ASSERT_NO_THROW(storage->cleanup());
        }
        ASSERT_THROW(write_in_store(*storage, "after_cleanup"), UnexpectedLMDBErrorException);
    }
}

// cleanup() while other threads read. Reads keep the environment open until they finish; under ASan this fails if
// cleanup() can close it under one.
TEST_F(LMDBStorageTestBase, ReadDuringCleanup) {
    for (int round = 0; round < NUM_ROUNDS; ++round) {
        auto storage = make_test_lmdb_storage(fmt::format("read_cleanup_{}", round), MAP_SIZE);
        write_in_store(*storage, "sym");
        {
            Workers readers([&storage](int, int) { read_in_store(*storage, "sym"); });
            ASSERT_TRUE(readers.wait_for_first_completion());
            [[maybe_unused]] const bool files_removed = [&storage] {
                try {
                    storage->cleanup();
                    return true;
                } catch (const UnexpectedLMDBErrorException&) {
                    return false;
                }
            }();
#ifndef _WIN32
            // Only Windows refuses to delete files that a read still maps
            ASSERT_TRUE(files_removed);
#endif
        }
        ASSERT_THROW(read_in_store(*storage, "sym"), UnexpectedLMDBErrorException);
    }
}

// Opens, writes to and closes storages over different paths on several threads. Needs TSan to catch unsynchronised
// access to the process-wide count of opens per path.
TEST_F(LMDBStorageTestBase, ConcurrentOpenAndClose) {
    constexpr int num_threads = 8;
    constexpr int num_iterations = 25;

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([t] {
            for (int i = 0; i < num_iterations; ++i) {
                try {
                    auto storage = make_test_lmdb_storage(fmt::format("open_close_{}_{}", t, i), MAP_SIZE);
                    write_in_store(*storage, "sym");
                } catch (const ArcticException& e) {
                    ADD_FAILURE() << e.what();
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
}
