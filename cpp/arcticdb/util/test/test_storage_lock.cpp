/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/util/storage_lock.hpp>
#include <gtest/gtest.h>
#include <folly/executors/FutureExecutor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <arcticdb/util/test/gtest_utils.hpp>

using namespace arcticdb;
using namespace folly;

TEST(StorageLock, SingleThreaded) {
    SKIP_MAC("StorageLock is not supported");
    auto store = std::make_shared<InMemoryStore>();
    StorageLock lock1{StringId{"test_lock"}};
    StorageLock lock2{StringId{"test_lock"}};
    ASSERT_EQ(lock1.try_lock(store), true);
    ASSERT_EQ(!lock2.try_lock(store), true);
    ASSERT_EQ(!lock2.try_lock(store), true);
    ASSERT_EQ(!lock1.try_lock(store), true);
    lock1.unlock(store);
    ASSERT_EQ(lock2.try_lock(store), true);
    lock2.unlock(store);
    lock1.lock(store);
    ASSERT_EQ(!lock2.try_lock(store), true);
    lock1.unlock(store);
}

TEST(StorageLock, Timeout) {
    SKIP_MAC("StorageLock is not supported");
    auto store = std::make_shared<InMemoryStore>();
    StorageLock lock{"test_lock"};
    StorageLock lock2{"test_lock"};
    auto begin ARCTICDB_UNUSED = util::SysClock::nanos_since_epoch();
    ASSERT_EQ(lock.try_lock(store), true);
    auto end ARCTICDB_UNUSED = util::SysClock::nanos_since_epoch();
    ASSERT_GT(end - begin, 10000);
    ASSERT_EQ(!lock.try_lock(store), true);
    lock.unlock(store);
    ASSERT_EQ(lock.try_lock(store), true);
    lock.unlock(store);
}

struct LockData {
    std::string lock_name_ = "stress_test_lock";
    std::shared_ptr<InMemoryStore> store_ = std::make_shared<InMemoryStore>();
    volatile uint64_t vol_ { 0 };
    std::atomic<uint64_t> atomic_ = { 0 };
    std::atomic<bool> contended_ { false };
    std::atomic<bool> timedout_ { false };
    const size_t num_tests_;

    explicit LockData(size_t num_tests) :
    num_tests_(num_tests){}

    void increment_counters_under_lock() {
        // This is done in order to test whether any racing has occurred by checking if vol_ and atomic_ have diverged

        using namespace std::chrono_literals;
        const uint64_t vol = vol_ + 1;
        std::this_thread::sleep_for(10ms);
        vol_ = vol;
        ++atomic_;
    }

    bool no_race_happened() {
        return atomic_ == vol_;
    }
};

struct LockTaskWithoutRetry {
    std::shared_ptr<LockData> data_;

    explicit LockTaskWithoutRetry(std::shared_ptr<LockData> data) :
        data_(std::move(data)) {
    }

    folly::Future<folly::Unit> operator()() {
        StorageLock<> lock{data_->lock_name_};

        for (auto i = size_t(0); i < data_->num_tests_; ++i) {
            if (!lock.try_lock(data_->store_)) {
                data_->contended_ = true;
            }
            else {
                data_->increment_counters_under_lock();
                lock.unlock(data_->store_);
            }
        }
        return makeFuture(Unit{});
    }
};


TEST(StorageLock, Contention) {
    SKIP_MAC("StorageLock is not supported");
    using namespace arcticdb;

    auto lock_data = std::make_shared<LockData>(4);
    folly::FutureExecutor<folly::CPUThreadPoolExecutor> exec{4};

    std::vector<Future<Unit>> futures;
    for(auto i = size_t{0}; i < 4; ++i) {
        futures.emplace_back(exec.addFuture(LockTaskWithoutRetry{lock_data}));
    }
    collect(futures).get();

    ASSERT_TRUE(lock_data->no_race_happened());
    ASSERT_EQ(lock_data->contended_, true);
}

struct LockTaskWithRetry {
    std::shared_ptr<LockData> data_;
    std::optional<size_t> timeout_ms_;

    explicit LockTaskWithRetry(std::shared_ptr<LockData> data, std::optional<size_t> timeout_ms = std::nullopt) :
        data_(std::move(data)),
        timeout_ms_(timeout_ms){
    }

    folly::Future<folly::Unit> operator()() {
        StorageLock<> lock{data_->lock_name_};

        for (auto i = size_t(0); i < data_->num_tests_; ++i) {
            try {
                if(timeout_ms_)
                    lock.lock_timeout(data_->store_, *timeout_ms_);
                else
                    lock.lock(data_->store_);

                data_->increment_counters_under_lock();
                lock.unlock(data_->store_);
            }
            catch(const StorageLockTimeout&) {
                data_->timedout_ = true;
            }
        }
        return makeFuture(Unit{});
    }
};

struct ForceReleaseLockTask {
    std::shared_ptr<LockData> data_;
    size_t timeout_ms_;

    ForceReleaseLockTask(std::shared_ptr<LockData> data, size_t timeout_ms) :
        data_(std::move(data)),
        timeout_ms_(timeout_ms)
        {
    }

    folly::Future<folly::Unit> operator()() const {
        StorageLock<> lock{data_->lock_name_};

        try {
            lock.lock_timeout(data_->store_, timeout_ms_);
            // As of C++20, '++' expression of 'volatile'-qualified type is deprecated.
            data_->increment_counters_under_lock();
            // Dont unlock
        }
        catch(const StorageLockTimeout&) {
            data_->timedout_ = true;
        }

        // Clean up locks to avoid "mutex destroyed while active" errors on Windows debug build
        lock._test_release_local_lock();
        return makeFuture(Unit{});
    }
};

struct OptimisticForceReleaseLockTask {
    std::shared_ptr<LockData> data_;
    size_t timeout_ms_;
    size_t retry_ms_;

    OptimisticForceReleaseLockTask(std::shared_ptr<LockData> data, size_t timeout_ms, size_t retry_ms) :
            data_(std::move(data)),
            timeout_ms_(timeout_ms),
            retry_ms_(retry_ms)
    {
    }

    folly::Future<folly::Unit> operator()() const {
        StorageLock<> lock{data_->lock_name_};
        bool contended = true;
        size_t total_wait = 0;
        while (contended && total_wait < timeout_ms_) {
            contended = !lock.try_lock(data_->store_);
            std::this_thread::sleep_for(std::chrono::milliseconds(retry_ms_));
            total_wait += retry_ms_;
        }
        if (contended) {
            data_->timedout_ = true;
            data_->contended_ = true;
        } else {
            data_->increment_counters_under_lock();
            // Dont unlock
        }
        // Clean up locks to avoid "mutex destroyed while active" errors on Windows debug build
        lock._test_release_local_lock();
        return makeFuture(Unit{});
    }
};

TEST(StorageLock, Wait) {
    SKIP_MAC("StorageLock is not supported");
    using namespace arcticdb;

    auto lock_data = std::make_shared<LockData>(4);
    folly::FutureExecutor<folly::CPUThreadPoolExecutor> exec{4};

    std::vector<Future<Unit>> futures;
    for(auto i = size_t{0}; i < 4; ++i) {
        futures.emplace_back(exec.addFuture(LockTaskWithRetry{lock_data}));
    }
    collect(futures).get();

    ASSERT_EQ(16u, lock_data->atomic_);
    ASSERT_EQ(16u, lock_data->vol_);
}

TEST(StorageLock, Timeouts) {
    SKIP_MAC("StorageLock is not supported");
    using namespace arcticdb;

    auto lock_data = std::make_shared<LockData>(4);
    folly::FutureExecutor<folly::CPUThreadPoolExecutor> exec{4};

    std::vector<Future<Unit>> futures;
    for(auto i = size_t{0}; i < 4; ++i) {
        futures.emplace_back(exec.addFuture(LockTaskWithRetry{lock_data, 20}));
    }
    collect(futures).get();
    ASSERT_TRUE(lock_data->timedout_);
}

int count_occurrences(std::string search, std::string pattern) {
    if (search.size() < pattern.size()) return false;
    int count = 0;
    for (size_t pos = 0; pos <= search.size() - pattern.size(); pos++) {
        if (search.substr(pos, pattern.size()) == pattern)
            count++;
    }
    return count;
}

TEST(StorageLock, ForceReleaseLock) {
    // Verify that lock() will take the lock when the TTL has expired.
    // Then each thread simulates forgetting to release the lock, so that other threads need to rely
    // on the TTL expiring to be able to acquire the lock.
    // Initially take the lock, so that the first thread also has to wait for the TTL of that lock
    // to expire.
    SKIP_MAC("StorageLock is not supported");
    using namespace arcticdb;

    auto lock_data = std::make_shared<LockData>(4);
    folly::FutureExecutor<folly::CPUThreadPoolExecutor> exec{4};
    // WaitMs is set in milliseconds => 50ms for the preempting check; TTL is set in nanoseconds => 200ms for the TTL
    ScopedConfig scoped_config({{"StorageLock.WaitMs", 50}, {"StorageLock.TTL", 200 * 1000 * 1000}});


    // Create a first lock that the others will have to force release
    auto first_lock = StorageLock<>(lock_data->lock_name_);
    first_lock.lock(lock_data->store_);

    testing::internal::CaptureStderr();
    testing::internal::CaptureStdout();
    std::vector<Future<Unit>> futures;
    for(auto i = size_t{0}; i < 4; ++i) {
        futures.emplace_back(exec.addFuture(ForceReleaseLockTask{lock_data, 10 * 1000}));
    }

    collect(futures).get();
    ASSERT_FALSE(lock_data->timedout_);
    ASSERT_EQ(4u, lock_data->atomic_);
    ASSERT_EQ(4u, lock_data->vol_);

    std::string stdout_str  = testing::internal::GetCapturedStdout();
    std::string stderr_str = testing::internal::GetCapturedStderr();
    std::string expected = "more than TTL";

    ASSERT_EQ(count_occurrences("abab", "ab"), 2u);
    // If a lock is preempted, then it will still print the warning about having overridden the
    // lock due to the TTL expiring, but will then have to retry, so there may be more than the expected
    // number of log messages.
    // Skip on Windows as capturing logs doesn't work. TODO: Configure the logger with the file output
#ifndef _WIN32
    ASSERT_TRUE(
            count_occurrences(stdout_str, expected) >= 4 ||
            count_occurrences(stderr_str, expected) >= 4
    );
#endif

    // Clean up locks to avoid "mutex destroyed while active" errors on Windows debug build
    first_lock._test_release_local_lock();
}

TEST(StorageLock, OptimisticForceReleaseLock) {
    // Verify that try_lock() will take the lock when the TTL has expired.
    // Since this method does not retry automatically, the threads will retry periodically
    // until the TTL expires, and then they should be able to take the lock. The threads
    // will then simulate forgetting to release the lock, so that other threads need to rely
    // on the TTL expiring to be able to acquire the lock.
    // Initially take the lock using the lock() method, so that the first thread also has to
    // wait for the TTL of that lock to expire.
    SKIP_MAC("StorageLock is not supported");
    using namespace arcticdb;

    auto lock_data = std::make_shared<LockData>(4);
    folly::FutureExecutor<folly::CPUThreadPoolExecutor> exec{4};
    // WaitMs is set in milliseconds => 50ms for the preempting check; TTL is set in nanoseconds => 200ms for the TTL
    ScopedConfig scoped_config({{"StorageLock.WaitMs", 50}, {"StorageLock.TTL", 200 * 1000 * 1000}});

    // Create a first lock that the others will have to force release
    auto first_lock = StorageLock<>(lock_data->lock_name_);
    first_lock.lock(lock_data->store_);

    testing::internal::CaptureStderr();
    testing::internal::CaptureStdout();
    std::vector<Future<Unit>> futures;
    for(auto i = size_t{0}; i < 4; ++i) {
        futures.emplace_back(exec.addFuture(OptimisticForceReleaseLockTask{lock_data, 10 * 1000, 100}));
    }

    collect(futures).get();
    ASSERT_FALSE(lock_data->timedout_);
    ASSERT_FALSE(lock_data->contended_);
    ASSERT_EQ(4u, lock_data->atomic_);
    ASSERT_EQ(4u, lock_data->vol_);

    std::string stdout_str  = testing::internal::GetCapturedStdout();
    std::string stderr_str = testing::internal::GetCapturedStderr();
    std::string expected = "more than TTL";

    std::cout << stdout_str << std::endl;
    std::cout << stderr_str << std::endl;

    ASSERT_EQ(count_occurrences("abab", "ab"), 2u);
    // If a lock is preempted, then it will still print the warning about having overridden the
    // lock due to the TTL expiring, but will then have to retry, so there may be more than the expected
    // number of log messages.
    // Skip on Windows as capturing logs doesn't work. TODO: Configure the logger with the file output
#ifndef _WIN32
    ASSERT_TRUE(
        count_occurrences(stdout_str, expected) >= 4 ||
        count_occurrences(stderr_str, expected) >= 4
    );
#endif

    // Clean up locks to avoid "mutex destroyed while active" errors on Windows debug build
    first_lock._test_release_local_lock();
}


class StorageLockWithSlowWrites : public ::testing::TestWithParam<std::tuple<int, int, int>> {
protected:
    void SetUp() override {
        log::lock().set_level(spdlog::level::debug);
        StorageFailureSimulator::reset();
    }
};

class StorageLockWithAndWithoutRetry : public ::testing::TestWithParam<bool> {
    protected:
    void SetUp() override {
        StorageFailureSimulator::reset();
    }
};

TEST_P(StorageLockWithSlowWrites, ConcurrentWrites) {
    const int first_delay = std::get<0>(GetParam());
    const int second_delay = std::get<1>(GetParam());
    const int expected_locks = std::get<2>(GetParam());

    const StorageFailureSimulator::ParamActionSequence SLOW_ACTIONS = {
        action_factories::slow_action(1, first_delay, first_delay),
        action_factories::slow_action(1, second_delay, second_delay)
    };

    constexpr size_t num_writers = 2;
    FutureExecutor<CPUThreadPoolExecutor> exec{num_writers};

    StorageFailureSimulator::instance()->configure({{FailureType::WRITE, SLOW_ACTIONS}});

    std::vector<Future<Unit>> futures;
    auto lock_data = std::make_shared<LockData>(1);
    lock_data->store_ = std::make_shared<InMemoryStore>();

    for (size_t i = 0; i < num_writers; ++i) {
        futures.emplace_back(exec.addFuture(LockTaskWithoutRetry(lock_data)));
    }
    collect(futures).get();

    ASSERT_EQ(lock_data->atomic_, expected_locks);
    ASSERT_TRUE(lock_data->no_race_happened());
}

TEST(StorageLock, ConcurrentWritesWithRetrying) {
    constexpr size_t num_writers = 3;

    auto lock_data = std::make_shared<LockData>(1);
    lock_data->store_ = std::make_shared<InMemoryStore>();
    FutureExecutor<CPUThreadPoolExecutor> exec{num_writers};
    std::vector<Future<Unit>> futures;
    for(size_t i = 0; i < num_writers; ++i) {
        futures.emplace_back(exec.addFuture(LockTaskWithRetry(lock_data, 3000)));
    }
    collect(futures).get();

    ASSERT_EQ(lock_data->atomic_, 3);
    ASSERT_TRUE(lock_data->no_race_happened());
}

TEST_P(StorageLockWithAndWithoutRetry, StressManyWriters) {
    const StorageFailureSimulator::ParamActionSequence SLOW_ACTIONS = {
        action_factories::slow_action(0.3, 600, 1100),
    };

    constexpr size_t num_writers = 50;
    FutureExecutor<CPUThreadPoolExecutor> exec{num_writers};

    StorageFailureSimulator::instance()->configure({{FailureType::WRITE, SLOW_ACTIONS}});

    std::vector<Future<Unit>> futures;
    const auto lock_data = std::make_shared<LockData>(1);
    lock_data->store_ = std::make_shared<InMemoryStore>();
    const bool with_retry = GetParam();
    for (size_t i = 0; i < num_writers; ++i) {
        auto future = with_retry ? exec.addFuture(LockTaskWithRetry(lock_data, 10000)) : exec.addFuture(LockTaskWithoutRetry(lock_data));
        futures.emplace_back(std::move(future));
    }
    collect(futures).get();

    ASSERT_TRUE(lock_data->no_race_happened());
}


INSTANTIATE_TEST_SUITE_P(, StorageLockWithAndWithoutRetry,
        ::testing::Bool()
        );

INSTANTIATE_TEST_SUITE_P(, StorageLockWithSlowWrites,
        ::testing::Values(// first delay, second delay, expected locks
            std::make_tuple(0, 0, 1),
            std::make_tuple(10, 800, 0), // If the delay is betweeen ~ 0.5 * wait_ms and 1 * wait_ms we expect both locks to fail.
            std::make_tuple(10, 1700, 1),
            std::make_tuple(10, 2000, 1)
        )
);

TEST(StorageLock, SlowWrites) {
    const auto current_lock_sleep_wait_ms = ConfigsMap::instance()->get_int("StorageLock.WaitMs", StorageLock<>::DEFAULT_WAIT_MS);
    const auto min_ms = current_lock_sleep_wait_ms * 1.5;
    const auto max_ms = current_lock_sleep_wait_ms * 2;
    const StorageFailureSimulator::ParamActionSequence SLOW_WRITE = {
        action_factories::slow_action(1, min_ms, max_ms)
    };
    StorageFailureSimulator::instance()->configure({{FailureType::WRITE, SLOW_WRITE}});
    auto lock = StorageLock("test");
    ASSERT_FALSE(lock.try_lock(std::make_shared<InMemoryStore>()));
}

TEST(StorageLock, DISABLED_LockSameTimestamp) { // Not yet implemented
    log::lock().set_level(spdlog::level::debug);
    constexpr size_t num_writers = 2;

    using StorageLockType = StorageLock<util::ManualClock>;
    util::ManualClock::time_ = 1;
    std::vector<std::unique_ptr<StorageLockType>> locks;
    locks.reserve(num_writers);
    while (locks.size() < num_writers) { locks.emplace_back(std::make_unique<StorageLockType>("test")); }
    auto store = std::make_shared<InMemoryStore>();
    folly::FutureExecutor<folly::CPUThreadPoolExecutor> exec{num_writers};
    
    auto lock_data = std::make_shared<LockData>(1);
    lock_data->store_ = std::make_shared<InMemoryStore>();
    std::vector<Future<Unit>> futures;
    for(size_t i = 0; i < num_writers; ++i) {
        futures.emplace_back(exec.addFuture(LockTaskWithoutRetry(lock_data)));
    }
    collect(futures).get();

    ASSERT_EQ(lock_data->atomic_, 1);
    ASSERT_TRUE(lock_data->no_race_happened());
}