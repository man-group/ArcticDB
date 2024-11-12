/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/util/reliable_storage_lock.hpp>
#include <gtest/gtest.h>
#include <folly/executors/FutureExecutor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <arcticdb/util/test/gtest_utils.hpp>
#include <arcticdb/stream/piloted_clock.hpp>

using namespace arcticdb;
using namespace lock;

// These tests test the actual implementation

TEST(ReliableStorageLock, SingleThreaded) {
    auto store = std::make_shared<InMemoryStore>();
    using Clock = PilotedClockNoAutoIncrement;
    // We have 2 locks, one with timeout of 20 and another with a timeout of 10
    ReliableStorageLock<Clock> lock1{StringId{"test_lock"}, store, 20};
    ReliableStorageLock<Clock> lock2{StringId{"test_lock"}, store, 10};

    auto count_locks = [&]() {
        auto number_of_lock_keys = 0;
        store->iterate_type(KeyType::SLOW_LOCK, [&number_of_lock_keys](VariantKey&& _ [[maybe_unused]]){++number_of_lock_keys;});
        return number_of_lock_keys;
    };

    // We take the first lock at 0 and it should not expire until 20
    Clock::time_ = 0;
    ASSERT_EQ(lock1.try_take_lock(), std::optional<uint64_t>{0});
    Clock::time_ = 5;
    ASSERT_EQ(lock2.try_take_lock(), std::nullopt);
    Clock::time_ = 10;
    ASSERT_EQ(lock2.try_take_lock(), std::nullopt);
    Clock::time_ = 19;
    ASSERT_EQ(lock1.try_take_lock(), std::nullopt);

    // Once the first lock has expired we can take a new lock with epoch=1
    Clock::time_ = 20;
    ASSERT_EQ(lock2.try_take_lock(), std::optional<uint64_t>{1});
    ASSERT_EQ(count_locks(), 2);

    // We can extend the lock timeout at 25 to 35 and get an epoch=2
    Clock::time_ = 25;
    ASSERT_EQ(lock1.try_take_lock(), std::nullopt);
    ASSERT_EQ(lock2.try_extend_lock(1), std::optional<uint64_t>{2});
    ASSERT_EQ(count_locks(), 3);
    Clock::time_ = 34;
    ASSERT_EQ(lock1.try_take_lock(), std::nullopt);

    // At time 35 the lock with epoch=2 has expired and we can re-aquire the lock
    Clock::time_ = 35;
    ASSERT_EQ(lock1.try_take_lock(), std::optional<uint64_t>{3});
    ASSERT_EQ(count_locks(), 4);
    ASSERT_EQ(lock2.try_take_lock(), std::nullopt);

    // And we can free the lock immediately to allow re-aquiring without waiting for timeout
    lock1.free_lock(3);
    ASSERT_EQ(lock2.try_take_lock(), std::optional<uint64_t>{4});
    // Taking lock2 with timeout=10 means we should clear all locks which have expired before 25. In this case just epoch=0
    ASSERT_EQ(count_locks(), 4);

    // But if we take a lock at 100 all locks would have expired a timeout=10 ago, and we should clear all apart from latest epoch=5
    Clock::time_ = 100;
    ASSERT_EQ(lock2.try_take_lock(), std::optional<uint64_t>{5});
    ASSERT_EQ(count_locks(), 1);
}

struct SlowIncrementTask : async::BaseTask {
    int& cnt_;
    const ReliableStorageLock<>& lock_;
    std::chrono::milliseconds sleep_time_;
    bool lock_lost_ = false;

    SlowIncrementTask(int& cnt, ReliableStorageLock<>& lock, std::chrono::milliseconds sleep_time) :
        cnt_(cnt), lock_(lock), sleep_time_(sleep_time) {}

    void operator()() {
        auto guard = ReliableStorageLockGuard(lock_, [that = this](){
            that->lock_lost_ = true;
        });
        auto value_before_sleep = cnt_;
        // std::cout<<"Taken a lock with "<<value_before_sleep<<std::endl;
        std::this_thread::sleep_for(sleep_time_);
        if (lock_lost_) {
            // Return early on a lost lock. We will raise an issue if this happens.
            std::cout<<"Lost a lock with "<<value_before_sleep<<std::endl;
            return;
        }
        // std::cout<<"Freeing a lock with "<<value_before_sleep<<std::endl;
        cnt_ = value_before_sleep + 1;
    }
};


TEST(ReliableStorageLock, StressMultiThreaded) {
    // It is hard to use a piloted clock for these tests because the folly::FunctionScheduler we use for the lock
    // extensions doesn't support a custom clock. Thus this test will need to run for about 2 minutes.
    auto threads = 100u;
    folly::FutureExecutor<folly::CPUThreadPoolExecutor> exec{threads};
    auto store = std::make_shared<InMemoryStore>();
    // Running the test with tighter timeout than the 1000ms timeout causes it to fail occasionally.
    // Seemingly because the heartbeating thread might occasionally not run for long periods of time. This problem disappears with larger timouts like 1000ms.
    ReliableStorageLock<> lock{StringId{"test_lock"}, store, ONE_SECOND};

    int counter = 0;

    std::vector<folly::Future<folly::Unit>> futures;
    for(auto i = 0u; i < threads; ++i) {
        // We use both fast and slow tasks to test both fast lock frees and lock extensions
        auto sleep_time = std::chrono::milliseconds(i%2 * 2000);
        futures.emplace_back(exec.addFuture(SlowIncrementTask{counter, lock, sleep_time}));
    }
    folly::collectAll(futures).get();

    // If no locks were lost and no races happened each thread will have incremented the counter exactly once
    ASSERT_EQ(counter, threads);

    // Also the lock should be free by the end (i.e. we can take a new lock)
    ASSERT_EQ(lock.try_take_lock().has_value(), true);
}
