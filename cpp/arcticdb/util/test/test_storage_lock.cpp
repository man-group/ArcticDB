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
    std::string lock_name_;
    std::shared_ptr<InMemoryStore> store_;
    volatile uint64_t vol_;
    std::atomic<uint64_t> atomic_;
    std::mutex mutex_;
    bool contended_;
    const size_t num_tests_;
    bool timedout_;

    LockData(size_t num_tests) :
    lock_name_("stress_test_lock"),
    store_(std::make_shared<InMemoryStore>()),
    vol_(0),
    atomic_(0),
    contended_(false),
    num_tests_(num_tests),
    timedout_(false){
    }

};

struct OptimisticLockTask {
    std::shared_ptr<LockData> data_;

    explicit OptimisticLockTask(std::shared_ptr<LockData> data) :
        data_(std::move(data)) {
    }

    folly::Future<folly::Unit> operator()() {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        StorageLock<> lock{data_->lock_name_};

        for (auto i = size_t(0); i < data_->num_tests_; ++i) {
            if (!lock.try_lock(data_->store_)) {
                data_->contended_ = true;
            }
            else {
                ++data_->vol_;
                // This should be unnecessary as we are already locked
                std::lock_guard l{data_->mutex_};
                ++data_->atomic_;
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
        futures.emplace_back(exec.addFuture(OptimisticLockTask{lock_data}));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    collect(futures).get();

    ASSERT_EQ(lock_data->atomic_, lock_data->vol_);
    //ASSERT_EQ(lock_data->contended_, true); Alas Jenkins is too rubbish for this, uncomment for tests on headnode
}

struct PessimisticLockTask {
    std::shared_ptr<LockData> data_;
    std::optional<size_t> timeout_ms_;

    PessimisticLockTask(std::shared_ptr<LockData> data, std::optional<size_t> timeout_ms = std::nullopt) :
        data_(std::move(data)),
        timeout_ms_(timeout_ms){
    }

    folly::Future<folly::Unit> operator()() {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        StorageLock<> lock{data_->lock_name_};

        for (auto i = size_t(0); i < data_->num_tests_; ++i) {
            try {
                if(timeout_ms_)
                    lock.lock_timeout(data_->store_, *timeout_ms_);
                else
                    lock.lock(data_->store_);

                ++data_->vol_;
                // This should be unnecessary as we are already locked
                std::lock_guard l{data_->mutex_};
                ++data_->atomic_;
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

    folly::Future<folly::Unit> operator()() {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        StorageLock<> lock{data_->lock_name_};

        try {
            lock.lock_timeout(data_->store_, timeout_ms_);
            ++data_->vol_;
            // This should be unnecessary as we are already locked
            std::lock_guard l{data_->mutex_};
            ++data_->atomic_;
            // Dont unlock
        }
        catch(const StorageLockTimeout&) {
            data_->timedout_ = true;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        // Clean up locks to avoid "mutex destroyed while active" errors on Windows debug build
        lock.unlock(data_->store_);
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
        futures.emplace_back(exec.addFuture(PessimisticLockTask{lock_data}));
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
        futures.emplace_back(exec.addFuture(PessimisticLockTask{lock_data, 20}));
    }
    collect(futures).get();
    ASSERT_TRUE(lock_data->timedout_);
}

TEST(StorageLock, ForceReleaseLock) {
    SKIP_MAC("StorageLock is not supported");
    using namespace arcticdb;

    auto lock_data = std::make_shared<LockData>(4);
    folly::FutureExecutor<folly::CPUThreadPoolExecutor> exec{4};
    // This is set in nanoseconds => 1ms
    ConfigsMap::instance()->set_int("StorageLock.TTL", 1000 * 1000);

    std::vector<std::shared_ptr<StorageLock<>>> locks;

    // Create a first lock that the others will have to force release
    auto first_lock = StorageLock<>(lock_data->lock_name_);
    first_lock.lock(lock_data->store_);

    std::vector<Future<Unit>> futures;
    for(auto i = size_t{0}; i < 4; ++i) {
        futures.emplace_back(exec.addFuture(ForceReleaseLockTask{lock_data, 10 * 1000}));
    }

    collect(futures).get();
    ASSERT_FALSE(lock_data->timedout_);
    ASSERT_EQ(4u, lock_data->atomic_);
    ASSERT_EQ(4u, lock_data->vol_);

    // Clean up locks to avoid "mutex destroyed while active" errors on Windows debug build
    first_lock.unlock(lock_data->store_);
}
