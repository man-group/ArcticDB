/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/async/async_store.hpp>
#include <arcticdb/util/reliable_storage_lock.hpp>
#include <gtest/gtest.h>
#include <future>
#include <folly/executors/FutureExecutor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <arcticdb/util/clock.hpp>

#include <arcticdb/storage/common.hpp>
#include <arcticdb/storage/config_resolvers.hpp>
#include <arcticdb/storage/library_index.hpp>
#include <arcticdb/util/test/config_common.hpp>

#include <arcticdb/storage/s3/s3_api.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/s3/detail-inl.hpp>
#include <arcticdb/storage/s3/s3_client_wrapper.hpp>
#include <arcticdb/storage/mock/storage_mock_client.hpp>
#include <aws/core/Aws.h>

using namespace arcticdb;
using namespace lock;
namespace aa = arcticdb::async;
namespace as = arcticdb::storage;

// These tests test the actual implementation

TEST(ReliableStorageLock, SingleThreaded) {
    auto store = std::make_shared<InMemoryStore>();
    using Clock = util::ManualClock;
    // We have 2 locks, one with timeout of 20 and another with a timeout of 10
    ReliableStorageLock<Clock> lock1{StringId{"test_lock"}, store, 20};
    ReliableStorageLock<Clock> lock2{StringId{"test_lock"}, store, 10};

    auto count_locks = [&]() {
        auto number_of_lock_keys = 0;
        store->iterate_type(KeyType::ATOMIC_LOCK, [&number_of_lock_keys](VariantKey&& _ [[maybe_unused]]) {
            ++number_of_lock_keys;
        });
        return number_of_lock_keys;
    };

    // We take the first lock at 0 and it should not expire until 20
    Clock::time_ = 0;
    ASSERT_EQ(lock1.try_take_lock(), ReliableLockResult{AcquiredLock{0}});
    ASSERT_EQ(lock2.try_take_lock(), ReliableLockResult{LockInUse{}});
    Clock::time_ = 5;
    ASSERT_EQ((lock1.try_take_lock()), ReliableLockResult{LockInUse{}});
    ASSERT_EQ((lock2.try_take_lock()), ReliableLockResult{LockInUse{}});
    Clock::time_ = 10;
    ASSERT_EQ(lock1.try_take_lock(), ReliableLockResult{LockInUse{}});
    ASSERT_EQ(lock2.try_take_lock(), ReliableLockResult{LockInUse{}});
    Clock::time_ = 19;
    ASSERT_EQ(lock1.try_take_lock(), ReliableLockResult{LockInUse{}});
    ASSERT_EQ(lock2.try_take_lock(), ReliableLockResult{LockInUse{}});

    // Once the first lock has expired we can take a new lock with lock_id=1
    Clock::time_ = 20;
    ASSERT_EQ(lock2.try_take_lock(), ReliableLockResult{AcquiredLock{1}});
    ASSERT_EQ(count_locks(), 2);

    // We can extend the lock timeout at 25 to 35 and get an lock_id=2
    Clock::time_ = 25;
    ASSERT_EQ(lock1.try_take_lock(), ReliableLockResult{LockInUse{}});
    ASSERT_EQ(lock2.try_extend_lock(1), ReliableLockResult{AcquiredLock{2}});
    ASSERT_EQ(count_locks(), 3);
    Clock::time_ = 34;
    ASSERT_EQ(lock1.try_take_lock(), ReliableLockResult{LockInUse{}});

    // At time 35 the lock with lock_id=2 has expired and we can re-acquire the lock
    Clock::time_ = 35;
    ASSERT_EQ(lock1.try_take_lock(), ReliableLockResult{AcquiredLock{3}});
    ASSERT_EQ(count_locks(), 4);
    ASSERT_EQ(lock2.try_take_lock(), ReliableLockResult{LockInUse{}});

    // And we can free the lock immediately to allow re-acquiring without waiting for timeout
    lock1.free_lock(3);
    ASSERT_EQ(lock2.try_take_lock(), ReliableLockResult{AcquiredLock{4}});
    ASSERT_EQ(count_locks(), 5);

    // But if we take a lock at 1000 all locks would have expired a 10xtimeout=100 ago, and we should clear all apart
    // from latest lock_id=5
    Clock::time_ = 1000;
    ASSERT_EQ(lock2.try_take_lock(), ReliableLockResult{AcquiredLock{5}});
    ASSERT_EQ(count_locks(), 1);
}

struct SlowIncrementTask : async::BaseTask {
    int& cnt_;
    const ReliableStorageLock<>& lock_;
    std::chrono::milliseconds sleep_time_;
    bool lock_lost_ = false;

    SlowIncrementTask(int& cnt, ReliableStorageLock<>& lock, std::chrono::milliseconds sleep_time) :
        cnt_(cnt),
        lock_(lock),
        sleep_time_(sleep_time) {}

    void operator()() {
        auto acquired = lock_.retry_until_take_lock();
        auto guard = ReliableStorageLockGuard(lock_, acquired, [that = this]() { that->lock_lost_ = true; });
        auto value_before_sleep = cnt_;
        std::this_thread::sleep_for(sleep_time_);
        if (lock_lost_) {
            // Return early on a lost lock. We will raise an issue if this happens.
            return;
        }
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
    // Seemingly because the heartbeating thread might occasionally not run for long periods of time. This problem
    // disappears with larger timouts like 1000ms. The failures are likely present only on WSL whose clock can
    // occasionally jump back by a few seconds, which causes folly's stable clock to not increase and hence skips a
    // heartbeat.
    ReliableStorageLock<> lock{StringId{"test_lock"}, store, ONE_SECOND};

    int counter = 0;

    std::vector<folly::Future<folly::Unit>> futures;
    for (auto i = 0u; i < threads; ++i) {
        // We use both fast and slow tasks to test both fast lock frees and lock extensions
        auto sleep_time = std::chrono::milliseconds(i % 2 * 2000);
        futures.emplace_back(exec.addFuture(SlowIncrementTask{counter, lock, sleep_time}));
    }
    folly::collectAll(futures).get();

    // If no locks were lost and no races happened each thread will have incremented the counter exactly once
    ASSERT_EQ(counter, threads);

    // Also the lock should be free by the end (i.e. we can take a new lock)
    ASSERT_EQ(std::holds_alternative<AcquiredLock>(lock.try_take_lock()), true);
}

TEST(ReliableStorageLock, NotImplementedException) {
    using namespace arcticdb::async;

    // Given
    as::EnvironmentName environment_name{"research"};
    as::StorageName storage_name("storage_name");
    as::LibraryPath library_path{"a", "b"};
    namespace ap = arcticdb::pipelines;

    // We set the suffix for the storage test to fail.
    std::string failure_suffix = storage::s3::S3ClientTestWrapper::get_failure_trigger(
            "suffix", storage::StorageOperation::WRITE, Aws::S3::S3Errors::UNKNOWN
    );
    ConfigsMap::instance()->set_string("Storage.AtomicSupportTestSuffix", failure_suffix);

    auto failed_config = proto::s3_storage::Config();
    failed_config.set_use_mock_storage_for_testing(true);

    auto failed_env_config = arcticdb::get_test_environment_config(
            library_path, storage_name, environment_name, std::make_optional(failed_config)
    );
    auto failed_config_resolver = as::create_in_memory_resolver(failed_env_config);
    as::LibraryIndex failed_library_index{environment_name, failed_config_resolver};

    as::UserAuth user_auth{"abc"};
    auto codec_opt = std::make_shared<arcticdb::proto::encoding::VariantCodec>();

    auto lib = failed_library_index.get_library(
            library_path, as::OpenMode::WRITE, user_auth, storage::NativeVariantStorage()
    );
    auto store = std::make_shared<aa::AsyncStore<>>(aa::AsyncStore(lib, *codec_opt, EncodingVersion::V1));

    EXPECT_THROW(
            { ReliableStorageLock<> lock(StringId("test_lock"), store, ONE_SECOND); },
            UnsupportedAtomicOperationException
    );
}

TEST(ReliableStorageLock, AdminTools) {
    auto store = std::make_shared<InMemoryStore>();
    using Clock = util::ManualClock;
    ReliableStorageLock<Clock> lock{StringId{"test_lock"}, store, 10};

    Clock::time_ = 0;
    // No existing lock
    ASSERT_EQ(lock.inspect_latest_lock(), std::nullopt);
    // When we take a lock at time 0, inspecting it shows it expires at 10.
    ASSERT_EQ(lock.try_take_lock(), ReliableLockResult{AcquiredLock{0}});
    ASSERT_EQ(lock.inspect_latest_lock(), std::make_optional<ActiveLock>(AcquiredLockId{0}, 10));

    Clock::time_ = 15;
    // Even when lock is expired we can inspect it
    ASSERT_EQ(lock.inspect_latest_lock(), std::make_optional<ActiveLock>(AcquiredLockId{0}, 10));
    ASSERT_EQ(lock.try_take_lock(), ReliableLockResult{AcquiredLock{1}});

    Clock::time_ = 20;
    // Last lock still holds and we can't take it with regular operation
    ASSERT_EQ(lock.inspect_latest_lock(), std::make_optional<ActiveLock>(AcquiredLockId{1}, 25));
    ASSERT_EQ(lock.try_take_lock(), ReliableLockResult{LockInUse{}});
    // But we can force_take it with a custom timeout of 15. The force increases the id by 10.
    ASSERT_EQ(lock.force_take_lock(15), AcquiredLockId{11});
    ASSERT_EQ(lock.inspect_latest_lock(), std::make_optional<ActiveLock>(AcquiredLockId{11}, 35));

    Clock::time_ = 30;
    // We still can't take the lock by regular means
    ASSERT_EQ(lock.try_take_lock(), ReliableLockResult{LockInUse{}});
    // But we can force free it by using a negative timeout, to write an expired lock.
    ASSERT_EQ(lock.force_take_lock(-5), AcquiredLockId{21});
    ASSERT_EQ(lock.inspect_latest_lock(), std::make_optional<ActiveLock>(AcquiredLockId{21}, 25));
    // And now we can take a lock with the regular try_take_lock
    ASSERT_EQ(lock.try_take_lock(), ReliableLockResult{AcquiredLock{22}});
    ASSERT_EQ(lock.inspect_latest_lock(), std::make_optional<ActiveLock>(AcquiredLockId{22}, 40));
}

TEST(ReliableStorageLock, GuardInvokesOnLostLockOnStealAndSetAfterLoss) {
    auto store = std::make_shared<InMemoryStore>();
    ReliableStorageLock<> lock{StringId{"test_lock_loss"}, store, 50 * ONE_MILLISECOND};

    auto res = lock.try_take_lock();
    ASSERT_TRUE(std::holds_alternative<AcquiredLock>(res));
    const auto acquired = std::get<AcquiredLock>(res);

    std::atomic<int> loss_count{0};
    std::promise<void> lost_promise;
    auto lost_future = lost_promise.get_future();

    {
        ReliableStorageLockGuard guard(lock, acquired, [&]() {
            if (loss_count.fetch_add(1) == 0) {
                lost_promise.set_value();
            }
        });

        // Steal the lock via force_take_lock so the next heartbeat observes LockInUse.
        const auto stolen_id = lock.force_take_lock(5 * ONE_SECOND);
        ASSERT_EQ(lost_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
        EXPECT_EQ(loss_count.load(), 1);

        // Installing a new callback after the lock was already lost must invoke it immediately outside state_mutex_.
        bool immediate_called = false;
        guard.set_on_lost_lock([&]() { immediate_called = true; });
        EXPECT_TRUE(immediate_called);

        // Destroying guard after loss must NOT free the thief's lock.
        EXPECT_EQ(stolen_id, lock.inspect_latest_lock()->lock_id);
    }

    EXPECT_EQ(lock.try_take_lock(), ReliableLockResult{LockInUse{}});
}

TEST(ReliableStorageLock, GuardInvokesOnLostLockOnHeartbeatStorageException) {
    class FailingExtendStore : public InMemoryStore {
      public:
        void set_fail_atomic_writes(bool fail) { fail_atomic_writes_.store(fail); }

        VariantKey write_if_none_sync(KeyType key_type, const StreamId& stream_id, SegmentInMemory&& segment) override {
            if (key_type == KeyType::ATOMIC_LOCK && fail_atomic_writes_.load()) {
                throw StorageException("injected heartbeat storage failure");
            }
            return InMemoryStore::write_if_none_sync(key_type, stream_id, std::move(segment));
        }

      private:
        std::atomic<bool> fail_atomic_writes_{false};
    };

    auto store = std::make_shared<FailingExtendStore>();
    ReliableStorageLock<> lock{StringId{"test_lock_storage_err"}, store, 50 * ONE_MILLISECOND};
    auto res = lock.try_take_lock();
    ASSERT_TRUE(std::holds_alternative<AcquiredLock>(res));

    std::atomic<int> loss_count{0};
    std::promise<void> lost_promise;
    auto lost_future = lost_promise.get_future();

    ReliableStorageLockGuard guard(lock, std::get<AcquiredLock>(res), [&]() {
        if (loss_count.fetch_add(1) == 0) {
            lost_promise.set_value();
        }
    });
    store->set_fail_atomic_writes(true);

    ASSERT_EQ(lost_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(loss_count.load(), 1);
}

TEST(ReliableStorageLock, GuardConcurrentSetOnLostLockAndDestructionDuringLoss) {
    class PausedExtendStore : public InMemoryStore {
      public:
        VariantKey write_if_none_sync(KeyType key_type, const StreamId& stream_id, SegmentInMemory&& segment) override {
            if (key_type == KeyType::ATOMIC_LOCK && intercept_.exchange(false)) {
                entered_.set_value();
                release_.wait();
                throw StorageException("injected concurrent loss during heartbeat extend");
            }
            return InMemoryStore::write_if_none_sync(key_type, stream_id, std::move(segment));
        }

        void arm() { intercept_.store(true); }
        std::future<void> entered_future() { return entered_.get_future(); }
        void release() {
            intercept_.store(false);
            if (!released_.exchange(true)) {
                release_promise_.set_value();
            }
        }

      private:
        std::atomic<bool> intercept_{false};
        std::atomic<bool> released_{false};
        std::promise<void> entered_;
        std::promise<void> release_promise_;
        std::shared_future<void> release_{release_promise_.get_future().share()};
    };

    for (int iter = 0; iter < 8; ++iter) {
        auto store = std::make_shared<PausedExtendStore>();
        ReliableStorageLock<> lock{StringId{"test_lock_race_" + std::to_string(iter)}, store, 20 * ONE_MILLISECOND};
        auto res = lock.try_take_lock();
        ASSERT_TRUE(std::holds_alternative<AcquiredLock>(res));

        std::atomic<int> callbacks{0};
        auto entered = store->entered_future();
        {
            ReliableStorageLockGuard guard(lock, std::get<AcquiredLock>(res), std::nullopt);
            auto unblock_guard = folly::makeGuard([&]() { store->release(); });
            // Wait until the heartbeat thread is inside try_extend_lock's write_if_none_sync,
            // then steal the lock and release the heartbeat right as set_on_lost_lock & dtor run.
            store->arm();
            ASSERT_EQ(entered.wait_for(std::chrono::seconds(5)), std::future_status::ready);
            (void)lock.force_take_lock(5 * ONE_SECOND);
            store->release();
            guard.set_on_lost_lock([&]() { callbacks.fetch_add(1); });
            // Guard destructor joins heartbeat thread cleanly while racing with loss notification.
        }
        EXPECT_EQ(callbacks.load(), 1);
    }
}
