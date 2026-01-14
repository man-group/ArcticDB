/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/store.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/constants.hpp>
#include <arcticdb/util/configs_map.hpp>

#include <mutex>
#include <folly/experimental/FunctionScheduler.h>

namespace arcticdb {

namespace lock {

// AcquiredLockId is a unique lock identifier. The ids are guarded by atomic storage operations so that only one process
// can ever hold a specific lock id. Only the latest id is valid and only it can be extended or freed.
using AcquiredLockId = uint64_t;

using AcquiredLock = AcquiredLockId;
using LockInUse = std::monostate;

using ReliableLockResult = std::variant<AcquiredLock, LockInUse>;

struct ActiveLock {
    AcquiredLockId lock_id;
    timestamp expiration;

    bool operator==(const ActiveLock& other) const {
        return lock_id == other.lock_id && expiration == other.expiration;
    }
};

// The ReliableStorageLock is a storage lock which relies on atomic If-None-Match Put and ListObject operations to
// provide a more reliable lock than the StorageLock but it requires the backend to support atomic operations. It should
// be completely consistent unless a process holding a lock gets paused for times comparable to the lock timeout.
// It lock follows the algorithm described here:
// https://www.morling.dev/blog/leader-election-with-s3-conditional-writes/
// Note that the ReliableStorageLock just provides methods for requesting or extending acquired locks. It doesn't hold
// any information about the acquired locks so far and none of its APIs are re-entrant. Thus the user is responsible for
// protecting and extending the acquired locks (which can be done through the ReliableStorageLockGuard).
template<class ClockType = util::SysClock>
class ReliableStorageLock {
  public:
    ReliableStorageLock(const std::string& base_name, const std::shared_ptr<Store> store, timestamp timeout);
    ReliableStorageLock(const ReliableStorageLock<ClockType>& other) = default;

    AcquiredLockId retry_until_take_lock() const;
    ReliableLockResult try_take_lock() const;
    ReliableLockResult try_extend_lock(AcquiredLockId acquired_lock) const;
    void free_lock(AcquiredLockId acquired_lock) const;
    timestamp timeout() const;

    // Below APIs are for admin management of the lock
    std::optional<ActiveLock> inspect_latest_lock() const;

    // Forcefully takes a new lock without waiting for any timeouts. If custom_timeout is negative the resulting lock
    // will be expired at creation (hence will result in forcefully freeing the locks).
    AcquiredLockId force_take_lock(timestamp custom_timeout) const;

    void force_clear_locks() const;

  private:
    ReliableLockResult try_take_id(
            const std::vector<AcquiredLockId>& existing_locks, AcquiredLockId lock_id,
            std::optional<timestamp> timeout_override = std::nullopt
    ) const;
    std::pair<std::vector<AcquiredLockId>, std::optional<AcquiredLockId>> get_all_locks() const;
    timestamp get_expiration(RefKey lock_key) const;
    void clear_locks(const std::vector<AcquiredLockId>& acquired_locks, bool old_only = true) const;
    StreamId get_stream_id(AcquiredLockId acquired_lock) const;
    RefKey get_ref_key(AcquiredLockId acquired_lock) const;
    std::string base_name_;
    std::shared_ptr<Store> store_;
    timestamp timeout_;
};

// The ReliableStorageLockGuard protects an AcquiredLockId and frees it on destruction. While the lock is held it
// periodically extends its timeout in a heartbeating thread. If for some reason the lock is lost we get notified
// via the on_lock_lost.
class ReliableStorageLockGuard {
  public:
    ReliableStorageLockGuard(
            const ReliableStorageLock<>& lock, AcquiredLockId acquired_lock, std::optional<folly::Func>&& on_lost_lock
    );

    ~ReliableStorageLockGuard();

    // Will immediately trigger [on_lost_lock] if lock is already lost.
    void set_on_lost_lock(folly::Func&& on_lost_lock);

  private:
    void cleanup_on_lost_lock();
    const ReliableStorageLock<> lock_;
    std::optional<AcquiredLockId> acquired_lock_;
    std::optional<folly::Func> on_lost_lock_;
    folly::FunctionScheduler extend_lock_heartbeat_;
};

// Only used for python tests
struct LostReliableLock : std::exception {};
class ReliableStorageLockManager {
  public:
    void take_lock_guard(const ReliableStorageLock<>& lock);
    void free_lock_guard();

  private:
    std::optional<std::shared_ptr<ReliableStorageLockGuard>> guard = std::nullopt;
};

} // namespace lock

} // namespace arcticdb

#include "arcticdb/util/reliable_storage_lock-inl.hpp"