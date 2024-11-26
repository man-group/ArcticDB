/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
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

using Epoch = uint64_t;

// The ReliableStorageLock is a storage lock which relies on atomic If-None-Match Put and ListObject operations to
// provide a slower but more reliable lock than the StorageLock. It should be completely consistent unless a process
// holding a lock get's paused for times comparable to the lock timeout.
// It lock follows the algorithm described here:
// https://www.morling.dev/blog/leader-election-with-s3-conditional-writes/
template <class ClockType = util::SysClock>
class ReliableStorageLock {
public:
    ReliableStorageLock(const std::string& base_name, const std::shared_ptr<Store> store, timestamp timeout);

    Epoch retry_until_take_lock() const;
    std::optional<Epoch> try_take_lock() const;
    std::optional<Epoch> try_extend_lock(Epoch held_lock_epoch) const;
    void free_lock(Epoch held_lock_epoch) const;
    timestamp timeout() const;
private:
    std::optional<Epoch> try_take_next_epoch(const std::vector<Epoch>& existing_locks, std::optional<Epoch> latest) const;
    std::pair<std::vector<Epoch>, std::optional<Epoch>> get_all_locks() const;
    timestamp get_expiration(RefKey lock_key) const;
    void clear_old_locks(const std::vector<Epoch>& epochs) const;
    StreamId get_stream_id(Epoch e) const;
    std::string base_name_;
    std::shared_ptr<Store> store_;
    timestamp timeout_;
};

// The ReliableStorageLockGuard protects an aquired ReliableStorageLock::Epoch and frees it on destruction. While the lock
// is held it periodically extends its timeout in a heartbeating thread. If for some reason the lock is lost we get notified
// via the on_lock_lost.
class ReliableStorageLockGuard {
public:
    ReliableStorageLockGuard(const ReliableStorageLock<> &lock, Epoch aquired_epoch, folly::Func&& on_lost_lock);

    ~ReliableStorageLockGuard();
private:
    void cleanup_on_lost_lock();
    const ReliableStorageLock<> &lock_;
    std::optional<Epoch> aquired_epoch_;
    folly::Func on_lost_lock_;
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

}

}

#include "arcticdb/util/reliable_storage_lock.tpp"