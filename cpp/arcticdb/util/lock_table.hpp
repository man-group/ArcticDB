/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <mutex>
#include <unordered_map>

namespace arcticdb {
struct Lock {
    std::mutex mutex_;

    void lock() { mutex_.lock(); }

    void unlock() { mutex_.unlock(); }
};

struct ScopedLock {
    std::shared_ptr<Lock> lock_;

    ARCTICDB_NO_MOVE_OR_COPY(ScopedLock)

    explicit ScopedLock(std::shared_ptr<Lock> lock) : lock_(std::move(lock)) { lock_->lock(); }

    ~ScopedLock() { lock_->unlock(); }
};

class LockTable {
    std::unordered_map<StreamId, std::shared_ptr<Lock>> locks_;
    std::mutex mutex_;

  public:
    LockTable() = default;
    std::shared_ptr<Lock> get_lock_object(const StreamId& stream_id) {
        std::lock_guard lock(mutex_);

        if (auto it = locks_.find(stream_id); it != std::end(locks_))
            return it->second;

        return locks_.try_emplace(stream_id, std::make_shared<Lock>()).first->second;
    }
};
} // namespace arcticdb