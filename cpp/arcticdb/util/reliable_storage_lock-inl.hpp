/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/reliable_storage_lock.hpp>

#include <arcticdb/storage/store.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/constants.hpp>
#include <arcticdb/util/configs_map.hpp>

#include <mutex>
#include <folly/experimental/FunctionScheduler.h>

namespace arcticdb {

namespace lock {

const auto SEPARATOR = '*';
const auto EXTENDS_PER_TIMEOUT = 5u;
const auto REMOVE_AFTER_TIMEOUTS = 10u;

inline StreamDescriptor lock_stream_descriptor(const StreamId &stream_id) {
    return stream_descriptor(
            stream_id,
            stream::RowCountIndex(),
            {scalar_field(DataType::INT64, "expiration")});
}

inline SegmentInMemory lock_segment(const StreamId &name, timestamp expiration) {
    SegmentInMemory output{lock_stream_descriptor(name)};
    output.set_scalar(0, expiration);
    output.end_row();
    return output;
}

template <class ClockType>
ReliableStorageLock<ClockType>::ReliableStorageLock(const std::string &base_name, const std::shared_ptr<Store> store, timestamp timeout) :
    base_name_(base_name), store_(store), timeout_(timeout) {
    storage::check<ErrorCode::E_UNSUPPORTED_ATOMIC_OPERATION>(store_->supports_atomic_writes(), "Storage does not support atomic writes, so we can't create a lock");
}

template <class ClockType>
timestamp ReliableStorageLock<ClockType>::timeout() const {
    return timeout_;
}

inline AcquiredLockId get_next_id(std::optional<AcquiredLockId> maybe_prev) {
    if (maybe_prev.has_value()) {
        return maybe_prev.value() + 1;
    }
    return 0;
}

template <class ClockType>
StreamId ReliableStorageLock<ClockType>::get_stream_id(AcquiredLockId lock_id) const {
    return fmt::format("{}{}{}", base_name_, SEPARATOR, lock_id);
}

inline AcquiredLockId extract_lock_id_from_stream_id(const StreamId& stream_id) {
    auto string_id = std::get<StringId>(stream_id);
    auto lock_id_string = string_id.substr(string_id.find(SEPARATOR)+1, string_id.size());
    return std::stoull(lock_id_string);
}

template <class ClockType>
std::pair<std::vector<AcquiredLockId>, std::optional<AcquiredLockId>> ReliableStorageLock<ClockType>::get_all_locks() const {
    std::vector<AcquiredLockId> lock_ids;
    store_->iterate_type(
            KeyType::ATOMIC_LOCK,
            [&lock_ids](VariantKey &&key) {
                auto current_lock_id = extract_lock_id_from_stream_id(variant_key_id(key));
                lock_ids.push_back(current_lock_id);
            }, base_name_ + SEPARATOR);
    std::optional<AcquiredLockId> latest = lock_ids.size() == 0 ? std::nullopt : std::make_optional<>(*std::max_element(lock_ids.begin(), lock_ids.end()));
    return {lock_ids, latest};
}

template<class ClockType>
timestamp ReliableStorageLock<ClockType>::get_expiration(RefKey lock_key) const {
    auto kv = store_->read_sync(lock_key, storage::ReadKeyOpts{});
    return kv.second.template scalar_at<timestamp>(0, 0).value();
}

template <class ClockType>
void ReliableStorageLock<ClockType>::clear_old_locks(const std::vector<AcquiredLockId>& lock_ids) const {
    auto now = ClockType::nanos_since_epoch();
    auto to_delete = std::vector<VariantKey>();
    // We only clear locks that have expired more than 10 timeouts (we assume a process can't be paused for more than the timeout) ago.
    // We do this to avoid a process mistakenly taking a lock if:
    // 1. Process A lists locks and gets [4, 5, 6]
    // 2. Process A decides to attempt taking lock 7
    // 3. Process A gets paused
    // 4. Process B takes locks 7 and 8
    // 5. Process B decides to clear lock 7 since it's not the latest
    // 6. Process A succeeds in taking lock 7
    for (auto lock_id : lock_ids) {
        auto lock_key = RefKey{get_stream_id(lock_id), KeyType::ATOMIC_LOCK};
        if (get_expiration(lock_key) + REMOVE_AFTER_TIMEOUTS * timeout_ < now) {
            to_delete.emplace_back(lock_key);
        }
    }
    store_->remove_keys_sync(to_delete);
}

template <class ClockType>
std::optional<AcquiredLockId> ReliableStorageLock<ClockType>::try_take_lock() const {
    auto [existing_locks, latest] = get_all_locks();
    if (latest.has_value()) {
        auto expires = get_expiration(RefKey{get_stream_id(latest.value()), KeyType::ATOMIC_LOCK});
        if (expires > ClockType::nanos_since_epoch()) {
            // An unexpired lock exists
            return std::nullopt;
        }
    }
    return try_take_next_id(existing_locks, latest);
}

template<class ClockType>
AcquiredLockId ReliableStorageLock<ClockType>::retry_until_take_lock() const {
    // We don't use the ExponentialBackoff because we want to be able to wait indefinitely
    auto max_wait = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(timeout()));
    auto min_wait = max_wait / 16;
    auto current_wait = min_wait;
    std::minstd_rand gen(std::random_device{}());
    std::uniform_real_distribution<> dist(1.0, 1.2);
    auto jittered_wait = [&]() {
        auto factor = dist(gen);
        return current_wait * factor;
    };

    auto acquired_lock = try_take_lock();
    while (!acquired_lock.has_value()) {
        std::this_thread::sleep_for(jittered_wait());
        current_wait = std::min(current_wait * 2, max_wait);
        acquired_lock = try_take_lock();
    }
    return acquired_lock.value();
}

template <class ClockType>
std::optional<AcquiredLockId> ReliableStorageLock<ClockType>::try_extend_lock(AcquiredLockId acquired_lock) const {
    auto [existing_locks, latest] = get_all_locks();
    util::check(latest.has_value() && latest.value() >= acquired_lock,
                "We are trying to extend a newer lock_id than the existing one in storage. Extend lock_id: {}",
                acquired_lock);
    if (latest.value() != acquired_lock) {
        // We have lost the lock while holding it (most likely due to timeout).
        return std::nullopt;
    }
    return try_take_next_id(existing_locks, latest);
}

template <class ClockType>
void ReliableStorageLock<ClockType>::free_lock(AcquiredLockId acquired_lock) const {
    auto [existing_locks, latest_lock_id] = get_all_locks();
    util::check(latest_lock_id.has_value() && latest_lock_id.value() >= acquired_lock,
                "We are trying to free a newer lock_id than the existing one in storage. Free lock_id: {}, Existing lock_id: {}",
                acquired_lock, latest_lock_id);
    if (latest_lock_id.value() != acquired_lock) {
        // Lock is already lost
        return;
    }
    auto lock_stream_id = get_stream_id(acquired_lock);
    auto expiration = ClockType::nanos_since_epoch(); // Write current time to mark lock as expired as of now
    store_->write_sync(KeyType::ATOMIC_LOCK, lock_stream_id, lock_segment(lock_stream_id, expiration));
}

template <class ClockType>
std::optional<AcquiredLockId> ReliableStorageLock<ClockType>::try_take_next_id(const std::vector<AcquiredLockId>& existing_locks, std::optional<AcquiredLockId> latest) const {
    AcquiredLockId lock_id = get_next_id(latest);
    auto lock_stream_id = get_stream_id(lock_id);
    auto expiration = ClockType::nanos_since_epoch() + timeout_;
    try {
        store_->write_if_none_sync(KeyType::ATOMIC_LOCK, lock_stream_id, lock_segment(lock_stream_id, expiration));
    } catch (const StorageException& e) {
        // There is no specific Aws::S3::S3Errors for the failed atomic operation, so we catch any StorageException.
        // Either way it's safe to assume we have failed to acquire the lock in case of transient S3 error.
        // If error persists we'll approprieately raise in the next attempt to LIST/GET the existing lock and propagate
        // the transient error.
        log::lock().warn("Failed to acquire lock (likely someone acquired it before us): {}", e.what());
        return std::nullopt;
    }
    // We clear old locks only after aquiring the lock to avoid duplicating the deletion work
    clear_old_locks(existing_locks);
    return lock_id;
}

inline ReliableStorageLockGuard::ReliableStorageLockGuard(const ReliableStorageLock<> &lock, AcquiredLockId acquired_lock, folly::Func&& on_lost_lock) :
        lock_(lock), acquired_lock_(std::nullopt), on_lost_lock_(std::move(on_lost_lock)) {
    acquired_lock_ = acquired_lock;
    // We heartbeat 5 times per lock timeout to extend the lock.
    auto hearbeat_frequency = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::nanoseconds(lock_.timeout() / EXTENDS_PER_TIMEOUT));
    extend_lock_heartbeat_.addFunction(
        [that=this](){
            if (that->acquired_lock_.has_value()) {
                that->acquired_lock_ = that->lock_.try_extend_lock(that->acquired_lock_.value());
                if (!that->acquired_lock_.has_value()) {
                    // Clean up if we have lost the lock.
                    that->cleanup_on_lost_lock();
                }
            }
        }, hearbeat_frequency, "Extend lock", hearbeat_frequency);
    extend_lock_heartbeat_.start();
}

inline void ReliableStorageLockGuard::cleanup_on_lost_lock() {
    // We do not use shutdown because we don't want to run it from within a FunctionScheduler thread to avoid a deadlock
    extend_lock_heartbeat_.cancelAllFunctions();
    on_lost_lock_();
}

inline ReliableStorageLockGuard::~ReliableStorageLockGuard() {
    extend_lock_heartbeat_.shutdown();
    if (acquired_lock_.has_value()) {
        lock_.free_lock(acquired_lock_.value());
    }
}

inline void ReliableStorageLockManager::take_lock_guard(const ReliableStorageLock<> &lock) {
    auto acquired = lock.retry_until_take_lock();
    guard = std::make_shared<ReliableStorageLockGuard>(lock, acquired, [](){
        throw LostReliableLock();
    });
}

inline void ReliableStorageLockManager::free_lock_guard() {
    guard = std::nullopt;
}

}

}
