/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

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

inline StreamDescriptor lock_stream_descriptor(const StreamId& stream_id) {
    return stream_descriptor(stream_id, stream::RowCountIndex(), {scalar_field(DataType::INT64, "expiration")});
}

inline SegmentInMemory lock_segment(const StreamId& name, timestamp expiration) {
    SegmentInMemory output{lock_stream_descriptor(name)};
    output.set_scalar(0, expiration);
    output.end_row();
    return output;
}

template<class ClockType>
ReliableStorageLock<ClockType>::ReliableStorageLock(
        const std::string& base_name, const std::shared_ptr<Store> store, timestamp timeout
) :
    base_name_(base_name),
    store_(store),
    timeout_(timeout) {
    storage::check<ErrorCode::E_UNSUPPORTED_ATOMIC_OPERATION>(
            store_->supports_atomic_writes(), "Storage does not support atomic writes, so we can't create a lock"
    );
}

template<class ClockType>
timestamp ReliableStorageLock<ClockType>::timeout() const {
    return timeout_;
}

inline AcquiredLockId get_next_id(std::optional<AcquiredLockId> maybe_prev) {
    if (maybe_prev.has_value()) {
        return maybe_prev.value() + 1;
    }
    return 0;
}

inline AcquiredLockId get_force_next_id(std::optional<AcquiredLockId> maybe_prev) {
    // When taking a lock with force we use a higher lock id so we can force acquire it even if there is high lock
    // contention.
    return maybe_prev.value_or(0) + 10;
}

template<class ClockType>
StreamId ReliableStorageLock<ClockType>::get_stream_id(AcquiredLockId lock_id) const {
    return fmt::format("{}{}{}", base_name_, SEPARATOR, lock_id);
}

template<class ClockType>
RefKey ReliableStorageLock<ClockType>::get_ref_key(AcquiredLockId lock_id) const {
    return RefKey{get_stream_id(lock_id), KeyType::ATOMIC_LOCK};
}

inline AcquiredLockId extract_lock_id_from_stream_id(const StreamId& stream_id) {
    auto string_id = std::get<StringId>(stream_id);
    auto lock_id_string = string_id.substr(string_id.find(SEPARATOR) + 1, string_id.size());
    return std::stoull(lock_id_string);
}

template<class ClockType>
std::pair<std::vector<AcquiredLockId>, std::optional<AcquiredLockId>> ReliableStorageLock<ClockType>::get_all_locks(
) const {
    std::vector<AcquiredLockId> lock_ids;
    store_->iterate_type(
            KeyType::ATOMIC_LOCK,
            [&lock_ids](VariantKey&& key) {
                auto current_lock_id = extract_lock_id_from_stream_id(variant_key_id(key));
                lock_ids.push_back(current_lock_id);
            },
            base_name_ + SEPARATOR
    );
    std::optional<AcquiredLockId> latest =
            lock_ids.size() == 0 ? std::nullopt
                                 : std::make_optional<>(*std::max_element(lock_ids.begin(), lock_ids.end()));
    return {lock_ids, latest};
}

template<class ClockType>
timestamp ReliableStorageLock<ClockType>::get_expiration(RefKey lock_key) const {
    auto kv = store_->read_sync(lock_key, storage::ReadKeyOpts{});
    return kv.second.template scalar_at<timestamp>(0, 0).value();
}

template<class ClockType>
void ReliableStorageLock<ClockType>::clear_locks(const std::vector<AcquiredLockId>& lock_ids, bool old_only) const {
    auto now = ClockType::nanos_since_epoch();
    auto to_delete = std::vector<VariantKey>();
    // We only clear locks that have expired more than 10 timeouts (we assume a process can't be paused for more than
    // the timeout) ago. We do this to avoid a process mistakenly taking a lock if:
    // 1. Process A lists locks and gets [4, 5, 6]
    // 2. Process A decides to attempt taking lock 7
    // 3. Process A gets paused
    // 4. Process B takes locks 7 and 8
    // 5. Process B decides to clear lock 7 since it's not the latest
    // 6. Process A succeeds in taking lock 7
    for (auto lock_id : lock_ids) {
        auto lock_key = get_ref_key(lock_id);
        if (!old_only || get_expiration(lock_key) + REMOVE_AFTER_TIMEOUTS * timeout_ < now) {
            to_delete.emplace_back(lock_key);
        }
    }
    store_->remove_keys_sync(to_delete);
}

template<class ClockType>
ReliableLockResult ReliableStorageLock<ClockType>::try_take_lock() const {
    auto [existing_locks, latest] = get_all_locks();
    if (latest.has_value()) {
        auto expires = get_expiration(get_ref_key(latest.value()));
        if (expires > ClockType::nanos_since_epoch()) {
            // An unexpired lock exists
            return LockInUse{};
        }
    }
    auto next_id = get_next_id(latest);
    return try_take_id(existing_locks, next_id);
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

    while (!std::holds_alternative<AcquiredLock>(acquired_lock)) {
        std::this_thread::sleep_for(jittered_wait());
        current_wait = std::min(current_wait * 2, max_wait);
        acquired_lock = try_take_lock();
    }
    return std::get<AcquiredLock>(acquired_lock);
}

template<class ClockType>
ReliableLockResult ReliableStorageLock<ClockType>::try_extend_lock(AcquiredLockId acquired_lock) const {
    auto [existing_locks, latest] = get_all_locks();
    util::check(
            latest.has_value() && latest.value() >= acquired_lock,
            "We are trying to extend a newer lock_id than the existing one in storage. Extend lock_id: {}",
            acquired_lock
    );
    if (latest.value() != acquired_lock) {
        // We have lost the lock while holding it (most likely due to timeout).
        return LockInUse{};
    }
    auto next_id = get_next_id(latest);
    return try_take_id(existing_locks, next_id);
}

template<class ClockType>
void ReliableStorageLock<ClockType>::free_lock(AcquiredLockId acquired_lock) const {
    auto [existing_locks, latest_lock_id] = get_all_locks();
    util::check(
            latest_lock_id.has_value() && latest_lock_id.value() >= acquired_lock,
            "We are trying to free a newer lock_id than the existing one in storage. Free lock_id: {}, Existing "
            "lock_id: {}",
            acquired_lock,
            latest_lock_id
    );
    if (latest_lock_id.value() != acquired_lock) {
        // Lock is already lost
        return;
    }
    auto lock_stream_id = get_stream_id(acquired_lock);
    auto expiration = ClockType::nanos_since_epoch(); // Write current time to mark lock as expired as of now
    store_->write_sync(KeyType::ATOMIC_LOCK, lock_stream_id, lock_segment(lock_stream_id, expiration));
}

template<class ClockType>
std::optional<ActiveLock> ReliableStorageLock<ClockType>::inspect_latest_lock() const {
    auto [existing_locks, latest_lock_id] = get_all_locks();
    if (latest_lock_id.has_value()) {
        auto expiration = get_expiration(get_ref_key(latest_lock_id.value()));
        return {{latest_lock_id.value(), expiration}};
    }
    return std::nullopt;
}

template<class ClockType>
AcquiredLockId ReliableStorageLock<ClockType>::force_take_lock(timestamp custom_timeout) const {
    auto [existing_locks, latest] = get_all_locks();
    auto force_next_id = get_force_next_id(latest);
    auto result = try_take_id(existing_locks, force_next_id, custom_timeout);
    return util::variant_match(
            result,
            [&](AcquiredLock& acquired_lock) {
                log::lock().info("Forcefully acquired a lock with id {}", acquired_lock);
                return acquired_lock;
            },
            [&](LockInUse&) -> AcquiredLockId {
                log::lock().error("Failed to acquire a lock with force.");
                throw LostReliableLock{};
            }
    );
}

template<class ClockType>
ReliableLockResult ReliableStorageLock<ClockType>::try_take_id(
        const std::vector<AcquiredLockId>& existing_locks, AcquiredLockId lock_id,
        std::optional<timestamp> timeout_override
) const {
    auto lock_stream_id = get_stream_id(lock_id);
    auto expiration = ClockType::nanos_since_epoch() + timeout_override.value_or(timeout_);
    try {
        store_->write_if_none_sync(KeyType::ATOMIC_LOCK, lock_stream_id, lock_segment(lock_stream_id, expiration));
    } catch (const AtomicOperationFailedException& e) {
        log::lock().debug("Failed to acquire lock (likely someone acquired it before us): {}", e.what());
        return LockInUse{};
    }
    // We clear old locks only after acquiring the lock to avoid duplicating the deletion work
    clear_locks(existing_locks);
    return AcquiredLock{lock_id};
}

template<class ClockType>
void ReliableStorageLock<ClockType>::force_clear_locks() const {
    auto [existing_locks, latest] = get_all_locks();
    clear_locks(existing_locks, false);
}

inline ReliableStorageLockGuard::ReliableStorageLockGuard(
        const ReliableStorageLock<>& lock, AcquiredLockId acquired_lock, std::optional<folly::Func>&& on_lost_lock
) :
    lock_(lock),
    acquired_lock_(std::nullopt),
    on_lost_lock_(std::move(on_lost_lock)) {
    acquired_lock_ = acquired_lock;
    // We heartbeat 5 times per lock timeout to extend the lock.
    auto hearbeat_frequency = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::nanoseconds(lock_.timeout() / EXTENDS_PER_TIMEOUT)
    );
    extend_lock_heartbeat_.addFunction(
            [that = this]() {
                if (that->acquired_lock_.has_value()) {
                    try {
                        auto result = that->lock_.try_extend_lock(that->acquired_lock_.value());
                        util::variant_match(
                                result,
                                [&](AcquiredLock& acquired_lock) { that->acquired_lock_ = acquired_lock; },
                                [&](LockInUse&) {
                                    // Clean up if we have lost the lock.
                                    log::lock().error("Unexpectedly lost the lock in heartbeating thread. Maybe lock "
                                                      "timeout is too small.");
                                    that->cleanup_on_lost_lock();
                                }
                        );
                    } catch (StorageException& e) {
                        // If we get an unexpected storage exception (e.g. network error) we declare the lock as lost
                        // and still need to exit the heartbeating thread gracefully.
                        log::lock().error(
                                "Received an unexpected storage error in lock heartbeating thread. Assuming lock is "
                                "lost. {}",
                                e.what()
                        );
                        that->cleanup_on_lost_lock();
                    }
                }
            },
            hearbeat_frequency,
            "Extend lock",
            hearbeat_frequency
    );
    extend_lock_heartbeat_.start();
}

inline void ReliableStorageLockGuard::cleanup_on_lost_lock() {
    // We do not use shutdown because we don't want to run it from within a FunctionScheduler thread to avoid a deadlock
    extend_lock_heartbeat_.cancelAllFunctions();
    if (on_lost_lock_.has_value()) {
        on_lost_lock_.value()();
    }
}

inline ReliableStorageLockGuard::~ReliableStorageLockGuard() {
    extend_lock_heartbeat_.shutdown();
    if (acquired_lock_.has_value()) {
        try {
            lock_.free_lock(acquired_lock_.value());
        } catch (const std::exception& e) {
            log::lock().error("Failed to free lock: {}", e.what());
        }
    }
}

inline void ReliableStorageLockGuard::set_on_lost_lock(folly::Func&& on_lost_lock) {
    on_lost_lock_ = std::make_optional<folly::Func>(std::move(on_lost_lock));
    if (!acquired_lock_.has_value()) {
        // Lock was lost before we set on_lost_lock. Running callback immediately.
        on_lost_lock_.value()();
    }
}

inline void ReliableStorageLockManager::take_lock_guard(const ReliableStorageLock<>& lock) {
    auto acquired = lock.retry_until_take_lock();
    guard = std::make_shared<ReliableStorageLockGuard>(lock, acquired, []() { throw LostReliableLock(); });
}

inline void ReliableStorageLockManager::free_lock_guard() { guard = std::nullopt; }

} // namespace lock

} // namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::lock::ReliableLockResult> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(arcticdb::lock::ReliableLockResult result, FormatContext& ctx) const {
        arcticdb::util::variant_match(
                result,
                [&](arcticdb::lock::AcquiredLock& lock) { return fmt::format_to(ctx.out(), "Acquired_lock_{}", lock); },
                [&](arcticdb::lock::LockInUse&) {
                    // Clean up if we have lost the lock.
                    return fmt::format_to(ctx.out(), "Lock in use");
                }
        );
    }
};
} // namespace fmt