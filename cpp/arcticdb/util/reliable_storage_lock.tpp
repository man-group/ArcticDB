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

inline StreamDescriptor lock_stream_descriptor(const StreamId &stream_id) {
    return StreamDescriptor{stream_descriptor(
            stream_id,
            stream::RowCountIndex(),
            {scalar_field(DataType::INT64, "expiration")})};
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
    auto s3_timeout = ConfigsMap::instance()->get_int("S3Storage.RequestTimeoutMs", 200000) * ONE_MILLISECOND;
    if (2 * s3_timeout > timeout) {
        log::lock().warn(
                "Reliable Lock is created with a timeout < twice the s3 timeout. This is not recommended, as it increases the risk for a faulty lock timeout in s3."
                "Lock timeout: {}, S3 timeout: {}", timeout, s3_timeout);
    }
}

template <class ClockType>
timestamp ReliableStorageLock<ClockType>::timeout() const {
    return timeout_;
}

inline Epoch get_next_epoch(std::optional<Epoch> maybe_prev) {
    if (maybe_prev.has_value()) {
        return maybe_prev.value() + 1;
    }
    return 0;
}

template <class ClockType>
StreamId ReliableStorageLock<ClockType>::get_stream_id(Epoch e) const {
    return fmt::format("{}{}{}", base_name_, SEPARATOR, e);
}

inline Epoch extract_epoch_from_stream_id(const StreamId& stream_id) {
    auto string_id = std::get<StringId>(stream_id);
    auto epoch_string = string_id.substr(string_id.find(SEPARATOR)+1, string_id.size());
    return std::stoull(epoch_string);
}

template <class ClockType>
std::pair<std::vector<Epoch>, std::optional<Epoch>> ReliableStorageLock<ClockType>::get_all_locks() const {
    std::vector<Epoch> epochs;
    store_->iterate_type(
            KeyType::SLOW_LOCK,
            [&epochs](VariantKey &&key){
                auto current_epoch = extract_epoch_from_stream_id(variant_key_id(key));
                epochs.push_back(current_epoch);
            }, base_name_ + SEPARATOR);
    std::optional<Epoch> latest = epochs.size()==0 ? std::nullopt : std::make_optional<>(*std::max_element(epochs.begin(), epochs.end()));
    return {epochs, latest};
}

template<class ClockType>
timestamp ReliableStorageLock<ClockType>::get_expiration(RefKey lock_key) const {
    auto kv = store_->read_sync(lock_key, storage::ReadKeyOpts{});
    return kv.second.template scalar_at<timestamp>(0, 0).value();
}

template <class ClockType>
void ReliableStorageLock<ClockType>::clear_old_locks(const std::vector<Epoch>& epochs) const {
    auto now = ClockType::nanos_since_epoch();
    auto to_delete = std::vector<VariantKey>();
    // We only clear locks that have expired more than a timeout (we assume a process can't be paused for more than the timeout) ago.
    // We do this to avoid a process mistakenly taking a lock if:
    // 1. Process A lists locks and gets [4, 5, 6]
    // 2. Process A decides to attempt taking lock 7
    // 3. Process A gets paused
    // 4. Process B takes locks 7 and 8
    // 5. Process B decides to clear lock 7 since it's not the latest
    // 6. Process A succeeds in taking lock 7
    for (auto epoch : epochs) {
        auto lock_key = RefKey{get_stream_id(epoch), KeyType::SLOW_LOCK};
        if (get_expiration(lock_key) + timeout_ < now) {
            to_delete.emplace_back(lock_key);
        }
    }
    store_->remove_keys_sync(to_delete);
}

template <class ClockType>
std::optional<Epoch> ReliableStorageLock<ClockType>::try_take_lock() const {
    auto [existing_locks, latest] = get_all_locks();
    if (latest.has_value()) {
        auto expires = get_expiration(RefKey{get_stream_id(latest.value()), KeyType::SLOW_LOCK});
        if (expires > ClockType::nanos_since_epoch()) {
            // An unexpired lock exists
            return std::nullopt;
        }
    }
    return try_take_next_epoch(existing_locks, latest);
}

template<class ClockType>
Epoch ReliableStorageLock<ClockType>::retry_until_take_lock() const {
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

    auto aquired_epoch = try_take_lock();
    while (!aquired_epoch.has_value()) {
        std::this_thread::sleep_for(jittered_wait());
        current_wait = std::min(current_wait * 2, max_wait);
        aquired_epoch = try_take_lock();
    }
    return aquired_epoch.value();
}

template <class ClockType>
std::optional<Epoch> ReliableStorageLock<ClockType>::try_extend_lock(Epoch held_lock_epoch) const {
    auto [existing_locks, latest] = get_all_locks();
    util::check(latest.has_value() && latest.value() >= held_lock_epoch,
                "We are trying to extend a newer epoch than the existing one in storage. Extend epoch: {}",
                held_lock_epoch);
    if (latest.value() != held_lock_epoch) {
        // We have lost the lock while holding it (most likely due to timeout).
        return std::nullopt;
    }
    return try_take_next_epoch(existing_locks, latest);
}

template <class ClockType>
void ReliableStorageLock<ClockType>::free_lock(Epoch held_lock_epoch) const {
    auto [existing_locks, latest_epoch] = get_all_locks();
    util::check(latest_epoch.has_value() && latest_epoch.value() >= held_lock_epoch,
                "We are trying to free a newer epoch than the existing one in storage. Free epoch: {}, Existing epoch: {}",
                held_lock_epoch, latest_epoch);
    if (latest_epoch.value() != held_lock_epoch) {
        // Lock is already lost
        return;
    }
    auto lock_stream_id = get_stream_id(held_lock_epoch);
    auto expiration = ClockType::nanos_since_epoch(); // Write current time to mark lock as expired as of now
    store_->write_sync(KeyType::SLOW_LOCK, lock_stream_id, lock_segment(lock_stream_id, expiration));
}

template <class ClockType>
std::optional<Epoch> ReliableStorageLock<ClockType>::try_take_next_epoch(const std::vector<Epoch>& existing_locks, std::optional<Epoch> latest) const {
    Epoch epoch = get_next_epoch(latest);
    auto lock_stream_id = get_stream_id(epoch);
    auto expiration = ClockType::nanos_since_epoch() + timeout_;
    try {
        store_->write_if_none_sync(KeyType::SLOW_LOCK, lock_stream_id, lock_segment(lock_stream_id, expiration));
    } catch (const StorageException& e) {
        // There is no specific Aws::S3::S3Errors for the failed atomic operation, so we catch any StorageException.
        // Either way it's safe to assume we have failed to aquire the lock in case of transient S3 error.
        // If error persists we'll approprieately raise in the next attempt to LIST/GET the existing lock and propagate
        // the transient error.
        log::lock().warn("Failed to aquire lock (likely someone aquired it before us): {}", e.what());
        return std::nullopt;
    }
    // We clear old locks only after aquiring the lock to avoid duplicating the deletion work
    clear_old_locks(existing_locks);
    return epoch;
}

inline ReliableStorageLockGuard::ReliableStorageLockGuard(const ReliableStorageLock<> &lock, Epoch aquired_epoch, folly::Func&& on_lost_lock) :
        lock_(lock), aquired_epoch_(std::nullopt), on_lost_lock_(std::move(on_lost_lock)) {
    aquired_epoch_ = aquired_epoch;
    // We heartbeat 5 times per lock timeout to extend the lock.
    auto hearbeat_frequency = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::nanoseconds(lock_.timeout() / EXTENDS_PER_TIMEOUT));
    extend_lock_heartbeat_.addFunction(
        [that=this](){
            if (that->aquired_epoch_.has_value()) {
                that->aquired_epoch_ = that->lock_.try_extend_lock(that->aquired_epoch_.value());
                if (!that->aquired_epoch_.has_value()) {
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
    if (aquired_epoch_.has_value()) {
        lock_.free_lock(aquired_epoch_.value());
    }
}

inline void ReliableStorageLockManager::take_lock_guard(const ReliableStorageLock<> &lock) {
    auto aquired = lock.retry_until_take_lock();
    guard = std::make_shared<ReliableStorageLockGuard>(lock, aquired, [](){
        throw LostReliableLock();
    });
}

inline void ReliableStorageLockManager::free_lock_guard() {
    guard = std::nullopt;
}

}

}
