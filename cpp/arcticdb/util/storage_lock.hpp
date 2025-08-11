/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/store.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/util/exponential_backoff.hpp>
#include <arcticdb/storage/failure_simulation.hpp>
#include <arcticdb/util/configs_map.hpp>

#include <fmt/std.h>
#include <mutex>
#include <thread>

namespace arcticdb {

namespace {
inline StreamDescriptor lock_stream_descriptor(const StreamId &stream_id) {
    return StreamDescriptor{stream_descriptor(
            stream_id,
            stream::RowCountIndex(),
            {scalar_field(DataType::UINT64, "version")})};
}

SegmentInMemory lock_segment(const StreamId &name, uint64_t timestamp) {
    SegmentInMemory output{lock_stream_descriptor(name)};
    output.set_scalar(0, timestamp);
    output.end_row();
    return output;
}

} // namespace

struct OnExit {
    folly::Func func_;
    bool released_ = false;

    ARCTICDB_NO_MOVE_OR_COPY(OnExit);

    explicit OnExit(folly::Func&& func) :
        func_(std::move(func)) {}

    ~OnExit() {
        if(!released_) {
            // Must not throw in destructor to avoid crashes
            try {
                func_();
            } catch (const std::exception& e) {
                log::lock().error("Exception in OnExit: {}", e.what());
            }
        }
    }

    void release() {
        released_ = true;
    }
};

struct StorageLockTimeout : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

// This StorageLock is inherently unreliable. It does not use atomic operations and it is possible for two processes to acquire if the timing is right.
// If you want a reliable alternative which is slower but uses atomic primitives you can look at the `ReliableStorageLock`.
template <class ClockType = util::SysClock>
class StorageLock {
    std::mutex mutex_;
    const StreamId name_;
    timestamp ts_ = 0;

  public:
    static constexpr int64_t DEFAULT_TTL_INTERVAL = ONE_MINUTE * 60 * 24; // 1 Day
    static constexpr int64_t DEFAULT_WAIT_MS = 1000; // After writing the lock, waiting this time before checking if the written lock is still ours.
    static constexpr int64_t DEFAULT_INITIAL_WAIT_MS = 10;

    static void force_release_lock(const StreamId& name, const std::shared_ptr<Store>& store) {
        do_remove_ref_key(store, name);
    }

    explicit StorageLock(StreamId name) :
        name_(std::move(name)) {}

    ARCTICDB_NO_MOVE_OR_COPY(StorageLock)

    void lock(const std::shared_ptr<Store>& store) {
        do_lock(store);
    }

    void lock_timeout(const std::shared_ptr<Store>& store, size_t timeout_ms) {
        do_lock(store, timeout_ms);
    }

    void unlock(const std::shared_ptr<Store>& store) {
        if(auto read_ts = read_timestamp(store); !read_ts || *read_ts != ts_) {
            log::version().warn("Unexpected lock timestamp, {} != {}", read_ts ? *read_ts : 0, ts_);
            mutex_.unlock();
            return;
        }
        remove_ref_key(store);
        mutex_.unlock();
    }

    bool try_lock(const std::shared_ptr<Store>& store) {
        ARCTICDB_DEBUG(log::lock(), "Storage lock: try lock");
        if(!mutex_.try_lock()) {
            ARCTICDB_DEBUG(log::lock(), "Storage lock: failed local lock");
            return false;
        }

        OnExit x{[that=this] () {
            that->mutex_.unlock();
        }};

        const bool try_lock = try_acquire_lock(store);
        if (try_lock) {
            x.release();
        }

        return try_lock;
    }

    void _test_release_local_lock() {
        mutex_.unlock();
    }

  private:
    void do_lock(const std::shared_ptr<Store>& store, std::optional<size_t> timeout_ms = std::nullopt) {
        mutex_.lock();
        size_t wait_ms = ConfigsMap::instance()->get_int("StorageLock.InitialWaitMs", DEFAULT_INITIAL_WAIT_MS);
        thread_local std::uniform_int_distribution<size_t> dist;
        thread_local std::minstd_rand gen(std::random_device{}());
        size_t total_wait = 0;

        while (!try_acquire_lock(store)) {
            if (timeout_ms && total_wait > *timeout_ms) {
                ts_ = 0;
                log::lock().info("Lock timed out, giving up after {}", wait_ms);
                mutex_.unlock();
                throw StorageLockTimeout{fmt::format("Storage lock {} timeout out after {} ms.", name_, total_wait)};
            }
            wait_ms += dist(gen, decltype(dist)::param_type{0, wait_ms / 2});
            log::lock().info("Didn't get lock, waiting {}", wait_ms);
            sleep_ms(wait_ms);
            total_wait += wait_ms;
            wait_ms *= 2;
        }
    }

    bool try_acquire_lock(const std::shared_ptr<Store>& store) {
        if (!exists_active_lock(store)) {
            ts_ = create_ref_key(store);
            const auto lock_sleep_ms = ConfigsMap::instance()->get_int("StorageLock.WaitMs", DEFAULT_WAIT_MS);
            ARCTICDB_DEBUG(log::lock(), "Waiting for {} ms..", lock_sleep_ms);
            std::this_thread::sleep_for(std::chrono::milliseconds(lock_sleep_ms));
            ARCTICDB_DEBUG(log::lock(), "Waited for {} ms", lock_sleep_ms);
            auto read_ts = read_timestamp(store);
            if(read_ts && *read_ts == ts_) {
                ARCTICDB_DEBUG(log::lock(), "Storage lock: succeeded, written_timestamp: {} current_timestamp: {}", ts_, read_ts);
                return true;
            }
            ARCTICDB_DEBUG(log::lock(), "Storage lock: pre-empted, written_timestamp: {} current_timestamp: {}", ts_, read_ts);
            ts_ = 0;
            return false;
        }
        ARCTICDB_DEBUG(log::lock(), "Storage lock: failed, lock already taken");
        return false;
    }

    void sleep_ms(size_t ms) const {
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    }

    timestamp create_ref_key(const std::shared_ptr<Store>& store) {
        auto ts = ClockType::nanos_since_epoch();
        StorageFailureSimulator::instance()->go(FailureType::WRITE_LOCK);
        store->write_sync(KeyType::LOCK, name_, lock_segment(name_, ts));
        ARCTICDB_DEBUG(log::lock(), "Created lock with timestamp {}", ts);
        return ts;
    }

    static RefKey get_ref_key(const StreamId& name) {
        return RefKey{name, KeyType::LOCK};
    }

    RefKey ref_key() const {
        return get_ref_key(name_);
    }

    static void do_remove_ref_key(const std::shared_ptr<Store>& store, const StreamId& name) {
        ARCTICDB_DEBUG(log::lock(), "Removing ref key");
        try {
            store->remove_key_sync(get_ref_key(name));
        } catch (const storage::KeyNotFoundException&) {
            log::storage().warn("Key not found in storage unlock");
        }
    }

    void remove_ref_key(const std::shared_ptr<Store>& store) const {
        do_remove_ref_key(store, name_);
    }

    std::optional<timestamp> read_timestamp(const std::shared_ptr<Store>& store) const {
        try {
            auto key_seg = store->read_sync(ref_key());
            return key_seg.second.template scalar_at<timestamp>(0, 0).value();
        } catch (const std::invalid_argument&) {
            return std::nullopt;
        } catch (const storage::KeyNotFoundException&) {
            return std::nullopt;
        }
    }

    bool exists_active_lock(const std::shared_ptr<Store>& store) const {
        if (auto read_ts = read_timestamp(store)) {
            // check TTL
            auto ttl = ConfigsMap::instance()->get_int("StorageLock.TTL", DEFAULT_TTL_INTERVAL);
            if (ClockType::coarse_nanos_since_epoch() - *read_ts < ttl) {
                return true;
            }
            log::lock().warn("StorageLock {} taken since {}, which is more than TTL (default 1 day). Ignoring it.", name_, *read_ts);
        }
        return false;
    }
};

class StorageLockWrapper {
    std::shared_ptr<Store> store_;
    std::shared_ptr<StorageLock<>> lock_;

public:
    StorageLockWrapper(const StreamId& stream_id, std::shared_ptr<Store> store) :
        store_(std::move(store)),
        lock_(std::make_shared<StorageLock<>>(stream_id)){
    }

    void lock() {
        lock_->lock(store_);
    }

    void lock_timeout(size_t timeout_ms) {
        lock_->lock_timeout(store_, timeout_ms);
    }

    void unlock() {
        lock_->unlock(store_);
    }

    bool try_lock() {
        return lock_->try_lock(store_);
    }
};

} //namespace arcticdb
