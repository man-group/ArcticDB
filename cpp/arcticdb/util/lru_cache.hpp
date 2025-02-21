/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <shared_mutex>
#include <memory>

#include <ankerl/unordered_dense.h>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/log/log.hpp>

namespace arcticdb {

template<typename KeyType, typename ValueType>
class LRUCache {
    struct Node {
        KeyType key;
        ValueType value;
        Node(const KeyType& k, ValueType&& v) : key(k), value(std::move(v)) {}
    };

    size_t capacity_;
    mutable std::list<Node> list_;
    mutable std::shared_mutex mutex_;
    ankerl::unordered_dense::map<KeyType, typename std::list<Node>::iterator> cache_;

  public:
    explicit LRUCache(size_t capacity) noexcept : capacity_(capacity) {}

    ARCTICDB_NO_MOVE_OR_COPY(LRUCache)

    [[nodiscard]] size_t capacity() const noexcept { return capacity_; }

    [[nodiscard]] std::optional<ValueType> get(const KeyType& key) const {
        std::shared_lock lock(mutex_);
        ARCTICDB_DEBUG(log::inmem(), "Looking for key {}", key);
        auto it = cache_.find(key);
        if (it == cache_.end()) {
            ARCTICDB_DEBUG(log::inmem(), "Key {} does not exist", key);
            return std::nullopt;
        }

        ARCTICDB_DEBUG(log::inmem(), "Key {} found", key);
        list_.splice(list_.begin(), list_, it->second);
        return it->second->value;
    }

    void remove(const KeyType& key) {
        std::unique_lock lock(mutex_);
        ARCTICDB_DEBUG(log::inmem(), "Removing key {}", key);
        auto it = cache_.find(key);
        if (it == cache_.end())
            return;

        list_.erase(it->second);
        cache_.erase(it);
    }

    void put(const KeyType& key, ValueType value) {
        std::unique_lock lock(mutex_);
        ARCTICDB_DEBUG(log::inmem(), "Adding key {}", key);
        auto it = cache_.find(key);
        if (it != cache_.end()) {
            it->second->value = std::move(value);
            list_.splice(list_.begin(), list_, it->second);
        } else {
            if (cache_.size() >= capacity_) {
                ARCTICDB_DEBUG(log::inmem(), "Evicting key {}", list_.back().key);
                cache_.erase(list_.back().key);
                list_.pop_back();
            }
            list_.emplace_front(key, std::move(value));
            cache_[list_.front().key] = list_.begin();
        }
    }
};

} // namespace arcticdb