/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/write_capture_cache.hpp>

#include <arcticdb/codec/segment.hpp>
#include <arcticdb/storage/key_segment_pair.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/version/enriched_op_log_payload.hpp>

#include <algorithm>

namespace arcticdb {

bool is_enriched_op_log_enabled() { return ConfigsMap::instance()->get_int(kEnrichedOpLogConfigKey, 0) != 0; }

size_t enriched_op_log_max_index_bytes() {
    const int64_t raw =
            ConfigsMap::instance()->get_int(kEnrichedOpLogMaxIndexBytesConfigKey, kDefaultEnrichedOpLogMaxIndexBytes);
    const int64_t clamped = std::clamp<int64_t>(raw, 0, static_cast<int64_t>(kMaxEnrichedOpLogBlobBytes));
    return static_cast<size_t>(clamped);
}

void WriteCaptureCache::sync_tracked_count_locked() noexcept {
    tracked_count_.store(entries_.size() + tainted_.size(), std::memory_order_release);
}

void WriteCaptureCache::erase_entry_locked(std::unordered_map<CacheKey, EntryList::iterator, CacheKeyHash>::iterator it
) noexcept {
    total_bytes_ -= it->second->bytes;
    lru_.erase(it->second);
    entries_.erase(it);
}

void WriteCaptureCache::taint_locked(const CacheKey& key) {
    if (auto it = entries_.find(key); it != entries_.end()) {
        erase_entry_locked(it);
    }
    if (auto tit = tainted_.find(key); tit != tainted_.end()) {
        tainted_lru_.splice(tainted_lru_.end(), tainted_lru_, tit->second);
        sync_tracked_count_locked();
        return;
    }
    while (tainted_.size() >= kMaxTaintedEntries && !tainted_lru_.empty()) {
        tainted_.erase(tainted_lru_.front());
        tainted_lru_.pop_front();
    }
    auto list_it = tainted_lru_.emplace(tainted_lru_.end(), key);
    try {
        tainted_.emplace(key, list_it);
    } catch (...) {
        tainted_lru_.erase(list_it);
        sync_tracked_count_locked();
        throw;
    }
    sync_tracked_count_locked();
}

void WriteCaptureCache::evict_to_fit_locked(size_t extra_bytes, const CacheKey* protected_key) {
    auto it = lru_.begin();
    while (it != lru_.end() && (total_bytes_ + extra_bytes > kMaxCachedBytes)) {
        if (protected_key != nullptr && it->key == *protected_key) {
            ++it;
            continue;
        }
        const CacheKey evicted_key = it->key;
        ++it;
        taint_locked(evicted_key);
    }
}

WriteCaptureCache::EntryList::iterator WriteCaptureCache::get_or_create_locked(const CacheKey& key) {
    auto it = entries_.find(key);
    if (it != entries_.end()) {
        lru_.splice(lru_.end(), lru_, it->second);
        return it->second;
    }
    while (entries_.size() >= kMaxCachedEntries && !lru_.empty()) {
        const CacheKey evicted_key = lru_.front().key;
        taint_locked(evicted_key);
    }
    auto list_it = lru_.emplace(lru_.end(), CachedSlot{key, CapturedVersionWrite{}, 0});
    try {
        entries_.emplace(key, list_it);
    } catch (...) {
        lru_.erase(list_it);
        throw;
    }
    sync_tracked_count_locked();
    return list_it;
}

void WriteCaptureCache::record_data(const entity::AtomKey& key) {
    std::lock_guard lock(mutex_);
    const CacheKey cache_key{key.id(), key.version_id()};
    if (tainted_.contains(cache_key)) {
        return;
    }
    auto slot_it = get_or_create_locked(cache_key);
    const size_t add_bytes = sizeof(entity::AtomKey);
    if (slot_it->bytes + add_bytes > kMaxCachedBytes) {
        taint_locked(cache_key);
        return;
    }
    evict_to_fit_locked(add_bytes, &cache_key);
    slot_it->value.newly_written_data_keys.push_back(key);
    slot_it->bytes += add_bytes;
    total_bytes_ += add_bytes;
}

void WriteCaptureCache::record_index(const entity::AtomKey& key, Segment& segment) {
    const size_t max_index_bytes = enriched_op_log_max_index_bytes();
    const size_t seg_size = segment.calculate_size();
    const CacheKey cache_key{key.id(), key.version_id()};
    if (max_index_bytes == 0 || seg_size > max_index_bytes || seg_size > kMaxCachedBytes) {
        std::lock_guard lock(mutex_);
        taint_locked(cache_key);
        return;
    }
    auto serialized = serialize_segment_to_bytes(segment);

    std::lock_guard lock(mutex_);
    if (tainted_.contains(cache_key)) {
        return;
    }
    auto slot_it = get_or_create_locked(cache_key);
    const size_t old_index_bytes = slot_it->value.compressed_index_bytes.size();
    const size_t new_index_bytes = serialized.size();
    const size_t new_slot_bytes = slot_it->bytes - old_index_bytes + new_index_bytes;
    if (new_slot_bytes > kMaxCachedBytes) {
        taint_locked(cache_key);
        return;
    }
    if (new_index_bytes > old_index_bytes) {
        evict_to_fit_locked(new_index_bytes - old_index_bytes, &cache_key);
    }
    total_bytes_ = total_bytes_ - old_index_bytes + new_index_bytes;
    slot_it->bytes = new_slot_bytes;
    slot_it->value.index_key = key;
    slot_it->value.compressed_index_bytes = std::move(serialized);
}

void WriteCaptureCache::record_column_stats(const entity::AtomKey& key, Segment& segment) {
    const size_t max_meta_bytes = enriched_op_log_max_index_bytes();
    const size_t seg_size = segment.calculate_size();
    std::vector<uint8_t> serialized;
    if (max_meta_bytes > 0 && seg_size <= max_meta_bytes && seg_size <= kMaxCachedBytes) {
        serialized = serialize_segment_to_bytes(segment);
    }

    std::lock_guard lock(mutex_);
    const CacheKey cache_key{key.id(), key.version_id()};
    if (tainted_.contains(cache_key)) {
        return;
    }
    auto slot_it = get_or_create_locked(cache_key);
    const size_t old_cs_bytes = slot_it->value.column_stats_bytes.size();
    size_t new_cs_bytes = serialized.size();
    if (slot_it->bytes - old_cs_bytes + new_cs_bytes > kMaxCachedBytes) {
        serialized.clear();
        new_cs_bytes = 0;
    }
    if (new_cs_bytes > old_cs_bytes) {
        evict_to_fit_locked(new_cs_bytes - old_cs_bytes, &cache_key);
    }
    total_bytes_ = total_bytes_ - old_cs_bytes + new_cs_bytes;
    slot_it->bytes = slot_it->bytes - old_cs_bytes + new_cs_bytes;
    slot_it->value.column_stats_key = key;
    slot_it->value.column_stats_bytes = std::move(serialized);
}

void WriteCaptureCache::record_key_segment(storage::KeySegmentPair& ks) {
    if (!std::holds_alternative<entity::AtomKey>(ks.variant_key())) {
        return;
    }
    const auto& ak = ks.atom_key();
    if (ak.type() == entity::KeyType::TABLE_DATA) {
        record_data(ak);
        return;
    }
    auto seg_ptr = ks.segment_ptr();
    if (!seg_ptr) {
        return;
    }
    if (ak.type() == entity::KeyType::TABLE_INDEX) {
        record_index(ak, *seg_ptr);
    } else if (ak.type() == entity::KeyType::COLUMN_STATS) {
        record_column_stats(ak, *seg_ptr);
    }
}

std::optional<CapturedVersionWrite> WriteCaptureCache::take(const StreamId& stream_id, entity::VersionId version_id) {
    if (tracked_count_.load(std::memory_order_acquire) == 0) {
        return std::nullopt;
    }
    std::lock_guard lock(mutex_);
    const CacheKey cache_key{stream_id, version_id};
    if (auto tit = tainted_.find(cache_key); tit != tainted_.end()) {
        tainted_lru_.erase(tit->second);
        tainted_.erase(tit);
        sync_tracked_count_locked();
        return std::nullopt;
    }
    auto it = entries_.find(cache_key);
    if (it == entries_.end()) {
        return std::nullopt;
    }
    auto out = std::move(it->second->value);
    erase_entry_locked(it);
    sync_tracked_count_locked();
    return out;
}

void WriteCaptureCache::discard(const StreamId& stream_id, entity::VersionId version_id) {
    if (tracked_count_.load(std::memory_order_acquire) == 0) {
        return;
    }
    std::lock_guard lock(mutex_);
    const CacheKey cache_key{stream_id, version_id};
    if (auto it = entries_.find(cache_key); it != entries_.end()) {
        erase_entry_locked(it);
    }
    if (auto tit = tainted_.find(cache_key); tit != tainted_.end()) {
        tainted_lru_.erase(tit->second);
        tainted_.erase(tit);
    }
    sync_tracked_count_locked();
}

size_t WriteCaptureCache::size() const {
    std::lock_guard lock(mutex_);
    return entries_.size();
}

size_t WriteCaptureCache::total_bytes() const {
    std::lock_guard lock(mutex_);
    return total_bytes_;
}

} // namespace arcticdb
