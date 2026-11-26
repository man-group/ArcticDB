/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/variant_key.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <list>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

namespace arcticdb {

class Segment;

namespace storage {
class KeySegmentPair;
}

inline constexpr const char* kEnrichedOpLogConfigKey = "VersionStore.EnrichedOpLog";
inline constexpr const char* kEnrichedOpLogMaxIndexBytesConfigKey = "VersionStore.EnrichedOpLogMaxIndexBytes";

inline constexpr int64_t kDefaultEnrichedOpLogMaxIndexBytes = int64_t{4} * 1024 * 1024;
inline constexpr uint32_t kMaxEnrichedOpLogKeysPerPayload = 1'000'000;
inline constexpr uint32_t kMaxEnrichedOpLogBlobBytes = 64u * 1024u * 1024u;

[[nodiscard]] bool is_enriched_op_log_enabled();
[[nodiscard]] size_t enriched_op_log_max_index_bytes();

// Captures TABLE_DATA key metadata and compressed TABLE_INDEX / COLUMN_STATS segments written during a
// single version write so they can be embedded in the subsequent KeyType::LOG PutObject with 0 extra RPCs.
struct CapturedVersionWrite {
    std::optional<entity::AtomKey> index_key;
    std::vector<uint8_t> compressed_index_bytes;
    std::vector<entity::AtomKey> newly_written_data_keys;
    std::optional<entity::AtomKey> column_stats_key;
    std::vector<uint8_t> column_stats_bytes;
};

// Thread-safe, memory-bounded per-Store capture cache keyed by (StreamId, VersionId).
// Evicted or oversized keys are tainted until take()/discard() so a partial write can never be resurrected
// into an enriched op-log missing earlier TABLE_DATA keys.
class WriteCaptureCache {
  public:
    static constexpr size_t kMaxCachedEntries = 1024;
    static constexpr size_t kMaxCachedBytes = size_t{64} * 1024 * 1024;
    static constexpr size_t kMaxTaintedEntries = 65536;

    void record_data(const entity::AtomKey& key);
    void record_index(const entity::AtomKey& key, Segment& segment);
    void record_column_stats(const entity::AtomKey& key, Segment& segment);
    void record_key_segment(storage::KeySegmentPair& ks);

    [[nodiscard]] std::optional<CapturedVersionWrite> take(const StreamId& stream_id, entity::VersionId version_id);
    void discard(const StreamId& stream_id, entity::VersionId version_id);

    [[nodiscard]] size_t size() const;
    [[nodiscard]] size_t total_bytes() const;

  private:
    struct CacheKey {
        StreamId stream_id;
        entity::VersionId version_id{0};

        bool operator==(const CacheKey&) const = default;
    };

    struct CacheKeyHash {
        size_t operator()(const CacheKey& k) const noexcept {
            return folly::hash::hash_combine(k.stream_id, k.version_id);
        }
    };

    struct CachedSlot {
        CacheKey key;
        CapturedVersionWrite value;
        size_t bytes{0};
    };

    using EntryList = std::list<CachedSlot>;
    using TaintedList = std::list<CacheKey>;

    void sync_tracked_count_locked() noexcept;
    void erase_entry_locked(std::unordered_map<CacheKey, EntryList::iterator, CacheKeyHash>::iterator it) noexcept;
    void taint_locked(const CacheKey& key);
    void evict_to_fit_locked(size_t extra_bytes, const CacheKey* protected_key);
    EntryList::iterator get_or_create_locked(const CacheKey& key);

    mutable std::mutex mutex_;
    std::atomic<size_t> tracked_count_{0};
    size_t total_bytes_{0};
    EntryList lru_;
    std::unordered_map<CacheKey, EntryList::iterator, CacheKeyHash> entries_;
    TaintedList tainted_lru_;
    std::unordered_map<CacheKey, TaintedList::iterator, CacheKeyHash> tainted_;
};

} // namespace arcticdb
