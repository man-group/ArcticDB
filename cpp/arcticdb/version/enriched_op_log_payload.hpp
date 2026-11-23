/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/codec/segment.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/configs_map.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace arcticdb {

// Config keys (general-purpose core VersionStore op-log settings).
inline constexpr const char* kEnrichedOpLogConfigKey = "VersionStore.EnrichedOpLog";
inline constexpr const char* kEnrichedOpLogMaxIndexBytesConfigKey = "VersionStore.EnrichedOpLogMaxIndexBytes";

inline constexpr int64_t kDefaultEnrichedOpLogMaxIndexBytes = 4 * 1024 * 1024;
inline constexpr std::string_view kEnrichedOpLogPayloadColumn = "__enriched_op_log__";

[[nodiscard]] inline bool is_enriched_op_log_enabled() {
    return ConfigsMap::instance()->get_int(kEnrichedOpLogConfigKey, 0) != 0;
}

[[nodiscard]] inline size_t enriched_op_log_max_index_bytes() {
    const int64_t raw =
            ConfigsMap::instance()->get_int(kEnrichedOpLogMaxIndexBytesConfigKey, kDefaultEnrichedOpLogMaxIndexBytes);
    return static_cast<size_t>(std::max<int64_t>(0, raw));
}

// Self-describing payload embedded inside an existing KeyType::LOG write segment.
// Older readers that only inspect KeyType::LOG key names ignore this payload harmlessly.
struct EnrichedOpLogPayload {
    entity::AtomKey new_index_key;
    std::optional<entity::AtomKey> prev_index_key;
    bool is_clean_chain_successor{false};
    std::vector<uint8_t> compressed_index_bytes;
    std::vector<entity::AtomKey> newly_written_data_keys;
    bool has_column_stats{false};
    std::optional<entity::AtomKey> column_stats_key;
    std::vector<uint8_t> column_stats_bytes;
};

// Serializes an already-compressed Segment (V1 or V2) into its exact wire-format bytes.
[[nodiscard]] std::vector<uint8_t> serialize_segment_to_bytes(Segment& segment);

// Reconstructs an owning compressed Segment (with valid V1/V2 preamble) from wire-format bytes.
// Returns std::nullopt if the bytes are truncated or corrupt.
[[nodiscard]] std::optional<Segment> deserialize_segment_from_bytes(std::span<const uint8_t> bytes);

// Encodes/decodes an EnrichedOpLogPayload with magic header, version, flags, and XXH64 checksum.
// Decoding validates checksum, bounds, and exact consumption; returns std::nullopt on any error.
[[nodiscard]] std::vector<uint8_t> encode_enriched_op_log_payload(const EnrichedOpLogPayload& payload);
[[nodiscard]] std::optional<EnrichedOpLogPayload> decode_enriched_op_log_payload(std::span<const uint8_t> bytes);

// Wraps an already-encoded binary payload into a 1-row SegmentInMemory suitable for KeyType::LOG.
[[nodiscard]] SegmentInMemory make_enriched_op_log_segment(
        const std::string& action, std::span<const uint8_t> encoded_payload
);
[[nodiscard]] SegmentInMemory make_enriched_op_log_segment(
        const std::string& action, const EnrichedOpLogPayload& payload
);

// Extracts and validates an EnrichedOpLogPayload from a decoded KeyType::LOG SegmentInMemory.
// Returns std::nullopt for legacy 0-row op-log segments or any malformed/corrupted payload.
[[nodiscard]] std::optional<EnrichedOpLogPayload> extract_enriched_op_log_payload(const SegmentInMemory& seg);

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
class WriteCaptureCache {
  public:
    static constexpr size_t kMaxCachedEntries = 1024;

    void record_data(const entity::AtomKey& key);
    void record_adopted_data(
            const StreamId& stream_id, entity::VersionId target_version_id, std::span<const entity::AtomKey> keys
    );
    void record_index(const entity::AtomKey& key, Segment& segment);
    void record_column_stats(const entity::AtomKey& key, Segment& segment);

    [[nodiscard]] std::optional<CapturedVersionWrite> take(const StreamId& stream_id, entity::VersionId version_id);
    void discard(const StreamId& stream_id, entity::VersionId version_id);
    void clear();

    [[nodiscard]] size_t size() const;
    [[nodiscard]] size_t order_size() const;

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

    using EntryList = std::list<std::pair<CacheKey, CapturedVersionWrite>>;

    CapturedVersionWrite& get_or_create_locked(const CacheKey& key);

    mutable std::mutex mutex_;
    EntryList lru_;
    std::unordered_map<CacheKey, EntryList::iterator, CacheKeyHash> entries_;
};

} // namespace arcticdb
