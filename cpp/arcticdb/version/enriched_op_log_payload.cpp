/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/enriched_op_log_payload.hpp>

#include <arcticdb/codec/magic_words.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/util/buffer.hpp>

#include <xxhash.h>

#include <array>
#include <cstring>
#include <limits>

namespace arcticdb {

namespace {

constexpr std::array<uint8_t, 8> kMagic = {'A', 'D', 'B', 'E', 'O', 'P', 'L', '1'};
constexpr uint32_t kFormatVersion = 1;
constexpr size_t kHeaderBytes = 8 + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint64_t); // 24 bytes

constexpr uint32_t kFlagHasPrevIndexKey = 1u << 0;
constexpr uint32_t kFlagHasColumnStats = 1u << 1;
constexpr uint32_t kFlagHasInlineColumnStats = 1u << 2;
constexpr uint32_t kFlagIsCleanChainSuccessor = 1u << 3;

constexpr uint8_t kVariantTagNumeric = 1;
constexpr uint8_t kVariantTagString = 2;
constexpr uint8_t kVariantTagUnsigned = 3;

// Defensive limits against corrupted/malicious payloads.
constexpr uint32_t kMaxKeysPerPayload = 1000000;
constexpr uint32_t kMaxStringBytes = 64 * 1024;
constexpr uint32_t kMaxBlobBytes = 64 * 1024 * 1024;

void append_u8(std::vector<uint8_t>& out, uint8_t v) { out.push_back(v); }

void append_u32(std::vector<uint8_t>& out, uint32_t v) {
    const size_t pos = out.size();
    out.resize(pos + sizeof(uint32_t));
    out[pos + 0] = static_cast<uint8_t>(v & 0xFFu);
    out[pos + 1] = static_cast<uint8_t>((v >> 8) & 0xFFu);
    out[pos + 2] = static_cast<uint8_t>((v >> 16) & 0xFFu);
    out[pos + 3] = static_cast<uint8_t>((v >> 24) & 0xFFu);
}

void append_u64(std::vector<uint8_t>& out, uint64_t v) {
    const size_t pos = out.size();
    out.resize(pos + sizeof(uint64_t));
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        out[pos + i] = static_cast<uint8_t>((v >> (i * 8)) & 0xFFu);
    }
}

void append_i64(std::vector<uint8_t>& out, int64_t v) { append_u64(out, static_cast<uint64_t>(v)); }

void append_bytes(std::vector<uint8_t>& out, std::span<const uint8_t> bytes) {
    util::check(
            bytes.size() <= std::numeric_limits<uint32_t>::max(),
            "Enriched op-log blob size {} exceeds uint32_t limit",
            bytes.size()
    );
    append_u32(out, static_cast<uint32_t>(bytes.size()));
    if (!bytes.empty()) {
        const size_t pos = out.size();
        out.resize(pos + bytes.size());
        std::memcpy(out.data() + pos, bytes.data(), bytes.size());
    }
}

void append_string(std::vector<uint8_t>& out, std::string_view sv) {
    append_bytes(out, std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(sv.data()), sv.size()));
}

template<typename VariantT>
void append_variant_id(std::vector<uint8_t>& out, const VariantT& v) {
    if (const auto* num = std::get_if<int64_t>(&v)) {
        append_u8(out, kVariantTagNumeric);
        append_i64(out, *num);
    } else if (const auto* str = std::get_if<std::string>(&v)) {
        append_u8(out, kVariantTagString);
        append_string(out, *str);
    } else if (const auto* unum = std::get_if<uint64_t>(&v)) {
        append_u8(out, kVariantTagUnsigned);
        append_u64(out, *unum);
    } else {
        util::raise_rte("Unexpected variant alternative in enriched op-log key encoding");
    }
}

void append_atom_key(std::vector<uint8_t>& out, const entity::AtomKey& key) {
    append_u8(out, static_cast<uint8_t>(key.type()));
    append_variant_id(out, key.id());
    append_u64(out, key.version_id());
    append_i64(out, key.creation_ts());
    append_u64(out, key.content_hash());
    append_variant_id(out, key.start_index());
    append_variant_id(out, key.end_index());
}

struct ByteReader {
    const uint8_t* ptr{nullptr};
    const uint8_t* end{nullptr};

    [[nodiscard]] size_t remaining() const noexcept { return static_cast<size_t>(end - ptr); }

    bool read_u8(uint8_t& out) noexcept {
        if (remaining() < 1) {
            return false;
        }
        out = *ptr++;
        return true;
    }

    bool read_u32(uint32_t& out) noexcept {
        if (remaining() < sizeof(uint32_t)) {
            return false;
        }
        out = static_cast<uint32_t>(ptr[0]) | (static_cast<uint32_t>(ptr[1]) << 8) |
              (static_cast<uint32_t>(ptr[2]) << 16) | (static_cast<uint32_t>(ptr[3]) << 24);
        ptr += sizeof(uint32_t);
        return true;
    }

    bool read_u64(uint64_t& out) noexcept {
        if (remaining() < sizeof(uint64_t)) {
            return false;
        }
        uint64_t val = 0;
        for (size_t i = 0; i < sizeof(uint64_t); ++i) {
            val |= static_cast<uint64_t>(ptr[i]) << (i * 8);
        }
        ptr += sizeof(uint64_t);
        out = val;
        return true;
    }

    bool read_i64(int64_t& out) noexcept {
        uint64_t raw = 0;
        if (!read_u64(raw)) {
            return false;
        }
        out = static_cast<int64_t>(raw);
        return true;
    }

    bool read_bytes(std::vector<uint8_t>& out, uint32_t max_len) {
        uint32_t len = 0;
        if (!read_u32(len) || len > max_len || remaining() < len) {
            return false;
        }
        out.resize(len);
        if (len > 0) {
            std::memcpy(out.data(), ptr, len);
            ptr += len;
        }
        return true;
    }

    bool read_string(std::string& out) {
        uint32_t len = 0;
        if (!read_u32(len) || len > kMaxStringBytes || remaining() < len) {
            return false;
        }
        out.assign(reinterpret_cast<const char*>(ptr), len);
        ptr += len;
        return true;
    }

    template<typename VariantT>
    bool read_variant_id(VariantT& out) {
        uint8_t tag = 0;
        if (!read_u8(tag)) {
            return false;
        }
        if (tag == kVariantTagNumeric) {
            int64_t v = 0;
            if (!read_i64(v)) {
                return false;
            }
            out = v;
            return true;
        }
        if (tag == kVariantTagString) {
            std::string s;
            if (!read_string(s)) {
                return false;
            }
            out = std::move(s);
            return true;
        }
        if (tag == kVariantTagUnsigned) {
            uint64_t v = 0;
            if (!read_u64(v)) {
                return false;
            }
            out = v;
            return true;
        }
        return false;
    }

    bool read_atom_key(entity::AtomKey& out) {
        uint8_t kt_raw = 0;
        if (!read_u8(kt_raw)) {
            return false;
        }
        if (kt_raw >= static_cast<uint8_t>(entity::KeyType::UNDEFINED)) {
            return false;
        }
        StreamId id;
        uint64_t version_id = 0;
        int64_t creation_ts = 0;
        uint64_t content_hash = 0;
        entity::IndexValue start_idx;
        entity::IndexValue end_idx;

        if (!read_variant_id(id) || !read_u64(version_id) || !read_i64(creation_ts) || !read_u64(content_hash) ||
            !read_variant_id(start_idx) || !read_variant_id(end_idx)) {
            return false;
        }

        out = entity::AtomKeyBuilder()
                      .version_id(version_id)
                      .creation_ts(creation_ts)
                      .content_hash(content_hash)
                      .start_index(start_idx)
                      .end_index(end_idx)
                      .build(std::move(id), static_cast<entity::KeyType>(kt_raw));
        return true;
    }
};

} // namespace

std::vector<uint8_t> serialize_segment_to_bytes(Segment& segment) {
    (void)segment.calculate_size();
    auto [ptr, len, owned_buf] = segment.serialize_header();
    if (ptr == nullptr || len == 0) {
        return {};
    }
    return std::vector<uint8_t>(ptr, ptr + len);
}

std::optional<Segment> deserialize_segment_from_bytes(std::span<const uint8_t> bytes) {
    if (bytes.size() < FIXED_HEADER_SIZE) {
        return std::nullopt;
    }
    try {
        auto buf = std::make_shared<Buffer>();
        buf->ensure(bytes.size());
        std::memcpy(buf->data(), bytes.data(), bytes.size());
        buf->set_bytes(bytes.size());
        return Segment::from_buffer(buf);
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

std::vector<uint8_t> encode_enriched_op_log_payload(const EnrichedOpLogPayload& payload) {
    std::vector<uint8_t> out;
    out.reserve(kHeaderBytes + payload.compressed_index_bytes.size() + 256);

    // Reserve space for header; fill after body is encoded so we can compute XXH64.
    out.resize(kHeaderBytes, 0);

    append_atom_key(out, payload.new_index_key);
    if (payload.prev_index_key.has_value()) {
        append_atom_key(out, *payload.prev_index_key);
    }

    append_bytes(out, payload.compressed_index_bytes);

    util::check(
            payload.newly_written_data_keys.size() <= kMaxKeysPerPayload,
            "Too many newly written data keys in enriched op-log payload: {}",
            payload.newly_written_data_keys.size()
    );
    append_u32(out, static_cast<uint32_t>(payload.newly_written_data_keys.size()));
    for (const auto& dk : payload.newly_written_data_keys) {
        append_atom_key(out, dk);
    }

    const bool write_col_stats = payload.has_column_stats && payload.column_stats_key.has_value();
    const bool write_inline_col_stats = write_col_stats && !payload.column_stats_bytes.empty();
    if (write_col_stats) {
        append_atom_key(out, *payload.column_stats_key);
        if (write_inline_col_stats) {
            append_bytes(out, payload.column_stats_bytes);
        }
    }

    uint32_t flags = 0;
    if (payload.prev_index_key.has_value()) {
        flags |= kFlagHasPrevIndexKey;
    }
    if (write_col_stats) {
        flags |= kFlagHasColumnStats;
    }
    if (write_inline_col_stats) {
        flags |= kFlagHasInlineColumnStats;
    }
    if (payload.is_clean_chain_successor) {
        flags |= kFlagIsCleanChainSuccessor;
    }

    const uint64_t body_hash = XXH64(out.data() + kHeaderBytes, out.size() - kHeaderBytes, 0);

    std::memcpy(out.data(), kMagic.data(), kMagic.size());
    out[8] = static_cast<uint8_t>(kFormatVersion & 0xFFu);
    out[9] = static_cast<uint8_t>((kFormatVersion >> 8) & 0xFFu);
    out[10] = static_cast<uint8_t>((kFormatVersion >> 16) & 0xFFu);
    out[11] = static_cast<uint8_t>((kFormatVersion >> 24) & 0xFFu);

    out[12] = static_cast<uint8_t>(flags & 0xFFu);
    out[13] = static_cast<uint8_t>((flags >> 8) & 0xFFu);
    out[14] = static_cast<uint8_t>((flags >> 16) & 0xFFu);
    out[15] = static_cast<uint8_t>((flags >> 24) & 0xFFu);

    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        out[16 + i] = static_cast<uint8_t>((body_hash >> (i * 8)) & 0xFFu);
    }

    return out;
}

std::optional<EnrichedOpLogPayload> decode_enriched_op_log_payload(std::span<const uint8_t> bytes) {
    if (bytes.size() < kHeaderBytes) {
        return std::nullopt;
    }
    if (std::memcmp(bytes.data(), kMagic.data(), kMagic.size()) != 0) {
        return std::nullopt;
    }

    ByteReader reader{bytes.data() + kMagic.size(), bytes.data() + bytes.size()};
    uint32_t version = 0;
    uint32_t flags = 0;
    uint64_t expected_hash = 0;
    if (!reader.read_u32(version) || !reader.read_u32(flags) || !reader.read_u64(expected_hash)) {
        return std::nullopt;
    }
    if (version != kFormatVersion) {
        return std::nullopt;
    }

    const uint64_t actual_hash = XXH64(reader.ptr, reader.remaining(), 0);
    if (actual_hash != expected_hash) {
        return std::nullopt;
    }

    EnrichedOpLogPayload payload;
    payload.is_clean_chain_successor = (flags & kFlagIsCleanChainSuccessor) != 0;

    if (!reader.read_atom_key(payload.new_index_key)) {
        return std::nullopt;
    }

    if ((flags & kFlagHasPrevIndexKey) != 0) {
        entity::AtomKey prev_key;
        if (!reader.read_atom_key(prev_key)) {
            return std::nullopt;
        }
        payload.prev_index_key = std::move(prev_key);
    }

    if (!reader.read_bytes(payload.compressed_index_bytes, kMaxBlobBytes) || payload.compressed_index_bytes.empty()) {
        return std::nullopt;
    }

    uint32_t num_data_keys = 0;
    if (!reader.read_u32(num_data_keys) || num_data_keys > kMaxKeysPerPayload) {
        return std::nullopt;
    }
    payload.newly_written_data_keys.reserve(num_data_keys);
    for (uint32_t i = 0; i < num_data_keys; ++i) {
        entity::AtomKey dk;
        if (!reader.read_atom_key(dk)) {
            return std::nullopt;
        }
        payload.newly_written_data_keys.push_back(std::move(dk));
    }

    if ((flags & kFlagHasColumnStats) != 0) {
        payload.has_column_stats = true;
        entity::AtomKey cs_key;
        if (!reader.read_atom_key(cs_key)) {
            return std::nullopt;
        }
        payload.column_stats_key = std::move(cs_key);
        if ((flags & kFlagHasInlineColumnStats) != 0) {
            if (!reader.read_bytes(payload.column_stats_bytes, kMaxBlobBytes) || payload.column_stats_bytes.empty()) {
                return std::nullopt;
            }
        }
    }

    if (reader.remaining() != 0) {
        return std::nullopt;
    }

    return payload;
}

SegmentInMemory make_enriched_op_log_segment(
        const std::string& action, std::span<const uint8_t> encoded_payload
) {
    auto desc = stream::stream_descriptor(
            action,
            stream::RowCountIndex(),
            {entity::scalar_field(entity::DataType::ASCII_DYNAMIC64, std::string{kEnrichedOpLogPayloadColumn})}
    );
    SegmentInMemory seg{std::move(desc)};
    seg.set_string(0, std::string_view(reinterpret_cast<const char*>(encoded_payload.data()), encoded_payload.size()));
    seg.end_row();
    return seg;
}

SegmentInMemory make_enriched_op_log_segment(const std::string& action, const EnrichedOpLogPayload& payload) {
    const auto encoded = encode_enriched_op_log_payload(payload);
    return make_enriched_op_log_segment(action, encoded);
}

std::optional<EnrichedOpLogPayload> extract_enriched_op_log_payload(const SegmentInMemory& seg) {
    if (seg.num_columns() != 1 || seg.row_count() != 1) {
        return std::nullopt;
    }
    if (seg.descriptor().field(0).name() != kEnrichedOpLogPayloadColumn) {
        return std::nullopt;
    }
    const auto sv = seg.string_at(0, 0);
    if (!sv.has_value() || sv->empty()) {
        return std::nullopt;
    }
    return decode_enriched_op_log_payload(
            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(sv->data()), sv->size())
    );
}

CapturedVersionWrite& WriteCaptureCache::get_or_create_locked(const CacheKey& key) {
    auto it = entries_.find(key);
    if (it != entries_.end()) {
        lru_.splice(lru_.end(), lru_, it->second);
        return it->second->second;
    }
    while (entries_.size() >= kMaxCachedEntries && !lru_.empty()) {
        entries_.erase(lru_.front().first);
        lru_.pop_front();
    }
    auto list_it = lru_.emplace(lru_.end(), key, CapturedVersionWrite{});
    try {
        entries_.emplace(key, list_it);
    } catch (...) {
        lru_.erase(list_it);
        throw;
    }
    return list_it->second;
}

void WriteCaptureCache::record_data(const entity::AtomKey& key) {
    std::lock_guard lock(mutex_);
    auto& entry = get_or_create_locked(CacheKey{key.id(), key.version_id()});
    entry.newly_written_data_keys.push_back(key);
}

void WriteCaptureCache::record_index(const entity::AtomKey& key, Segment& segment) {
    const size_t max_index_bytes = enriched_op_log_max_index_bytes();
    const size_t seg_size = segment.calculate_size();
    if (max_index_bytes == 0 || seg_size > max_index_bytes) {
        discard(key.id(), key.version_id());
        return;
    }
    auto serialized = serialize_segment_to_bytes(segment);

    std::lock_guard lock(mutex_);
    auto& entry = get_or_create_locked(CacheKey{key.id(), key.version_id()});
    entry.index_key = key;
    entry.compressed_index_bytes = std::move(serialized);
}

void WriteCaptureCache::record_column_stats(const entity::AtomKey& key, Segment& segment) {
    const size_t max_meta_bytes = enriched_op_log_max_index_bytes();
    const size_t seg_size = segment.calculate_size();
    std::vector<uint8_t> serialized;
    if (max_meta_bytes > 0 && seg_size <= max_meta_bytes) {
        serialized = serialize_segment_to_bytes(segment);
    }

    std::lock_guard lock(mutex_);
    auto& entry = get_or_create_locked(CacheKey{key.id(), key.version_id()});
    entry.column_stats_key = key;
    entry.column_stats_bytes = std::move(serialized);
}

std::optional<CapturedVersionWrite> WriteCaptureCache::take(const StreamId& stream_id, entity::VersionId version_id) {
    std::lock_guard lock(mutex_);
    auto it = entries_.find(CacheKey{stream_id, version_id});
    if (it == entries_.end()) {
        return std::nullopt;
    }
    auto out = std::move(it->second->second);
    lru_.erase(it->second);
    entries_.erase(it);
    return out;
}

void WriteCaptureCache::discard(const StreamId& stream_id, entity::VersionId version_id) {
    std::lock_guard lock(mutex_);
    auto it = entries_.find(CacheKey{stream_id, version_id});
    if (it != entries_.end()) {
        lru_.erase(it->second);
        entries_.erase(it);
    }
}

void WriteCaptureCache::clear() {
    std::lock_guard lock(mutex_);
    entries_.clear();
    lru_.clear();
}

size_t WriteCaptureCache::size() const {
    std::lock_guard lock(mutex_);
    return entries_.size();
}

size_t WriteCaptureCache::order_size() const {
    std::lock_guard lock(mutex_);
    return lru_.size();
}

} // namespace arcticdb
