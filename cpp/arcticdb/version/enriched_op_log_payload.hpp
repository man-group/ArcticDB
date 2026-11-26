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
#include <arcticdb/version/write_capture_cache.hpp>

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace arcticdb {

inline constexpr std::string_view kEnrichedOpLogPayloadColumn = "__enriched_op_log__";

// Self-describing payload embedded inside an existing KeyType::LOG write segment.
// Older readers that only inspect KeyType::LOG key names ignore this payload harmlessly.
struct EnrichedOpLogPayload {
    entity::AtomKey new_index_key;
    std::optional<entity::AtomKey> prev_index_key;
    bool is_clean_chain_successor{false};
    std::vector<uint8_t> compressed_index_bytes;
    std::vector<entity::AtomKey> newly_written_data_keys;
    std::optional<entity::AtomKey> column_stats_key;
    std::vector<uint8_t> column_stats_bytes;
};

// Serializes an already-compressed Segment (V1 or V2) into its exact wire-format bytes.
[[nodiscard]] std::vector<uint8_t> serialize_segment_to_bytes(Segment& segment);

// Reconstructs an owning compressed Segment (with valid V1/V2 preamble) from wire-format bytes.
// Validates header and payload bounds before invoking Segment::from_buffer; returns std::nullopt if truncated/corrupt.
[[nodiscard]] std::optional<Segment> deserialize_segment_from_bytes(std::span<const uint8_t> bytes);

// Encodes/decodes an EnrichedOpLogPayload with magic header, version, flags, and XXH64 checksum covering
// version, flags, and body. Decoding validates checksum, bounds, and exact consumption; returns std::nullopt on error.
[[nodiscard]] std::vector<uint8_t> encode_enriched_op_log_payload(const EnrichedOpLogPayload& payload);
[[nodiscard]] std::optional<EnrichedOpLogPayload> decode_enriched_op_log_payload(std::span<const uint8_t> bytes);

// Wraps an already-encoded binary payload into a 1-row SegmentInMemory suitable for KeyType::LOG.
[[nodiscard]] SegmentInMemory make_enriched_op_log_segment(
        const std::string& action, std::span<const uint8_t> encoded_payload
);

// Extracts and validates an EnrichedOpLogPayload from a decoded KeyType::LOG SegmentInMemory.
// Returns std::nullopt for legacy 0-row op-log segments or any malformed/corrupted payload.
[[nodiscard]] std::optional<EnrichedOpLogPayload> extract_enriched_op_log_payload(const SegmentInMemory& seg);

} // namespace arcticdb
