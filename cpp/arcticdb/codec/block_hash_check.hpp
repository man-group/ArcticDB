/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/configs_map.hpp>

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace arcticdb::codec {

/// Codec.VerifyBlockHash: when non-zero, every decoded block is checked against the hash its encoder stored.
inline bool block_hash_verification_enabled() {
    static const std::atomic<int64_t>& option = ConfigsMap::instance()->watch_int("Codec.VerifyBlockHash", 0);
    return option.load(std::memory_order_relaxed) != 0;
}

/// Raises DataIntegrityException unless `decoded` hashes to `stored_hash` (XXH64, the encoders' seed).
void check_block_hash(
        uint64_t stored_hash, const void* decoded, std::size_t decoded_bytes, std::size_t encoded_bytes,
        std::string_view codec_name
);

} // namespace arcticdb::codec
