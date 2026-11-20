/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/block_hash_check.hpp>
#include <arcticdb/util/hash.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb::codec {

void check_block_hash(
        uint64_t stored_hash, const void* decoded, std::size_t decoded_bytes, std::size_t encoded_bytes,
        std::string_view codec_name
) {
    const auto computed_hash = XXH64(decoded, decoded_bytes, HashAccum::DEFAULT_SEED);
    storage::check<ErrorCode::E_DATA_INTEGRITY>(
            computed_hash == stored_hash,
            "Block hash mismatch: stored {:#018x}, computed {:#018x} ({} codec, {} encoded bytes, {} decoded bytes)",
            stored_hash,
            computed_hash,
            codec_name,
            encoded_bytes,
            decoded_bytes
    );
}

} // namespace arcticdb::codec
