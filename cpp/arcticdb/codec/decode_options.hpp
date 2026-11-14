/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/memory_layout.hpp>

namespace arcticdb {

class SegmentHeader;

/// How to decode the fields of one segment: derived from its header and ConfigsMap, unlike the user's ReadOptions.
struct DecodeOptions {
    EncodingVersion encoding_version;
    /// Check every decoded block against the hash its encoder stored (Codec.VerifyBlockHash)
    bool verify_block_hashes;
};

/// The options for decoding one segment. Reads Codec.VerifyBlockHash, so build it once per segment, not per block.
DecodeOptions decode_options_for(const SegmentHeader& hdr);

} // namespace arcticdb
