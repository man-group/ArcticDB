/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/util/hash.hpp>

namespace arcticdb {

inline void hash_buffer(const ChunkedBuffer& buffer, HashAccum& accum) {
    for (const auto& block : buffer.blocks()) {
        accum(block->data(), block->physical_bytes());
    }
}

}
