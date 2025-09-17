/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/bitset.hpp>

namespace arcticdb {

// filter_bitset is passed by copy deliberately, as the same bitset is used for multiple segments, and is modified by
// the segment filtering implementation
inline SegmentInMemory filter_segment(
        const SegmentInMemory& input, util::BitSet filter_bitset, bool filter_down_stringpool = false,
        bool validate = false
) {
    return input.filter(std::move(filter_bitset), filter_down_stringpool, validate);
}

inline std::vector<SegmentInMemory> partition_segment(
        const SegmentInMemory& input, const std::vector<uint8_t>& row_to_segment,
        const std::vector<uint64_t>& segment_counts
) {
    return input.partition(row_to_segment, segment_counts);
}

} // namespace arcticdb
