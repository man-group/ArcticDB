/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/bitset.hpp>
#include <folly/container/Enumerate.h>
#include <arcticdb/entity/types.hpp>

namespace arcticdb {

inline SegmentInMemory filter_segment(const SegmentInMemory& input,
                                      const util::BitSet& filter_bitset,
                                      bool filter_down_stringpool=false,
                                      bool validate=false) {
    return input.filter(filter_bitset, filter_down_stringpool, validate);
}

inline SegmentInMemory truncate_segment(const SegmentInMemory& input,
                                        size_t start,
                                        size_t end) {
    return input.truncate(start, end);
}

inline std::vector<SegmentInMemory> partition_segment(const SegmentInMemory& input,
                                      const std::vector<std::optional<uint8_t>>& row_to_segment,
                                      const std::vector<uint64_t>& segment_counts) {
    return input.partition(row_to_segment, segment_counts);
}

} //namespace arcticdb
