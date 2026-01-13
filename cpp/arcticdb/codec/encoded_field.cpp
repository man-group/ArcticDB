/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/codec/segment.hpp>

namespace arcticdb {

std::pair<const uint8_t*, const uint8_t*> get_segment_begin_end(const Segment& segment, const SegmentHeader& hdr) {
    const uint8_t* data = segment.buffer().data();
    util::check(data != nullptr, "Got null data ptr from segment in get_segment_begin_end");
    const uint8_t* begin = data;
    const auto fields_offset = hdr.footer_offset();
    const auto end = begin + fields_offset;
    return {begin, end};
}

} // namespace arcticdb