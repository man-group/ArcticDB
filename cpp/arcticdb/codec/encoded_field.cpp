#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/codec/segment.hpp>

namespace arcticdb {

std::pair<const uint8_t *, const uint8_t *> get_segment_begin_end(
        const Segment &segment,
        const arcticdb::proto::encoding::SegmentHeader &hdr) {
    const uint8_t *data = segment.buffer().data();
    util::check(data != nullptr, "Got null data ptr from segment");
    const uint8_t *begin = data;

    const auto fields_offset = hdr.column_fields().offset();
    const auto end = begin + fields_offset;
    return {begin, end};
}

}