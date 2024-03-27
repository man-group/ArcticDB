#include <arcticdb/codec/segment_header.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

namespace arcticdb {

size_t field_collection_encoded_field_bytes(const FieldCollection& fields) {
    return calc_field_bytes(fields.num_blocks());
}

size_t SegmentHeader::required_bytes(const SegmentInMemory& in_mem_seg) {
    size_t required = 0UL;

    if(in_mem_seg.has_index_descriptor())
        required += field_collection_encoded_field_bytes(in_mem_seg.index_descriptor().fields());

    if(in_mem_seg.has_string_pool())
        required += calc_field_bytes(in_mem_seg.const_string_pool().num_blocks());

    if(!in_mem_seg.descriptor().empty())
        required += field_collection_encoded_field_bytes(in_mem_seg.descriptor().fields());

    // Metadata and column fields are allocated in one contiguous buffer
    if(in_mem_seg.metadata())
        required += calc_field_bytes(2);

    if(!in_mem_seg.empty())
        required += calc_field_bytes(1);

    required += sizeof(offset_);
    required += sizeof(HeaderData);
    return required;
}
} //namespace arcticdb
