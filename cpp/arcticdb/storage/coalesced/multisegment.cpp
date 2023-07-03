#include "multisegment.hpp"

#include <arcticdb/storage/key_segment_pair.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/storage/coalesced/coalesced_storage_common.hpp>


namespace arcticdb::storage {

MultiSegment::MultiSegment(
        StreamId id,
        std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta,
        EncodingVersion encoding_version) :
    header_(std::move(id)),
    codec_meta_(codec_meta),
    encoding_version_(encoding_version){
}

MultiSegment::MultiSegment(SegmentInMemory&& segment, const AtomKey& key) :
    header_(std::move(segment)),
    read_key_(key) {

}



size_t MultiSegment::required_bytes() const {
    return segment_->total_segment_size() + body_bytes_; // TODO magic_nums
}

void MultiSegment::write_to(uint8_t* destination) {
    segment_->write_to(destination, segment_->segment_header_bytes_size());
    destination += segment_->total_segment_size();

    for(auto&& k : key_segs_) {
        auto key_seg = std::move(ks);
        key_seg.segment().write_to(destination, key_seg.segment().segment_header_bytes_size());
        destination += key_seg.segment().total_segment_size();
    }
}

template <typename GetPartialFunc>
MultiSegment load_segment_head(const AtomKey& key, size_t size, GetPartialFunc&& func) {
    auto get_partial = std::move(func);
    auto buffer = get_partial(key, 0, size);
    auto segment = Segment::from_bytes(buffer.data(), size);
    auto memory_segment = decode_segment(std::move(segment));
    return MultiSegment(std::move(memory_segment));
}

template <typename GetPartialFunc>
std::optional<Segment> segment_from_coalesced_key(const AtomKey& key, const MultiSegment& multi_segment) {
    auto opt_position_data = multi_segment.get_offset_for_key(key);


}

}  //namespace arcticdb