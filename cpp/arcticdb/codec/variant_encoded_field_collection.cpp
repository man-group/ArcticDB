#include <codec/variant_encoded_field_collection.hpp>
#include <codec/codec.hpp>

namespace arcticdb {

VariantEncodedFieldCollection::VariantEncodedFieldCollection(const Segment& segment) {
    if(EncodingVersion(segment.header().encoding_version()) == EncodingVersion::V2) {
        const auto& hdr = segment.header();
        auto [begin, encoded_fields_ptr] = get_segment_begin_end(segment, segment.header());
        check_magic<EncodedMagic>(encoded_fields_ptr);
        auto encoded_fields_buffer = decode_encoded_fields(hdr, encoded_fields_ptr, begin);
        fields_ = EncodedFieldCollection{std::move(encoded_fields_buffer)};
    } else {
        is_proto_ = true;
        header_ = &segment.header();
    }
}

} //namespace arcticdb