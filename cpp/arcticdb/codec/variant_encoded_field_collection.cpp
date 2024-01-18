/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/variant_encoded_field_collection.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/codec/magic_words.hpp>

namespace arcticdb {

VariantEncodedFieldCollection::VariantEncodedFieldCollection(const Segment& segment) {
    if(EncodingVersion(segment.header().encoding_version()) == EncodingVersion::V2) {
        const auto& hdr = segment.header();
        auto [begin, encoded_fields_ptr] = get_segment_begin_end(segment, segment.header());
        util::check_magic<EncodedMagic>(encoded_fields_ptr);
        auto encoded_fields_buffer = decode_encoded_fields(hdr, encoded_fields_ptr, begin);
        fields_ = EncodedFieldCollection{std::move(encoded_fields_buffer)};
    } else {
        is_proto_ = true;
        header_ = &segment.header();
    }
}

VariantField VariantEncodedFieldCollection::at(size_t pos) const {
    if (is_proto_)
        return &header_->fields(static_cast<int>(pos));
    else
        return &fields_.at(pos);
}

size_t VariantEncodedFieldCollection::size() const {
    if (is_proto_)
        return header_->fields_size();
    else
        return fields_.size();
}

} //namespace arcticdb
