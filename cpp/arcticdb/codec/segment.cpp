/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/segment.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>

#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/util/dump_bytes.hpp>
#include <arcticdb/codec/codec.hpp>

namespace arcticdb {
namespace segment_size {

std::tuple<size_t, size_t> compressed(const arcticdb::proto::encoding::SegmentHeader &seg_hdr) {
    std::size_t buffer_size = 0;

    size_t string_pool_size = 0;
    if (seg_hdr.has_string_pool_field())
        string_pool_size = encoding_sizes::ndarray_field_compressed_size(seg_hdr.string_pool_field().ndarray());

    if (EncodingVersion(seg_hdr.encoding_version()) == EncodingVersion::V1) {
        size_t metadata_size = 0;
        if (seg_hdr.has_metadata_field())
            metadata_size = encoding_sizes::ndarray_field_compressed_size(seg_hdr.metadata_field().ndarray());

        buffer_size = encoding_sizes::segment_compressed_size(seg_hdr.fields()) + metadata_size + string_pool_size;
    }
    else
        buffer_size = seg_hdr.column_fields().offset() + sizeof(EncodedMagic) + encoding_sizes::ndarray_field_compressed_size(seg_hdr.column_fields().ndarray());

    return {string_pool_size, buffer_size};
}

struct SegmentCompressedSize {
    size_t string_pool_size_ = 0U;
    size_t total_buffer_size_ = 0U;
    size_t body_size_ = 0U;
};

SegmentCompressedSize compressed(const SegmentHeader &seg_hdr) {
    size_t string_pool_size = 0;
    if (seg_hdr.has_string_pool_field())
        string_pool_size = encoding_sizes::ndarray_field_compressed_size(seg_hdr.string_pool_field().ndarray());

    util::check(seg_hdr.encoding_version() == EncodingVersion::V2, "Unexpected version 1 encoding in version 2 header");
    const auto buffer_size = seg_hdr.footer_offset() + sizeof(EncodedMagic) + encoding_sizes::ndarray_field_compressed_size(seg_hdr.column_fields().ndarray());
    return {string_pool_size, buffer_size, seg_hdr.footer_offset()};
}
}

FieldCollection decode_fields(
    const SegmentHeader& hdr,
    const uint8_t* data) {
    FieldCollection fields;
    if (hdr.has_descriptor_field()) {
        ARCTICDB_TRACE(log::codec(), "Decoding string pool");
        std::optional<util::BitMagic> bv;
        (void)decode_field(FieldCollection::type(),
            hdr.descriptor_field(),
            data,
            fields,
            bv,
            hdr.encoding_version());

        ARCTICDB_TRACE(log::codec(), "Decoded string pool to position {}", data-begin);
    }
    fields.regenerate_offsets();
    return fields;
}

struct SegmentHeaderProtoWrapper {
    arcticdb::proto::encoding::SegmentHeader* header_;
    std::unique_ptr<google::protobuf::Arena> arena_;

    [[nodiscard]] const auto& proto() const { return *header_; }

    [[nodiscard]] auto& proto() { return *header_; }
};

SegmentHeaderProtoWrapper decode_protobuf_header(const uint8_t* data, size_t header_bytes_size) {
    google::protobuf::io::ArrayInputStream ais(data, static_cast<int>(header_bytes_size));

    auto arena = std::make_unique<google::protobuf::Arena>();
    auto seg_hdr = google::protobuf::Arena::CreateMessage<arcticdb::proto::encoding::SegmentHeader>(arena.get());
    seg_hdr->ParseFromZeroCopyStream(&ais);
    return {seg_hdr, std::move(arena)};
}

template <typename SegmentHeaderType>
FieldCollection deserialize_fields_collection(const uint8_t* src, const SegmentHeaderType& seg_hdr) {
    FieldCollection fields;
    const auto* fields_ptr = src;
    util::check_magic<MetadataMagic>(fields_ptr);
    if(seg_hdr.has_metadata_field())
        fields_ptr += encoding_sizes::field_compressed_size(seg_hdr.metadata_field());

    util::check_magic<DescriptorMagic>(fields_ptr);
    if(seg_hdr.has_descriptor_field() && seg_hdr.descriptor_field().has_ndarray())
         fields = decode_fields(seg_hdr, fields_ptr);

    return fields;
}

EncodedFieldCollection deserialize_body_fields(const SegmentHeader& hdr, const uint8_t* data) {
    const auto* encoded_fields_ptr = data;
    util::check(hdr.has_column_fields(), "Expected column fields in v2 encoding");
    util::check_magic<EncodedMagic>(encoded_fields_ptr);

    return EncodedFieldCollection{decode_encoded_fields(hdr, encoded_fields_ptr, data)};
}


std::tuple<SegmentHeader, FieldCollection, std::shared_ptr<StreamDescriptorDataImpl>, std::optional<SegmentHeaderProtoWrapper>> decode_header_and_fields(const uint8_t* src) {
    auto* fixed_hdr = reinterpret_cast<const FixedHeader*>(src);
    ARCTICDB_DEBUG(log::codec(), "Reading header: {} + {} = {}",
                   FIXED_HEADER_SIZE,
                   fixed_hdr->header_bytes,
                   header_bytes);

    util::check_arg(fixed_hdr->magic_number == MAGIC_NUMBER, "expected first 2 bytes: {}, actual {}", fixed_hdr->magic_number, MAGIC_NUMBER);

    SegmentHeader segment_header;
    FieldCollection fields;
    auto data = std::make_shared<StreamDescriptorDataImpl>(); //TODO decode
    std::optional<SegmentHeaderProtoWrapper> proto_wrapper;

    const auto* header_ptr = src + FIXED_HEADER_SIZE;
    const auto* fields_ptr = header_ptr + fixed_hdr->header_bytes;
    if(const auto header_version = fixed_hdr->encoding_version; header_version == HEADER_VERSION_V1) {
       proto_wrapper = decode_protobuf_header(header_ptr, fixed_hdr->header_bytes);
       segment_header.deserialize_from_proto(proto_wrapper->proto());
       util::check(segment_header.encoding_version() == EncodingVersion::V1, "Expected v1 header to contain legacy encoding version");
       field_collection_from_proto(std::move(*proto_wrapper->proto().mutable_stream_descriptor()->mutable_fields()));
    } else {
        segment_header.deserialize_from_bytes(header_ptr, fixed_hdr->header_bytes);
        util::check(segment_header.encoding_version() == EncodingVersion::V2, "Expected V2 encoding in binary header");
        fields = deserialize_fields_collection(fields_ptr, segment_header);
    }

    return {std::move(segment_header), std::move(fields), std::move(data), std::move(proto_wrapper)};
}

void check_encoding(EncodingVersion encoding_version) {
    util::check(encoding_version == EncodingVersion::V1 || encoding_version == EncodingVersion::V2 ,
                "expected encoding_version < 2, actual {}",
                encoding_version);
}

void check_size(const FixedHeader* fixed_hdr, size_t buffer_bytes, size_t readable_size, size_t string_pool_size) {
    util::check(FIXED_HEADER_SIZE + fixed_hdr->header_bytes + buffer_bytes <= readable_size,
                "Size disparity, fixed header size {} + variable header size {} + buffer size {}  (string pool size {}) >= total size {}",
                FIXED_HEADER_SIZE,
                fixed_hdr->header_bytes,
                buffer_bytes,
                string_pool_size,
                readable_size);
}

void set_body_fields(SegmentHeader& seg_hdr, const uint8_t* src, const std::optional<SegmentHeaderProtoWrapper>& proto_wrapper) {
    const uint8_t *begin = src;
    const auto fields_offset = seg_hdr.footer_offset();
    const auto end = begin + fields_offset;
    if(begin != end) {
        if(seg_hdr.has_column_fields()) {
            auto encoded_fields = deserialize_body_fields(seg_hdr, begin);
            seg_hdr.set_body_fields(std::move(encoded_fields));
        } else {
            util::check(proto_wrapper.has_value(), "Expected legacy protobuf header in decoding encoded fields");
            auto fields_from_proto = encoded_fields_from_proto(proto_wrapper->proto());
            seg_hdr.set_body_fields(std::move(fields_from_proto));
        }
    }
}

Segment Segment::from_bytes(const std::uint8_t* src, std::size_t readable_size, bool copy_data /* = false */) {
    ARCTICDB_SAMPLE(SegmentFromBytes, 0)
    util::check(src != nullptr, "Got null data ptr from segment");
    auto* fixed_hdr = reinterpret_cast<const FixedHeader*>(src);
    auto [seg_hdr, fields, desc_data, proto_wrapper] = decode_header_and_fields(src);
    check_encoding(seg_hdr.encoding_version());
    const auto[string_pool_size, buffer_bytes, body_bytes] = segment_size::compressed(seg_hdr);
    check_size(fixed_hdr, buffer_bytes, readable_size, string_pool_size);
    ARCTICDB_DEBUG(log::codec(), "Reading string pool {} header {} + {} and buffer bytes {}", string_pool_size, FIXED_HEADER_SIZE, fixed_hdr->header_bytes, buffer_bytes);
    ARCTICDB_SUBSAMPLE(CreateBufferView, 0)
    VariantBuffer variant_buffer;
    if (copy_data) {
        auto buf = std::make_shared<Buffer>();
        buf->ensure(body_bytes);
        memcpy(buf->data(), src, body_bytes);
        variant_buffer = std::move(buf);
    } else {
        variant_buffer = BufferView{const_cast<uint8_t*>(src), buffer_bytes};
    }

    set_body_fields(seg_hdr, src, proto_wrapper);
    return {std::move(seg_hdr), std::move(variant_buffer), std::move(desc_data), std::make_shared<FieldCollection>(std::move(fields))};
}


Segment Segment::from_buffer(const std::shared_ptr<Buffer>& buffer) {
    ARCTICDB_SAMPLE(SegmentFromBuffer, 0)
    auto* fixed_hdr = reinterpret_cast<FixedHeader*>(buffer->data());
    auto readable_size = buffer->bytes();
    auto [seg_hdr, fields, desc_data, proto_wrapper] = decode_header_and_fields(buffer->data());
    check_encoding(seg_hdr.encoding_version());

    ARCTICDB_SUBSAMPLE(ReadHeaderAndSegment, 0)
    auto header_bytes ARCTICDB_UNUSED = FIXED_HEADER_SIZE + fixed_hdr->header_bytes;
    ARCTICDB_DEBUG(log::codec(), "Reading header: {} + {} = {}",
                  FIXED_HEADER_SIZE,
                  fixed_hdr->header_bytes,
                  header_bytes);

    const auto[string_pool_size, buffer_bytes, body_bytes] = segment_size::compressed(seg_hdr);
    ARCTICDB_DEBUG(log::codec(), "Reading string pool {} and buffer bytes {}", string_pool_size, buffer_bytes);
    check_size(fixed_hdr, buffer_bytes, readable_size, string_pool_size);

    set_body_fields(seg_hdr, buffer->data(), proto_wrapper);
    buffer->set_preamble(FIXED_HEADER_SIZE + fixed_hdr->header_bytes);
    ARCTICDB_SUBSAMPLE(CreateSegment, 0)
    return{std::move(seg_hdr), buffer, std::move(desc_data), std::make_shared<FieldCollection>(std::move(fields))};

}

void Segment::write_header(uint8_t* dst, size_t hdr_size) const {
    FixedHeader hdr = {MAGIC_NUMBER, HEADER_VERSION_V1, std::uint32_t(hdr_size)};
    write_fixed_header(dst, hdr);
    if(!header_.has_metadata_field())
        ARCTICDB_DEBUG(log::codec(), "Header has no metadata field");
}

std::pair<uint8_t*, size_t> Segment::try_internal_write(std::shared_ptr<Buffer>& tmp, size_t hdr_size) {
    auto total_hdr_size = hdr_size + FIXED_HEADER_SIZE;
    if(buffer_.is_owning() && buffer_.preamble_bytes() >= total_hdr_size) {
        const auto& buffer = buffer_.get_owning_buffer();
        auto base_ptr = buffer->preamble() + (buffer->preamble_bytes() - total_hdr_size);
        util::check(base_ptr + total_hdr_size == buffer->data(), "Expected base ptr to align with data ptr, {} != {}", fmt::ptr(base_ptr + total_hdr_size), fmt::ptr(buffer->data()));
        ARCTICDB_TRACE(log::codec(), "Buffer contents before header write: {}", dump_bytes(buffer->data(), buffer->bytes(), 100u));
        write_header(base_ptr, hdr_size);
        ARCTICDB_TRACE(log::storage(), "Header fits in internal buffer {:x} with {} bytes space: {}", uintptr_t (base_ptr), buffer->preamble_bytes() - total_hdr_size, dump_bytes(buffer->data(), buffer->bytes(), 100u));
        return std::make_pair(base_ptr, total_segment_size(hdr_size));
    }
    else {
        tmp = std::make_shared<Buffer>();
        ARCTICDB_DEBUG(log::storage(), "Header doesn't fit in internal buffer, needed {} bytes but had {}, writing to temp buffer at {:x}", hdr_size, std::get<std::shared_ptr<Buffer>>(buffer_)->preamble_bytes(), uintptr_t(tmp->data()));
        tmp->ensure(total_segment_size(hdr_size));
        write_to(tmp->preamble(), hdr_size);
        return std::make_pair(tmp->preamble(), total_segment_size(hdr_size));
    }
}

void Segment::write_to(std::uint8_t* dst, std::size_t hdr_sz) {
    ARCTICDB_SAMPLE(SegmentWriteToStorage, RMTSF_Aggregate)
    ARCTICDB_SUBSAMPLE(SegmentWriteHeader, RMTSF_Aggregate)
    write_header(dst, hdr_sz);
    ARCTICDB_SUBSAMPLE(SegmentWriteBody, RMTSF_Aggregate)
    ARCTICDB_DEBUG(log::codec(), "Writing {} bytes to body at offset {}",
                       buffer().bytes(),
                       FIXED_HEADER_SIZE + hdr_sz);

    std::memcpy(dst + FIXED_HEADER_SIZE + hdr_sz,
                buffer().data(),
                buffer().bytes());
}

} //namespace arcticdb
