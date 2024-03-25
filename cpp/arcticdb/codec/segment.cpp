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

struct SegmentHeaderProtoWrapper {
    arcticdb::proto::encoding::SegmentHeader* header_;
    std::unique_ptr<google::protobuf::Arena> arena_;

    [[nodiscard]] const auto& proto() const { return *header_; }

    [[nodiscard]] auto& proto() { return *header_; }
};

namespace segment_size {


SegmentCompressedSize compressed(const SegmentHeader &seg_hdr, const std::optional<SegmentHeaderProtoWrapper>& proto_wrapper) {
    size_t string_pool_size = 0;
    if (seg_hdr.has_string_pool_field())
        string_pool_size = encoding_sizes::ndarray_field_compressed_size(seg_hdr.string_pool_field().ndarray());

    size_t metadata_size = 0;
    if (seg_hdr.has_metadata_field())
        metadata_size = encoding_sizes::ndarray_field_compressed_size(seg_hdr.metadata_field().ndarray());

    auto buffer_size = 0U;
    auto body_size = 0U;
    if(seg_hdr.encoding_version() == EncodingVersion::V1) {
        buffer_size = encoding_sizes::segment_compressed_size(proto_wrapper->proto().fields()) + metadata_size + string_pool_size;
        body_size = buffer_size;
    } else {
        buffer_size = seg_hdr.footer_offset() + sizeof(EncodedMagic) + encoding_sizes::ndarray_field_compressed_size(seg_hdr.column_fields().ndarray());
        body_size = seg_hdr.footer_offset();
    }

    return {string_pool_size, buffer_size, body_size};
}
}

FieldCollection decode_descriptor_fields(
    const SegmentHeader& hdr,
    const uint8_t* data,
    const uint8_t* begin) {
    FieldCollection fields;
    if (hdr.has_descriptor_field()) {
        ARCTICDB_TRACE(log::codec(), "Decoding descriptor");
        std::optional<util::BitMagic> bv;
        (void)decode_field(FieldCollection::type(),
            hdr.descriptor_field(),
            data,
            fields,
            bv,
            hdr.encoding_version());

        ARCTICDB_TRACE(log::codec(), "Decoded descriptor to position {}", data-begin);
    }
    fields.regenerate_offsets();
    return fields;
}


SegmentHeaderProtoWrapper decode_protobuf_header(const uint8_t* data, size_t header_bytes_size) {
    google::protobuf::io::ArrayInputStream ais(data, static_cast<int>(header_bytes_size));

    auto arena = std::make_unique<google::protobuf::Arena>();
    auto seg_hdr = google::protobuf::Arena::CreateMessage<arcticdb::proto::encoding::SegmentHeader>(arena.get());
    seg_hdr->ParseFromZeroCopyStream(&ais);
    log::codec().info("Serialized header bytes output: {}", dump_bytes(data, header_bytes_size));
    log::codec().info("Decoded protobuf header: {}", seg_hdr->DebugString());
    return {seg_hdr, std::move(arena)};
}

FieldCollection deserialize_descriptor_fields_collection(const uint8_t* src, const SegmentHeader& seg_hdr) {
    FieldCollection fields;
    const auto* fields_ptr = src;
    util::check_magic<MetadataMagic>(fields_ptr);
    if(seg_hdr.has_metadata_field())
        fields_ptr += encoding_sizes::field_compressed_size(seg_hdr.metadata_field());

    util::check_magic<DescriptorMagic>(fields_ptr);
    if(seg_hdr.has_descriptor_field() && seg_hdr.descriptor_field().has_ndarray())
         fields = decode_descriptor_fields(seg_hdr, fields_ptr, src);

    return fields;
}

EncodedFieldCollection deserialize_body_fields(const SegmentHeader& hdr, const uint8_t* data) {
    const auto* encoded_fields_ptr = data;
    util::check(hdr.has_column_fields(), "Expected column fields in v2 encoding");
    util::check_magic<EncodedMagic>(encoded_fields_ptr);

    return EncodedFieldCollection{decode_encoded_fields(hdr, encoded_fields_ptr, data)};
}

struct DeserializedSegmentData {
    SegmentHeader segment_header_;
    std::shared_ptr<FieldCollection> fields_;
    std::shared_ptr<FrameDescriptorImpl> frame_desc_;
    std::optional<SegmentHeaderProtoWrapper> proto_wrapper_;
};

DeserializedSegmentData decode_header_and_fields(const uint8_t*& src) {
    auto* fixed_hdr = reinterpret_cast<const FixedHeader*>(src);
    ARCTICDB_DEBUG(log::codec(), "Reading header: {} + {} = {}",
                   FIXED_HEADER_SIZE,
                   fixed_hdr->header_bytes,
                   FIXED_HEADER_SIZE + fixed_hdr->header_bytes);

    util::check_arg(fixed_hdr->magic_number == MAGIC_NUMBER, "expected first 2 bytes: {}, actual {}", fixed_hdr->magic_number, MAGIC_NUMBER);
    std::optional<SegmentHeaderProtoWrapper> proto_wrapper;

    const auto* header_ptr = src + FIXED_HEADER_SIZE;
    if(const auto header_version = fixed_hdr->encoding_version; header_version == HEADER_VERSION_V1) {
       proto_wrapper = decode_protobuf_header(header_ptr, fixed_hdr->header_bytes);
       auto data = std::make_shared<SegmentDescriptorImpl>(segment_descriptor_from_proto(proto_wrapper->proto().stream_descriptor()));
       auto segment_header = deserialize_segment_header_from_proto(proto_wrapper->proto());
       util::check(segment_header.encoding_version() == EncodingVersion::V1, "Expected v1 header to contain legacy encoding version");
       auto fields = std::make_shared<FieldCollection>(field_collection_from_proto(std::move(proto_wrapper->proto().stream_descriptor().fields())));
       src += FIXED_HEADER_SIZE + fixed_hdr->header_bytes;
       return {std::move(segment_header), std::move(fields), std::move(data), std::move(proto_wrapper)};
    } else {
        SegmentHeader segment_header;
        const auto* fields_ptr = header_ptr + fixed_hdr->header_bytes;
        auto data = std::make_shared<SegmentInMemoryImpl>(read_segment_descriptor
        segment_header.deserialize_from_bytes(header_ptr);
        util::check(segment_header.encoding_version() == EncodingVersion::V2, "Expected V2 encoding in binary header");
        auto fields = deserialize_descriptor_fields_collection(fields_ptr, segment_header);
        return {std::move(segment_header), std::move(fields), std::move(data), std::move(proto_wrapper)};
    }
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
    if(seg_hdr.has_column_fields()) {
        auto encoded_fields = deserialize_body_fields(seg_hdr, src +  seg_hdr.footer_offset());
        seg_hdr.set_body_fields(std::move(encoded_fields));
    } else {
        util::check(proto_wrapper.has_value(), "Expected legacy protobuf header in decoding encoded fields");
        auto fields_from_proto = encoded_fields_from_proto(proto_wrapper->proto());
        seg_hdr.set_body_fields(std::move(fields_from_proto));
    }
}

Segment Segment::from_bytes(const std::uint8_t* src, std::size_t readable_size, bool copy_data /* = false */) {
    ARCTICDB_SAMPLE(SegmentFromBytes, 0)
    util::check(src != nullptr, "Got null data ptr from segment");
    auto* fixed_hdr = reinterpret_cast<const FixedHeader*>(src);
    auto [seg_hdr, fields, desc_data, proto_wrapper] = decode_header_and_fields(src);
    check_encoding(seg_hdr.encoding_version());
    const auto[string_pool_size, buffer_bytes, body_bytes] = segment_size::compressed(seg_hdr, proto_wrapper);
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
    const auto* src = buffer->data();
    auto [seg_hdr, fields, desc_data, proto_wrapper] = decode_header_and_fields(src);
    check_encoding(seg_hdr.encoding_version());

    ARCTICDB_SUBSAMPLE(ReadHeaderAndSegment, 0)
    auto header_bytes ARCTICDB_UNUSED = FIXED_HEADER_SIZE + fixed_hdr->header_bytes;
    ARCTICDB_DEBUG(log::codec(), "Reading header: {} + {} = {}",
                  FIXED_HEADER_SIZE,
                  fixed_hdr->header_bytes,
                  header_bytes);

    const auto[string_pool_size, buffer_bytes, body_bytes] = segment_size::compressed(seg_hdr, proto_wrapper);
    ARCTICDB_DEBUG(log::codec(), "Reading string pool {} and buffer bytes {}", string_pool_size, buffer_bytes);
    check_size(fixed_hdr, buffer_bytes, readable_size, string_pool_size);

    set_body_fields(seg_hdr, src, proto_wrapper);
    buffer->set_preamble(FIXED_HEADER_SIZE + fixed_hdr->header_bytes);
    ARCTICDB_SUBSAMPLE(CreateSegment, 0)
    return{std::move(seg_hdr), buffer, std::move(desc_data), std::make_shared<FieldCollection>(std::move(fields))};
}

size_t Segment::write_proto_header(uint8_t* dst) const {
    const auto hdr_size = proto_size();
    const auto& header = header_proto();
    FixedHeader hdr = {MAGIC_NUMBER, HEADER_VERSION_V1, std::uint32_t(hdr_size)};
    write_fixed_header(dst, hdr);

    google::protobuf::io::ArrayOutputStream aos(dst + FIXED_HEADER_SIZE, static_cast<int>(hdr_size));
    header.SerializeToZeroCopyStream(&aos);
    log::codec().info("Serialized header bytes: {}", dump_bytes(dst + FIXED_HEADER_SIZE, hdr_size));
    return hdr_size;
}

std::pair<uint8_t*, size_t> Segment::serialize_header_v2(std::shared_ptr<Buffer>&) {
    const auto header_bytes = header_.bytes();
    util::check(header_bytes == buffer_.preamble_bytes(), "Expected v2 header of size {} to fit exactly into buffer preamble of size {}", header_.bytes(), buffer_.preamble_bytes());
    const auto &buffer = buffer_.get_owning_buffer();
    header_.serialize_to_bytes(buffer->preamble());
    return std::make_pair(buffer->preamble(), total_segment_size());
}

std::pair<uint8_t*, size_t> Segment::serialize_header_v1(std::shared_ptr<Buffer>& tmp) {
    auto proto_header = serialize_proto_header();
    const auto hdr_size = proto_header.ByteSizeLong();
    auto total_hdr_size = hdr_size + FIXED_HEADER_SIZE;

    if (buffer_.is_owning() && buffer_.preamble_bytes() >= total_hdr_size) {
        const auto &buffer = buffer_.get_owning_buffer();
        auto base_ptr = buffer->preamble() + (buffer->preamble_bytes() - total_hdr_size);
        util::check(base_ptr + total_hdr_size == buffer->data(),
                    "Expected base ptr to align with data ptr, {} != {}",
                    fmt::ptr(base_ptr + total_hdr_size),
                    fmt::ptr(buffer->data()));
        ARCTICDB_TRACE(log::codec(),
                       "Buffer contents before header write: {}",
                       dump_bytes(buffer->data(), buffer->bytes(), 100u));
        write_proto_header(base_ptr);
        ARCTICDB_TRACE(log::storage(),
                       "Header fits in internal buffer {:x} with {} bytes space: {}",
                       uintptr_t (base_ptr),
                       buffer->preamble_bytes() - total_hdr_size,
                       dump_bytes(buffer->data(), buffer->bytes(), 100u));
        return std::make_pair(base_ptr, total_segment_size());
    } else {
        tmp = std::make_shared<Buffer>();
        ARCTICDB_DEBUG(log::storage(),
                       "Header doesn't fit in internal buffer, needed {} bytes but had {}, writing to temp buffer at {:x}",
                       hdr_size,
                       buffer_.preamble_bytes(),
                       uintptr_t(tmp->data()));
        tmp->ensure(total_segment_size());
        auto* dst = tmp->preamble();
        write_proto_header(dst);
        std::memcpy(dst + FIXED_HEADER_SIZE + hdr_size,
                    buffer().data(),
                    buffer().bytes());
        return std::make_pair(tmp->preamble(), total_segment_size());
    }
}

std::pair<uint8_t*, size_t> Segment::serialize_header(std::shared_ptr<Buffer>& tmp) {
    if (header_.encoding_version() == EncodingVersion::V1)
        return serialize_header_v1(tmp);
    else
        return serialize_header_v2(tmp);
}

[[nodiscard]] std::shared_ptr<FieldCollection> Segment::fields_ptr() const {
    return desc_.fields_ptr();
}

[[nodiscard]] size_t Segment::fields_size() const {
    return desc_.field_count();
}

[[nodiscard]] const Field& Segment::fields(size_t pos) const {
    return desc_.fields(pos);
}

arcticdb::proto::encoding::SegmentHeader Segment::serialize_proto_header() const {
    arcticdb::proto::encoding::SegmentHeader segment_header;
    if(header_.has_metadata_field())
        copy_encoded_field_to_proto(header_.metadata_field(), *segment_header.mutable_metadata_field());

    if(header_.has_string_pool_field())
        copy_encoded_field_to_proto(header_.string_pool_field(), *segment_header.mutable_string_pool_field());

    copy_stream_descriptor_to_proto(desc_, *segment_header.mutable_stream_descriptor());
    copy_encoded_fields_to_proto(header_.body_fields(), segment_header);

    segment_header.set_compacted(header_.compacted());
    segment_header.set_encoding_version(static_cast<uint16_t>(header_.encoding_version()));
    log::codec().info("Encoded segment header {}", segment_header.DebugString());
    return segment_header;
}

const arcticdb::proto::encoding::SegmentHeader& Segment::header_proto() const {
    util::check(header_.encoding_version() == EncodingVersion::V1, "Got proto request in V2 encoding");
    if(!proto_)
        proto_ = std::make_unique<arcticdb::proto::encoding::SegmentHeader>(serialize_proto_header());

    return *proto_;
}

void Segment::write_to(std::uint8_t* dst) {
    ARCTICDB_SAMPLE(SegmentWriteToStorage, RMTSF_Aggregate)
    ARCTICDB_SUBSAMPLE(SegmentWriteHeader, RMTSF_Aggregate)

    size_t header_size = 0U;
    if(header_.encoding_version() == EncodingVersion::V1)
        header_size = write_proto_header(dst);

    ARCTICDB_SUBSAMPLE(SegmentWriteBody, RMTSF_Aggregate)
    ARCTICDB_DEBUG(log::codec(), "Writing {} bytes to body at offset {}",
                       buffer().bytes(),
                       FIXED_HEADER_SIZE + header_size);

    std::memcpy(dst + FIXED_HEADER_SIZE + header_size,
                buffer().data(),
                buffer().bytes());
}

} //namespace arcticdb
