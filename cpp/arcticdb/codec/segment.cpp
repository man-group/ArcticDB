/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/segment.hpp>
#include <arcticdb/entity/performance_tracing.hpp>

#include <arcticdb/util/dump_bytes.hpp>
#include <arcticdb/codec/codec.hpp>

namespace arcticdb {
namespace segment_size {
std::tuple<size_t, size_t> compressed(const arcticdb::proto::encoding::SegmentHeader &seg_hdr) {
    size_t string_pool_size = 0;
    if (seg_hdr.has_string_pool_field())
        string_pool_size = encoding_sizes::ndarray_field_compressed_size(seg_hdr.string_pool_field().ndarray());

    std::size_t buffer_size = 0;
    if (EncodingVersion(seg_hdr.encoding_version()) == EncodingVersion::V1) {
        size_t metadata_size = 0;
        // If we have metadata it is part of the buffer size, otherwise the allocated buffer is much too small
        if (seg_hdr.has_metadata_field())
            metadata_size = encoding_sizes::ndarray_field_compressed_size(seg_hdr.metadata_field().ndarray());

        buffer_size = encoding_sizes::segment_compressed_size(seg_hdr.fields()) + metadata_size + string_pool_size;
    }
    else
        buffer_size = seg_hdr.column_fields().offset() + sizeof(EncodedMagic) + encoding_sizes::ndarray_field_compressed_size(seg_hdr.column_fields().ndarray());

    return {string_pool_size, buffer_size};
}
}

FieldCollection decode_fields(
    const arcticdb::proto::encoding::SegmentHeader& hdr,
    const uint8_t* data) {
    const auto begin ARCTICDB_UNUSED = data;
    FieldCollection fields;
    if (hdr.has_descriptor_field()) {
        ARCTICDB_TRACE(log::codec(), "Decoding string pool");
        std::optional<util::BitMagic> bv;
        data += decode_field(FieldCollection::type(),
            hdr.descriptor_field(),
            data,
            fields,
            bv,
            to_encoding_version(hdr.encoding_version()));

        ARCTICDB_TRACE(log::codec(), "Decoded string pool to position {}", data-begin);
    }
    fields.regenerate_offsets();
    return fields;
}

std::optional<FieldCollection> decode_index_fields(
    const arcticdb::proto::encoding::SegmentHeader& hdr,
    const uint8_t*& data,
    const uint8_t* const begin ARCTICDB_UNUSED
    ) {
    if(hdr.has_index_descriptor_field()) {
        FieldCollection fields;
        ARCTICDB_TRACE(log::codec(), "Decoding string pool");
        std::optional<util::BitMagic> bv;
        data += decode_field(FieldCollection::type(),
            hdr.index_descriptor_field(),
            data,
            fields,
            bv,
            to_encoding_version(hdr.encoding_version()));

        ARCTICDB_TRACE(log::codec(), "Decoded string pool to position {}", data-begin);
        return std::make_optional<FieldCollection>(std::move(fields));
    } else {
        return std::nullopt;
    }
}

Segment Segment::from_bytes(const std::uint8_t* src, std::size_t readable_size, bool copy_data /* = false */) {
    ARCTICDB_SAMPLE(SegmentFromBytes, 0)
    auto* fixed_hdr = reinterpret_cast<const Segment::FixedHeader*>(src);
    util::check_arg(fixed_hdr->magic_number == MAGIC_NUMBER, "expected first 2 bytes: {}, actual {}", fixed_hdr->magic_number, MAGIC_NUMBER);


    ARCTICDB_SUBSAMPLE(ReadHeaderAndSegment, 0)
    auto header_bytes ARCTICDB_UNUSED = arcticdb::Segment::FIXED_HEADER_SIZE + fixed_hdr->header_bytes;
    ARCTICDB_DEBUG(log::codec(), "Reading header: {} + {} = {}",
                       arcticdb::Segment::FIXED_HEADER_SIZE,
                       fixed_hdr->header_bytes,
                       header_bytes);
    google::protobuf::io::ArrayInputStream ais(src + arcticdb::Segment::FIXED_HEADER_SIZE, static_cast<int>(fixed_hdr->header_bytes));
    auto arena = std::make_unique<google::protobuf::Arena>();
    auto seg_hdr = google::protobuf::Arena::CreateMessage<arcticdb::proto::encoding::SegmentHeader>(arena.get());
    seg_hdr->ParseFromZeroCopyStream(&ais);
    if(!seg_hdr->has_metadata_field())
        ARCTICDB_DEBUG(log::storage(), "Segment has no medatadata field");


    src += arcticdb::Segment::FIXED_HEADER_SIZE + fixed_hdr->header_bytes;

    auto version = EncodingVersion(seg_hdr->encoding_version());
    util::check(version == EncodingVersion::V1 || version == EncodingVersion::V2 ,
                "expected encoding_version < 2, actual {}",
                seg_hdr->encoding_version());

    FieldCollection fields;
    if(version == EncodingVersion::V1)
        fields = fields_from_proto(seg_hdr->stream_descriptor());
    else {
        const auto* fields_ptr = src;
        util::check_magic<MetadataMagic>(fields_ptr);
        if(seg_hdr->has_metadata_field())
            fields_ptr += encoding_sizes::field_compressed_size(seg_hdr->metadata_field());

        util::check_magic<DescriptorMagic>(fields_ptr);
        if(seg_hdr->has_descriptor_field() && seg_hdr->descriptor_field().has_ndarray())
            fields = decode_fields(*seg_hdr, fields_ptr);
    }

    const auto[string_pool_size, buffer_bytes] = segment_size::compressed(*seg_hdr);
    ARCTICDB_DEBUG(log::codec(), "Reading string pool {} header {} + {} and buffer bytes {}", string_pool_size, arcticdb::Segment::FIXED_HEADER_SIZE, fixed_hdr->header_bytes, buffer_bytes);
    util::check(arcticdb::Segment::FIXED_HEADER_SIZE + fixed_hdr->header_bytes + buffer_bytes <= readable_size,
                "Size disparity, fixed header size {} + variable header size {} + buffer size {}  (string pool size {}) >= total size {}",
                arcticdb::Segment::FIXED_HEADER_SIZE,
                fixed_hdr->header_bytes,
                buffer_bytes,
                string_pool_size,
                readable_size
    );

    ARCTICDB_SUBSAMPLE(CreateBufferView, 0)
    if (copy_data) {
        auto buf = std::make_shared<Buffer>();
        buf->ensure(buffer_bytes);
        memcpy(buf->data(), src, buffer_bytes);
        return {std::move(arena), seg_hdr, std::move(buf), std::make_shared<FieldCollection>(std::move(fields))};
    } else {
        BufferView bv{const_cast<uint8_t*>(src), buffer_bytes};
        return {std::move(arena), seg_hdr, std::move(bv), std::make_shared<FieldCollection>(std::move(fields))};
    }
}


Segment Segment::from_buffer(std::shared_ptr<Buffer>&& buffer) {
    ARCTICDB_SAMPLE(SegmentFromBytes, 0)
    auto* fixed_hdr = reinterpret_cast<Segment::FixedHeader*>(buffer->data());
    auto readable_size = buffer->bytes();
    util::check_arg(fixed_hdr->magic_number == MAGIC_NUMBER, "expected first 2 bytes: {}, actual {}",
                    MAGIC_NUMBER, fixed_hdr->magic_number);
    util::check_arg(fixed_hdr->encoding_version == HEADER_VERSION_V1,
                    "expected encoding_version {}, actual {}",
                    HEADER_VERSION_V1 , fixed_hdr->encoding_version);

    ARCTICDB_SUBSAMPLE(ReadHeaderAndSegment, 0)
    auto header_bytes ARCTICDB_UNUSED = arcticdb::Segment::FIXED_HEADER_SIZE + fixed_hdr->header_bytes;
    ARCTICDB_DEBUG(log::codec(), "Reading header: {} + {} = {}",
                  arcticdb::Segment::FIXED_HEADER_SIZE,
                  fixed_hdr->header_bytes,
                  header_bytes);
    google::protobuf::io::ArrayInputStream ais(buffer->data() + arcticdb::Segment::FIXED_HEADER_SIZE, fixed_hdr->header_bytes);
    auto arena = std::make_unique<google::protobuf::Arena>();
    auto seg_hdr = google::protobuf::Arena::CreateMessage<arcticdb::proto::encoding::SegmentHeader>(arena.get());
    seg_hdr->ParseFromZeroCopyStream(&ais);

    const auto[string_pool_size, buffer_bytes] = segment_size::compressed(*seg_hdr);
    ARCTICDB_DEBUG(log::codec(), "Reading string pool {} and buffer bytes {}", string_pool_size, buffer_bytes);
    util::check(arcticdb::Segment::FIXED_HEADER_SIZE + fixed_hdr->header_bytes + buffer_bytes <= readable_size,
                "Size disparity, fixed header size {} + variable header size {} + buffer size {}  (string pool size {}) >= total size {}",
                arcticdb::Segment::FIXED_HEADER_SIZE,
                fixed_hdr->header_bytes,
                buffer_bytes,
                string_pool_size,
                readable_size
    );

    auto version = EncodingVersion(seg_hdr->encoding_version());
    util::check(version == EncodingVersion::V1 || version == EncodingVersion::V2,
                "expected encoding_version < 2, actual {}",
                seg_hdr->encoding_version());


    auto preamble_size = arcticdb::Segment::FIXED_HEADER_SIZE + fixed_hdr->header_bytes;

    FieldCollection fields;
    if(version == EncodingVersion::V1) {
        fields = fields_from_proto(seg_hdr->stream_descriptor());
    }
    else {
        const auto* fields_ptr = buffer->data() + preamble_size;
        util::check_magic<MetadataMagic>(fields_ptr);
        if(seg_hdr->has_metadata_field())
            fields_ptr += encoding_sizes::field_compressed_size(seg_hdr->metadata_field());

        util::check_magic<DescriptorMagic>(fields_ptr);
        if(seg_hdr->has_descriptor_field() && seg_hdr->descriptor_field().has_ndarray())
            fields = decode_fields(*seg_hdr, fields_ptr);
    }

    buffer->set_preamble(arcticdb::Segment::FIXED_HEADER_SIZE + fixed_hdr->header_bytes);
    ARCTICDB_SUBSAMPLE(CreateSegment, 0)
    return{std::move(arena), seg_hdr, std::move(buffer), std::make_shared<FieldCollection>(std::move(fields))};

}

void Segment::write_header(uint8_t* dst, size_t hdr_size) const {
    FixedHeader hdr = {MAGIC_NUMBER, HEADER_VERSION_V1, std::uint32_t(hdr_size)};
    hdr.write(dst);
    if(!header_->has_metadata_field())
        ARCTICDB_DEBUG(log::codec(), "Expected metadata field");

    google::protobuf::io::ArrayOutputStream aos(dst + FIXED_HEADER_SIZE, static_cast<int>(hdr_size));
    header_->SerializeToZeroCopyStream(&aos);
}

std::pair<uint8_t*, size_t> Segment::try_internal_write(std::shared_ptr<Buffer>& tmp, size_t hdr_size) {
    auto total_hdr_size = hdr_size + FIXED_HEADER_SIZE;
    if(std::holds_alternative<std::shared_ptr<Buffer>>(buffer_) && std::get<std::shared_ptr<Buffer>>(buffer_)->preamble_bytes() >= total_hdr_size) {
        auto& buffer = std::get<std::shared_ptr<Buffer>>(buffer_);
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
                       arcticdb::Segment::FIXED_HEADER_SIZE + hdr_sz);

    std::memcpy(dst + arcticdb::Segment::FIXED_HEADER_SIZE + hdr_sz,
                buffer().data(),
                buffer().bytes());
}

} //namespace arcticdb
