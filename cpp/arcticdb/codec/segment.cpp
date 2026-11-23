/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/segment.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>
#include <arcticdb/entity/protobuf_mappings.hpp>

#include <arcticdb/util/dump_bytes.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/codec/segment_identifier.hpp>

namespace arcticdb {

arcticdb::proto::encoding::SegmentHeader generate_v1_header(const SegmentHeader& header, const StreamDescriptor& desc) {
    arcticdb::proto::encoding::SegmentHeader segment_header;
    if (header.has_metadata_field())
        copy_encoded_field_to_proto(header.metadata_field(), *segment_header.mutable_metadata_field());

    if (header.has_string_pool_field())
        copy_encoded_field_to_proto(header.string_pool_field(), *segment_header.mutable_string_pool_field());

    copy_stream_descriptor_to_proto(desc, *segment_header.mutable_stream_descriptor());
    copy_encoded_fields_to_proto(header.body_fields(), segment_header);

    segment_header.set_compacted(header.compacted());
    segment_header.set_encoding_version(static_cast<uint16_t>(header.encoding_version()));

    ARCTICDB_TRACE(
            log::codec(),
            "Encoded segment header bytes {}: {}",
            segment_header.ByteSizeLong(),
            segment_header.DebugString()
    );
    return segment_header;
}

namespace segment_size {

size_t column_fields_size(const SegmentHeader& seg_hdr) {
    if (!seg_hdr.has_column_fields())
        return 0;

    return encoding_sizes::ndarray_field_compressed_size(seg_hdr.column_fields().ndarray());
}

SegmentCompressedSize compressed(
        const SegmentHeader& seg_hdr, const std::optional<SegmentHeaderProtoWrapper>& proto_wrapper
) {
    size_t string_pool_size = 0;
    if (seg_hdr.has_string_pool_field())
        string_pool_size = encoding_sizes::ndarray_field_compressed_size(seg_hdr.string_pool_field().ndarray());

    size_t metadata_size = 0;
    if (seg_hdr.has_metadata_field())
        metadata_size = encoding_sizes::ndarray_field_compressed_size(seg_hdr.metadata_field().ndarray());

    size_t buffer_size;
    size_t body_size;
    if (seg_hdr.encoding_version() == EncodingVersion::V1) {
        const auto fields_size = encoding_sizes::segment_compressed_size(proto_wrapper->proto().fields());
        ARCTICDB_DEBUG(
                log::codec(),
                "Calculating total size: {} fields + {} metadata + {} string pool = {}",
                fields_size,
                metadata_size,
                string_pool_size,
                fields_size + metadata_size + string_pool_size
        );
        buffer_size = fields_size + metadata_size + string_pool_size;
        body_size = buffer_size;
    } else {
        buffer_size = seg_hdr.footer_offset();
        if (seg_hdr.has_column_fields())
            buffer_size += sizeof(EncodedMagic) + column_fields_size(seg_hdr);

        body_size = seg_hdr.footer_offset();
        ARCTICDB_DEBUG(log::codec(), "V2 size buffer: {} body {}", buffer_size, body_size);
    }

    return {string_pool_size, buffer_size, body_size};
}
} // namespace segment_size

namespace {
constexpr size_t kMaxHeaderFieldUncompressedBytes = 64ULL * 1024 * 1024;
} // namespace

FieldCollection decode_descriptor_fields(
        const SegmentHeader& hdr, const uint8_t* data, const uint8_t* begin ARCTICDB_UNUSED,
        const uint8_t* end = nullptr
) {
    FieldCollection fields;
    if (hdr.has_descriptor_field()) {
        std::optional<util::BitMagic> bv;
        util::check(hdr.descriptor_field().has_ndarray(), "Expected descriptor field to be ndarray");
        const size_t uncomp = encoding_sizes::uncompressed_size(hdr.descriptor_field());
        util::check_arg(
                uncomp <= kMaxHeaderFieldUncompressedBytes,
                "Corrupt V2 segment: descriptor field uncompressed size {} exceeds limit {}",
                uncomp,
                kMaxHeaderFieldUncompressedBytes
        );
        (void)decode_ndarray(
                FieldCollection::type(), hdr.descriptor_field(), data, fields, bv, hdr.encoding_version(), end
        );

        ARCTICDB_TRACE(log::codec(), "Decoded descriptor to position {}", data - begin);
    }
    fields.regenerate_offsets();
    return fields;
}

SegmentHeaderProtoWrapper decode_protobuf_header(const uint8_t* data, size_t header_bytes_size) {
    util::check_arg(
            header_bytes_size <= static_cast<size_t>(std::numeric_limits<int>::max()),
            "V1 protobuf header size {} exceeds INT_MAX",
            header_bytes_size
    );
    google::protobuf::io::ArrayInputStream ais(data, static_cast<int>(header_bytes_size));

    auto arena = std::make_unique<google::protobuf::Arena>();
    auto seg_hdr = google::protobuf::Arena::Create<arcticdb::proto::encoding::SegmentHeader>(arena.get());
    util::check_arg(seg_hdr->ParseFromZeroCopyStream(&ais), "Failed to parse V1 SegmentHeader protobuf");
    ARCTICDB_TRACE(log::codec(), "Decoded protobuf header: {}", seg_hdr->DebugString());
    return {seg_hdr, std::move(arena)};
}

void skip_metadata_field(const uint8_t*& src, const uint8_t* end, const SegmentHeader& seg_hdr) {
    util::check_arg(
            static_cast<size_t>(end - src) >= sizeof(MetadataMagic),
            "Truncated V2 segment: insufficient bytes for MetadataMagic"
    );
    util::check_magic<MetadataMagic>(src);
    if (seg_hdr.has_metadata_field()) {
        const auto metadata_size = encoding_sizes::field_compressed_size(seg_hdr.metadata_field());
        util::check_arg(
                metadata_size <= static_cast<size_t>(end - src),
                "Corrupt V2 segment: metadata_size {} exceeds remaining {} bytes",
                metadata_size,
                static_cast<size_t>(end - src)
        );
        ARCTICDB_TRACE(log::codec(), "Skipping {} bytes of metadata", metadata_size);
        src += metadata_size;
    }
}

FieldCollection deserialize_descriptor_fields_collection(
        const uint8_t* src, const uint8_t* end, const SegmentHeader& seg_hdr
) {
    FieldCollection fields;

    util::check_arg(
            static_cast<size_t>(end - src) >= sizeof(DescriptorFieldsMagic),
            "Truncated V2 segment: insufficient bytes for DescriptorFieldsMagic"
    );
    util::check_magic<DescriptorFieldsMagic>(src);
    if (seg_hdr.has_descriptor_field() && seg_hdr.descriptor_field().has_ndarray()) {
        const auto desc_compressed = encoding_sizes::ndarray_field_compressed_size(seg_hdr.descriptor_field().ndarray());
        util::check_arg(
                desc_compressed <= static_cast<size_t>(end - src),
                "Corrupt V2 segment: descriptor field size {} exceeds remaining {} bytes",
                desc_compressed,
                static_cast<size_t>(end - src)
        );
        fields = decode_descriptor_fields(seg_hdr, src, src, end);
    }

    return fields;
}

EncodedFieldCollection deserialize_body_fields(const SegmentHeader& hdr, const uint8_t* data, const uint8_t* end) {
    const auto* encoded_fields_ptr = data;
    util::check(hdr.has_column_fields(), "Expected column fields in v2 encoding");
    util::check_arg(
            static_cast<size_t>(end - encoded_fields_ptr) >= sizeof(EncodedMagic),
            "Truncated V2 segment: insufficient bytes for EncodedMagic"
    );
    util::check_magic<EncodedMagic>(encoded_fields_ptr);
    const auto col_compressed = encoding_sizes::ndarray_field_compressed_size(hdr.column_fields().ndarray());
    util::check_arg(
            col_compressed <= static_cast<size_t>(end - encoded_fields_ptr),
            "Corrupt V2 segment: column fields size {} exceeds remaining {} bytes",
            col_compressed,
            static_cast<size_t>(end - encoded_fields_ptr)
    );
    const size_t col_uncomp = encoding_sizes::uncompressed_size(hdr.column_fields());
    util::check_arg(
            col_uncomp <= kMaxHeaderFieldUncompressedBytes,
            "Corrupt V2 segment: column fields uncompressed size {} exceeds limit {}",
            col_uncomp,
            kMaxHeaderFieldUncompressedBytes
    );

    return EncodedFieldCollection{decode_encoded_fields(hdr, encoded_fields_ptr, data, end)};
}

struct DeserializedSegmentData {
    SegmentHeader segment_header_;
    std::shared_ptr<FieldCollection> fields_;
    std::shared_ptr<SegmentDescriptorImpl> segment_desc_;
    std::optional<SegmentHeaderProtoWrapper> proto_wrapper_;
    StreamId stream_id_;
};

DeserializedSegmentData decode_header_and_fields(const uint8_t*& src, size_t readable_size, bool copy_data) {
    util::check(src != nullptr, "Got null data ptr from segment");
    util::check_arg(
            readable_size >= FIXED_HEADER_SIZE,
            "Truncated segment buffer: {} bytes < FIXED_HEADER_SIZE ({})",
            readable_size,
            FIXED_HEADER_SIZE
    );
    auto* fixed_hdr = reinterpret_cast<const FixedHeader*>(src);
    util::check_arg(
            fixed_hdr->magic_number == MAGIC_NUMBER,
            "expected first 2 bytes: {}, actual {}",
            fixed_hdr->magic_number,
            MAGIC_NUMBER
    );
    util::check_arg(
            static_cast<size_t>(fixed_hdr->header_bytes) <= readable_size - FIXED_HEADER_SIZE,
            "Corrupt segment header: header_bytes ({}) exceeds readable_size - FIXED_HEADER_SIZE ({})",
            fixed_hdr->header_bytes,
            readable_size - FIXED_HEADER_SIZE
    );
    ARCTICDB_DEBUG(
            log::codec(),
            "Reading header: {} + {} = {}",
            FIXED_HEADER_SIZE,
            fixed_hdr->header_bytes,
            FIXED_HEADER_SIZE + static_cast<size_t>(fixed_hdr->header_bytes)
    );

    std::optional<SegmentHeaderProtoWrapper> proto_wrapper;
    const uint8_t* const end = src + readable_size;
    const auto* header_ptr = src + FIXED_HEADER_SIZE;
    const size_t total_header_size = FIXED_HEADER_SIZE + static_cast<size_t>(fixed_hdr->header_bytes);
    if (const auto header_version = fixed_hdr->encoding_version; header_version == HEADER_VERSION_V1) {
        proto_wrapper = decode_protobuf_header(header_ptr, fixed_hdr->header_bytes);
        auto data = std::make_shared<SegmentDescriptorImpl>(
                segment_descriptor_from_proto(proto_wrapper->proto().stream_descriptor())
        );
        auto segment_header = deserialize_segment_header_from_proto(proto_wrapper->proto());
        util::check(
                segment_header.encoding_version() == EncodingVersion::V1,
                "Expected v1 header to contain legacy encoding version"
        );
        auto fields = std::make_shared<FieldCollection>(
                field_collection_from_proto(proto_wrapper->proto().stream_descriptor().fields())
        );
        src += total_header_size;
        auto stream_id = stream_id_from_proto(proto_wrapper->proto().stream_descriptor());
        return {std::move(segment_header), std::move(fields), std::move(data), std::move(proto_wrapper), stream_id};
    } else if (header_version == HEADER_VERSION_V2) {
        SegmentHeader segment_header;
        const auto* fields_ptr = header_ptr + fixed_hdr->header_bytes;
        segment_header.deserialize_from_bytes(header_ptr, fixed_hdr->header_bytes, copy_data);
        skip_metadata_field(fields_ptr, end, segment_header);
        util::check_arg(
                static_cast<size_t>(end - fields_ptr) >= sizeof(SegmentDescriptorMagic) + sizeof(SegmentDescriptorImpl),
                "Truncated V2 segment before segment descriptor"
        );
        auto segment_desc = std::make_shared<SegmentDescriptorImpl>(read_segment_descriptor(fields_ptr));
        auto stream_id = read_identifier_bounded(fields_ptr, end);
        util::check(segment_header.encoding_version() == EncodingVersion::V2, "Expected V2 encoding in binary header");
        auto fields = std::make_shared<FieldCollection>(
                deserialize_descriptor_fields_collection(fields_ptr, end, segment_header)
        );
        src += total_header_size;
        return {std::move(segment_header),
                std::move(fields),
                std::move(segment_desc),
                std::move(proto_wrapper),
                stream_id};
    } else {
        util::raise_rte("Unsupported segment header version {}", header_version);
    }
}

void check_encoding(EncodingVersion encoding_version) {
    util::check(
            encoding_version == EncodingVersion::V1 || encoding_version == EncodingVersion::V2,
            "expected encoding_version < 2, actual {}",
            encoding_version
    );
}

void check_size(const FixedHeader* fixed_hdr, size_t buffer_bytes, size_t readable_size, size_t string_pool_size) {
    const size_t total_hdr = FIXED_HEADER_SIZE + static_cast<size_t>(fixed_hdr->header_bytes);
    util::check(
            total_hdr <= readable_size && buffer_bytes <= readable_size - total_hdr,
            "Size disparity, fixed header size {} + variable header size {} + buffer size {}  (string pool size {}) > "
            "total size {}",
            FIXED_HEADER_SIZE,
            fixed_hdr->header_bytes,
            buffer_bytes,
            string_pool_size,
            readable_size
    );
}

void set_body_fields(SegmentHeader& seg_hdr, const uint8_t* src, size_t buffer_bytes) {
    if (seg_hdr.has_column_fields()) {
        util::check_arg(
                seg_hdr.footer_offset() <= buffer_bytes,
                "Corrupt V2 segment: footer_offset ({}) exceeds buffer_bytes ({})",
                seg_hdr.footer_offset(),
                buffer_bytes
        );
        auto encoded_fields = deserialize_body_fields(seg_hdr, src + seg_hdr.footer_offset(), src + buffer_bytes);
        encoded_fields.regenerate_offsets();
        size_t total_body_compressed = 0;
        auto add_checked = [&](size_t addend) {
            util::check_arg(
                    addend <= seg_hdr.footer_offset() && total_body_compressed <= seg_hdr.footer_offset() - addend,
                    "Corrupt V2 segment: decoded body fields compressed size exceeds footer_offset ({})",
                    seg_hdr.footer_offset()
            );
            total_body_compressed += addend;
        };
        const size_t num_fields = encoded_fields.size();
        for (size_t fi = 0; fi < num_fields; ++fi) {
            const uint64_t off = encoded_fields.get_offset(fi);
            util::check_arg(
                    off <= encoded_fields.data_bytes() &&
                            EncodedFieldImpl::Size <= encoded_fields.data_bytes() - off,
                    "Corrupt V2 segment: body field offset {} out of bounds",
                    off
            );
            const auto& ef = encoded_fields.to_field(off);
            const size_t ef_bytes = encoded_field_bytes(ef);
            util::check_arg(
                    ef_bytes <= encoded_fields.data_bytes() - off,
                    "Corrupt V2 segment: body field length {} exceeds data bytes",
                    ef_bytes
            );
            if (ef.has_ndarray()) {
                const auto& nda = ef.ndarray();
                for (const auto& s : nda.shapes()) {
                    add_checked(static_cast<size_t>(s.out_bytes()));
                }
                for (const auto& v : nda.values()) {
                    add_checked(static_cast<size_t>(v.out_bytes()));
                }
                if (nda.sparse_map_bytes() > 0) {
                    util::check_arg(
                            nda.sparse_map_bytes() >= util::combined_bit_magic_delimiters_size(),
                            "Corrupt V2 segment: sparse_map_bytes ({}) smaller than delimiters",
                            nda.sparse_map_bytes()
                    );
                    add_checked(static_cast<size_t>(nda.sparse_map_bytes()));
                }
            }
        }
        if (seg_hdr.has_string_pool_field()) {
            const auto& sp = seg_hdr.string_pool_field();
            for (const auto& s : sp.shapes()) {
                add_checked(static_cast<size_t>(s.out_bytes()));
            }
            for (const auto& v : sp.values()) {
                add_checked(static_cast<size_t>(v.out_bytes()));
            }
        }
        seg_hdr.set_body_fields(std::move(encoded_fields));
    }
}

void set_body_fields(SegmentHeader& seg_hdr, const uint8_t* src) {
    if (seg_hdr.has_column_fields()) {
        const size_t min_len = seg_hdr.footer_offset() + sizeof(EncodedMagic) + segment_size::column_fields_size(seg_hdr);
        auto encoded_fields = deserialize_body_fields(seg_hdr, src + seg_hdr.footer_offset(), src + min_len);
        seg_hdr.set_body_fields(std::move(encoded_fields));
    }
}

Segment Segment::from_bytes(const std::uint8_t* src, std::size_t readable_size, bool copy_data /* = false */) {
    ARCTICDB_SAMPLE(SegmentFromBytes, 0)
    util::check(src != nullptr, "Got null data ptr from segment");
    util::check_arg(
            readable_size >= FIXED_HEADER_SIZE,
            "Truncated segment buffer: {} bytes < FIXED_HEADER_SIZE ({})",
            readable_size,
            FIXED_HEADER_SIZE
    );
    auto* fixed_hdr = reinterpret_cast<const FixedHeader*>(src);
    auto [seg_hdr, fields, desc_data, proto_wrapper, stream_id] = decode_header_and_fields(src, readable_size, copy_data);
    check_encoding(seg_hdr.encoding_version());
    const auto [string_pool_size, buffer_bytes, body_bytes] = segment_size::compressed(seg_hdr, proto_wrapper);
    check_size(fixed_hdr, buffer_bytes, readable_size, string_pool_size);
    ARCTICDB_DEBUG(
            log::codec(),
            "Reading string pool {} header {} + {} and buffer bytes {}",
            string_pool_size,
            FIXED_HEADER_SIZE,
            fixed_hdr->header_bytes,
            buffer_bytes
    );
    ARCTICDB_SUBSAMPLE(CreateBufferView, 0)
    VariantBuffer variant_buffer;
    if (copy_data) {
        auto buf = std::make_shared<Buffer>();
        buf->ensure(buffer_bytes);
        memcpy(buf->data(), src, buffer_bytes);
        variant_buffer = std::move(buf);
    } else {
        variant_buffer = BufferView{const_cast<uint8_t*>(src), buffer_bytes};
    }

    set_body_fields(seg_hdr, src, buffer_bytes);
    return {std::move(seg_hdr),
            std::move(variant_buffer),
            std::move(desc_data),
            std::move(fields),
            stream_id,
            readable_size};
}

Segment Segment::from_buffer(const std::shared_ptr<Buffer>& buffer) {
    ARCTICDB_SAMPLE(SegmentFromBuffer, 0)
    util::check(buffer != nullptr, "Got null buffer in Segment::from_buffer");
    auto readable_size = buffer->bytes();
    util::check_arg(
            readable_size >= FIXED_HEADER_SIZE,
            "Truncated segment buffer: {} bytes < FIXED_HEADER_SIZE ({})",
            readable_size,
            FIXED_HEADER_SIZE
    );
    auto* fixed_hdr = reinterpret_cast<FixedHeader*>(buffer->data());
    const auto* src = buffer->data();
    auto [seg_hdr, fields, desc_data, proto_wrapper, stream_id] = decode_header_and_fields(src, readable_size, false);
    check_encoding(seg_hdr.encoding_version());

    ARCTICDB_SUBSAMPLE(ReadHeaderAndSegment, 0)
    auto header_bytes ARCTICDB_UNUSED = FIXED_HEADER_SIZE + static_cast<size_t>(fixed_hdr->header_bytes);
    ARCTICDB_TRACE(
            log::codec(), "Reading header: {} + {} = {}", FIXED_HEADER_SIZE, fixed_hdr->header_bytes, header_bytes
    );

    const auto [string_pool_size, buffer_bytes, body_bytes] = segment_size::compressed(seg_hdr, proto_wrapper);
    ARCTICDB_TRACE(log::codec(), "Reading string pool {} and buffer bytes {}", string_pool_size, buffer_bytes);
    check_size(fixed_hdr, buffer_bytes, readable_size, string_pool_size);

    set_body_fields(seg_hdr, src, buffer_bytes);
    buffer->set_preamble(FIXED_HEADER_SIZE + static_cast<size_t>(fixed_hdr->header_bytes));
    ARCTICDB_SUBSAMPLE(CreateSegment, 0)
    return {std::move(seg_hdr), buffer, std::move(desc_data), std::move(fields), stream_id, readable_size};
}

size_t Segment::write_proto_header(uint8_t* dst, size_t header_size) {
    const auto& header = generate_header_proto();
    const auto hdr_size = proto_size();
    FixedHeader hdr = {MAGIC_NUMBER, HEADER_VERSION_V1, std::uint32_t(hdr_size)};
    write_fixed_header(dst, hdr);
    util::check(header_size == proto_size(), "Header size mismatch: {} != {}", header_size, proto_size());
    google::protobuf::io::ArrayOutputStream aos(dst + FIXED_HEADER_SIZE, static_cast<int>(hdr_size));
    header.SerializeToZeroCopyStream(&aos);
    return hdr_size;
}

size_t Segment::write_binary_header(uint8_t* dst) const {
    auto bytes_written = header_.serialize_to_bytes(dst + sizeof(FixedHeader));
    FixedHeader hdr = {MAGIC_NUMBER, HEADER_VERSION_V2, std::uint32_t(bytes_written)};
    write_fixed_header(dst, hdr);
    return bytes_written;
}

std::pair<uint8_t*, size_t> Segment::serialize_header_v2(size_t expected_bytes) {
    ARCTICDB_TRACE(log::codec(), "Calculating bytes for header {}", header_);
    const auto header_bytes = header_.bytes() + sizeof(FixedHeader);
    FixedHeader hdr = {MAGIC_NUMBER, HEADER_VERSION_V2, std::uint32_t(expected_bytes)};
    util::check(
            header_bytes == buffer_.preamble_bytes(),
            "Expected v2 header of size {} to fit exactly into buffer preamble of size {}",
            header_.bytes(),
            buffer_.preamble_bytes()
    );
    const auto& buffer = buffer_.get_owning_buffer();
    auto* dst = buffer->preamble();
    write_fixed_header(dst, hdr);
    header_.serialize_to_bytes(dst + FIXED_HEADER_SIZE, expected_bytes);
    return std::make_pair(buffer->preamble(), calculate_size());
}

std::pair<uint8_t*, size_t> Segment::serialize_v1_header_in_place(size_t hdr_size) {
    const auto total_hdr_size = hdr_size + FIXED_HEADER_SIZE;
    const auto& buffer = buffer_.get_owning_buffer();
    auto base_ptr = buffer->preamble() + (buffer->preamble_bytes() - total_hdr_size);
    util::check(buffer->data() != nullptr, "Unexpected null base pointer in v1 header serialization");
    util::check(
            base_ptr + total_hdr_size == buffer->data(),
            "Expected base ptr to align with data ptr, {} != {}",
            fmt::ptr(base_ptr + total_hdr_size),
            fmt::ptr(buffer->data())
    );
    auto red_zone = *buffer->data();
    auto header_bytes_written = write_proto_header(base_ptr, hdr_size);
    ARCTICDB_TRACE(
            log::storage(),
            "Header fits in internal buffer {:x} with {} bytes space: {}",
            intptr_t(base_ptr),
            buffer->preamble_bytes() - total_hdr_size,
            dump_bytes(buffer->data(), buffer->bytes(), 10u)
    );
    auto check_red_zone = *buffer->data();
    util::check(red_zone == check_red_zone, "Data overwrite occurred {} != {}", check_red_zone, red_zone);
    util::check(
            header_bytes_written == hdr_size,
            "Wrote unexpected number of header bytes {} != {}",
            header_bytes_written,
            total_hdr_size
    );
    return std::make_pair(base_ptr, calculate_size());
}

std::tuple<uint8_t*, size_t, std::unique_ptr<Buffer>> Segment::serialize_v1_header_to_buffer(size_t hdr_size) {
    auto tmp = std::make_unique<Buffer>();
    auto bytes_to_copy = buffer().bytes();
    auto offset = FIXED_HEADER_SIZE + hdr_size;

    auto total_size = offset + bytes_to_copy;
    tmp->ensure(total_size);

    util::check(
            tmp->available() >= total_size,
            "Buffer available space {} is less than required size {}",
            tmp->available(),
            total_size
    );

    auto calculated_size = calculate_size();
    util::check(
            total_size == calculated_size,
            "Expected total size {} to be equal to calculated size {}",
            total_size,
            calculated_size
    );

    auto* dst = tmp->preamble();
    util::check(dst != nullptr, "Expected dst to be non-null");
    auto header_bytes_written = write_proto_header(dst, hdr_size);

    // This is a bit redundant since the size is also checked in write_proto_header, but the consequences of getting
    // it wrong are pretty bad (corrupt data) so will leave it in for future-proofing
    util::check(
            header_bytes_written == hdr_size,
            "Expected written header size {} to be equal to expected header size {}",
            header_bytes_written,
            hdr_size
    );

    if (buffer().data() != nullptr) {
        std::memcpy(dst + offset, buffer().data(), buffer().bytes());
    } else {
        util::check(bytes_to_copy == 0, "Expected bytes_to_copy to be 0 when src is nullptr");
        ARCTICDB_DEBUG(log::codec(), "src is nullptr, skipping memcpy");
    }

    return std::make_tuple(tmp->preamble(), calculate_size(), std::move(tmp));
}

std::tuple<uint8_t*, size_t, std::unique_ptr<Buffer>> Segment::serialize_header_v1() {
    auto proto_header = generate_header_proto();
    const auto hdr_size = proto_size();
    auto total_hdr_size = hdr_size + FIXED_HEADER_SIZE;

    if (buffer_.is_owning_buffer() && buffer_.preamble_bytes() >= total_hdr_size) {
        auto [dst, size] = serialize_v1_header_in_place(hdr_size);
        return std::make_tuple(dst, size, std::unique_ptr<Buffer>());
    } else {
        return serialize_v1_header_to_buffer(hdr_size);
    }
}

std::tuple<uint8_t*, size_t, std::unique_ptr<Buffer>> Segment::serialize_header() {
    if (header_.encoding_version() == EncodingVersion::V1) {
        return serialize_header_v1();
    } else {
        auto [dst, size] = serialize_header_v2(buffer_.preamble_bytes() - FIXED_HEADER_SIZE);
        return std::make_tuple(dst, size, std::unique_ptr<Buffer>());
    }
}

[[nodiscard]] std::shared_ptr<FieldCollection> Segment::fields_ptr() const { return desc_.fields_ptr(); }

[[nodiscard]] size_t Segment::fields_size() const { return desc_.field_count(); }

[[nodiscard]] const Field& Segment::fields(size_t pos) const { return desc_.fields(pos); }

const arcticdb::proto::encoding::SegmentHeader& Segment::generate_header_proto() {
    if (!proto_) {
        proto_ = std::make_unique<arcticdb::proto::encoding::SegmentHeader>(generate_v1_header(header_, desc_));
        proto_size_ = proto_->ByteSizeLong();
    }

    return *proto_;
}

void Segment::write_to(std::uint8_t* dst) {
    ARCTICDB_SAMPLE(SegmentWriteToStorage, RMTSF_Aggregate)
    ARCTICDB_SUBSAMPLE(SegmentWriteHeader, RMTSF_Aggregate)

    size_t header_size;
    if (header_.encoding_version() == EncodingVersion::V1)
        header_size = write_proto_header(dst, proto_size());
    else
        header_size = write_binary_header(dst);

    ARCTICDB_SUBSAMPLE(SegmentWriteBody, RMTSF_Aggregate)
    ARCTICDB_DEBUG(
            log::codec(), "Writing {} bytes to body at offset {}", buffer().bytes(), FIXED_HEADER_SIZE + header_size
    );
    std::memcpy(dst + FIXED_HEADER_SIZE + header_size, buffer().data(), buffer().bytes());
    ARCTICDB_DEBUG(
            log::codec(),
            "Wrote segment {} header {} body ({} bytes)",
            header_size + FIXED_HEADER_SIZE,
            buffer().bytes(),
            header_size + buffer().bytes() + FIXED_HEADER_SIZE
    );
}

void Segment::set_fields(FieldCollection&& fields) {
    desc_.fields_ = std::make_shared<FieldCollection>(std::move(fields));
    proto_.reset();
    proto_size_ = 0;
    size_.reset();
}

uint64_t Segment::estimate_uncompressed_bytes() const {
    auto add_sat = [](uint64_t& acc, uint64_t v) {
        if (v > std::numeric_limits<uint64_t>::max() - acc) {
            acc = std::numeric_limits<uint64_t>::max();
        } else {
            acc += v;
        }
    };
    auto add_field = [&](uint64_t& acc, const EncodedFieldImpl& field) {
        if (field.encoding_case() != EncodedFieldType::NDARRAY || !field.has_ndarray()) {
            return;
        }
        const auto& nda = field.ndarray();
        for (const auto& shape : nda.shapes()) {
            add_sat(acc, static_cast<uint64_t>(shape.in_bytes()));
        }
        for (const auto& val : nda.values()) {
            add_sat(acc, static_cast<uint64_t>(val.in_bytes()));
        }
        add_sat(acc, static_cast<uint64_t>(nda.sparse_map_bytes()));
    };

    uint64_t total = 0;
    if (header_.has_metadata_field()) {
        add_field(total, header_.metadata_field());
    }
    if (header_.has_string_pool_field()) {
        add_field(total, header_.string_pool_field());
    }
    if (header_.has_descriptor_field()) {
        add_field(total, header_.descriptor_field());
    }
    if (header_.has_index_descriptor_field()) {
        add_field(total, header_.index_descriptor_field());
    }
    if (header_.has_column_fields()) {
        add_field(total, header_.column_fields());
    }
    const auto& bfs = header_.body_fields();
    const size_t n_bf = bfs.size();
    for (size_t fi = 0; fi < n_bf; ++fi) {
        add_field(total, bfs.at(fi));
    }
    return total;
}

} // namespace arcticdb
