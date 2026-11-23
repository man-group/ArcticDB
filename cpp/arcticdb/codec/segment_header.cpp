/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/codec/segment_header.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/column_store/string_pool.hpp>

namespace arcticdb {

size_t field_collection_encoded_field_bytes(const FieldCollection& fields) {
    return calc_field_bytes(
            fields.num_blocks() == 0 ? 0 : fields.num_blocks() + 1
    ); // Non-empty field collection always has shapes buffer
}

void check_expected_bytes_match(std::optional<size_t> expected_bytes, size_t bytes_written) {
    util::check(
            !expected_bytes || (bytes_written == *expected_bytes),
            "Mismatch between actual and expected bytes: {} != {}",
            bytes_written,
            expected_bytes ? *expected_bytes : 0
    );
}

size_t SegmentHeader::serialize_to_bytes(uint8_t* dst, std::optional<size_t> expected_bytes) const {
    const auto* begin = dst;
    data_.field_buffer_.fields_bytes_ = static_cast<uint32_t>(header_fields_.data_bytes());
    data_.field_buffer_.offset_bytes_ = static_cast<uint32_t>(header_fields_.offset_bytes());
    memcpy(dst, &data_, sizeof(HeaderData));
    dst += sizeof(HeaderData);
    ARCTICDB_TRACE(log::codec(), "Wrote header data in {} bytes", dst - begin);
    header_fields_.write_data_to(dst);
    memcpy(dst, header_fields_.offsets_buffer(), header_fields_.offset_bytes());
    dst += header_fields_.offset_bytes();
    memcpy(dst, &offset_, sizeof(offset_));
    ARCTICDB_TRACE(log::codec(), "Wrote header fields in {} bytes", dst - begin);
    dst += sizeof(offset_);
    ARCTICDB_TRACE(log::codec(), "Wrote offsets in {} bytes", dst - begin);
    size_t bytes_written = dst - begin;
    check_expected_bytes_match(expected_bytes, bytes_written);
    ARCTICDB_TRACE(
            log::codec(), "Wrote V2 header with {} bytes ({} expected)", bytes_written, expected_bytes.value_or(0)
    );
    return bytes_written;
}

size_t calc_required_header_fields_bytes(const SegmentInMemory& in_mem_seg) {
    size_t required = 0UL;
    if (in_mem_seg.has_index_descriptor()) {
        const auto index_descriptor_size =
                field_collection_encoded_field_bytes(in_mem_seg.index_descriptor().fields()) + sizeof(uint64_t);
        required += index_descriptor_size;
        ARCTICDB_TRACE(log::codec(), "Index descriptor size {}", index_descriptor_size);
    }

    if (in_mem_seg.has_string_pool()) {
        const auto string_pool_size = calc_field_bytes(in_mem_seg.const_string_pool().num_blocks() + 1) +
                                      sizeof(uint64_t); // String pool has a custom shapes buffer
        required += string_pool_size;
        ARCTICDB_TRACE(log::codec(), "String pool size {}", string_pool_size);
    }

    if (!in_mem_seg.descriptor().empty()) {
        const auto descriptor_size =
                field_collection_encoded_field_bytes(in_mem_seg.descriptor().fields()) + sizeof(uint64_t);
        required += descriptor_size;
        ARCTICDB_TRACE(log::codec(), "Descriptor size {}", descriptor_size);
    }

    // Metadata and column fields are allocated in one contiguous buffer with dimension 1
    if (in_mem_seg.metadata()) {
        const auto metadata_size = calc_field_bytes(2) + sizeof(uint64_t);
        required += metadata_size;
        ARCTICDB_TRACE(log::codec(), "Metadata size {}", metadata_size);
    }

    if (in_mem_seg.row_count() > 0) {
        const auto column_fields_size = calc_field_bytes(1) + sizeof(uint64_t);
        ARCTICDB_TRACE(log::codec(), "Column fields size {}", column_fields_size);
        required += column_fields_size;
    }

    ARCTICDB_TRACE(log::codec(), "Required header bytes: {}", required);
    return required;
}

void SegmentHeader::deserialize_from_bytes(const uint8_t* data, bool copy_data) {
    deserialize_from_bytes(data, std::numeric_limits<size_t>::max(), copy_data);
}

void SegmentHeader::deserialize_from_bytes(const uint8_t* data, size_t max_bytes, bool copy_data) {
    util::check_arg(data != nullptr, "Null data pointer in SegmentHeader::deserialize_from_bytes");
    util::check_arg(
            max_bytes >= sizeof(HeaderData) + sizeof(offset_),
            "Truncated V2 SegmentHeader: {} bytes < minimum {}",
            max_bytes,
            sizeof(HeaderData) + sizeof(offset_)
    );
    memcpy(&data_, data, sizeof(HeaderData));
    data += sizeof(HeaderData);
    ChunkedBuffer fields_buffer;
    const size_t fields_bytes = data_.field_buffer_.fields_bytes_;
    const size_t offset_bytes = data_.field_buffer_.offset_bytes_;
    const size_t remaining = max_bytes - sizeof(HeaderData) - sizeof(offset_);
    util::check_arg(
            fields_bytes <= remaining && offset_bytes <= remaining - fields_bytes,
            "Corrupt V2 SegmentHeader: fields_bytes ({}) + offset_bytes ({}) exceeds remaining {} bytes",
            fields_bytes,
            offset_bytes,
            remaining
    );
    util::check_arg(
            offset_bytes % sizeof(uint64_t) == 0,
            "Corrupt V2 SegmentHeader: offset_bytes ({}) not aligned to uint64_t",
            offset_bytes
    );

    if (copy_data) {
        fields_buffer.ensure(fields_bytes);
        memcpy(fields_buffer.data(), data, fields_bytes);
    } else {
        fields_buffer.add_external_block(data, fields_bytes);
    }

    data += fields_bytes;
    Buffer offsets_buffer{offset_bytes};
    memcpy(offsets_buffer.data(), data, offset_bytes);
    data += offsets_buffer.bytes();
    header_fields_ = EncodedFieldCollection{std::move(fields_buffer), std::move(offsets_buffer)};
    auto* offsets = reinterpret_cast<const uint32_t*>(data);
    for (auto i = 0UL; i < offset_.size(); ++i)
        offset_[i] = *offsets++;

    // P0-12: Validate every header_fields_ offset and field bounds, and every active FieldOffset index.
    const size_t num_hdr_fields = header_fields_.size();
    for (size_t fi = 0; fi < num_hdr_fields; ++fi) {
        const uint64_t off = header_fields_.get_offset(fi);
        util::check_arg(
                off <= fields_bytes && EncodedFieldImpl::Size <= fields_bytes - off,
                "Corrupt V2 SegmentHeader: field {} offset {} out of bounds (fields_bytes={})",
                fi,
                off,
                fields_bytes
        );
        const auto& ef = header_fields_.to_field(off);
        const size_t ef_bytes = encoded_field_bytes(ef);
        util::check_arg(
                ef_bytes <= fields_bytes - off,
                "Corrupt V2 SegmentHeader: field {} length {} exceeds fields_bytes {}",
                fi,
                ef_bytes,
                fields_bytes
        );
    }
    for (size_t i = 0; i < static_cast<size_t>(FieldOffset::COUNT); ++i) {
        const auto fo = static_cast<FieldOffset>(i);
        if (has_field(fo)) {
            util::check_arg(
                    static_cast<size_t>(offset_[i]) < num_hdr_fields,
                    "Corrupt V2 SegmentHeader: FieldOffset {} index {} >= num_hdr_fields {}",
                    i,
                    offset_[i],
                    num_hdr_fields
            );
        }
    }
}

size_t SegmentHeader::required_bytes(const SegmentInMemory& in_mem_seg) {
    size_t required = 0UL;
    required += FIXED_HEADER_SIZE;
    required += sizeof(HeaderData);
    required += sizeof(offset_);
    ARCTICDB_TRACE(
            log::codec(),
            "Overhead size {} + {} + {} = {}",
            FIXED_HEADER_SIZE,
            sizeof(HeaderData),
            sizeof(offset_),
            required
    );

    required += calc_required_header_fields_bytes(in_mem_seg);
    ARCTICDB_TRACE(log::codec(), "Total calculated header size: {}", required);
    return required;
}

} // namespace arcticdb
