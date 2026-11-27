/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/enriched_op_log_payload.hpp>

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/codec/magic_words.hpp>
#include <arcticdb/codec/segment_identifier.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/util/buffer.hpp>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <xxhash.h>

#include <array>
#include <cstddef>
#include <cstring>
#include <limits>

namespace arcticdb {

namespace {

constexpr std::array<uint8_t, 8> kMagic = {'A', 'D', 'B', 'E', 'O', 'P', 'L', '1'};
constexpr uint32_t kFormatVersion = 1;
constexpr size_t kHeaderBytes = 8 + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint64_t); // 24 bytes

constexpr uint32_t kFlagHasPrevIndexKey = 1u << 0;
constexpr uint32_t kFlagHasColumnStats = 1u << 1;
constexpr uint32_t kFlagHasInlineColumnStats = 1u << 2;
constexpr uint32_t kFlagIsCleanChainSuccessor = 1u << 3;
constexpr uint32_t kKnownFlagsMask =
        kFlagHasPrevIndexKey | kFlagHasColumnStats | kFlagHasInlineColumnStats | kFlagIsCleanChainSuccessor;

constexpr uint8_t kVariantTagNumeric = 1;
constexpr uint8_t kVariantTagString = 2;
constexpr uint8_t kVariantTagUnsigned = 3;

// Defensive limits against corrupted/malicious payloads.
constexpr uint32_t kMaxStringBytes = 64 * 1024;
constexpr size_t kMaxUncompressedMetaBytes = size_t{256} * 1024 * 1024;
// Minimum wire size of one encoded AtomKey: 1 (type) + 5 (empty string id) + 3*8 (ver/ts/hash) + 2*5 (empty indices).
constexpr size_t kMinEncodedAtomKeyBytes = 40;

void write_u32_le(uint8_t* dst, uint32_t v) noexcept {
    dst[0] = static_cast<uint8_t>(v & 0xFFu);
    dst[1] = static_cast<uint8_t>((v >> 8) & 0xFFu);
    dst[2] = static_cast<uint8_t>((v >> 16) & 0xFFu);
    dst[3] = static_cast<uint8_t>((v >> 24) & 0xFFu);
}

void write_u64_le(uint8_t* dst, uint64_t v) noexcept {
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        dst[i] = static_cast<uint8_t>((v >> (i * 8)) & 0xFFu);
    }
}

uint64_t compute_payload_checksum(const uint8_t* version_and_flags_8b, const uint8_t* body, size_t body_len) noexcept {
    XXH64_state_t state;
    XXH64_reset(&state, 0);
    XXH64_update(&state, version_and_flags_8b, 8);
    if (body_len > 0) {
        XXH64_update(&state, body, body_len);
    }
    return XXH64_digest(&state);
}

void append_u8(std::vector<uint8_t>& out, uint8_t v) { out.push_back(v); }

void append_u32(std::vector<uint8_t>& out, uint32_t v) {
    const size_t pos = out.size();
    out.resize(pos + sizeof(uint32_t));
    write_u32_le(out.data() + pos, v);
}

void append_u64(std::vector<uint8_t>& out, uint64_t v) {
    const size_t pos = out.size();
    out.resize(pos + sizeof(uint64_t));
    write_u64_le(out.data() + pos, v);
}

void append_i64(std::vector<uint8_t>& out, int64_t v) { append_u64(out, static_cast<uint64_t>(v)); }

void append_bytes(std::vector<uint8_t>& out, std::span<const uint8_t> bytes) {
    util::check(
            bytes.size() <= std::numeric_limits<uint32_t>::max(),
            "Enriched op-log blob size {} exceeds uint32_t limit",
            bytes.size()
    );
    append_u32(out, static_cast<uint32_t>(bytes.size()));
    if (!bytes.empty()) {
        const size_t pos = out.size();
        out.resize(pos + bytes.size());
        std::memcpy(out.data() + pos, bytes.data(), bytes.size());
    }
}

void append_string(std::vector<uint8_t>& out, std::string_view sv) {
    append_bytes(out, std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(sv.data()), sv.size()));
}

template<typename VariantT>
void append_variant_id(std::vector<uint8_t>& out, const VariantT& v) {
    if (const auto* num = std::get_if<int64_t>(&v)) {
        append_u8(out, kVariantTagNumeric);
        append_i64(out, *num);
    } else if (const auto* str = std::get_if<std::string>(&v)) {
        append_u8(out, kVariantTagString);
        append_string(out, *str);
    } else if (const auto* unum = std::get_if<uint64_t>(&v)) {
        append_u8(out, kVariantTagUnsigned);
        append_u64(out, *unum);
    } else {
        util::raise_rte("Unexpected variant alternative in enriched op-log key encoding");
    }
}

void append_atom_key(std::vector<uint8_t>& out, const entity::AtomKey& key) {
    append_u8(out, static_cast<uint8_t>(key.type()));
    append_variant_id(out, key.id());
    append_u64(out, key.version_id());
    append_i64(out, key.creation_ts());
    append_u64(out, key.content_hash());
    append_variant_id(out, key.start_index());
    append_variant_id(out, key.end_index());
}

struct ByteReader {
    const uint8_t* ptr{nullptr};
    const uint8_t* end{nullptr};

    [[nodiscard]] size_t remaining() const noexcept { return static_cast<size_t>(end - ptr); }

    bool read_u8(uint8_t& out) noexcept {
        if (remaining() < 1) {
            return false;
        }
        out = *ptr++;
        return true;
    }

    bool read_u32(uint32_t& out) noexcept {
        if (remaining() < sizeof(uint32_t)) {
            return false;
        }
        out = static_cast<uint32_t>(ptr[0]) | (static_cast<uint32_t>(ptr[1]) << 8) |
              (static_cast<uint32_t>(ptr[2]) << 16) | (static_cast<uint32_t>(ptr[3]) << 24);
        ptr += sizeof(uint32_t);
        return true;
    }

    bool read_u64(uint64_t& out) noexcept {
        if (remaining() < sizeof(uint64_t)) {
            return false;
        }
        uint64_t val = 0;
        for (size_t i = 0; i < sizeof(uint64_t); ++i) {
            val |= static_cast<uint64_t>(ptr[i]) << (i * 8);
        }
        ptr += sizeof(uint64_t);
        out = val;
        return true;
    }

    bool read_i64(int64_t& out) noexcept {
        uint64_t raw = 0;
        if (!read_u64(raw)) {
            return false;
        }
        out = static_cast<int64_t>(raw);
        return true;
    }

    bool read_bytes(std::vector<uint8_t>& out, uint32_t max_len) {
        uint32_t len = 0;
        if (!read_u32(len) || len > max_len || remaining() < len) {
            return false;
        }
        out.resize(len);
        if (len > 0) {
            std::memcpy(out.data(), ptr, len);
            ptr += len;
        }
        return true;
    }

    bool read_string(std::string& out) {
        uint32_t len = 0;
        if (!read_u32(len) || len > kMaxStringBytes || remaining() < len) {
            return false;
        }
        out.assign(reinterpret_cast<const char*>(ptr), len);
        ptr += len;
        return true;
    }

    template<typename VariantT>
    bool read_variant_id(VariantT& out) {
        uint8_t tag = 0;
        if (!read_u8(tag)) {
            return false;
        }
        if (tag == kVariantTagNumeric) {
            int64_t v = 0;
            if (!read_i64(v)) {
                return false;
            }
            out = v;
            return true;
        }
        if (tag == kVariantTagString) {
            std::string s;
            if (!read_string(s)) {
                return false;
            }
            out = std::move(s);
            return true;
        }
        if (tag == kVariantTagUnsigned) {
            uint64_t v = 0;
            if (!read_u64(v)) {
                return false;
            }
            out = v;
            return true;
        }
        return false;
    }

    bool read_atom_key(entity::AtomKey& out) {
        uint8_t kt_raw = 0;
        if (!read_u8(kt_raw) || kt_raw >= static_cast<uint8_t>(entity::KeyType::UNDEFINED)) {
            return false;
        }
        StreamId id;
        uint64_t version_id = 0;
        int64_t creation_ts = 0;
        uint64_t content_hash = 0;
        entity::IndexValue start_idx;
        entity::IndexValue end_idx;

        if (!read_variant_id(id) || !read_u64(version_id) || !read_i64(creation_ts) || !read_u64(content_hash) ||
            !read_variant_id(start_idx) || !read_variant_id(end_idx)) {
            return false;
        }

        out = entity::AtomKeyBuilder()
                      .version_id(version_id)
                      .creation_ts(creation_ts)
                      .content_hash(content_hash)
                      .start_index(start_idx)
                      .end_index(end_idx)
                      .build(std::move(id), static_cast<entity::KeyType>(kt_raw));
        return true;
    }
};

bool decode_and_verify_header(std::span<const uint8_t> bytes, ByteReader& reader, uint32_t& flags) noexcept {
    if (bytes.size() < kHeaderBytes || std::memcmp(bytes.data(), kMagic.data(), kMagic.size()) != 0) {
        return false;
    }
    reader = ByteReader{bytes.data() + kMagic.size(), bytes.data() + bytes.size()};
    uint32_t version = 0;
    uint64_t expected_hash = 0;
    if (!reader.read_u32(version) || !reader.read_u32(flags) || !reader.read_u64(expected_hash)) {
        return false;
    }
    if (version != kFormatVersion || (flags & ~kKnownFlagsMask) != 0) {
        return false;
    }
    if ((flags & kFlagHasInlineColumnStats) != 0 && (flags & kFlagHasColumnStats) == 0) {
        return false;
    }
    const uint64_t actual_hash = compute_payload_checksum(bytes.data() + kMagic.size(), reader.ptr, reader.remaining());
    return actual_hash == expected_hash;
}

bool decode_data_keys(ByteReader& reader, std::vector<entity::AtomKey>& out) {
    uint32_t num_data_keys = 0;
    if (!reader.read_u32(num_data_keys) || num_data_keys > kMaxEnrichedOpLogKeysPerPayload ||
        num_data_keys > reader.remaining() / kMinEncodedAtomKeyBytes) {
        return false;
    }
    out.reserve(num_data_keys);
    for (uint32_t i = 0; i < num_data_keys; ++i) {
        entity::AtomKey dk;
        if (!reader.read_atom_key(dk)) {
            return false;
        }
        out.push_back(std::move(dk));
    }
    return true;
}

bool decode_column_stats(ByteReader& reader, uint32_t flags, EnrichedOpLogPayload& payload) {
    if ((flags & kFlagHasColumnStats) == 0) {
        return true;
    }
    entity::AtomKey cs_key;
    if (!reader.read_atom_key(cs_key)) {
        return false;
    }
    payload.column_stats_key = std::move(cs_key);
    if ((flags & kFlagHasInlineColumnStats) != 0) {
        if (!reader.read_bytes(payload.column_stats_bytes, kMaxEnrichedOpLogBlobBytes) ||
            payload.column_stats_bytes.empty()) {
            return false;
        }
    }
    return true;
}

bool validate_v1_segment_bounds(std::span<const uint8_t> bytes, const FixedHeader& fixed_hdr) {
    if (fixed_hdr.header_bytes == 0 ||
        fixed_hdr.header_bytes > static_cast<uint32_t>(std::numeric_limits<int>::max())) {
        return false;
    }
    google::protobuf::io::ArrayInputStream ais(
            bytes.data() + FIXED_HEADER_SIZE, static_cast<int>(fixed_hdr.header_bytes)
    );
    arcticdb::proto::encoding::SegmentHeader proto_hdr;
    return proto_hdr.ParseFromZeroCopyStream(&ais);
}

bool validate_v2_header_fields(const HeaderData& hdata, std::span<const uint8_t> header_span) {
    constexpr size_t kOffsetsArrayBytes = sizeof(std::array<uint32_t, 5>);
    constexpr size_t kMinV2HeaderBytes = sizeof(HeaderData) + kOffsetsArrayBytes;
    if (header_span.size() < kMinV2HeaderBytes || hdata.encoding_version_ != EncodingVersion::V2) {
        return false;
    }
    const size_t fields_bytes = hdata.field_buffer_.fields_bytes_;
    const size_t offset_bytes = hdata.field_buffer_.offset_bytes_;
    const size_t available = header_span.size() - kMinV2HeaderBytes;
    if (fields_bytes > available || offset_bytes > available - fields_bytes || (offset_bytes % sizeof(uint64_t)) != 0) {
        return false;
    }
    const size_t num_offsets = offset_bytes / sizeof(uint64_t);
    const uint8_t* offsets_ptr = header_span.data() + sizeof(HeaderData) + fields_bytes;
    const uint8_t* field_idx_ptr = offsets_ptr + offset_bytes;
    std::array<uint32_t, 5> field_indices{};
    std::memcpy(field_indices.data(), field_idx_ptr, kOffsetsArrayBytes);
    for (size_t i = 0; i < static_cast<size_t>(FieldOffset::COUNT); ++i) {
        if ((hdata.fields_ & SegmentHeader::field_mask(FieldOffset(i))) == 0) {
            continue;
        }
        if (field_indices[i] >= num_offsets) {
            return false;
        }
        uint64_t field_pos = 0;
        std::memcpy(&field_pos, offsets_ptr + field_indices[i] * sizeof(uint64_t), sizeof(uint64_t));
        if (field_pos > fields_bytes || fields_bytes - field_pos < EncodedFieldImpl::Size) {
            return false;
        }
        uint32_t shapes_count = 0;
        uint32_t values_count = 0;
        const uint8_t* field_ptr = header_span.data() + sizeof(HeaderData) + field_pos;
        std::memcpy(&shapes_count, field_ptr + offsetof(EncodedField, shapes_count_), sizeof(shapes_count));
        std::memcpy(&values_count, field_ptr + offsetof(EncodedField, values_count_), sizeof(values_count));
        const uint64_t total_blocks = uint64_t{shapes_count} + uint64_t{values_count};
        const size_t rem = fields_bytes - field_pos;
        if (total_blocks > static_cast<uint64_t>(std::numeric_limits<int>::max()) ||
            total_blocks > (rem - EncodedFieldImpl::Size) / sizeof(EncodedBlock) ||
            calc_field_bytes(static_cast<size_t>(total_blocks)) > rem) {
            return false;
        }
    }
    return true;
}

bool validate_v2_ndarray_meta_field(
        const EncodedFieldImpl& field, size_t available_bytes, size_t max_uncompressed_bytes
) {
    if (!field.has_ndarray()) {
        return false;
    }
    const auto& nda = field.ndarray();
    if (nda.sparse_map_bytes() != 0) {
        return false;
    }
    uint64_t compressed_total = 0;
    uint64_t uncompressed_total = 0;
    for (const auto& blk : nda.shapes()) {
        compressed_total += blk.out_bytes();
        uncompressed_total += blk.in_bytes();
        if (compressed_total > available_bytes || uncompressed_total > max_uncompressed_bytes) {
            return false;
        }
    }
    for (const auto& blk : nda.values()) {
        compressed_total += blk.out_bytes();
        uncompressed_total += blk.in_bytes();
        if (compressed_total > available_bytes || uncompressed_total > max_uncompressed_bytes) {
            return false;
        }
    }
    return true;
}

bool validate_v2_body_bounds(const SegmentHeader& hdr, std::span<const uint8_t> body_span) {
    size_t pos = 0;
    if (body_span.size() < sizeof(MetadataMagic)) {
        return false;
    }
    pos += sizeof(MetadataMagic);
    if (hdr.has_metadata_field()) {
        const size_t meta_sz = encoding_sizes::field_compressed_size(hdr.metadata_field());
        if (meta_sz > body_span.size() - pos) {
            return false;
        }
        pos += meta_sz;
    }
    if (body_span.size() - pos <
        sizeof(SegmentDescriptorMagic) + sizeof(SegmentDescriptorImpl) + SegmentIdentifierSize) {
        return false;
    }
    pos += sizeof(SegmentDescriptorMagic) + sizeof(SegmentDescriptorImpl);
    SegmentIdentifierHeader id_hdr{};
    std::memcpy(&id_hdr, body_span.data() + pos, SegmentIdentifierSize);
    pos += SegmentIdentifierSize;
    if (id_hdr.type_ == IdentifierType::STRING) {
        if (id_hdr.size_ > body_span.size() - pos) {
            return false;
        }
        pos += id_hdr.size_;
    } else if (id_hdr.type_ != IdentifierType::NUMERIC) {
        return false;
    }
    if (body_span.size() - pos < sizeof(DescriptorFieldsMagic)) {
        return false;
    }
    pos += sizeof(DescriptorFieldsMagic);
    if (hdr.has_descriptor_field() &&
        !validate_v2_ndarray_meta_field(hdr.descriptor_field(), body_span.size() - pos, kMaxUncompressedMetaBytes)) {
        return false;
    }
    if (hdr.footer_offset() > body_span.size()) {
        return false;
    }
    if (hdr.has_column_fields()) {
        const size_t footer_rem = body_span.size() - hdr.footer_offset();
        if (footer_rem < sizeof(EncodedMagic) ||
            !validate_v2_ndarray_meta_field(
                    hdr.column_fields(), footer_rem - sizeof(EncodedMagic), kMaxUncompressedMetaBytes
            )) {
            return false;
        }
    }
    return true;
}

bool validate_segment_bytes_bounds(std::span<const uint8_t> bytes) {
    if (bytes.size() < FIXED_HEADER_SIZE) {
        return false;
    }
    FixedHeader fixed_hdr{};
    std::memcpy(&fixed_hdr, bytes.data(), sizeof(FixedHeader));
    if (fixed_hdr.magic_number != MAGIC_NUMBER) {
        return false;
    }
    if (static_cast<size_t>(fixed_hdr.header_bytes) > bytes.size() - FIXED_HEADER_SIZE) {
        return false;
    }
    if (fixed_hdr.encoding_version == HEADER_VERSION_V1) {
        return validate_v1_segment_bounds(bytes, fixed_hdr);
    }
    if (fixed_hdr.encoding_version != HEADER_VERSION_V2) {
        return false;
    }
    const auto header_span = bytes.subspan(FIXED_HEADER_SIZE, fixed_hdr.header_bytes);
    const auto body_span = bytes.subspan(FIXED_HEADER_SIZE + fixed_hdr.header_bytes);
    if (header_span.size() < sizeof(HeaderData)) {
        return false;
    }
    HeaderData hdata{};
    std::memcpy(&hdata, header_span.data(), sizeof(HeaderData));
    if (!validate_v2_header_fields(hdata, header_span)) {
        return false;
    }
    SegmentHeader hdr;
    hdr.deserialize_from_bytes(header_span.data(), false);
    return validate_v2_body_bounds(hdr, body_span);
}

} // namespace

std::vector<uint8_t> serialize_segment_to_bytes(Segment& segment) {
    const size_t total_size = segment.calculate_size();
    if (total_size == 0) {
        return {};
    }
    if (segment.buffer().data() != nullptr) {
        std::vector<uint8_t> out(total_size);
        segment.write_to(out.data());
        return out;
    }
    auto [ptr, len, owned_buf] = segment.serialize_header();
    if (ptr == nullptr || len == 0) {
        return {};
    }
    return std::vector<uint8_t>(ptr, ptr + len);
}

std::optional<Segment> deserialize_segment_from_bytes(std::span<const uint8_t> bytes) {
    try {
        if (!validate_segment_bytes_bounds(bytes)) {
            return std::nullopt;
        }
        auto buf = std::make_shared<Buffer>();
        buf->ensure(bytes.size());
        std::memcpy(buf->data(), bytes.data(), bytes.size());
        buf->set_bytes(bytes.size());
        return Segment::from_buffer(buf);
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

std::vector<uint8_t> encode_enriched_op_log_payload(const EnrichedOpLogPayload& payload) {
    std::vector<uint8_t> out;
    out.reserve(kHeaderBytes + payload.compressed_index_bytes.size() + 256);

    // Reserve space for header; fill after body is encoded so we can compute XXH64 over version, flags, and body.
    out.resize(kHeaderBytes, 0);

    append_atom_key(out, payload.new_index_key);
    if (payload.prev_index_key.has_value()) {
        append_atom_key(out, *payload.prev_index_key);
    }

    append_bytes(out, payload.compressed_index_bytes);

    util::check(
            payload.newly_written_data_keys.size() <= kMaxEnrichedOpLogKeysPerPayload,
            "Too many newly written data keys in enriched op-log payload: {}",
            payload.newly_written_data_keys.size()
    );
    append_u32(out, static_cast<uint32_t>(payload.newly_written_data_keys.size()));
    for (const auto& dk : payload.newly_written_data_keys) {
        append_atom_key(out, dk);
    }

    const bool write_col_stats = payload.column_stats_key.has_value();
    const bool write_inline_col_stats = write_col_stats && !payload.column_stats_bytes.empty();
    if (write_col_stats) {
        append_atom_key(out, *payload.column_stats_key);
        if (write_inline_col_stats) {
            append_bytes(out, payload.column_stats_bytes);
        }
    }

    uint32_t flags = 0;
    if (payload.prev_index_key.has_value()) {
        flags |= kFlagHasPrevIndexKey;
    }
    if (write_col_stats) {
        flags |= kFlagHasColumnStats;
    }
    if (write_inline_col_stats) {
        flags |= kFlagHasInlineColumnStats;
    }
    if (payload.is_clean_chain_successor) {
        flags |= kFlagIsCleanChainSuccessor;
    }

    std::memcpy(out.data(), kMagic.data(), kMagic.size());
    write_u32_le(out.data() + 8, kFormatVersion);
    write_u32_le(out.data() + 12, flags);
    const uint64_t hash =
            compute_payload_checksum(out.data() + 8, out.data() + kHeaderBytes, out.size() - kHeaderBytes);
    write_u64_le(out.data() + 16, hash);

    return out;
}

std::optional<EnrichedOpLogPayload> decode_enriched_op_log_payload(std::span<const uint8_t> bytes) {
    ByteReader reader;
    uint32_t flags = 0;
    if (!decode_and_verify_header(bytes, reader, flags)) {
        return std::nullopt;
    }

    EnrichedOpLogPayload payload;
    payload.is_clean_chain_successor = (flags & kFlagIsCleanChainSuccessor) != 0;

    if (!reader.read_atom_key(payload.new_index_key)) {
        return std::nullopt;
    }
    if ((flags & kFlagHasPrevIndexKey) != 0) {
        entity::AtomKey prev_key;
        if (!reader.read_atom_key(prev_key)) {
            return std::nullopt;
        }
        payload.prev_index_key = std::move(prev_key);
    }
    if (!reader.read_bytes(payload.compressed_index_bytes, kMaxEnrichedOpLogBlobBytes) ||
        payload.compressed_index_bytes.empty()) {
        return std::nullopt;
    }
    if (!decode_data_keys(reader, payload.newly_written_data_keys) || !decode_column_stats(reader, flags, payload)) {
        return std::nullopt;
    }
    if (reader.remaining() != 0) {
        return std::nullopt;
    }
    return payload;
}

SegmentInMemory make_enriched_op_log_segment(const std::string& action, std::span<const uint8_t> encoded_payload) {
    auto desc = stream::stream_descriptor(
            action,
            stream::RowCountIndex(),
            {entity::scalar_field(entity::DataType::ASCII_DYNAMIC64, std::string{kEnrichedOpLogPayloadColumn})}
    );
    SegmentInMemory seg{std::move(desc)};
    seg.set_string(0, std::string_view(reinterpret_cast<const char*>(encoded_payload.data()), encoded_payload.size()));
    seg.end_row();
    return seg;
}

std::optional<EnrichedOpLogPayload> extract_enriched_op_log_payload(const SegmentInMemory& seg) {
    if (seg.num_columns() != 1 || seg.row_count() != 1) {
        return std::nullopt;
    }
    const auto& field = seg.descriptor().field(0);
    if (field.name() != kEnrichedOpLogPayloadColumn || field.type().data_type() != entity::DataType::ASCII_DYNAMIC64) {
        return std::nullopt;
    }
    const auto sv = seg.string_at(0, 0);
    if (!sv.has_value() || sv->empty()) {
        return std::nullopt;
    }
    return decode_enriched_op_log_payload(
            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(sv->data()), sv->size())
    );
}

} // namespace arcticdb
