#pragma once

#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/codec/encoded_field_collection.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/codec/protobuf_encoding.hpp>
#include <folly/container/Enumerate.h>
#include <arcticdb/util/cursored_buffer.hpp>
#include <arcticdb/codec/encoding_version.hpp>

namespace arcticdb {

void write_fixed_header(std::uint8_t *dst, const FixedHeader& hdr) {
    ARCTICDB_DEBUG(log::codec(), "Writing header with size {}", header_bytes);
    auto h = reinterpret_cast<FixedHeader *>(dst);
    *h = hdr;
}

void write_fixed_header(std::ostream &dst, const FixedHeader& hdr){
    dst.write(reinterpret_cast<const char*>(&hdr), sizeof(FixedHeader));
}

class SegmentHeader {
    HeaderData data_;
    EncodedFieldCollection header_fields_;
    EncodedFieldCollection body_fields_;
    std::array<uint32_t, 5> offset_ = {};

    enum class FieldOffset : uint8_t {
        METADATA,
        STRING_POOL,
        DESCRIPTOR,
        INDEX,
        COLUMN
    };

    static constexpr std::array<std::string_view, 5> offset_names_ = {
        "METADATA",
        "STRING_POOL",
        "DESCRIPTOR",
        "INDEX",
        "COLUMN"
    };

    static constexpr bool UNSET = false;

public:
    explicit SegmentHeader(EncodingVersion encoding_version) {
        data_.encoding_version_ = encoding_version;
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(SegmentHeader)

    SegmentHeader() = default;

    [[nodiscard]] bool empty() const {
        return header_fields_.empty();
    }

    [[nodiscard]] bool compacted() const {
        return data_.compacted_;
    }

    void set_compacted(bool compacted) {
        data_.compacted_ = compacted;
    }

    [[nodiscard]] size_t bytes() const {
        return sizeof(HeaderData) + header_fields_.bytes();
    }

    [[nodiscard]] static constexpr size_t as_offset(FieldOffset field_offset) {
        return static_cast<size_t>(field_offset);
    }

    [[nodiscard]] int32_t get_offset(FieldOffset field_offset) const {
        return data_.optional_header_fields_[as_offset(field_offset)];
    }

    [[nodiscard]] static constexpr std::string_view offset_name(FieldOffset field_offset) {
        return offset_names_[as_offset(field_offset)];
    }

    [[nodiscard]] bool has_field(FieldOffset field_offset) const {
        return get_offset(field_offset) != UNSET;
    }

    [[nodiscard]] bool has_metadata_field() const {
        return has_field(FieldOffset::METADATA);
    }

    [[nodiscard]] bool has_string_pool_field() const {
        return has_field(FieldOffset::STRING_POOL);
    }

    [[nodiscard]] bool has_descriptor_field() const {
        return has_field(FieldOffset::DESCRIPTOR);
    }

    [[nodiscard]] bool has_index_descriptor_field() const {
        return has_field(FieldOffset::INDEX);
    }

    [[nodiscard]] bool has_column_fields() const {
        return has_field(FieldOffset::COLUMN);
    }

    template <FieldOffset field_offset>
    [[nodiscard]] const EncodedFieldImpl& get_field() const {
        util::check(has_field(field_offset), "Field {} has not been set", offset_name(field_offset));
        return header_fields_.at(offset_[as_offset(field_offset)]);
    }

    [[nodiscard]] const EncodedFieldImpl& metadata_field() const {
        return get_field<FieldOffset::METADATA>();
    }

    [[nodiscard]] const EncodedFieldImpl& string_pool_field() const {
        return get_field<FieldOffset::STRING_POOL>();
    }
    [[nodiscard]] const EncodedFieldImpl& descriptor_field() const {
        return get_field<FieldOffset::DESCRIPTOR>();
    }

    [[nodiscard]] const EncodedFieldImpl& index_descriptor_field() const {
        return get_field<FieldOffset::INDEX>();
    }

    [[nodiscard]] const EncodedFieldImpl& column_fields() const {
        return get_field<FieldOffset::COLUMN>();
    }

    [[nodiscard]] EncodingVersion encoding_version() const {
        return data_.encoding_version_;
    }

    void set_footer_offset(uint64_t offset) {
        data_.footer_offset_ = offset;
    }

    [[nodiscard]] uint64_t footer_offset() const {
        return data_.footer_offset_;
    }

    void serialize_to_proto(uint8_t* dst) const {
        arcticdb::proto::encoding::SegmentHeader segment_header;
        if(has_metadata_field())
            proto_from_encoded_field(metadata_field(), *segment_header.mutable_metadata_field());

        if(has_string_pool_field())
            proto_from_encoded_field(metadata_field(), *segment_header.mutable_metadata_field());

        if(has_descriptor_field())
            proto_from_encoded_field(metadata_field(), *segment_header.mutable_metadata_field());

        if(has_index_descriptor_field())
            proto_from_encoded_field(metadata_field(), *segment_header.mutable_metadata_field());

        if(has_column_fields())
            proto_from_encoded_field(metadata_field(), *segment_header.mutable_metadata_field());

        const auto hdr_size = segment_header.ByteSizeLong();
        google::protobuf::io::ArrayOutputStream aos(dst + FIXED_HEADER_SIZE, static_cast<int>(hdr_size));
        segment_header.SerializeToZeroCopyStream(&aos);
    }

    void serialize_to_bytes(uint8_t* dst) const {
        memcpy(dst, &data_, sizeof(HeaderData));
        dst += sizeof(HeaderData);
        memcpy(dst, header_fields_.data(), header_fields_.bytes());
    }

    void deserialize_proto_field(
            FieldOffset field_offset,
            CursoredBuffer<Buffer>& buffer,
            const arcticdb::proto::encoding::EncodedField& field,
            size_t& pos) {
        data_.optional_header_fields_[as_offset(field_offset)] = true;
        offset_[as_offset(field_offset)] = pos++;
        const auto field_size = calc_encoded_field_buffer_size(field);
        buffer.ensure<uint8_t>(field_size);
        auto* data = buffer.data();
        encoded_field_from_proto(field, *reinterpret_cast<EncodedFieldImpl*>(data));
    }

    void deserialize_from_proto(const arcticdb::proto::encoding::SegmentHeader& header) {
        data_.encoding_version_ = EncodingVersion(header.encoding_version());
        data_.compacted_ = header.compacted();

        auto pos = 0UL;
        CursoredBuffer<Buffer> buffer;
        if(header.has_metadata_field())
            deserialize_proto_field(FieldOffset::METADATA, buffer, header.descriptor_field(), pos);

        if(header.has_string_pool_field())
            deserialize_proto_field(FieldOffset::STRING_POOL, buffer, header.string_pool_field(), pos);

        if(header.has_descriptor_field())
            deserialize_proto_field(FieldOffset::DESCRIPTOR, buffer, header.descriptor_field(), pos);

        if(header.has_index_descriptor_field())
            deserialize_proto_field(FieldOffset::INDEX, buffer, header.index_descriptor_field(), pos);

        if(header.has_column_fields())
            deserialize_proto_field(FieldOffset::COLUMN, buffer, header.column_fields(), pos);
    }

    void deserialize_from_bytes(const uint8_t* data, size_t header_size) {
       memcpy(&data_, data, sizeof(HeaderData));
       data += sizeof(HeaderData);
       header_size -= sizeof(HeaderData);
       Buffer buffer(header_size);
       memcpy(buffer.data(), data, header_size);
       header_fields_ = EncodedFieldCollection{std::move(buffer)};

       auto pos = 0U;
       for(auto has_field : folly::enumerate(data_.optional_header_fields_)) {
           if(*has_field) {
               offset_[has_field.index] = pos++;
           }
       }

    }

    const EncodedFieldCollection& body_fields() const {
        return body_fields_;
    }

    void set_body_fields(EncodedFieldCollection&& body_fields) {
        body_fields_ = std::move(body_fields);
    }
};


}