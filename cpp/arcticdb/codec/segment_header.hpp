#pragma once

#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/codec/encoded_field_collection.hpp>
//#include <arcticdb/codec/codec.hpp>
#include <arcticdb/util/cursored_buffer.hpp>

namespace arcticdb {

enum class FieldOffset : uint8_t {
    METADATA,
    STRING_POOL,
    DESCRIPTOR,
    INDEX,
    COLUMN,
    COUNT
};

static constexpr std::array<std::string_view, 5> offset_names_ = {
    "METADATA",
    "STRING_POOL",
    "DESCRIPTOR",
    "INDEX",
    "COLUMN"
};

void write_fixed_header(std::uint8_t *dst, const FixedHeader& hdr) {
    ARCTICDB_DEBUG(log::codec(), "Writing header with size {}", hdr.header_bytes);
    auto h = reinterpret_cast<FixedHeader*>(dst);
    *h = hdr;
}

class SegmentHeader {
    HeaderData data_;
    EncodedFieldCollection header_fields_;
    EncodedFieldCollection body_fields_;
    std::array<uint32_t, 5> offset_ = {};

public:
    explicit SegmentHeader(EncodingVersion encoding_version) {
        data_.encoding_version_ = encoding_version;
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(SegmentHeader)

    SegmentHeader() = default;

    [[nodiscard]] bool empty() const {
        return header_fields_.empty();
    }

    static constexpr uint8_t flag_mask(HeaderFlag flag) {
        return 1 << static_cast<uint8_t>(flag);
    }

    void set_offset(FieldOffset field, uint32_t offset) {
        offset_[as_pos(field)] = offset;
    }
    
    template<HeaderFlag flag>
    void set_flag(bool value) {
       constexpr auto mask = flag_mask(flag);
       if(value)
           data_.flags_ |= mask;
       else
           data_.flags_ &= ~mask;
    }

    template<HeaderFlag flag>
    [[nodiscard]] bool get_flag() const {
        return data_.flags_ & flag_mask(flag);
    }

    [[nodiscard]] bool compacted() const {
        return get_flag<HeaderFlag::COMPACTED>();
    }

    void set_compacted(bool value) {
         set_flag<HeaderFlag::COMPACTED>(value);
    }

    [[nodiscard]] size_t bytes() const {
        return sizeof(HeaderData) + header_fields_.data_bytes() + header_fields_.offset_bytes();
    }

    [[nodiscard]] static constexpr size_t as_pos(FieldOffset field_offset) {
        return static_cast<size_t>(field_offset);
    }

    [[nodiscard]] int32_t get_offset(FieldOffset field_offset) const {
        return offset_[as_pos(field_offset)];
    }

    [[nodiscard]] static constexpr std::string_view offset_name(FieldOffset field_offset) {
        return offset_names_[as_pos(field_offset)];
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
        return header_fields_.at(offset_[as_pos(field_offset)]);
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

    template <FieldOffset field_offset>
    [[nodiscard]] EncodedFieldImpl& get_mutable_field() {
        util::check(has_field(field_offset), "Field {} has not been set", offset_name(field_offset));
        return header_fields_.at(offset_[as_pos(field_offset)]);
    }

    [[nodiscard]] EncodedFieldImpl& mutable_metadata_field() {
        return get_mutable_field<FieldOffset::METADATA>();
    }

    [[nodiscard]] EncodedFieldImpl& mutable_string_pool_field() {
        return get_mutable_field<FieldOffset::STRING_POOL>();
    }

    [[nodiscard]] EncodedFieldImpl& mutable_descriptor_field() {
        return get_mutable_field<FieldOffset::DESCRIPTOR>();
    }

    [[nodiscard]] EncodedFieldImpl& mutable_index_descriptor_field() {
        return get_mutable_field<FieldOffset::INDEX>();
    }

    [[nodiscard]] EncodedFieldImpl& mutable_column_fields() {
        return get_mutable_field<FieldOffset::COLUMN>();
    }

    [[nodiscard]] EncodingVersion encoding_version() const {
        return data_.encoding_version_;
    }

    void set_encoding_version(EncodingVersion encoding_version) {
        data_.encoding_version_ = encoding_version;
    }

    void set_footer_offset(uint64_t offset) {
        data_.footer_offset_ = offset;
    }

    [[nodiscard]] uint64_t footer_offset() const {
        return data_.footer_offset_;
    }

    void serialize_to_bytes(uint8_t* dst) const {
        data_.field_buffer_.fields_bytes_ = static_cast<uint32_t>(header_fields_.data_bytes());
        data_.field_buffer_.offset_bytes_ = static_cast<uint32_t>(header_fields_.offset_bytes());
        memcpy(dst, &data_, sizeof(HeaderData));
        dst += sizeof(HeaderData);
        memcpy(dst, header_fields_.data_buffer(), header_fields_.data_bytes());
        memcpy(dst, header_fields_.offsets_buffer(), header_fields_.offset_bytes());
    }

    static constexpr uint16_t field_mask(FieldOffset field_offset) {
       return static_cast<uint16_t>(field_offset) << 1U;
    }

    void set_field(FieldOffset field_offset) {
        data_.fields_ |= field_mask(field_offset);
    }

    [[nodiscard]] bool has_field(FieldOffset field_offset) const {
        return data_.fields_ & field_mask(field_offset);
    }

    void deserialize_from_bytes(const uint8_t* data) {
       memcpy(&data_, data, sizeof(HeaderData));
       data += sizeof(HeaderData);
       ChunkedBuffer fields_buffer;
       fields_buffer.add_external_block(data, data_.field_buffer_.fields_bytes_, 0UL);
       data += data_.field_buffer_.fields_bytes_;
       Buffer offsets_buffer{data_.field_buffer_.offset_bytes_};
       memcpy(offsets_buffer.data(), data, data_.field_buffer_.offset_bytes_);

       header_fields_ = EncodedFieldCollection{std::move(fields_buffer), std::move(offsets_buffer)};

       auto count = 0U;
       for(auto pos = 0U; pos < as_pos(FieldOffset::COUNT); ++pos) {
           if(has_field(FieldOffset(pos))) {
               offset_[pos] = header_fields_.get_offset(count++);
           }
       }
    }

    [[nodiscard]] const EncodedFieldCollection& body_fields() const {
        return body_fields_;
    }

    void set_body_fields(EncodedFieldCollection&& body_fields) {
        body_fields_ = std::move(body_fields);
    }
};

} //namespace arcticdb