#pragma once

#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/codec/encoded_field_collection.hpp>
//#include <arcticdb/codec/codec.hpp>
#include <arcticdb/util/cursored_buffer.hpp>

namespace arcticdb {

class SegmentInMemory;

static constexpr std::array<std::string_view, 5> offset_names_ = {
    "METADATA",
    "STRING_POOL",
    "DESCRIPTOR",
    "INDEX",
    "COLUMN"
};

inline void write_fixed_header(std::uint8_t *dst, const FixedHeader& hdr) {
    ARCTICDB_DEBUG(log::codec(), "Writing header with size {}", hdr.header_bytes);
    auto h = reinterpret_cast<FixedHeader*>(dst);
    *h = hdr;
}

class SegmentHeader {
    HeaderData data_;
    EncodedFieldCollection header_fields_;
    EncodedFieldCollection body_fields_;
    std::array<uint32_t, 5> offset_ = {};
    size_t field_count_ = 0U;

public:
    explicit SegmentHeader(EncodingVersion encoding_version) {
        data_.encoding_version_ = encoding_version;
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(SegmentHeader)

    SegmentHeader() = default;

    SegmentHeader clone() const {
        SegmentHeader output(data_.encoding_version_);
        output.data_ = data_;
        output.header_fields_ = header_fields_.clone();
        output.body_fields_ = body_fields_.clone();
        output.offset_ = offset_;
        return output;
    }

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

    void validate() const {
        for(auto i = 0U; i < static_cast<size_t>(FieldOffset::COUNT); ++i) {
            auto offset = FieldOffset(i);
            if(has_field(offset))
                header_fields_.at(offset_[as_pos(offset)]).validate();
        }
    }

    template <FieldOffset field_offset>
    EncodedFieldImpl& create_field(size_t num_blocks) {
        auto new_field = header_fields_.add_field(num_blocks);
        set_offset(field_offset, field_count_++);
        set_field(field_offset);
        return *new_field;
    }

    template <FieldOffset field_offset>
    [[nodiscard]] EncodedFieldImpl& get_mutable_field(size_t num_blocks) {
        if(has_field(field_offset)) {
            return header_fields_.at(offset_[as_pos(field_offset)]);
        } else {
            return create_field<field_offset>(num_blocks);
        }
    }

    [[nodiscard]] EncodedFieldImpl& mutable_metadata_field(size_t num_blocks) {
        return get_mutable_field<FieldOffset::METADATA>(num_blocks);
    }

    [[nodiscard]] EncodedFieldImpl& mutable_string_pool_field(size_t num_blocks) {
        return get_mutable_field<FieldOffset::STRING_POOL>(num_blocks);
    }

    [[nodiscard]] EncodedFieldImpl& mutable_descriptor_field(size_t num_blocks) {
        return get_mutable_field<FieldOffset::DESCRIPTOR>(num_blocks);
    }

    [[nodiscard]] EncodedFieldImpl& mutable_index_descriptor_field(size_t num_blocks) {
        return get_mutable_field<FieldOffset::INDEX>(num_blocks);
    }

    [[nodiscard]] EncodedFieldImpl& mutable_column_fields(size_t num_blocks) {
        return get_mutable_field<FieldOffset::COLUMN>(num_blocks);
    }

    size_t required_bytes(const SegmentInMemory& in_mem_seg);

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

    void serialize_to_bytes(uint8_t* dst, size_t expected_bytes) const {
        const auto* begin = dst;
        data_.field_buffer_.fields_bytes_ = static_cast<uint32_t>(header_fields_.data_bytes());
        data_.field_buffer_.offset_bytes_ = static_cast<uint32_t>(header_fields_.offset_bytes());
        memcpy(dst, &data_, sizeof(HeaderData));
        dst += sizeof(HeaderData);
        memcpy(dst, header_fields_.data_buffer(), header_fields_.data_bytes());
        dst += header_fields_.data_bytes();
        memcpy(dst, header_fields_.offsets_buffer(), header_fields_.offset_bytes());
        dst += header_fields_.offset_bytes();
        util::check(size_t(dst - begin) == expected_bytes, "Mismatch between actual and expected bytes: {} != {}", dst - begin, expected_bytes);
    }

    static constexpr uint16_t field_mask(FieldOffset field_offset) {
       return 1U << static_cast<uint16_t>(field_offset);
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

namespace fmt {
template<>
struct formatter<arcticdb::SegmentHeader> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const arcticdb::SegmentHeader &header, FormatContext &ctx) const {
        fmt::format_to(ctx.out(), "Segment header: encoding {}: {} bytes { \n", header.encoding_version(), header.bytes());

        if(header.has_descriptor_field())
            fmt::format_to(ctx.out(), "Descriptor: {}\n", header.descriptor_field());

        if(header.has_metadata_field())
            fmt::format_to(ctx.out(), "Metadata: {}\n", header.metadata_field());

        if(header.has_index_descriptor_field())
            fmt::format_to(ctx.out(), "Index: {}\n", header.index_descriptor_field());

        if(header.has_string_pool_field())
            fmt::format_to(ctx.out(), "String pool: {}\n", header.string_pool_field());

        if(header.has_column_fields())
            fmt::format_to(ctx.out(), "Columns: {}\n", header.column_fields());

        return fmt::format_to(ctx.out(), "}\n");
    }
};
}