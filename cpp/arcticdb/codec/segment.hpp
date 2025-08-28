/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/common.hpp>
#include <arcticdb/codec/segment_header.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/util/variant.hpp>
#include <google/protobuf/arena.h>
#include <variant>
#include <any>

namespace arcticdb {

namespace segment_size {

struct SegmentCompressedSize {
    size_t string_pool_size_ = 0U;
    size_t total_buffer_size_ = 0U;
    size_t body_size_ = 0U;
};

SegmentCompressedSize compressed(const arcticdb::proto::encoding::SegmentHeader& seg_hdr);
}

struct SegmentHeaderProtoWrapper {
    arcticdb::proto::encoding::SegmentHeader* header_;
    std::unique_ptr<google::protobuf::Arena> arena_;

    [[nodiscard]] const auto& proto() const { return *header_; }

    [[nodiscard]] auto& proto() { return *header_; }
};

SegmentHeaderProtoWrapper decode_protobuf_header(const uint8_t* data, size_t header_bytes_size);

arcticdb::proto::encoding::SegmentHeader generate_v1_header(const SegmentHeader& header, const StreamDescriptor& desc);

static constexpr uint16_t HEADER_VERSION_V1 = 1;
static constexpr uint16_t HEADER_VERSION_V2 = 2;

inline EncodingVersion encoding_version(const storage::LibraryDescriptor::VariantStoreConfig& cfg) {
    return util::variant_match(cfg,
       [](const arcticdb::proto::storage::VersionStoreConfig &version_config) {
           return EncodingVersion(version_config.encoding_version());
       },
       [](std::monostate) {
           return EncodingVersion::V1;
       }
    );
}
void set_body_fields(SegmentHeader& seg_hdr, const uint8_t* src);

/*
 * Segment contains compressed data as returned from storage. When reading data the next step will usually be to
 * decompress the Segment into a SegmentInMemory which allows for data access and modification. At the time of writing,
 * the primary method for decompressing a Segment into a SegmentInMemory is in codec.cpp::decode (and the reverse via
 * codec.cpp:encode).
 */
class Segment {
  public:
    Segment() = default;

    Segment(
        SegmentHeader&& header,
        std::shared_ptr<Buffer> buffer,
        std::shared_ptr<SegmentDescriptorImpl> data,
        std::shared_ptr<FieldCollection> fields,
        StreamId stream_id,
        size_t size) :
            header_(std::move(header)),
            buffer_(std::move(buffer)),
            desc_(std::move(data), std::move(fields), std::move(stream_id)),
            size_(size) {
    }

    Segment(
        SegmentHeader&& header,
        BufferView buffer,
        std::shared_ptr<SegmentDescriptorImpl> data,
        std::shared_ptr<FieldCollection> fields,
        StreamId stream_id,
        size_t size) :
            header_(std::move(header)),
            buffer_(buffer),
            desc_(std::move(data), std::move(fields), std::move(stream_id)),
            size_(size) {
    }

    Segment(
        SegmentHeader&& header,
        VariantBuffer &&buffer,
        std::shared_ptr<SegmentDescriptorImpl> data,
        std::shared_ptr<FieldCollection> fields,
        StreamId stream_id,
        size_t size) :
            header_(std::move(header)),
            buffer_(std::move(buffer)),
            desc_(std::move(data), std::move(fields), std::move(stream_id)),
            size_(size) {
    }

    Segment(Segment &&that) noexcept {
        using std::swap;
        swap(header_, that.header_);
        swap(desc_, that.desc_);
        swap(keepalive_, that.keepalive_);
        swap(size_, that.size_);
        buffer_.move_buffer(std::move(that.buffer_));
    }

    Segment &operator=(Segment &&that) noexcept {
        using std::swap;
        swap(header_, that.header_);
        swap(desc_, that.desc_);
        swap(keepalive_, that.keepalive_);
        swap(size_, that.size_);
        buffer_.move_buffer(std::move(that.buffer_));
        return *this;
    }

    ~Segment() = default;

    static Segment from_buffer(const std::shared_ptr<Buffer>& buf);

    void set_buffer(VariantBuffer&& buffer) {
        buffer_ = std::move(buffer);
    }

    static Segment from_bytes(const std::uint8_t *src, std::size_t readable_size, bool copy_data = false);

    void write_to(std::uint8_t *dst);

    std::tuple<uint8_t*, size_t, std::unique_ptr<Buffer>> serialize_header();

    size_t write_proto_header(uint8_t* dst, size_t total_header_size);

    [[nodiscard]] std::size_t size() const {
        util::check(size_.has_value(), "Segment size has not been set");
        return *size_;
    }

    [[nodiscard]] std::size_t calculate_size() {
        size_ = FIXED_HEADER_SIZE + segment_header_bytes_size() + buffer_bytes();

        return *size_;
    }

    const arcticdb::proto::encoding::SegmentHeader& generate_header_proto();

    [[nodiscard]] size_t proto_size() {
        util::check(static_cast<bool>(proto_), "Proto has not been generated");
        return proto_size_;
    }

    [[nodiscard]] std::size_t segment_header_bytes_size() {
        if(header_.encoding_version() == EncodingVersion::V1) {
            generate_header_proto();
            return proto_size();
        }
        else
            return header_.bytes();
    }

    [[nodiscard]] std::size_t buffer_bytes() const {
        return buffer_.bytes();
    }

    SegmentHeader &header() {
        return header_;
    }

    [[nodiscard]] const SegmentHeader &header() const {
        return header_;
    }

    [[nodiscard]] BufferView buffer() const {
        return buffer_.view();
    }

    [[nodiscard]] bool is_empty() const {
        return buffer_.is_uninitialized() || (buffer().bytes() == 0 && header_.empty());
    }

    [[nodiscard]] std::shared_ptr<FieldCollection> fields_ptr() const;

    [[nodiscard]] size_t fields_size() const;

    [[nodiscard]] const Field& fields(size_t pos) const;

    void force_own_buffer() {
        buffer_.force_own_buffer();
        keepalive_.reset();
    }

    // For external language tools, not efficient
    [[nodiscard]] std::vector<std::string_view> fields_vector() const {
        std::vector<std::string_view> fields;
        for(const auto& field : desc_.fields())
            fields.push_back(field.name());

        return fields;
    }

    void set_keepalive(std::any&& keepalive) {
        keepalive_ = std::move(keepalive);
    }

    [[nodiscard]] const std::any& keepalive() const  {
        return keepalive_;
    }

    [[nodiscard]] const StreamDescriptor& descriptor() const {
        return desc_;
    }

    Segment clone() const {
        return Segment{header_.clone(), buffer_.clone(), desc_.clone(), size_};
    }

    static Segment initialize(SegmentHeader&& header, std::shared_ptr<Buffer>&& buffer,  std::shared_ptr<SegmentDescriptorImpl> data, std::shared_ptr<FieldCollection> fields, StreamId stream_id) {
        return {std::move(header), std::move(buffer), std::move(data), std::move(fields), std::move(stream_id)};
    }

  private:
    Segment(
        SegmentHeader&& header,
        std::shared_ptr<Buffer> buffer,
        std::shared_ptr<SegmentDescriptorImpl> data,
        std::shared_ptr<FieldCollection> fields,
        StreamId stream_id) :
        header_(std::move(header)),
        buffer_(std::move(buffer)),
        desc_(std::move(data), std::move(fields), std::move(stream_id)) {
    }

    Segment(SegmentHeader&& header, VariantBuffer&& buffer, StreamDescriptor&& desc, const std::optional<size_t>& size) :
        header_(std::move(header)),
        buffer_(std::move(buffer)),
        desc_(std::move(desc)),
        size_(size) {
    }

    std::tuple<uint8_t*, size_t,  std::unique_ptr<Buffer>> serialize_v1_header_to_buffer(size_t total_hdr_size);
    std::pair<uint8_t*, size_t> serialize_v1_header_in_place(size_t total_header_size);
    std::tuple<uint8_t*, size_t, std::unique_ptr<Buffer>> serialize_header_v1();
    std::pair<uint8_t*, size_t> serialize_header_v2(size_t expected_bytes);
    size_t write_binary_header(uint8_t* dst) const;

    SegmentHeader header_;
    VariantBuffer buffer_;
    StreamDescriptor desc_;
    std::any keepalive_;
    std::unique_ptr<arcticdb::proto::encoding::SegmentHeader> proto_;
    size_t proto_size_ = 0UL;
    std::optional<size_t> size_;
};

} //namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::EncodingVersion> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(arcticdb::EncodingVersion version, FormatContext &ctx) const {
        char c = 'U';
        switch (version) {
        case arcticdb::EncodingVersion::V1:c = '1';
            break;
        case arcticdb::EncodingVersion::V2:c = '2';
            break;
        default:break;
        }
        return fmt::format_to(ctx.out(), "{:c}", c);
    }
};

} //namespace fmt
