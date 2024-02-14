/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include "util/buffer.hpp"
#include <arcticdb/storage/common.hpp>
#include <arcticdb/codec/encoding_sizes.hpp>
#include <arcticdb/codec/segment_header.hpp>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/arena.h>
#include <util/buffer_pool.hpp>
#include <arcticdb/entity/field_collection.hpp>
#include <arcticdb/codec/encoding_version.hpp>

#include <iostream>
#include <variant>
#include <any>

namespace arcticdb {

namespace segment_size {
std::tuple<size_t, size_t> compressed(const arcticdb::proto::encoding::SegmentHeader& seg_hdr);
}


template<typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
constexpr EncodingVersion to_encoding_version(T encoding_version) {
    util::check(encoding_version >= 0 && encoding_version < uint16_t(EncodingVersion::COUNT), "Invalid encoding version");
    return static_cast<EncodingVersion>(encoding_version);
}

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

/*
 * Segment contains compressed data as returned from storage. When reading data the next step will usually be to
 * decompress the Segment into a SegmentInMemory which allows for data access and modification. At the time of writing,
 * the primary method for decompressing a Segment into a SegmentInMemory is in codec.cpp::decode (and the reverse via
 * codec.cpp:encode).
 */
class Segment {
  public:
    Segment() = default;

    Segment(SegmentHeader&& header, std::shared_ptr<Buffer> buffer, std::shared_ptr<StreamDescriptorDataImpl> data, std::shared_ptr<FieldCollection> fields) :
        header_(std::move(header)),
        buffer_(std::move(buffer)),
        desc_(std::move(data), std::move(fields)){
    }

    Segment(SegmentHeader&& header, BufferView buffer, std::shared_ptr<StreamDescriptorDataImpl> data, std::shared_ptr<FieldCollection> fields) :
        header_(std::move(header)),
        buffer_(buffer),
        desc_(std::move(data), std::move(fields)) {
    }

    Segment(SegmentHeader&& header, VariantBuffer &&buffer, std::shared_ptr<StreamDescriptorDataImpl> data, std::shared_ptr<FieldCollection> fields) :
        header_(std::move(header)),
        buffer_(std::move(buffer)),
        desc_(std::move(data), std::move(fields)) {
    }

    Segment(Segment &&that) noexcept {
        using std::swap;
        swap(header_, that.header_);
        swap(desc_, that.desc_);
        swap(keepalive_, that.keepalive_);
        buffer_.move_buffer(std::move(that.buffer_));
    }

    Segment &operator=(Segment &&that) noexcept {
        using std::swap;
        swap(header_, that.header_);
        swap(desc_, that.desc_);
        swap(keepalive_, that.keepalive_);
        buffer_.move_buffer(std::move(that.buffer_));
        return *this;
    }

    ~Segment() = default;

    static Segment from_buffer(const std::shared_ptr<Buffer>& buf);

    void set_buffer(VariantBuffer&& buffer) {
        buffer_ = std::move(buffer);
    }

    static Segment from_bytes(const std::uint8_t *src, std::size_t readable_size, bool copy_data = false);

    void write_to(std::uint8_t *dst, std::size_t hdr_sz);

    std::pair<uint8_t*, size_t> try_internal_write(std::shared_ptr<Buffer>& tmp, size_t hdr_size);

    void write_header(uint8_t* dst, size_t hdr_size) const;

    [[nodiscard]] std::size_t total_segment_size() const {
        return total_segment_size(segment_header_bytes_size());
    }

    [[nodiscard]] std::size_t total_segment_size(std::size_t hdr_size) const {
        auto total = FIXED_HEADER_SIZE + hdr_size + buffer_bytes();
        ARCTICDB_TRACE(log::storage(), "Total segment size {} + {} + {} = {}", FIXED_HEADER_SIZE, hdr_size, buffer_bytes(), total);
        return total;
    }

    [[nodiscard]] std::size_t segment_header_bytes_size() const {
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

    [[nodiscard]] std::shared_ptr<FieldCollection> fields_ptr() const {
        return desc_.fields_ptr();
    }

    [[nodiscard]] size_t fields_size() const {
        return desc_.field_count();
    }

    [[nodiscard]] const Field& fields(size_t pos) const {
        return desc_.fields(pos);
    }

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

    const StreamDescriptor& descriptor() const {
        return desc_;
    }

  private:
    SegmentHeader header_;
    VariantBuffer buffer_;
    StreamDescriptor desc_;
    std::any keepalive_;
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
        return format_to(ctx.out(), "{:c}", c);
    }
};

} //namespace fmt
