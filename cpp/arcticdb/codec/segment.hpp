/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include "util/buffer.hpp"
#include <arcticdb/storage/common.hpp>
#include <codec/encoding_sizes.hpp>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/arena.h>
#include <util/buffer_pool.hpp>
#include <arcticdb/entity/field_collection.hpp>

#include <iostream>
#include <variant>
#include <any>

namespace arcticdb {

namespace segment_size {
std::tuple<size_t, size_t> compressed(const arcticdb::proto::encoding::SegmentHeader& seg_hdr);
}

enum class EncodingVersion : uint16_t {
    V1 = 0,
    V2 = 1,
    COUNT
};

template<typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
inline constexpr EncodingVersion to_encoding_version(T encoding_version) {
    util::check(encoding_version >= 0 && encoding_version < uint16_t(EncodingVersion::COUNT), "Invalid encoding version");
    return static_cast<EncodingVersion>(encoding_version);
}

static constexpr uint16_t HEADER_VERSION_V1 = 1;

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
    constexpr static uint16_t MAGIC_NUMBER = 0xFA57;

    struct FixedHeader {
        std::uint16_t magic_number;
        std::uint16_t encoding_version;
        std::uint32_t header_bytes;

        void write(std::uint8_t *dst) const {
            ARCTICDB_DEBUG(log::codec(), "Writing header with size {}", header_bytes);
            auto h = reinterpret_cast<FixedHeader *>(dst);
            *h = *this;
        }

        void write(std::ostream &dst){
            dst.write(reinterpret_cast<char*>(this), sizeof(FixedHeader));
        }
    };

    constexpr static std::size_t FIXED_HEADER_SIZE = sizeof(FixedHeader);

    Segment() :
        header_(google::protobuf::Arena::CreateMessage<arcticdb::proto::encoding::SegmentHeader>(arena_.get())) {
    }

    Segment(std::unique_ptr<google::protobuf::Arena>&& arena, arcticdb::proto::encoding::SegmentHeader* header, std::shared_ptr<Buffer> buffer, std::shared_ptr<FieldCollection> fields) :
        arena_(std::move(arena)),
        header_(header),
        buffer_(std::move(buffer)),
        fields_(std::move(fields)){
    }

    Segment(std::unique_ptr<google::protobuf::Arena>&& arena, arcticdb::proto::encoding::SegmentHeader* header, BufferView &&buffer, std::shared_ptr<FieldCollection> fields) :
        arena_(std::move(arena)),
        header_(header),
        buffer_(buffer),
        fields_(std::move(fields)){}

    // for rvo only, go to solution should be to move
    Segment(const Segment &that) :
            header_(google::protobuf::Arena::CreateMessage<arcticdb::proto::encoding::SegmentHeader>(arena_.get())),
            keepalive_(that.keepalive_) {
        header_->CopyFrom(*that.header_);
        auto b = std::make_shared<Buffer>();
        util::variant_match(that.buffer_,
            [] (const std::monostate&) {/* Uninitialized buffer */},
            [&b](const BufferView& buf) { buf.copy_to(*b); },
            [&b](const std::shared_ptr<Buffer>& buf) { buf->copy_to(*b); }
            );
        buffer_ = std::move(b);
        if(that.fields_)
            fields_ = std::make_shared<FieldCollection>(that.fields_->clone());
    }

    Segment &operator=(const Segment &that) {
        if(this == &that)
            return *this;

        header_->CopyFrom(*that.header_);
        auto b = std::make_shared<Buffer>();
        util::variant_match(that.buffer_,
                            [] (const std::monostate&) {/* Uninitialized buffer */},
                            [&b](const BufferView& buf) { buf.copy_to(*b); },
                            [&b](const std::shared_ptr<Buffer>& buf) { buf->copy_to(*b); }
                            );
        buffer_ = std::move(b);
        fields_ = that.fields_;
        keepalive_ = that.keepalive_;
        return *this;
    }

    Segment(Segment &&that) noexcept {
        using std::swap;
        swap(header_, that.header_);
        swap(arena_, that.arena_);
        swap(fields_, that.fields_);
        swap(keepalive_, that.keepalive_);
        move_buffer(std::move(that));
    }

    Segment &operator=(Segment &&that) noexcept {
        using std::swap;
        swap(header_, that.header_);
        swap(arena_, that.arena_);
        swap(fields_, that.fields_);
        swap(keepalive_, that.keepalive_);
        move_buffer(std::move(that));
        return *this;
    }

    ~Segment() = default;

    static Segment from_buffer(std::shared_ptr<Buffer>&& buf);

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
        return header_->ByteSizeLong();
    }

    [[nodiscard]] std::size_t buffer_bytes() const {
        std::size_t s = 0;
         util::variant_match(buffer_,
           [] (const std::monostate&) { /* Uninitialized buffer */},
           [&s](const BufferView& b) { s = b.bytes(); },
           [&s](const std::shared_ptr<Buffer>& b) { s = b->bytes(); });

        return s;
    }

    arcticdb::proto::encoding::SegmentHeader &header() {
        return *header_;
    }

    [[nodiscard]] const arcticdb::proto::encoding::SegmentHeader &header() const {
        return *header_;
    }

    [[nodiscard]] BufferView buffer() const {
        if (std::holds_alternative<std::shared_ptr<Buffer>>(buffer_)) {
            return std::get<std::shared_ptr<Buffer>>(buffer_)->view();
        } else {
            return std::get<BufferView>(buffer_);
        }
    }

    [[nodiscard]] bool is_uninitialized() const {
        return std::holds_alternative<std::monostate>(buffer_);
    }

    [[nodiscard]] bool is_empty() const {
        return is_uninitialized() || (buffer().bytes() == 0 && header_->ByteSizeLong() == 0);
    }

    [[nodiscard]] bool is_owning_buffer() const {
        return std::holds_alternative<std::shared_ptr<Buffer>>(buffer_);
    }

    [[nodiscard]] std::shared_ptr<FieldCollection> fields_ptr() const {
        return fields_;
    }

    [[nodiscard]] size_t fields_size() const {
        return fields_->size();
    }

    // For external language tools, not efficient
    [[nodiscard]] std::vector<std::string_view> fields_vector() const {
        std::vector<std::string_view> fields;
        for(const auto& field : *fields_)
            fields.push_back(field.name());

        return fields;
    }

    void force_own_buffer() {
        if (!is_owning_buffer()) {
            auto b = std::make_shared<Buffer>();
            std::get<BufferView>(buffer_).copy_to(*b);
            buffer_ = std::move(b);
        }
        keepalive_.reset();
    }

    void set_keepalive(std::any&& keepalive) {
        keepalive_ = std::move(keepalive);
    }

    const std::any& keepalive() const  {
        return keepalive_;
    }

  private:
    void move_buffer(Segment &&that) {
        if(is_uninitialized() || that.is_uninitialized()) {
            std::swap(buffer_, that.buffer_);
        } else if (!(is_owning_buffer() ^ that.is_owning_buffer())) {
            if (is_owning_buffer()) {
                swap(*std::get<std::shared_ptr<Buffer>>(buffer_), *std::get<std::shared_ptr<Buffer>>(that.buffer_));
            } else {
                swap(std::get<BufferView>(buffer_), std::get<BufferView>(that.buffer_));
            }
        } else if (is_owning_buffer()) {
            log::storage().info("Copying segment");
            // data of segment being moved is not owned, moving it is dangerous, copying instead
            that.buffer().copy_to(*std::get<std::shared_ptr<Buffer>>(buffer_));
        } else {
            // data of this segment is a view, but the move data is moved
            buffer_ = std::move(std::get<std::shared_ptr<Buffer>>(that.buffer_));
        }
    }
    std::unique_ptr<google::protobuf::Arena> arena_ = std::make_unique<google::protobuf::Arena>();
    arcticdb::proto::encoding::SegmentHeader* header_ = nullptr;
    VariantBuffer buffer_;
    std::shared_ptr<FieldCollection> fields_;
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
        return fmt::format_to(ctx.out(), "{:c}", c);
    }
};

} //namespace fmt
