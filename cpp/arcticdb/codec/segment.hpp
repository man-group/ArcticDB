/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/buffer.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/storage/common.hpp>
#include <arcticdb/codec/encoding_sizes.hpp>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/arena.h>
#include <arcticdb/util/buffer_pool.hpp>

#include <iostream>
#include <variant>

namespace arcticdb {

namespace segment_size {
inline std::tuple<size_t, size_t> compressed(const arcticdb::proto::encoding::SegmentHeader& seg_hdr)
{
    size_t metadata_size = 0;
    // If we have metadata it is part of the buffer size, otherwise the allocated buffer is much too small
    if (seg_hdr.has_metadata_field())
        metadata_size = encoding_size::compressed_size(seg_hdr.metadata_field().ndarray());

    size_t string_pool_size = 0;
    if (seg_hdr.has_string_pool_field())
        string_pool_size = encoding_size::compressed_size(seg_hdr.string_pool_field().ndarray());

    std::size_t buffer_size = encoding_size::compressed_size(seg_hdr) + metadata_size + string_pool_size;
    return {string_pool_size, buffer_size};
}

inline std::tuple<size_t, size_t> uncompressed(const arcticdb::proto::encoding::SegmentHeader& seg_hdr)
{
    size_t metadata_size = 0;
    // If we have metadata it is part of the buffer size, otherwise the allocated buffer is much too small
    if (seg_hdr.has_metadata_field())
        metadata_size = encoding_size::uncompressed_size(seg_hdr.metadata_field().ndarray());

    size_t string_pool_size = 0;
    if (seg_hdr.has_string_pool_field())
        string_pool_size = encoding_size::uncompressed_size(seg_hdr.string_pool_field().ndarray());

    std::size_t buffer_size = encoding_size::uncompressed_size(seg_hdr) + metadata_size + string_pool_size;
    return {string_pool_size, buffer_size};
}
} // namespace segment_size

/*
 * Segment contains compressed data as returned from storage. When reading data the next step will usually be to
 * decompress the Segment into a SegmentInMemory which allows for data access and modification. At the time of writing,
 * the primary method for decompressing a Segment into a SegmentInMemory is in codec.cpp::decode (and the reverse via
 * codec.cpp:encode).
 */
class Segment {
public:
    constexpr static uint16_t MAGIC_NUMBER = 0xFA57;
    constexpr static uint16_t ENCODING_VERSION = 1;

    struct FixedHeader {
        //TODO we have two magic number classes now
        std::uint16_t magic_number;
        std::uint16_t encoding_version;
        std::uint32_t header_bytes;

        void write(std::uint8_t* dst) const
        {
            ARCTICDB_DEBUG(log::codec(), "Writing header with size {}", header_bytes);
            auto h = reinterpret_cast<FixedHeader*>(dst);
            *h = *this;
        }

        void write(std::ostream& dst)
        {
            dst.write(reinterpret_cast<char*>(this), sizeof(FixedHeader));
        }
    };

    constexpr static std::size_t FIXED_HEADER_SIZE = sizeof(FixedHeader);

    Segment()
        : header_(google::protobuf::Arena::CreateMessage<arcticdb::proto::encoding::SegmentHeader>(arena_.get()))
    {
    }

    Segment(std::unique_ptr<google::protobuf::Arena>&& arena,
        arcticdb::proto::encoding::SegmentHeader* header,
        std::shared_ptr<Buffer>&& buffer)
        : arena_(std::move(arena)),
          header_(header),
          buffer_(std::move(buffer))
    {
    }

    Segment(std::unique_ptr<google::protobuf::Arena>&& arena,
        arcticdb::proto::encoding::SegmentHeader* header,
        BufferView&& buffer)
        : arena_(std::move(arena)),
          header_(header),
          buffer_(std::move(buffer))
    {
    }

    // for rvo only, go to solution should be to move
    Segment(const Segment& that)
        : header_(google::protobuf::Arena::CreateMessage<arcticdb::proto::encoding::SegmentHeader>(arena_.get()))
    {
        header_->CopyFrom(*that.header_);
        auto b = std::make_shared<Buffer>();
        util::variant_match(
            that.buffer_,
            [](const std::monostate&) { /* Uninitialized buffer */ },
            [&b](const BufferView& buf) { buf.copy_to(*b); },
            [&b](const std::shared_ptr<Buffer>& buf) { buf->copy_to(*b); });
        buffer_ = std::move(b);
    }

    Segment& operator=(const Segment& that)
    {
        header_->CopyFrom(*that.header_);
        auto b = std::make_shared<Buffer>();
        util::variant_match(
            that.buffer_,
            [](const std::monostate&) { /* Uninitialized buffer */ },
            [&b](const BufferView& buf) { buf.copy_to(*b); },
            [&b](const std::shared_ptr<Buffer>& buf) { buf->copy_to(*b); });
        buffer_ = std::move(b);
        return *this;
    }

    Segment(Segment&& that) noexcept
    {
        using std::swap;
        swap(header_, that.header_);
        swap(arena_, that.arena_);
        move_buffer(std::move(that));
    }

    Segment& operator=(Segment&& that) noexcept
    {
        using std::swap;
        swap(header_, that.header_);
        swap(arena_, that.arena_);
        move_buffer(std::move(that));
        return *this;
    }

    ~Segment() = default;

    static Segment from_buffer(std::shared_ptr<Buffer>&& buf);

    void set_buffer(VariantBuffer&& buffer)
    {
        buffer_ = std::move(buffer);
    }

    static Segment from_bytes(std::uint8_t* src, std::size_t readable_size, bool copy_data = false);

    void write_to(std::uint8_t* dst, std::size_t hdr_sz);

    std::pair<uint8_t*, size_t> try_internal_write(std::shared_ptr<Buffer>& tmp, size_t hdr_size);

    void write_header(uint8_t* dst, size_t hdr_size);

    [[nodiscard]] std::size_t total_segment_size() const
    {
        return total_segment_size(segment_header_bytes_size());
    }

    [[nodiscard]] std::size_t total_segment_size(std::size_t hdr_size) const
    {
        auto total = FIXED_HEADER_SIZE + hdr_size + buffer_bytes();
        ARCTICDB_TRACE(log::storage(),
            "Total segment size {} + {} + {} = {}",
            FIXED_HEADER_SIZE,
            hdr_size,
            buffer_bytes(),
            total);
        return total;
    }

    [[nodiscard]] std::size_t segment_header_bytes_size() const
    {
        return header_->ByteSizeLong();
    }

    [[nodiscard]] std::size_t buffer_bytes() const
    {
        std::size_t s = 0;
        util::variant_match(
            buffer_,
            [](const std::monostate&) { /* Uninitialized buffer */ },
            [&s](const BufferView& b) { s = b.bytes(); },
            [&s](const std::shared_ptr<Buffer>& b) { s = b->bytes(); });

        return s;
    }

    arcticdb::proto::encoding::SegmentHeader& header()
    {
        return *header_;
    }

    [[nodiscard]] const arcticdb::proto::encoding::SegmentHeader& header() const
    {
        return *header_;
    }

    [[nodiscard]] BufferView buffer() const
    {
        if (std::holds_alternative<std::shared_ptr<Buffer>>(buffer_)) {
            return std::get<std::shared_ptr<Buffer>>(buffer_)->view();
        } else {
            return std::get<BufferView>(buffer_);
        }
    }

    [[nodiscard]] bool is_uninitialized() const
    {
        return std::holds_alternative<std::monostate>(buffer_);
    }

    [[nodiscard]] bool is_empty() const
    {
        return is_uninitialized() || (buffer().bytes() == 0 && header_->ByteSizeLong() == 0);
    }

    [[nodiscard]] bool is_owning_buffer() const
    {
        return std::holds_alternative<std::shared_ptr<Buffer>>(buffer_);
    }

    void force_own_buffer()
    {
        if (!is_owning_buffer()) {
            auto b = std::make_shared<Buffer>();
            std::get<BufferView>(buffer_).copy_to(*b);
            buffer_ = std::move(b);
        }
    }

private:
    void move_buffer(Segment&& that)
    {
        if (is_uninitialized() || that.is_uninitialized()) {
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
};

} //namespace arcticdb