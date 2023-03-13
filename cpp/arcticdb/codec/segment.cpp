/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/segment.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>
#include <arcticdb/storage/common.hpp>

#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/util/dump_bytes.hpp>


namespace arcticdb {

Segment Segment::from_bytes(std::uint8_t* src, std::size_t readable_size, bool copy_data /* = false */) {
    ARCTICDB_SAMPLE(SegmentFromBytes, 0)
    auto* fixed_hdr = reinterpret_cast<Segment::FixedHeader*>(src);
    util::check_arg(fixed_hdr->magic_number == MAGIC_NUMBER, "expected first 2 bytes: {}, actual {}",
                    MAGIC_NUMBER, fixed_hdr->magic_number);
    util::check_arg(fixed_hdr->encoding_version == ENCODING_VERSION,
                    "expected encoding_version {}, actual {}",
                    ENCODING_VERSION, fixed_hdr->encoding_version);

    ARCTICDB_SUBSAMPLE(ReadHeaderAndSegment, 0)
    auto header_bytes ARCTICDB_UNUSED = arcticdb::Segment::FIXED_HEADER_SIZE + fixed_hdr->header_bytes;
    ARCTICDB_DEBUG(log::codec(), "Reading header: {} + {} = {}",
                       arcticdb::Segment::FIXED_HEADER_SIZE,
                       fixed_hdr->header_bytes,
                       header_bytes);
    google::protobuf::io::ArrayInputStream ais(src + arcticdb::Segment::FIXED_HEADER_SIZE, fixed_hdr->header_bytes);
    auto arena = std::make_unique<google::protobuf::Arena>();
    auto seg_hdr = google::protobuf::Arena::CreateMessage<arcticdb::proto::encoding::SegmentHeader>(arena.get());
    seg_hdr->ParseFromZeroCopyStream(&ais);

    const auto[string_pool_size, buffer_bytes] = segment_size::compressed(*seg_hdr);
    ARCTICDB_DEBUG(log::codec(), "Reading string pool {} and buffer bytes {}", string_pool_size, buffer_bytes);
    util::check(arcticdb::Segment::FIXED_HEADER_SIZE + fixed_hdr->header_bytes + buffer_bytes <= readable_size,
                "Size disparity, fixed header size {} + variable header size {} + buffer size {}  (string pool size {}) >= total size {}",
                arcticdb::Segment::FIXED_HEADER_SIZE,
                fixed_hdr->header_bytes,
                buffer_bytes,
                string_pool_size,
                readable_size
    );

    ARCTICDB_SUBSAMPLE(CreateBufferView, 0)
    if (copy_data) {
        auto buf = std::make_shared<Buffer>();
        buf->ensure(buffer_bytes);
        memcpy(buf->data(), src + arcticdb::Segment::FIXED_HEADER_SIZE + fixed_hdr->header_bytes, buffer_bytes);
        return {std::move(arena), seg_hdr, std::move(buf)};
    } else {
        BufferView bv{src + arcticdb::Segment::FIXED_HEADER_SIZE + fixed_hdr->header_bytes, buffer_bytes};
        return {std::move(arena), seg_hdr, std::move(bv)};
    }
}


Segment Segment::from_buffer(std::shared_ptr<Buffer>&& buffer) {
    ARCTICDB_SAMPLE(SegmentFromBytes, 0)
    auto* fixed_hdr = reinterpret_cast<Segment::FixedHeader*>(buffer->data());
    auto readable_size = buffer->bytes();
    util::check_arg(fixed_hdr->magic_number == MAGIC_NUMBER, "expected first 2 bytes: {}, actual {}",
                    MAGIC_NUMBER, fixed_hdr->magic_number);
    util::check_arg(fixed_hdr->encoding_version == ENCODING_VERSION,
                    "expected encoding_version {}, actual {}",
                    ENCODING_VERSION, fixed_hdr->encoding_version);

    ARCTICDB_SUBSAMPLE(ReadHeaderAndSegment, 0)
    auto header_bytes ARCTICDB_UNUSED = arcticdb::Segment::FIXED_HEADER_SIZE + fixed_hdr->header_bytes;
    ARCTICDB_DEBUG(log::codec(), "Reading header: {} + {} = {}",
                  arcticdb::Segment::FIXED_HEADER_SIZE,
                  fixed_hdr->header_bytes,
                  header_bytes);
    google::protobuf::io::ArrayInputStream ais(buffer->data() + arcticdb::Segment::FIXED_HEADER_SIZE, fixed_hdr->header_bytes);
    auto arena = std::make_unique<google::protobuf::Arena>();
    auto seg_hdr = google::protobuf::Arena::CreateMessage<arcticdb::proto::encoding::SegmentHeader>(arena.get());
    seg_hdr->ParseFromZeroCopyStream(&ais);

    const auto[string_pool_size, buffer_bytes] = segment_size::compressed(*seg_hdr);
    ARCTICDB_DEBUG(log::codec(), "Reading string pool {} and buffer bytes {}", string_pool_size, buffer_bytes);
    util::check(arcticdb::Segment::FIXED_HEADER_SIZE + fixed_hdr->header_bytes + buffer_bytes <= readable_size,
                "Size disparity, fixed header size {} + variable header size {} + buffer size {}  (string pool size {}) >= total size {}",
                arcticdb::Segment::FIXED_HEADER_SIZE,
                fixed_hdr->header_bytes,
                buffer_bytes,
                string_pool_size,
                readable_size
    );

    buffer->set_preamble(arcticdb::Segment::FIXED_HEADER_SIZE + fixed_hdr->header_bytes);
    ARCTICDB_SUBSAMPLE(CreateSegment, 0)
    return{std::move(arena), seg_hdr, std::move(buffer)};

}

void Segment::write_header(uint8_t* dst, size_t hdr_size) {
    FixedHeader hdr = {MAGIC_NUMBER, ENCODING_VERSION, std::uint32_t(hdr_size)};
    hdr.write(dst);
    google::protobuf::io::ArrayOutputStream aos(dst + FIXED_HEADER_SIZE, static_cast<int>(hdr_size));
    header_->SerializeToZeroCopyStream(&aos);
}

std::pair<uint8_t*, size_t> Segment::try_internal_write(std::shared_ptr<Buffer>& tmp, size_t hdr_size) {
    auto total_hdr_size = hdr_size + FIXED_HEADER_SIZE;
    if(std::holds_alternative<std::shared_ptr<Buffer>>(buffer_) && std::get<std::shared_ptr<Buffer>>(buffer_)->preamble_bytes() >= total_hdr_size) {
        auto& buffer = std::get<std::shared_ptr<Buffer>>(buffer_);
        auto base_ptr = buffer->preamble() + (buffer->preamble_bytes() - total_hdr_size);
        util::check(base_ptr + total_hdr_size == buffer->data(), "Expected base ptr to align with data ptr, {} != {}", fmt::ptr(base_ptr + total_hdr_size), fmt::ptr(buffer->data()));
        ARCTICDB_TRACE(log::codec(), "Buffer contents before header write: {}", dump_bytes(buffer->data(), buffer->bytes(), 100u));
        write_header(base_ptr, hdr_size);
        ARCTICDB_TRACE(log::storage(), "Header fits in internal buffer {:x} with {} bytes space: {}", uintptr_t (base_ptr), buffer->preamble_bytes() - total_hdr_size, dump_bytes(buffer->data(), buffer->bytes(), 100u));
        return std::make_pair(base_ptr, total_segment_size(hdr_size));
    }
    else {
        tmp = std::make_shared<Buffer>();
        ARCTICDB_DEBUG(log::storage(), "Header doesn't fit in internal buffer, needed {} bytes but had {}, writing to temp buffer at {:x}", hdr_size, std::get<std::shared_ptr<Buffer>>(buffer_)->preamble_bytes(), uintptr_t(tmp->data()));
        tmp->ensure(total_segment_size(hdr_size));
        write_to(tmp->preamble(), hdr_size);
        return std::make_pair(tmp->preamble(), total_segment_size(hdr_size));
    }
}

void Segment::write_to(std::uint8_t* dst, std::size_t hdr_sz) {
    ARCTICDB_SAMPLE(SegmentWriteToStorage, RMTSF_Aggregate)
    ARCTICDB_SUBSAMPLE(SegmentWriteHeader, RMTSF_Aggregate)
    write_header(dst, hdr_sz);
    ARCTICDB_SUBSAMPLE(SegmentWriteBody, RMTSF_Aggregate)
    ARCTICDB_DEBUG(log::codec(), "Writing {} bytes to body at offset {}",
                       buffer().bytes(),
                       arcticdb::Segment::FIXED_HEADER_SIZE + hdr_sz);

    std::memcpy(dst + arcticdb::Segment::FIXED_HEADER_SIZE + hdr_sz,
                buffer().data(),
                buffer().bytes());
}

} //namespace arcticdb
