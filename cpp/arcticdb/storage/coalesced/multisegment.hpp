#pragma once

#include <cstdint>
#include <vector>

#include "arcticdb/codec/segment.hpp"
#include <arcticdb/storage/coalesced/multisegment_header.hpp>
#include <arcticdb/codec/codec.hpp>

namespace arcticdb::storage {

class KeySegmentPair;

class MultiSegment {
public:
    MultiSegment(
        StreamId id,
        std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta,
        EncodingVersion encoding_version);

    MultiSegment(
        SegmentInMemory&& segment,
        const AtomKey& key);

    std::optional<std::pair<size_t, size_t>> get_offset_for_key(const AtomKey& key) const {
        return header_.get_offset_for_key(key);
    }

    void add(const AtomKey key, size_t size) {
        keys_.emplace_back(key, size);
    }

    size_t prepare() {
        util::check(std::is_sorted(std::begin(keys_), std::end(keys_), [] (const auto& left, auto& right) {
            return time_symbol_from_key(left.first) < time_symbol_from_key(right.first);
        }), "Multisegment got unsorted keys");

        size_t offset = 0;
        for(const auto& [key, size]: keys_) {
            header_.add_key_and_offset(key, offset, size);
            offset += size;
            body_bytes_ = size;
        }

        segment_ = std::make_unique<Segment>(encode_dispatch(std::move(header_.detach_segment()), *codec_meta_, encoding_version_));
        return segment_->total_segment_size();
    }

    template <typename GetSegmentFunc>
    void write_to(
        uint8_t* dst,
        GetSegmentFunc&& get_seg) const {
        auto get_segment = std::forward<GetSegmentFunc>(get_seg);

        memcpy(dst, segment_->buffer().data(), segment_->total_segment_size());
        auto offset = segment_->total_segment_size();
        for(const auto& [key, size]: keys_) {
            auto segment = get_segment(key);
            util::check(size == segment->total_segment_size(), "Size mismatch in multisegment write_to: {} != {}", size, segment_->total_segment_size());
            memcpy(dst, segment->buffer().data(), size);
            offset += segment.total_segment_size();
        }
    }

    size_t required_bytes() const;

    Segment read(const AtomKey& key);

private:
    std::vector<std::pair<AtomKey, size_t>> keys_;
    MultiSegmentHeader header_;
    size_t body_bytes_ = 0UL;
    std::unique_ptr<Segment> segment_;
    std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta_;
    EncodingVersion encoding_version_;
    std::optional<AtomKey> read_key_;
};

std::optional<Segment> segment_from_coalesced_key(const AtomKey& key);

} //namespace arcticdb


