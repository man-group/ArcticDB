#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/codec/segment.hpp>
#include <arcticdb/storage/coalesced/multi_segment.hpp>
#include <arcticdb/storage/coalesced/coalesced_storage_common.hpp>
#include <arcticdb/storage/key_segment_pair.hpp>
#include <arcticdb/stream/stream_utils.hpp>

namespace arcticdb::storage::coalesced {

struct MultiSegmentAggregatorStats {
    size_t bytes_ = 0;
    size_t num_segments_ = 0;

    void reset() {
        bytes_ = 0;
        num_segments_ = 0;
    }

    void update(size_t size) {
        ++num_segments_;
        bytes_ += size;
    }
};

struct BytesSizingPolicy {
    const uint64_t num_bytes_;

    explicit BytesSizingPolicy(uint64_t bytes) :
        num_bytes_(bytes) {
    }

    bool operator()(const MultiSegmentAggregatorStats& stats) {
        return stats.bytes_ >= num_bytes_;
    }
};

struct InfiniteSizingPolicy {
    InfiniteSizingPolicy() { }

    bool operator()(const MultiSegmentAggregatorStats&) {
        return false;
    }
};

template <class SizingPolicy, class CommitFunc, class GetSegmentFunc, class GetSizeFunc>
class MultiSegmentAggregator {
    StreamId stream_id_;
    MultiSegmentAggregatorStats stats_;
    SizingPolicy policy_;
    std::vector<AtomKey> keys_;
    CommitFunc commit_;
    GetSegmentFunc get_segment_;
    GetSizeFunc get_size_;
    std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta_;
    EncodingVersion encoding_version_;
    MultiSegment multi_seg_;

    MultiSegmentAggregator(
        const StreamId& stream_id,
        SizingPolicy sizing_policy,
        CommitFunc&& c,
        GetSegmentFunc&& g,
        GetSizeFunc&& s,
        std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta,
        EncodingVersion encoding_version) :
        stream_id_(std::move(stream_id)),
        policy_(sizing_policy),
        commit_(std::move(c)),
        get_segment_(std::move(g)),
        get_size_(std::move(s)),
        codec_meta_(codec_meta),
        encoding_version_(encoding_version) {
    }

    void commit() {
        MultiSegment seg(stream_id_, std::move(keys_), codec_meta_, encoding_version_);
        commit_(std::move(seg));
        stats_.reset();
    }

    void add(AtomKey&& key) {
        const auto size = get_size_(key);
        multi_seg_.add(key, size);
        stats_.update(size);
        if(policy_(stats_)) {
            commit();
        }
    }

    void add(AtomKey&& key, SegmentInMemory&& segment_in_memory) {
        auto segment = encode_dispatch(std::move(segment_in_memory), *codec_meta_, encoding_version_);
        keys_.emplace_back(std::make_pair(std::move(key), std::move(segment)));
        if(policy_(stats_)) {
            commit();
        }
    }
};

template <class AggregatorType>
struct MultiSegmentIndexer {
    StreamId stream_id_;
    SegmentInMemory index_;
    AggregatorType agg;

    MultiSegmentIndexer(
        KeyType key_type,
        std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta,
        EncodingVersion encoding_version) :

        index_(stream::idx_stream_desc(coalesced_id_for_key_type(key_type), UnsignedIndex{})) {
    }

    void add(AtomKey&& key, SegmentInMemory&& segment) {
          agg.add(key, segment);
    }

    void add(AtomKey&& key) {
        agg.add(key);
    }
};

} //namespace arcticdb::storage::coalesced