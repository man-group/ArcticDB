/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/stream/index.hpp>
#include <arcticdb/stream/stream_utils.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/types.hpp>

#include <folly/Function.h>

namespace arcticdb::stream {

inline void write_key_to_segment(SegmentInMemory &segment, const entity::AtomKey &key) {
    ARCTICDB_DEBUG(log::storage(), "Writing key row {}", key.view());
    std::visit([&segment](auto &&val) { segment.set_scalar(int(pipelines::index::Fields::start_index), val); }, key.start_index());
    std::visit([&segment](auto &&val) { segment.set_scalar(int(pipelines::index::Fields::end_index), val); }, key.end_index());
    segment.set_scalar(int(pipelines::index::Fields::version_id), key.version_id());
    std::visit([&segment](auto &&val) { segment.set_scalar(int(pipelines::index::Fields::stream_id), val); }, key.id());
    segment.set_scalar(int(pipelines::index::Fields::creation_ts), key.creation_ts());
    segment.set_scalar(int(pipelines::index::Fields::content_hash), key.content_hash());
    segment.set_scalar(int(pipelines::index::Fields::index_type), static_cast<uint8_t>(stream::get_index_value_type(key)));
    segment.set_scalar(int(pipelines::index::Fields::key_type), static_cast<uint8_t>(key.type()));
    segment.end_row();
}

template<class DataIndexType>
class FlatIndexingPolicy {
    using Callback = folly::Function<void(SegmentInMemory && )>;
  public:
    template<class C>
    FlatIndexingPolicy(StreamId stream_id, C&& c) :
        callback_(std::move(c)),
        schema_(idx_schema(stream_id, DataIndexType::default_index())),
        segment_(schema_.default_descriptor()) {}

    void add_key(const AtomKey &key) {
        write_key_to_segment(segment_, key);
    }

    void commit() {
        if (LIKELY(!segment_.empty())) {
            callback_(std::move(segment_));
            segment_ = SegmentInMemory(schema_.default_descriptor());
        }
    }

    void set_metadata(google::protobuf::Any &&meta) {
        segment_.set_metadata(std::move(meta));
    }

  private:
    Callback callback_;
    FixedSchema schema_;
    SegmentInMemory segment_;
};

template<class DataIndexType, class IndexingPolicy = FlatIndexingPolicy<DataIndexType>>
class IndexAggregator {
  public:
    template<class C>
    IndexAggregator(StreamId stream_id, C &&c):
        indexing_policy_(stream_id, std::move(c)) {}

    void add_key(const AtomKey &key) {
        indexing_policy_.add_key(key);
    }

    void commit() {
        indexing_policy_.commit();
    }

    void set_metadata(google::protobuf::Any &&meta) {
        indexing_policy_.set_metadata(std::move(meta));
    }

  private:
    IndexingPolicy indexing_policy_;
};

} // namespace arcticdb::stream

