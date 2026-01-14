/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/clock.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/stream/index_aggregator.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/stream/stream_utils.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>
#include <arcticdb/entity/protobufs.hpp>

#include <folly/futures/Future.h>
#include <boost/core/noncopyable.hpp>
#include <google/protobuf/any.pb.h>

namespace arcticdb::stream {

namespace pb = arcticdb::proto::descriptors;

template<class Verifier, class IndexType>
folly::Future<VariantKey> collect_and_commit(
        std::vector<folly::Future<VariantKey>>&& fut_keys, StreamId stream_id, KeyType key_type, VersionId version_id,
        std::optional<IndexRange> specified_range, std::shared_ptr<StreamSink> store, Verifier&& verifier
) {

    // Shared ptr here is used to keep the futures alive until the collect future is ready
    auto commit_keys = std::make_shared<std::vector<folly::Future<VariantKey>>>(std::move(fut_keys));
    auto keys_fut = folly::collect(*commit_keys);
    auto keys = keys_fut.wait().value();

    IndexValue start_index;
    IndexValue end_index;

    if (specified_range) {
        start_index = specified_range->start_;
        end_index = specified_range->end_;
    } else if (!keys.empty()) {
        start_index = to_atom(*keys.begin()).start_index();
        end_index = to_atom(*keys.rbegin()).end_index();
    }

    folly::Future<VariantKey> index_key = folly::Future<VariantKey>::makeEmpty();
    IndexAggregator<IndexType> idx_agg(stream_id, [&](auto&& segment) {
        index_key = store->write(key_type, version_id, stream_id, start_index, end_index, std::move(segment));
    });

    for (auto&& key : keys) {
        verifier(key);
        idx_agg.add_key(to_atom(key));
    }

    idx_agg.commit();
    util::check(index_key.valid(), "Empty key returned while committing index");
    return index_key;
}

template<class Index, class Schema, class SegmentingPolicy = RowCountSegmentPolicy>
class StreamWriter : boost::noncopyable {
  public:
    using IndexType = Index;
    using SegmentingPolicyType = SegmentingPolicy;
    using DataAggregator = Aggregator<Index, Schema, SegmentingPolicyType>;

    StreamWriter(
            Schema&& schema, std::shared_ptr<StreamSink> store, VersionId version_id,
            std::optional<IndexRange> index_range = std::nullopt,
            SegmentingPolicyType&& segmenting_policy = SegmentingPolicyType()
    ) :
        data_agg_(
                std::move(schema), [&](auto&& segment) { on_data_segment(std::move(segment)); },
                std::move(segmenting_policy)
        ),
        store_(store),
        version_id_(version_id),
        specified_range_(index_range),
        written_data_keys_() {}

    template<class... Args>
    typename DataAggregator::RowBuilderType& start_row(Args... args) {
        return data_agg_.start_row(std::forward<Args>(args)...);
    }

    typename DataAggregator::RowBuilderType& row_builder() { return data_agg_.row_builder(); }

    folly::Future<VariantKey> commit(KeyType key_type = KeyType::UNDEFINED) {
        SCOPE_FAIL {
            log::root().error("Failure while writing keys for version_id={},stream_id={}", version_id_, stream_id());
        };
        data_agg_.commit();

        std::scoped_lock l{commit_mutex_};
        auto verify = [version_id = version_id_, stream_id = stream_id()](const VariantKey& key) {
            util::check_arg(
                    version_id == to_atom(key).version_id(),
                    "Invalid key expected version_id={}, actual={}",
                    version_id,
                    key
            );
            util::check_arg(
                    stream_id == to_atom(key).id(), "Invalid key, expected symbol={}, actual={}", stream_id, key
            );
        };

        if (key_type == KeyType::UNDEFINED)
            key_type = get_key_type_for_index_stream(stream_id());

        return collect_and_commit<decltype(verify), IndexType>(
                std::move(written_data_keys_),
                stream_id(),
                key_type,
                version_id_,
                specified_range_,
                store_,
                std::move(verify)
        );
    }

    StreamId stream_id() const { return data_agg_.descriptor().id(); }

    VersionId version_id() const { return version_id_; }

    DataAggregator& aggregator() { return data_agg_; }

  private:
    void on_data_segment(SegmentInMemory&& segment) {
        auto seg_start = segment_start(segment);
        auto seg_end = segment_end(segment);

        written_data_keys_.emplace_back(store_->write(
                get_key_type_for_data_stream(stream_id()),
                version_id_,
                stream_id(),
                seg_start,
                seg_end,
                std::move(segment)
        ));
    }

    IndexValue segment_start(const SegmentInMemory& segment) const {
        return data_agg_.index().start_value_for_segment(segment);
    }

    IndexValue segment_end(const SegmentInMemory& segment) const {
        return data_agg_.index().end_value_for_segment(segment);
    }

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    void set_scalar(std::size_t pos, T val) {
        data_agg_.set_scalar(pos, val);
    }

    template<class T, std::enable_if_t<std::is_same_v<std::decay_t<T>, std::string>, int> = 0>
    void set_scalar(position_t pos, T val) {
        data_agg_.set_scalar(pos, val);
    }

    void end_row() { data_agg_.end_row(); }

    DataAggregator data_agg_;
    std::shared_ptr<StreamSink> store_;

    VersionId version_id_;
    std::optional<IndexRange> specified_range_;

    std::mutex commit_mutex_;
    std::vector<folly::Future<VariantKey>> written_data_keys_;
};

} // namespace arcticdb::stream
