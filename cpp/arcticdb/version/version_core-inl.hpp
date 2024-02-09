/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#ifndef ARCTICDB_VERSION_CORE_H_
#error "This should only be included by version_core.hpp"
#endif

#include <arcticdb/util/movable_priority_queue.hpp>
#include <arcticdb/stream/merge.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/stream/segment_aggregator.hpp>

namespace arcticdb {

template <typename IndexType, typename SegmentationPolicy, typename DensityPolicy, typename Callable>
void merge_frames_for_keys_impl(
    const StreamId& target_id,
    const IndexType& index,
    SegmentationPolicy segmentation_policy,
    const std::vector<AtomKey>& index_keys,
    const pipelines::ReadQuery &query,
    std::shared_ptr <Store> store,
    Callable&& func) {
    ARCTICDB_DEBUG(log::version(), "Merging keys");
    using namespace arcticdb::pipelines;
    using KeySupplier = folly::Function<std::vector<entity::AtomKey>()>;
    using StreamReaderType = arcticdb::stream::StreamReader<AtomKey, KeySupplier, SegmentInMemory::Row>;

    struct StreamMergeWrapper {
        StreamMergeWrapper(
            KeySupplier &&key_supplier,
            std::shared_ptr <StreamSource> store,
            const IndexRange &index_range,
            StreamId id) :
            stream_reader_(std::move(key_supplier), std::move(store), storage::ReadKeyOpts{}, index_range),
            iterator_(stream_reader_.iterator_rows()),
            row_(iterator_.next()),
            id_(std::move(id)){
        }

        bool advance() {
            row_ = iterator_.next();
            return static_cast<bool>(row_);
        }

        SegmentInMemory::Row& row() {
            return row_.value();
        }

        const StreamId& id() const { return id_; }

    private:
        StreamReaderType stream_reader_;
        StreamReaderType::RowsIteratorType iterator_;
        std::optional<SegmentInMemory::Row> row_;
        StreamId id_;
    };

    IndexRange index_range = unspecified_range();
    if (std::holds_alternative<IndexRange>(query.row_filter))
        index_range = std::get<IndexRange>(query.row_filter);

    auto compare =
        [=](const std::unique_ptr <StreamMergeWrapper> &left, const std::unique_ptr <StreamMergeWrapper> &right) {
            return pipelines::index::index_value_from_row(left->row(), IndexDescriptorImpl::Type::TIMESTAMP, 0) > pipelines::index::index_value_from_row(right->row(), IndexDescriptorImpl::Type::TIMESTAMP, 0);
        };

    movable_priority_queue<std::unique_ptr<StreamMergeWrapper>, std::vector<std::unique_ptr<StreamMergeWrapper>>, decltype(compare)> input_streams{compare};

    for (const auto& index_key : index_keys) {
        auto key_func = [index_key]() { return std::vector<AtomKey> {index_key}; };
        input_streams.emplace(std::make_unique<StreamMergeWrapper>(std::move(key_func), store, index_range, index_key.id()));
    }

    using AggregatorType = Aggregator<IndexType, DynamicSchema, SegmentationPolicy, DensityPolicy>;
    AggregatorType agg{DynamicSchema{index.create_stream_descriptor(target_id, {}), index}, std::move(func), std::move(segmentation_policy)};
    do_merge<IndexType, StreamMergeWrapper, AggregatorType, decltype(input_streams)>(input_streams, agg, true);
}

template <typename SegmentationPolicy, typename Callable>
void merge_frames_for_keys(
    const StreamId& target_id,
    stream::Index&& index,
    SegmentationPolicy&& segmentation_policy,
    stream::VariantColumnPolicy density_policy,
    const std::vector <AtomKey>& index_keys,
    const pipelines::ReadQuery &query,
    std::shared_ptr <Store> store,
    Callable&& func) {
    std::visit([&] (auto idx, auto density) {
        merge_frames_for_keys_impl<decltype(idx), SegmentationPolicy, decltype(density), std::decay_t<decltype(func)>>(
            target_id, idx, std::move(segmentation_policy), index_keys, query, store, std::move(func));
    }, index, density_policy);

}

template <typename IndexType, typename SchemaType, typename SegmentationPolicy, typename DensityPolicy, typename IteratorType>
void do_compact(
    IteratorType target_start,
    IteratorType target_end,
    const std::shared_ptr<pipelines::PipelineContext>& pipeline_context,
    std::vector<folly::Future<VariantKey>>& fut_vec,
    std::vector<pipelines::FrameSlice>& slices,
    const std::shared_ptr<Store>& store,
    bool convert_int_to_float,
    std::optional<size_t> segment_size){
        auto index = stream::index_type_from_descriptor(pipeline_context->descriptor());
        stream::SegmentAggregator<IndexType, SchemaType, SegmentationPolicy, DensityPolicy>
        aggregator{
            [&slices](pipelines::FrameSlice &&slice) {
                slices.emplace_back(std::move(slice));
            },
            SchemaType{pipeline_context->descriptor(), index},
            [&fut_vec, &store, &pipeline_context](SegmentInMemory &&segment) {
                auto local_index_start = IndexType::start_value_for_segment(segment);
                auto local_index_end = pipelines::end_index_generator(IndexType::end_value_for_segment(segment));
                stream::StreamSink::PartialKey
                pk{KeyType::TABLE_DATA, pipeline_context->version_id_, pipeline_context->stream_id_, local_index_start, local_index_end};
                fut_vec.emplace_back(store->write(pk, std::move(segment)));
            },
            segment_size.has_value() ? SegmentationPolicy{*segment_size} : SegmentationPolicy{}
        };

        for(auto it = target_start; it != target_end; ++it) {
            decltype(auto) sk = [&it](){
                if constexpr(std::is_same_v<IteratorType, pipelines::PipelineContext::iterator>)
                    return it->slice_and_key();
                else
                    return *it;
            }();
            aggregator.add_segment(
                std::move(sk.segment(store)),
                sk.slice(),
                convert_int_to_float
            );
            sk.unset_segment();
        }
        aggregator.commit();
}

[[nodiscard]] inline ReadOptions defragmentation_read_options_generator(const WriteOptions &options){
    ReadOptions read_options;
    read_options.set_dynamic_schema(options.dynamic_schema);
    return read_options;
}

} // namespace arcticdb
