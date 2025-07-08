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
#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/stream/segment_aggregator.hpp>
#include <arcticdb/stream/stream_reader.hpp>
#include <arcticdb/version/schema_checks.hpp>
#include <arcticdb/pipeline/write_options.hpp>
#include <arcticdb/pipeline/read_options.hpp>

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

    auto compare = [](const std::unique_ptr<StreamMergeWrapper>& left,
                      const std::unique_ptr<StreamMergeWrapper>& right) {
        return pipelines::index::index_value_from_row(left->row(), IndexDescriptor::Type::TIMESTAMP, 0) >
            pipelines::index::index_value_from_row(right->row(), IndexDescriptor::Type::TIMESTAMP, 0);
    };

    movable_priority_queue<std::unique_ptr<StreamMergeWrapper>, std::vector<std::unique_ptr<StreamMergeWrapper>>, decltype(compare)> input_streams{compare};

    for (const auto& index_key : index_keys) {
        auto key_func = [index_key]() { return std::vector<AtomKey> {index_key}; };
        input_streams.emplace(std::make_unique<StreamMergeWrapper>(std::move(key_func), store, index_range, index_key.id()));
    }

    using AggregatorType = Aggregator<IndexType, DynamicSchema, SegmentationPolicy, DensityPolicy>;
    AggregatorType agg{DynamicSchema{index.create_stream_descriptor(target_id, {}), index}, std::move(func), std::move(segmentation_policy)};
    do_merge<IndexType, AggregatorType, decltype(input_streams)>(input_streams, agg, true);
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

[[nodiscard]] inline ReadOptions defragmentation_read_options_generator(const WriteOptions &options){
    ReadOptions read_options;
    read_options.set_dynamic_schema(options.dynamic_schema);
    return read_options;
}

template <typename IndexType, typename SchemaType, typename SegmentationPolicy, typename DensityPolicy, typename IteratorType>
[[nodiscard]] CompactionResult do_compact(
    IteratorType to_compact_start,
    IteratorType to_compact_end,
    const std::shared_ptr<pipelines::PipelineContext>& pipeline_context,
    std::vector<pipelines::FrameSlice>& slices,
    const std::shared_ptr<Store>& store,
    std::optional<size_t> segment_size,
    const CompactionOptions& options) {
    CompactionResult result;
    auto index = stream::index_type_from_descriptor(pipeline_context->descriptor());

    std::vector<folly::Future<VariantKey>> write_futures;

    auto semaphore = std::make_shared<folly::NativeSemaphore>(n_segments_live_during_compaction());
    stream::SegmentAggregator<IndexType, SchemaType, SegmentationPolicy, DensityPolicy>
        aggregator{
        [&slices](pipelines::FrameSlice &&slice) {
            slices.emplace_back(std::move(slice));
        },
        SchemaType{pipeline_context->descriptor(), index},
        [&write_futures, &store, &pipeline_context, &semaphore](SegmentInMemory &&segment) {
            auto local_index_start = IndexType::start_value_for_segment(segment);
            auto local_index_end = pipelines::end_index_generator(IndexType::end_value_for_segment(segment));
            stream::StreamSink::PartialKey
                pk{KeyType::TABLE_DATA, pipeline_context->version_id_, pipeline_context->stream_id_, local_index_start, local_index_end};

            write_futures.emplace_back(store->write_maybe_blocking(pk, std::move(segment), semaphore));
        },
        segment_size.has_value() ? SegmentationPolicy{*segment_size} : SegmentationPolicy{}
    };

    [[maybe_unused]] size_t count = 0;
    for (auto it = to_compact_start; it != to_compact_end; ++it) {
        auto sk = [&it]() {
            if constexpr (std::is_same_v<IteratorType, pipelines::PipelineContext::iterator>)
                return it->slice_and_key();
            else
                return *it;
        }();
        if (sk.slice().rows().diff() == 0) {
            continue;
        }

        const SegmentInMemory& segment = sk.segment(store);
        ARCTICDB_DEBUG(log::version(), "do_compact Symbol {} Segment {}: Segment has rows {} columns {} uncompressed bytes {}",
                       pipeline_context->stream_id_, count++, segment.row_count(), segment.columns().size(), segment.descriptor().uncompressed_bytes());

        if(!index_names_match(segment.descriptor(), pipeline_context->descriptor())) {
            auto written_keys = folly::collect(write_futures).get();
            remove_written_keys(store.get(), std::move(written_keys));
            return Error{throw_error<ErrorCode::E_DESCRIPTOR_MISMATCH>, fmt::format("Index names in segment {} and pipeline context {} do not match", segment.descriptor(), pipeline_context->descriptor())};
        }

        if(options.validate_index && is_segment_unsorted(segment)) {
            auto written_keys = folly::collect(write_futures).get();
            remove_written_keys(store.get(), std::move(written_keys));
            return Error{throw_error<ErrorCode::E_UNSORTED_DATA>, "Cannot compact unordered segment"};
        }

        if constexpr (std::is_same_v<SchemaType, FixedSchema>) {
            if (options.perform_schema_checks) {
                CheckOutcome outcome = check_schema_matches_incomplete(segment.descriptor(), pipeline_context->descriptor(), options.convert_int_to_float);
                if (std::holds_alternative<Error>(outcome)) {
                    auto written_keys = folly::collect(write_futures).get();
                    remove_written_keys(store.get(), std::move(written_keys));
                    return std::get<Error>(std::move(outcome));
                }
            }
        }

        aggregator.add_segment(
            std::move(sk.segment(store)),
            sk.slice(),
            options.convert_int_to_float
        );
        sk.unset_segment();
    }

    aggregator.commit();
    return folly::collect(std::move(write_futures)).get();
}

} // namespace arcticdb
