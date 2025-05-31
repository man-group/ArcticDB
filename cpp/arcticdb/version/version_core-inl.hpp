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

} // namespace arcticdb
