/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/write_options.hpp>
#include <arcticdb/stream/incompletes.hpp>
#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/stream/segment_aggregator.hpp>
#include <arcticdb/entity/frame_and_descriptor.hpp>
#include <arcticdb/version/version_store_objects.hpp>
#include <arcticdb/version/schema_checks.hpp>
#include <arcticdb/pipeline/slicing.hpp>
#include <arcticdb/version/merge_options.hpp>
#include <arcticdb/entity/read_result.hpp>
#include <arcticdb/version/version_tasks.hpp>
#include <string>

namespace arcticdb::version_store {

using namespace entity;
using namespace pipelines;

VersionedItem write_dataframe_impl(
        const std::shared_ptr<Store>& store, VersionId version_id, const std::shared_ptr<InputFrame>& frame,
        const WriteOptions& options, const std::shared_ptr<DeDupMap>& de_dup_map = std::make_shared<DeDupMap>(),
        bool allow_sparse = false, bool validate_index = false
);

std::tuple<IndexPartialKey, SlicingPolicy> get_partial_key_and_slicing_policy(
        const WriteOptions& options, const InputFrame& frame, VersionId version_id, bool validate_index
);

folly::Future<entity::AtomKey> async_write_dataframe_impl(
        const std::shared_ptr<Store>& store, VersionId version_id, const std::shared_ptr<pipelines::InputFrame>& frame,
        const WriteOptions& options, const std::shared_ptr<DeDupMap>& de_dup_map, bool allow_sparse, bool validate_index
);

folly::Future<AtomKey> async_append_impl(
        const std::shared_ptr<Store>& store, const UpdateInfo& update_info, const std::shared_ptr<InputFrame>& frame,
        const WriteOptions& options, bool validate_index, bool empty_types
);

VersionedItem append_impl(
        const std::shared_ptr<Store>& store, const UpdateInfo& update_info, const std::shared_ptr<InputFrame>& frame,
        const WriteOptions& options, bool validate_index, bool empty_types
);

VersionedItem update_impl(
        const std::shared_ptr<Store>& store, const UpdateInfo& update_info, const UpdateQuery& query,
        const std::shared_ptr<InputFrame>& frame, WriteOptions&& options, bool dynamic_schema, bool empty_types
);

folly::Future<AtomKey> async_update_impl(
        const std::shared_ptr<Store>& store, const UpdateInfo& update_info, const UpdateQuery& query,
        const std::shared_ptr<InputFrame>& frame, WriteOptions&& options, bool dynamic_schema, bool empty_types
);

VersionedItem delete_range_impl(
        const std::shared_ptr<Store>& store, const StreamId& stream_id, const UpdateInfo& update_info,
        const UpdateQuery& query, const WriteOptions&& options, bool dynamic_schema
);

AtomKey index_key_to_column_stats_key(const IndexTypeKey& index_key);

void create_column_stats_impl(
        const std::shared_ptr<Store>& store, const VersionedItem& versioned_item, ColumnStats& column_stats,
        const ReadOptions& read_options
);

void drop_column_stats_impl(
        const std::shared_ptr<Store>& store, const VersionedItem& versioned_item,
        const std::optional<ColumnStats>& column_stats_to_drop
);

FrameAndDescriptor read_column_stats_impl(const std::shared_ptr<Store>& store, const VersionedItem& versioned_item);

ColumnStats get_column_stats_info_impl(const std::shared_ptr<Store>& store, const VersionedItem& versioned_item);

folly::Future<ReadVersionOutput> read_multi_key(
        const std::shared_ptr<Store>& store, const ReadOptions& read_options, const SegmentInMemory& index_key_seg,
        std::any& handler_data
);

folly::Future<std::vector<EntityId>> schedule_remaining_iterations(
        std::vector<std::vector<EntityId>>&& entity_ids_vec_fut,
        std::shared_ptr<std::vector<std::shared_ptr<Clause>>> clauses
);

folly::Future<std::vector<EntityId>> schedule_clause_processing(
        std::shared_ptr<ComponentManager> component_manager,
        std::vector<folly::Future<pipelines::SegmentAndSlice>>&& segment_and_slice_futures,
        std::vector<std::vector<size_t>>&& processing_unit_indexes,
        std::shared_ptr<std::vector<std::shared_ptr<Clause>>> clauses
);

FrameAndDescriptor read_index_impl(const std::shared_ptr<Store>& store, const VersionedItem& version);

std::variant<VersionedItem, CompactionError> compact_incomplete_impl(
        const std::shared_ptr<Store>& store, const StreamId& stream_id,
        const std::optional<proto::descriptors::UserDefinedMetadata>& user_meta, const UpdateInfo& update_info,
        const CompactIncompleteParameters& compaction_parameters, const WriteOptions& write_options,
        std::shared_ptr<PipelineContext>& pipeline_context
);

struct PredefragmentationInfo {
    std::shared_ptr<PipelineContext> pipeline_context;
    std::shared_ptr<ReadQuery> read_query;
    size_t segments_need_compaction;
    std::optional<size_t> append_after;
};

PredefragmentationInfo get_pre_defragmentation_info(
        const std::shared_ptr<Store>& store, const StreamId& stream_id, const UpdateInfo& update_info,
        const WriteOptions& options, size_t segment_size
);

bool is_symbol_fragmented_impl(size_t segments_need_compaction);

VersionedItem defragment_symbol_data_impl(
        const std::shared_ptr<Store>& store, const StreamId& stream_id, const UpdateInfo& update_info,
        const WriteOptions& options, size_t segment_size
);

std::variant<VersionedItem, CompactionError> sort_merge_impl(
        const std::shared_ptr<Store>& store, const StreamId& stream_id,
        const std::optional<proto::descriptors::UserDefinedMetadata>& user_meta, const UpdateInfo& update_info,
        const CompactIncompleteParameters& compaction_parameters, const WriteOptions& write_options,
        std::shared_ptr<PipelineContext>& pipeline_context
);

void add_index_columns_to_query(const ReadQuery& read_query, const TimeseriesDescriptor& desc);

folly::Future<ReadVersionOutput> read_frame_for_version(
        const std::shared_ptr<Store>& store, const VersionIdentifier& version_info,
        const std::shared_ptr<ReadQuery>& read_query, const ReadOptions& read_options, std::any& handler_data
);

folly::Future<SymbolProcessingResult> read_and_process(
        const std::shared_ptr<Store>& store, const VersionIdentifier& version_info,
        const std::shared_ptr<ReadQuery>& read_query, const ReadOptions& read_options,
        std::shared_ptr<ComponentManager> component_manager
);

class DeleteIncompleteKeysOnExit {
  public:
    DeleteIncompleteKeysOnExit(
            std::shared_ptr<PipelineContext> pipeline_context, std::shared_ptr<Store> store, bool via_iteration,
            std::optional<std::vector<StageResult>> stage_results
    );

    ARCTICDB_NO_MOVE_OR_COPY(DeleteIncompleteKeysOnExit)

    ~DeleteIncompleteKeysOnExit();

    void release() { released_ = true; }

  private:
    std::shared_ptr<PipelineContext> context_;
    std::shared_ptr<Store> store_;
    bool via_iteration_;
    bool released_ = false;
    std::optional<std::vector<StageResult>> stage_results_;
};
void delete_incomplete_keys(PipelineContext& pipeline_context, Store& store);

std::optional<DeleteIncompleteKeysOnExit> get_delete_keys_on_failure(
        const std::shared_ptr<PipelineContext>& pipeline_context, const std::shared_ptr<Store>& store,
        const CompactIncompleteParameters& parameters
);

folly::Future<SegmentInMemory> prepare_output_frame(
        std::vector<SliceAndKey>&& items, const std::shared_ptr<PipelineContext>& pipeline_context,
        const std::shared_ptr<Store>& store, const ReadOptions& read_options, std::any& handler_data
);

folly::Future<VersionedItem> read_modify_write_impl(
        const std::shared_ptr<Store>& store, const std::shared_ptr<ReadQuery>& read_query,
        const ReadOptions& read_options, const WriteOptions& write_options,
        const IndexPartialKey& target_partial_index_key, const std::shared_ptr<PipelineContext>& pipeline_context,
        std::optional<proto::descriptors::UserDefinedMetadata>&& user_meta_proto
);

folly::Future<VersionedItem> merge_update_impl(
        const std::shared_ptr<Store>& store, const VersionIdentifier& version_info, const ReadOptions& read_options,
        const WriteOptions& write_options, const IndexPartialKey& target_partial_index_key,
        std::vector<std::string>&& on, const MergeStrategy& strategy, std::shared_ptr<InputFrame> source
);

std::shared_ptr<PipelineContext> setup_pipeline_context(
        const std::shared_ptr<Store>& store, const VersionIdentifier& version_info, ReadQuery& read_query,
        const ReadOptions& read_options
);
} // namespace arcticdb::version_store

namespace arcticdb {

using CompactionWrittenKeys = std::vector<VariantKey>;
using CompactionResult = std::variant<CompactionWrittenKeys, Error>;

void remove_written_keys(Store* store, CompactionWrittenKeys&& written_keys);

bool is_segment_unsorted(const SegmentInMemory& segment);

size_t n_segments_live_during_compaction();

CheckOutcome check_schema_matches_incomplete(
        const StreamDescriptor& stream_descriptor_incomplete, const StreamDescriptor& pipeline_context,
        const bool convert_int_to_float = false
);

struct CompactionOptions {
    bool convert_int_to_float{false};
    bool validate_index{true};
    bool perform_schema_checks{true};
};

template<
        typename IndexType, typename SchemaType, typename SegmentationPolicy, typename DensityPolicy,
        typename IteratorType>
[[nodiscard]] CompactionResult do_compact(
        IteratorType to_compact_start, IteratorType to_compact_end,
        const std::shared_ptr<pipelines::PipelineContext>& pipeline_context, std::vector<pipelines::FrameSlice>& slices,
        const std::shared_ptr<Store>& store, std::optional<size_t> segment_size, const CompactionOptions& options
) {
    CompactionResult result;
    auto index = stream::index_type_from_descriptor(pipeline_context->descriptor());

    std::vector<folly::Future<VariantKey>> write_futures;

    auto semaphore = std::make_shared<folly::NativeSemaphore>(n_segments_live_during_compaction());
    stream::SegmentAggregator<IndexType, SchemaType, SegmentationPolicy, DensityPolicy> aggregator{
            [&slices](pipelines::FrameSlice&& slice) { slices.emplace_back(std::move(slice)); },
            SchemaType{pipeline_context->descriptor(), index},
            [&write_futures, &store, &pipeline_context, &semaphore](SegmentInMemory&& segment) {
                auto local_index_start = IndexType::start_value_for_segment(segment);
                auto local_index_end = pipelines::end_index_generator(IndexType::end_value_for_segment(segment));
                const PartialKey pk{
                        KeyType::TABLE_DATA,
                        pipeline_context->version_id_,
                        pipeline_context->stream_id_,
                        local_index_start,
                        local_index_end
                };

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
        ARCTICDB_DEBUG(
                log::version(),
                "do_compact Symbol {} Segment {}: Segment has rows {} columns {} uncompressed bytes {}",
                pipeline_context->stream_id_,
                count++,
                segment.row_count(),
                segment.columns().size(),
                segment.descriptor().uncompressed_bytes()
        );

        if (!index_names_match(segment.descriptor(), pipeline_context->descriptor())) {
            auto written_keys = folly::collect(write_futures).get();
            remove_written_keys(store.get(), std::move(written_keys));
            return Error{
                    throw_error<ErrorCode::E_DESCRIPTOR_MISMATCH>,
                    fmt::format(
                            "Index names in segment {} and pipeline context {} do not match",
                            segment.descriptor(),
                            pipeline_context->descriptor()
                    )
            };
        }

        if (options.validate_index && is_segment_unsorted(segment)) {
            auto written_keys = folly::collect(write_futures).get();
            remove_written_keys(store.get(), std::move(written_keys));
            return Error{throw_error<ErrorCode::E_UNSORTED_DATA>, "Cannot compact unordered segment"};
        }

        if constexpr (std::is_same_v<SchemaType, FixedSchema>) {
            if (options.perform_schema_checks) {
                CheckOutcome outcome = check_schema_matches_incomplete(
                        segment.descriptor(), pipeline_context->descriptor(), options.convert_int_to_float
                );
                if (std::holds_alternative<Error>(outcome)) {
                    auto written_keys = folly::collect(write_futures).get();
                    remove_written_keys(store.get(), std::move(written_keys));
                    return std::get<Error>(std::move(outcome));
                }
            }
        }

        aggregator.add_segment(std::move(sk.segment(store)), sk.slice(), options.convert_int_to_float);
        sk.unset_segment();
    }

    aggregator.commit();
    return folly::collect(std::move(write_futures)).get();
}

} // namespace arcticdb
