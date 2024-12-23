/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/write_options.hpp>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/stream/append_map.hpp>
#include <arcticdb/pipeline/read_pipeline.hpp>
#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/stream/stream_reader.hpp>
#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/stream/segment_aggregator.hpp>
#include <arcticdb/entity/frame_and_descriptor.hpp>
#include <arcticdb/version/version_store_objects.hpp>

#include <string>

namespace arcticdb::version_store {

using namespace arcticdb::entity;
using namespace arcticdb::pipelines;

struct CompactIncompleteOptions {
    bool prune_previous_versions_;
    bool append_;
    bool convert_int_to_float_;
    bool via_iteration_;
    bool sparsify_;
    bool validate_index_{true}; // Default value as unused in sort_merge
    bool delete_staged_data_on_failure_{false};
};

struct ReadVersionOutput {
    ReadVersionOutput() = delete;
    ReadVersionOutput(VersionedItem&& versioned_item, FrameAndDescriptor&& frame_and_descriptor):
        versioned_item_(std::move(versioned_item)),
        frame_and_descriptor_(std::move(frame_and_descriptor)) {}

    ARCTICDB_MOVE_ONLY_DEFAULT(ReadVersionOutput)

    VersionedItem versioned_item_;
    FrameAndDescriptor frame_and_descriptor_;
};

VersionedItem write_dataframe_impl(
    const std::shared_ptr<Store>& store,
    VersionId version_id,
    const std::shared_ptr<InputTensorFrame>& frame,
    const WriteOptions& options,
    const std::shared_ptr<DeDupMap>& de_dup_map = std::make_shared<DeDupMap>(),
    bool allow_sparse = false,
    bool validate_index = false
);

folly::Future<entity::AtomKey> async_write_dataframe_impl(
    const std::shared_ptr<Store>& store,
    VersionId version_id,
    const std::shared_ptr<InputTensorFrame>& frame,
    const WriteOptions& options,
    const std::shared_ptr<DeDupMap>& de_dup_map,
    bool allow_sparse,
    bool validate_index
);

folly::Future<AtomKey> async_append_impl(
    const std::shared_ptr<Store>& store,
    const UpdateInfo& update_info,
    const std::shared_ptr<InputTensorFrame>& frame,
    const WriteOptions& options,
    bool validate_index,
    bool empty_types);

VersionedItem append_impl(
    const std::shared_ptr<Store>& store,
    const UpdateInfo& update_info,
    const std::shared_ptr<InputTensorFrame>& frame,
    const WriteOptions& options,
    bool validate_index,
    bool empty_types);

VersionedItem update_impl(
    const std::shared_ptr<Store>& store,
    const UpdateInfo& update_info,
    const UpdateQuery & query,
    const std::shared_ptr<InputTensorFrame>& frame,
    const WriteOptions&& options,
    bool dynamic_schema,
    bool empty_types);

VersionedItem delete_range_impl(
    const std::shared_ptr<Store>& store,
    const StreamId& stream_id,
    const UpdateInfo& update_info,
    const UpdateQuery& query,
    const WriteOptions&& options,
    bool dynamic_schema);

AtomKey index_key_to_column_stats_key(const IndexTypeKey& index_key);

void create_column_stats_impl(
    const std::shared_ptr<Store>& store,
    const VersionedItem& versioned_item,
    ColumnStats& column_stats,
    const ReadOptions& read_options);

void drop_column_stats_impl(
    const std::shared_ptr<Store>& store,
    const VersionedItem& versioned_item,
    const std::optional<ColumnStats>& column_stats_to_drop);

FrameAndDescriptor read_column_stats_impl(
    const std::shared_ptr<Store>& store,
    const VersionedItem& versioned_item);

ColumnStats get_column_stats_info_impl(
    const std::shared_ptr<Store>& store,
    const VersionedItem& versioned_item);

folly::Future<ReadVersionOutput> read_multi_key(
    const std::shared_ptr<Store>& store,
    const SegmentInMemory& index_key_seg,
    std::any& handler_data);

folly::Future<std::vector<EntityId>> schedule_clause_processing(
    std::shared_ptr<ComponentManager> component_manager,
    std::vector<folly::Future<pipelines::SegmentAndSlice>>&& segment_and_slice_futures,
    std::vector<std::vector<size_t>>&& processing_unit_indexes,
    std::shared_ptr<std::vector<std::shared_ptr<Clause>>> clauses);

FrameAndDescriptor read_segment_impl(
    const std::shared_ptr<Store>& store,
    const VariantKey& key);

FrameAndDescriptor read_index_impl(
    const std::shared_ptr<Store>& store,
    const VersionedItem& version);

VersionedItem compact_incomplete_impl(
    const std::shared_ptr<Store>& store,
    const StreamId& stream_id,
    const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta,
    const UpdateInfo& update_info,
    const CompactIncompleteOptions& options,
    const WriteOptions& write_options,
    std::shared_ptr<PipelineContext>& pipeline_context);

struct PredefragmentationInfo{
    std::shared_ptr<PipelineContext> pipeline_context;
    std::shared_ptr<ReadQuery> read_query;
    size_t segments_need_compaction;
    std::optional<size_t> append_after;
};

PredefragmentationInfo get_pre_defragmentation_info(
        const std::shared_ptr<Store>& store,
        const StreamId& stream_id,
        const UpdateInfo& update_info,
        const WriteOptions& options,
        size_t segment_size);

bool is_symbol_fragmented_impl(size_t segments_need_compaction);

VersionedItem defragment_symbol_data_impl(
        const std::shared_ptr<Store>& store,
        const StreamId& stream_id,
        const UpdateInfo& update_info,
        const WriteOptions& options,
        size_t segment_size);

        
VersionedItem sort_merge_impl(
    const std::shared_ptr<Store>& store,
    const StreamId& stream_id,
    const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta,
    const UpdateInfo& update_info,
    const CompactIncompleteOptions& options,
    const WriteOptions& write_options,
    std::shared_ptr<PipelineContext>& pipeline_context);

void modify_descriptor(
    const std::shared_ptr<pipelines::PipelineContext>& pipeline_context,
    const ReadOptions& read_options);

void read_indexed_keys_to_pipeline(
    const std::shared_ptr<Store>& store,
    const std::shared_ptr<PipelineContext>& pipeline_context,
    const VersionedItem& version_info,
    ReadQuery& read_query,
    const ReadOptions& read_options);

void add_index_columns_to_query(
    const ReadQuery& read_query, 
    const TimeseriesDescriptor& desc);

folly::Future<ReadVersionOutput> read_frame_for_version(
    const std::shared_ptr<Store>& store,
    const std::variant<VersionedItem, StreamId>& version_info,
    const std::shared_ptr<ReadQuery>& read_query,
    const ReadOptions& read_options,
    std::any& handler_data
);

class DeleteIncompleteKeysOnExit {
public:
    DeleteIncompleteKeysOnExit(
        std::shared_ptr<PipelineContext> pipeline_context,
        std::shared_ptr<Store> store,
        bool via_iteration);

    ARCTICDB_NO_MOVE_OR_COPY(DeleteIncompleteKeysOnExit)

    ~DeleteIncompleteKeysOnExit();

    void release() {
        released_ = true;
    }

private:
    std::shared_ptr<PipelineContext> context_;
    std::shared_ptr<Store> store_;
    bool via_iteration_;
    bool released_ = false;
};
void delete_incomplete_keys(PipelineContext& pipeline_context, Store& store);

std::optional<DeleteIncompleteKeysOnExit> get_delete_keys_on_failure(
    const std::shared_ptr<PipelineContext>& pipeline_context,
    const std::shared_ptr<Store>& store,
    const CompactIncompleteOptions& options);

} //namespace arcticdb::version_store

namespace arcticdb {

struct Error {

    explicit Error(folly::Function<void(std::string)> raiser, std::string msg);
    void throw_error();

    folly::Function<void(std::string)> raiser_;
    std::string msg_;
};

using CheckOutcome = std::variant<Error, std::monostate>;
using StaticSchemaCompactionChecks = folly::Function<CheckOutcome(const StreamDescriptor&, const StreamDescriptor&)>;
using CompactionWrittenKeys = std::vector<VariantKey>;
using CompactionResult = std::variant<CompactionWrittenKeys, Error>;

void remove_written_keys(Store* store, CompactionWrittenKeys&& written_keys);

bool is_segment_unsorted(const SegmentInMemory& segment);

template <typename IndexType, typename SchemaType, typename SegmentationPolicy, typename DensityPolicy, typename IteratorType>
[[nodiscard]] CompactionResult do_compact(
    IteratorType to_compact_start,
    IteratorType to_compact_end,
    const std::shared_ptr<pipelines::PipelineContext>& pipeline_context,
    std::vector<pipelines::FrameSlice>& slices,
    const std::shared_ptr<Store>& store,
    bool convert_int_to_float,
    std::optional<size_t> segment_size,
    bool validate_index,
    StaticSchemaCompactionChecks&& checks) {
    CompactionResult result;
    auto index = stream::index_type_from_descriptor(pipeline_context->descriptor());

    std::vector<folly::Future<VariantKey>> write_futures;

    stream::SegmentAggregator<IndexType, SchemaType, SegmentationPolicy, DensityPolicy>
        aggregator{
        [&slices](pipelines::FrameSlice &&slice) {
            slices.emplace_back(std::move(slice));
        },
        SchemaType{pipeline_context->descriptor(), index},
        [&write_futures, &store, &pipeline_context](SegmentInMemory &&segment) {
            auto local_index_start = IndexType::start_value_for_segment(segment);
            auto local_index_end = pipelines::end_index_generator(IndexType::end_value_for_segment(segment));
            stream::StreamSink::PartialKey
                pk{KeyType::TABLE_DATA, pipeline_context->version_id_, pipeline_context->stream_id_, local_index_start, local_index_end};

            // TODO We should apply back pressure to the work we are generating here, to bound memory use
            write_futures.emplace_back(store->write(pk, std::move(segment)));
        },
        segment_size.has_value() ? SegmentationPolicy{*segment_size} : SegmentationPolicy{}
    };

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

        if(validate_index && is_segment_unsorted(segment)) {
            auto written_keys = folly::collect(write_futures).get();
            remove_written_keys(store.get(), std::move(written_keys));
            return Error{throw_error<ErrorCode::E_UNSORTED_DATA>, "Cannot compact unordered segment"};
        }

        if constexpr (std::is_same_v<SchemaType, FixedSchema>) {
            CheckOutcome outcome = checks(segment.descriptor(), pipeline_context->descriptor());
            if (std::holds_alternative<Error>(outcome)) {
                auto written_keys = folly::collect(write_futures).get();
                remove_written_keys(store.get(), std::move(written_keys));
                return std::get<Error>(std::move(outcome));
            }
        }

        aggregator.add_segment(
            std::move(sk.segment(store)),
            sk.slice(),
            convert_int_to_float
        );
        sk.unset_segment();
    }

    aggregator.commit();
    return folly::collect(std::move(write_futures)).get();
}

CheckOutcome check_schema_matches_incomplete(const StreamDescriptor& stream_descriptor_incomplete, const StreamDescriptor& pipeline_context);

}

#define ARCTICDB_VERSION_CORE_H_
#include <arcticdb/version/version_core-inl.hpp>