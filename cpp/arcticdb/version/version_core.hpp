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
#include <arcticdb/entity/frame_and_descriptor.hpp>
#include <arcticdb/version/version_store_objects.hpp>

#include <string>

namespace arcticdb::version_store {

using namespace arcticdb::entity;
using namespace arcticdb::pipelines;

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
    bool validate_index);

VersionedItem append_impl(
    const std::shared_ptr<Store>& store,
    const UpdateInfo& update_info,
    const std::shared_ptr<InputTensorFrame>& frame,
    const WriteOptions& options,
    bool validate_index);

VersionedItem update_impl(
    const std::shared_ptr<Store>& store,
    const UpdateInfo& update_info,
    const UpdateQuery & query,
    const std::shared_ptr<InputTensorFrame>& frame,
    const WriteOptions&& options,
    bool dynamic_schema);

VersionedItem delete_range_impl(
    const std::shared_ptr<Store>& store,
    const AtomKey& prev,
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

FrameAndDescriptor read_multi_key(
    const std::shared_ptr<Store>& store,
    const SegmentInMemory& index_key_seg);

FrameAndDescriptor read_dataframe_impl(
    const std::shared_ptr<Store>& store,
    const std::variant<VersionedItem, StreamId>& version_info,
    ReadQuery & read_query,
    const ReadOptions& read_options
    );

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
    bool append,
    bool convert_int_to_float,
    bool via_iteration,
    bool sparsify,
    const WriteOptions& write_options);

struct PredefragmentationInfo{
    std::shared_ptr<PipelineContext> pipeline_context;
    ReadQuery read_query;
    size_t segments_need_compaction;
    std::optional<size_t> append_after;
};
SegmentInMemory prepare_output_frame(
    std::vector<SliceAndKey>&& items,
    const std::shared_ptr<PipelineContext>& pipeline_context,
    const std::shared_ptr<Store>& store,
    const ReadOptions& read_options);

SegmentInMemory do_direct_read_or_process(
    const std::shared_ptr<Store>& store,
    ReadQuery& read_query,
    const ReadOptions& read_options,
    const std::shared_ptr<PipelineContext>& pipeline_context,
    const std::shared_ptr<BufferHolder>& buffers);

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
    bool append,
    bool convert_int_to_float,
    bool via_iteration,
    bool sparsify
    );

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

} //namespace arcticdb::version_store

#define ARCTICDB_VERSION_CORE_H_
#include <arcticdb/version/version_core-inl.hpp>