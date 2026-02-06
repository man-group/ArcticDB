/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/schema_item.hpp>
#include <arcticdb/version/version_map.hpp>
#include <arcticdb/async/async_store.hpp>
#include <arcticdb/version/symbol_list.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/stage_result.hpp>
#include <arcticdb/pipeline/write_options.hpp>
#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/util/storage_lock.hpp>
#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/version/version_core.hpp>
#include <arcticdb/version/version_store_objects.hpp>
#include <arcticdb/version/merge_options.hpp>

namespace arcticdb::version_store {

using namespace entity;
using namespace pipelines;

struct DeleteRangeOptions {
    bool dynamic_schema_;
    bool prune_previous_versions_;
};

enum class Slicing { NoSlicing, RowSlicing };

/**
 * The VersionedEngine interface contains methods that are portable between languages.
 *
 * It is currently implemented by LocalVersionedEngine, but could also be implemented with a RemoteVersionedEngine that
 * sends Protobufs down a wire (to a server with a LocalVersionedEngine talking directly to the storage).
 */
class VersionedEngine {

  public:
    virtual ~VersionedEngine() = default;

    virtual VersionedItem update_internal(
            const StreamId& stream_id, const UpdateQuery& query, const std::shared_ptr<InputFrame>& frame, bool upsert,
            bool dynamic_schema, bool prune_previous_versions
    ) = 0;

    virtual VersionedItem append_internal(
            const StreamId& stream_id, const std::shared_ptr<InputFrame>& frame, bool upsert,
            bool prune_previous_versions, bool validate_index
    ) = 0;

    virtual VersionedItem delete_range_internal(
            const StreamId& stream_id, const UpdateQuery& query, const DeleteRangeOptions& option
    ) = 0;

    virtual VersionedItem sort_index(
            const StreamId& stream_id, bool dynamic_schema, bool prune_previous_versions = false
    ) = 0;

    virtual void append_incomplete_frame(
            const StreamId& stream_id, const std::shared_ptr<InputFrame>& frame, bool validate_index
    ) const = 0;

    virtual void append_incomplete_segment(const StreamId& stream_id, SegmentInMemory&& seg) = 0;

    virtual void remove_incomplete(const StreamId& stream_id) = 0;

    virtual StageResult write_parallel_frame(
            const StreamId& stream_id, const std::shared_ptr<InputFrame>& frame, bool validate_index,
            bool sort_on_index, const std::optional<std::vector<std::string>>& sort_columns
    ) const = 0;

    /**
     * Delete the given index keys, and their associated data excluding those shared with keys not in the argument.
     *
     * @param checks Checks to perform on each key to find shared data
     */
    virtual void delete_tree(const std::vector<IndexTypeKey>& idx_to_be_deleted, const PreDeleteChecks& checks) = 0;

    virtual std::pair<VersionedItem, TimeseriesDescriptor> restore_version(
            const StreamId& id, const VersionQuery& version_query
    ) = 0;

    virtual ReadVersionWithNodesOutput read_dataframe_version_internal(
            const StreamId& stream_id, const VersionQuery& version_query, const std::shared_ptr<ReadQuery>& read_query,
            const ReadOptions& read_options, std::any& handler_data
    ) = 0;

    virtual SchemaItem read_schema_internal(
            const StreamId& stream_id, const VersionQuery& version_query, const ReadOptions& read_options,
            const std::shared_ptr<ReadQuery>& read_query
    ) = 0;

    virtual VersionedItem read_modify_write_internal(
            const StreamId& stream_id, const StreamId& target_stream, const VersionQuery& version_query,
            const std::shared_ptr<ReadQuery>& read_query, const ReadOptions& read_options, bool prune_previous_versions,
            std::optional<proto::descriptors::UserDefinedMetadata>&& user_meta_proto
    ) = 0;

    virtual VersionedItem write_versioned_dataframe_internal(
            const StreamId& stream_id, const std::shared_ptr<InputFrame>& frame, bool prune_previous_versions,
            bool allow_sparse, bool validate_index
    ) = 0;

    virtual VersionedItem write_segment(
            const StreamId& stream_id, SegmentInMemory&& segment, bool prune_previous_versions, Slicing slicing
    ) = 0;

    virtual std::set<StreamId> list_streams_internal(
            std::optional<SnapshotId> snap_name, const std::optional<std::string>& regex,
            const std::optional<std::string>& prefix, const std::optional<bool>& opt_use_symbol_list,
            const std::optional<bool>& opt_all_symbols
    ) = 0;

    virtual std::unordered_set<StreamId> list_streams_unordered_internal(
            std::optional<SnapshotId> snap_name, const std::optional<std::string>& regex,
            const std::optional<std::string>& prefix, const std::optional<bool>& opt_use_symbol_list,
            const std::optional<bool>& opt_all_symbols
    ) = 0;

    virtual size_t compact_symbol_list_internal() = 0;

    virtual std::variant<VersionedItem, CompactionError> compact_incomplete_dynamic(
            const StreamId& stream_id,
            const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta,
            const CompactIncompleteParameters& parameters
    ) = 0;

    virtual IndexRange get_index_range(const StreamId& stream_id, const VersionQuery& version_query) = 0;

    virtual std::set<StreamId> get_incomplete_symbols() = 0;

    virtual std::set<StreamId> get_incomplete_refs() = 0;

    virtual std::set<StreamId> get_active_incomplete_refs() = 0;

    virtual bool is_symbol_fragmented(const StreamId& stream_id, std::optional<size_t> segment_size) = 0;

    virtual VersionedItem defragment_symbol_data(
            const StreamId& stream_id, std::optional<size_t> segment_size, bool prune_previous_versions
    ) = 0;

    virtual void move_storage(KeyType key_type, timestamp horizon, size_t storage_index) = 0;

    virtual StorageLockWrapper get_storage_lock(const StreamId& stream_id) = 0;

    virtual void delete_storage(const bool continue_on_error = true) = 0;

    virtual void configure(const storage::LibraryDescriptor::VariantStoreConfig& cfg) = 0;

    [[nodiscard]] virtual WriteOptions get_write_options() const = 0;

    virtual std::shared_ptr<Store>& store() = 0;
    [[nodiscard]] virtual const arcticdb::proto::storage::VersionStoreConfig& cfg() const = 0;
    virtual std::shared_ptr<VersionMap>& version_map() = 0;
    virtual SymbolList& symbol_list() = 0;
    virtual void set_store(std::shared_ptr<Store> store) = 0;
    virtual timestamp latest_timestamp(const std::string& symbol) = 0;
    virtual void flush_version_map() = 0;

    virtual VersionedItem merge_internal(
            const StreamId& stream_id, std::shared_ptr<InputFrame> source, bool prune_previous_versions, bool upsert,
            const MergeStrategy& strategy, std::vector<std::string>&& on
    ) = 0;
};

} // namespace arcticdb::version_store
