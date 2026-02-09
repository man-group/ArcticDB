/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/data_error.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/version/version_map.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/version/version_core.hpp>
#include <arcticdb/version/local_versioned_engine.hpp>
#include <arcticdb/entity/read_result.hpp>
#include <arcticdb/arrow/arrow_output_frame.hpp>

namespace arcticdb::version_store {

using namespace arcticdb::entity;
namespace as = arcticdb::stream;

/**
 * The purpose of this class is to perform python-specific translations into either native C++ or protobuf objects
 * so that the LocalVersionedEngine contains only portable C++ code.
 */
class PythonVersionStore : public LocalVersionedEngine {

  public:
    template<class ClockType = util::SysClock>
    explicit PythonVersionStore(
            const std::shared_ptr<storage::Library>& library, const ClockType& ct = util::SysClock{}
    ) :
        LocalVersionedEngine(library, ct) {}

    template<class ClockType = util::SysClock>
    explicit PythonVersionStore(const std::shared_ptr<Store>& store, const ClockType& ct = util::SysClock{}) :
        LocalVersionedEngine(store, ct) {}

    VersionedItem write_dataframe_specific_version(
            const StreamId& stream_id, const py::tuple& item, const py::object& norm, const py::object& user_meta,
            VersionId version_id
    );

    VersionedItem write_versioned_dataframe(
            const StreamId& stream_id, const convert::InputItem& item, const py::object& norm,
            const py::object& user_meta, bool prune_previous_versions, bool allow_sparse, bool validate_index
    );

    VersionedItem test_write_versioned_segment(
            const StreamId& stream_id, SegmentInMemory& segment, bool prune_previous_versions, Slicing slicing
    );

    VersionedItem write_versioned_composite_data(
            const StreamId& stream_id, const py::object& metastruct, const std::vector<StreamId>& sub_keys,
            const std::vector<convert::InputItem>& items, const std::vector<py::object>& norm_metas,
            const py::object& user_meta, bool prune_previous_versions
    );

    VersionedItem write_partitioned_dataframe(
            const StreamId& stream_id, const py::tuple& item, const py::object& norm_meta,
            const std::vector<std::string>& partition_cols
    );

    ReadResult read_partitioned_dataframe(
            const StreamId& stream_id, const ReadQuery& query, const ReadOptions& read_options
    );

    VersionedItem append(
            const StreamId& stream_id, const convert::InputItem& item, const py::object& norm,
            const py::object& user_meta, bool upsert, bool prune_previous_versions, bool validate_index
    );

    VersionedItem update(
            const StreamId& stream_id, const UpdateQuery& query, const convert::InputItem& item, const py::object& norm,
            const py::object& user_meta, bool upsert, bool dynamic_schema, bool prune_previous_versions
    );

    VersionedItem delete_range(
            const StreamId& stream_id, const UpdateQuery& query, bool dynamic_schema, bool prune_previous_versions
    );

    void append_incomplete(
            const StreamId& stream_id, const py::tuple& item, const py::object& norm, const py::object& user_meta,
            bool validate_index
    ) const;

    std::variant<VersionedItem, CompactionError> compact_incomplete(
            const StreamId& stream_id, bool append, bool convert_int_to_float, bool via_iteration = true,
            bool sparsify = false, const std::optional<py::object>& user_meta = std::nullopt,
            bool prune_previous_versions = false, bool validate_index = false,
            bool delete_staged_data_on_failure = false,
            const std::optional<std::vector<StageResult>>& stage_results = std::nullopt
    );

    StageResult write_parallel(
            const StreamId& stream_id, const convert::InputItem& item, const py::object& norm, bool validate_index,
            bool sort_on_index, std::optional<std::vector<std::string>> sort_columns
    ) const;

    VersionedItem write_metadata(const StreamId& stream_id, const py::object& user_meta, bool prune_previous_versions);

    void create_column_stats_version(
            const StreamId& stream_id, ColumnStats& column_stats, const VersionQuery& version_query
    );

    void drop_column_stats_version(
            const StreamId& stream_id, const std::optional<ColumnStats>& column_stats_to_drop,
            const VersionQuery& version_query
    );

    ReadResult read_column_stats_version(
            const StreamId& stream_id, const VersionQuery& version_query, std::any& handler_data
    );

    ColumnStats get_column_stats_info_version(const StreamId& stream_id, const VersionQuery& version_query);

    ReadResult read_dataframe_version(
            const StreamId& stream_id, const VersionQuery& version_query, const std::shared_ptr<ReadQuery>& read_query,
            const ReadOptions& read_options, std::any& handler_data
    );

    // Creates a lazy record batch iterator that reads segments on-demand from storage.
    // Only reads the index (segment metadata) upfront; actual segment data is fetched
    // incrementally as next() is called, with a configurable prefetch buffer.
    std::shared_ptr<LazyRecordBatchIterator> create_lazy_record_batch_iterator(
            const StreamId& stream_id, const VersionQuery& version_query, const std::shared_ptr<ReadQuery>& read_query,
            const ReadOptions& read_options, size_t prefetch_size = 2
    );

    VersionedItem read_modify_write(
            const StreamId& stream_id, const StreamId& target_stream, const py::object& user_meta,
            const VersionQuery& version_query, const std::shared_ptr<ReadQuery>& read_query,
            const ReadOptions& read_options, bool prune_previous_versions
    );

    std::variant<VersionedItem, CompactionError> sort_merge(
            const StreamId& stream_id, const py::object& user_meta, bool append, bool convert_int_to_float,
            bool via_iteration, bool sparsify, bool prune_previous_versions, bool delete_staged_data_on_failure,
            const std::optional<std::vector<StageResult>>& stage_results = std::nullopt
    );

    std::pair<VersionedItem, py::object> read_metadata(const StreamId& stream_id, const VersionQuery& version_query);

    std::vector<std::variant<DescriptorItem, DataError>> batch_read_descriptor(
            const std::vector<StreamId>& stream_ids, const std::vector<VersionQuery>& version_queries,
            const BatchReadOptions& batch_read_options
    );

    DescriptorItem read_descriptor(const StreamId& stream_id, const VersionQuery& version_query);

    ReadResult read_index(
            const StreamId& stream_id, const VersionQuery& version_query, OutputFormat output_format,
            std::any& handler_data
    );

    void delete_snapshot(const SnapshotId& snap_name);

    void delete_version(const StreamId& stream_id, VersionId version_id);

    void delete_versions(const StreamId& stream_id, const std::vector<VersionId>& version_ids);

    std::vector<std::optional<DataError>> batch_delete(
            const std::vector<StreamId>& stream_ids, const std::vector<std::vector<VersionId>>& version_ids
    );

    void prune_previous_versions(const StreamId& stream_id);

    void delete_all_versions(const StreamId& stream_id);

    std::vector<timestamp> get_update_times(
            const std::vector<StreamId>& stream_ids, const std::vector<VersionQuery>& version_queries
    );

    timestamp get_update_time(const StreamId& stream_id, const VersionQuery& version_query);

    void fix_ref_key(StreamId stream_id) { version_map()->fix_ref_key(store(), std::move(stream_id)); }

    void remove_and_rewrite_version_keys(StreamId stream_id) {
        version_map()->remove_and_rewrite_version_keys(store(), std::move(stream_id));
    }

    bool check_ref_key(StreamId stream_id) { return version_map()->check_ref_key(store(), std::move(stream_id)); }

    bool indexes_sorted(const StreamId& stream_id) { return version_map()->indexes_sorted(store(), stream_id); }

    void verify_snapshot(const SnapshotId& snap_name);

    void snapshot(
            const SnapshotId& snap_name, const py::object& user_meta, const std::vector<StreamId>& skip_symbols,
            std::map<StreamId, VersionId>& versions, bool allow_partial_snapshot
    );

    std::vector<std::pair<SnapshotId, py::object>> list_snapshots(const std::optional<bool> load_metadata);

    void add_to_snapshot(
            const SnapshotId& snap_name, const std::vector<StreamId>& stream_ids,
            const std::vector<VersionQuery>& version_queries
    );

    void remove_from_snapshot(
            const SnapshotId& snap_name, const std::vector<StreamId>& stream_ids,
            const std::vector<VersionId>& version_ids
    );

    std::vector<std::tuple<StreamId, VersionId, timestamp, std::vector<SnapshotId>, bool>> list_versions(
            const std::optional<StreamId>& stream_id, const std::optional<SnapshotId>& snap_name, bool latest_only,
            bool skip_snapshots
    );

    // Batch methods
    std::vector<VersionedItemOrError> batch_write(
            const std::vector<StreamId>& stream_ids, const std::vector<convert::InputItem>& items,
            const std::vector<py::object>& norms, const std::vector<py::object>& user_metas,
            bool prune_previous_versions, bool validate_index, bool throw_on_error
    );

    std::vector<VersionedItemOrError> batch_write_metadata(
            const std::vector<StreamId>& stream_ids, const std::vector<py::object>& user_meta,
            bool prune_previous_versions, bool throw_on_error
    );

    std::vector<VersionedItemOrError> batch_append(
            const std::vector<StreamId>& stream_ids, const std::vector<convert::InputItem>& items,
            const std::vector<py::object>& norms, const std::vector<py::object>& user_metas,
            bool prune_previous_versions, bool validate_index, bool upsert, bool throw_on_error
    );

    std::vector<std::pair<VersionedItem, TimeseriesDescriptor>> batch_restore_version(
            const std::vector<StreamId>& id, const std::vector<VersionQuery>& version_query
    );

    std::vector<std::variant<ReadResult, DataError>> batch_read(
            const std::vector<StreamId>& stream_ids, const std::vector<VersionQuery>& version_queries,
            std::vector<std::shared_ptr<ReadQuery>>& read_queries, const BatchReadOptions& batch_read_options,
            std::any& handler_data
    );

    std::vector<VersionedItemOrError> batch_update(
            const std::vector<StreamId>& stream_ids, const std::vector<convert::InputItem>& items,
            const std::vector<py::object>& norms, const std::vector<py::object>& user_metas,
            const std::vector<UpdateQuery>& update_qeries, bool prune_previous_versions, bool upsert
    );

    ReadResult batch_read_and_join(
            std::shared_ptr<std::vector<StreamId>> stream_ids,
            std::shared_ptr<std::vector<VersionQuery>> version_queries,
            std::vector<std::shared_ptr<ReadQuery>>& read_queries, const ReadOptions& read_options,
            std::vector<std::shared_ptr<Clause>>&& clauses, std::any& handler_data
    );

    std::vector<std::variant<std::pair<VersionedItem, py::object>, DataError>> batch_read_metadata(
            const std::vector<StreamId>& stream_ids, const std::vector<VersionQuery>& version_queries,
            const BatchReadOptions& batch_read_options
    );

    std::set<StreamId> list_streams(
            const std::optional<SnapshotId>& snap_name = std::nullopt,
            const std::optional<std::string>& regex = std::nullopt,
            const std::optional<std::string>& prefix = std::nullopt,
            const std::optional<bool>& use_symbol_list = std::nullopt,
            const std::optional<bool>& all_symbols = std::nullopt
    );

    std::unordered_set<StreamId> list_streams_unordered(
            const std::optional<SnapshotId>& snap_name = std::nullopt,
            const std::optional<std::string>& regex = std::nullopt,
            const std::optional<std::string>& prefix = std::nullopt,
            const std::optional<bool>& use_symbol_list = std::nullopt,
            const std::optional<bool>& all_symbols = std::nullopt
    );

    size_t compact_symbol_list();

    void clear(const bool continue_on_error = true);

    bool empty();

    bool is_empty_excluding_key_types(const std::vector<KeyType>& excluded_key_types);

    void force_delete_symbol(const StreamId& stream_id);

    void _clear_symbol_list_keys();
    void reload_symbol_list();
    void _compact_version_map(const StreamId& id);
    void compact_library(size_t batch_size);

    void fix_symbol_trees(const std::vector<StreamId>& symbols);

    std::unordered_map<VersionId, bool> get_all_tombstoned_versions(const StreamId& stream_id);

    std::vector<SliceAndKey> list_incompletes(const StreamId& stream_id);

    std::vector<AtomKey> get_version_history(const StreamId& stream_id);

    VersionedItem merge(
            const StreamId& stream_id, const py::tuple& source, const py::object& norm, const py::object& user_meta,
            const bool prune_previous_versions, const bool upsert, const py::tuple& py_strategy,
            std::vector<std::string> on
    );

  private:
    void delete_snapshot_sync(const SnapshotId& snap_name, const VariantKey& snap_key);
};

void write_dataframe_to_file(
        const StreamId& stream_id, const std::string& path, const py::tuple& item, const py::object& norm,
        const py::object& user_meta
);

ReadResult read_dataframe_from_file(
        const StreamId& stream_id, const std::string& path, const std::shared_ptr<ReadQuery>& read_query,
        const ReadOptions& read_options, std::any& handler_data
);

struct ManualClockVersionStore : PythonVersionStore {
    ManualClockVersionStore(const std::shared_ptr<storage::Library>& library) :
        PythonVersionStore(library, util::ManualClock{}) {}
};

} // namespace arcticdb::version_store
