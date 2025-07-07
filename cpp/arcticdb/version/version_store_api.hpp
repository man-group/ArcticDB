/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/data_error.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/stream/index.hpp>
#include <pybind11/pybind11.h>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/async/async_store.hpp>
#include <arcticdb/version/version_map.hpp>
#include <arcticdb/version/snapshot.hpp>
#include <arcticdb/version/symbol_list.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/read_pipeline.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/version/version_core.hpp>
#include <arcticdb/version/local_versioned_engine.hpp>
#include <arcticdb/entity/read_result.hpp>

namespace arcticdb::version_store {

using namespace arcticdb::entity;
namespace as = arcticdb::stream;

/**
 * The purpose of this class is to perform python-specific translations into either native C++ or protobuf objects
 * so that the LocalVersionedEngine contains only partable C++ code.
 */
class PythonVersionStore : public LocalVersionedEngine {

  public:
    template<class ClockType = util::SysClock>
    explicit PythonVersionStore(const std::shared_ptr<storage::Library>& library, const ClockType& ct = util::SysClock{}) :
        LocalVersionedEngine(library, ct) {
    }

    VersionedItem write_dataframe_specific_version(
        const StreamId& stream_id,
        const py::tuple& item,
        const py::object& norm,
        const py::object& user_meta,
        VersionId version_id);

    VersionedItem write_versioned_dataframe(
        const StreamId& stream_id,
        const py::tuple& item,
        const py::object& norm,
        const py::object& user_meta,
        bool prune_previous_versions,
        bool allow_sparse,
        bool validate_index);

    VersionedItem write_versioned_composite_data(
        const StreamId& stream_id,
        const py::object &metastruct,
        const std::vector<StreamId> &sub_keys,
        const std::vector<py::tuple> &items,
        const std::vector<py::object> &norm_metas,
        const py::object &user_meta,
        bool prune_previous_versions);

    VersionedItem write_partitioned_dataframe(
        const StreamId& stream_id,
        const py::tuple &item,
        const py::object &norm_meta,
        const std::vector<std::string>& partition_cols);

    ReadResult read_partitioned_dataframe(
        const StreamId& stream_id,
        const ReadQuery& query,
        const ReadOptions& read_options);

    VersionedItem append(
        const StreamId& stream_id,
        const py::tuple &item,
        const py::object &norm,
        const py::object & user_meta,
        bool upsert,
        bool prune_previous_versions,
        bool validate_index);

    VersionedItem update(
        const StreamId& stream_id,
        const UpdateQuery & query,
        const py::tuple &item,
        const py::object &norm,
        const py::object & user_meta,
        bool upsert,
        bool dynamic_schema,
        bool prune_previous_versions);

    VersionedItem delete_range(
        const StreamId& stream_id,
        const UpdateQuery& query,
        bool dynamic_schema,
        bool prune_previous_versions);

    void append_incomplete(
        const StreamId& stream_id,
        const py::tuple &item,
        const py::object &norm,
        const py::object & user_meta,
        bool validate_index) const;

    VersionedItem compact_incomplete(
            const StreamId& stream_id,
            bool append,
            bool convert_int_to_float,
            bool via_iteration = true,
            bool sparsify = false,
            const std::optional<py::object>& user_meta = std::nullopt,
            bool prune_previous_versions = false,
            bool validate_index = false,
            bool delete_staged_data_on_failure=false);

    StageResult write_parallel(
        const StreamId& stream_id,
        const py::tuple& item,
        const py::object& norm,
        bool validate_index,
        bool sort_on_index,
        std::optional<std::vector<std::string>> sort_columns) const;

    VersionedItem write_metadata(
        const StreamId& stream_id,
        const py::object & user_meta,
        bool prune_previous_versions);

    void create_column_stats_version(
        const StreamId& stream_id,
        ColumnStats& column_stats,
        const VersionQuery& version_query);

    void drop_column_stats_version(
        const StreamId& stream_id,
        const std::optional<ColumnStats>& column_stats_to_drop,
        const VersionQuery& version_query);

    ReadResult read_column_stats_version(
        const StreamId& stream_id,
        const VersionQuery& version_query,
        std::any& handler_data);

    ColumnStats get_column_stats_info_version(
        const StreamId& stream_id,
        const VersionQuery& version_query);

    ReadResult read_dataframe_version(
        const StreamId &stream_id,
        const VersionQuery& version_query,
        const std::shared_ptr<ReadQuery>& read_query,
        const ReadOptions& read_options,
        std::any& handler_data);

    VersionedItem sort_merge(
            const StreamId& stream_id,
            const py::object& user_meta,
            bool append,
            bool convert_int_to_float,
            bool via_iteration,
            bool sparsify,
            bool prune_previous_versions,
            bool delete_staged_data_on_failure);

    std::pair<VersionedItem, py::object> read_metadata(
        const StreamId& stream_id,
        const VersionQuery& version_query
    );

    std::vector<std::variant<DescriptorItem, DataError>> batch_read_descriptor(
        const std::vector<StreamId>& stream_ids,
        const std::vector<VersionQuery>& version_queries,
        const ReadOptions& read_options);

    DescriptorItem read_descriptor(
        const StreamId& stream_id,
        const VersionQuery& version_query);

    ReadResult read_index(
        const StreamId& stream_id,
        const VersionQuery& version_query,
        OutputFormat output_format,
        std::any& handler_data);

    void delete_snapshot(
        const SnapshotId& snap_name);

    void delete_version(
        const StreamId& stream_id,
        VersionId version_id);

    void delete_versions(
        const StreamId& stream_id,
        const std::vector<VersionId>& version_ids);

    std::vector<std::optional<DataError>> batch_delete(
        const std::vector<StreamId>& stream_ids,
        const std::vector<std::vector<VersionId>>& version_ids);

    void prune_previous_versions(
        const StreamId& stream_id);

    void delete_all_versions(
        const StreamId& stream_id);

    std::vector<timestamp> get_update_times(
        const std::vector<StreamId>& stream_ids,
        const std::vector<VersionQuery>& version_queries);

    timestamp get_update_time(
        const StreamId& stream_id,
        const VersionQuery& version_query);

    inline void fix_ref_key(StreamId stream_id) {
        version_map()->fix_ref_key(store(), std::move(stream_id));
    }

    inline void remove_and_rewrite_version_keys(StreamId stream_id) {
        version_map()->remove_and_rewrite_version_keys(store(), std::move(stream_id));
    }

    inline bool check_ref_key(StreamId stream_id) {
        return version_map()->check_ref_key(store(), std::move(stream_id));
    }

    inline bool indexes_sorted(const StreamId& stream_id) {
        return version_map()->indexes_sorted(store(), stream_id);
    }

    void verify_snapshot(const SnapshotId& snap_name);

    void snapshot(
        const SnapshotId &snap_name,
        const py::object &user_meta,
        const std::vector<StreamId> &skip_symbols,
        std::map<StreamId, VersionId> &versions,
        bool allow_partial_snapshot);

    std::vector<std::pair<SnapshotId, py::object>> list_snapshots(const std::optional<bool> load_metadata);

    void add_to_snapshot(
        const SnapshotId& snap_name,
        const std::vector<StreamId>& stream_ids,
        const std::vector<VersionQuery>& version_queries
        );

    void remove_from_snapshot(
        const SnapshotId& snap_name,
        const std::vector<StreamId>& stream_ids,
        const std::vector<VersionId>& version_ids
        );

    std::vector<std::tuple<StreamId, VersionId, timestamp, std::vector<SnapshotId>, bool>> list_versions(
        const std::optional<StreamId> &stream_id,
        const std::optional<SnapshotId>& snap_name,
        const std::optional<bool> &latest_only,
        const std::optional<bool>& skip_snapshots);

    // Batch methods
    std::vector<VersionedItemOrError> batch_write(
        const std::vector<StreamId> &stream_ids,
        const std::vector<py::tuple> &items,
        const std::vector<py::object> &norms,
        const std::vector<py::object> &user_metas,
        bool prune_previous_versions,
        bool validate_index,
        bool throw_on_error);

    std::vector<VersionedItemOrError> batch_write_metadata(
        const std::vector<StreamId>& stream_ids,
        const std::vector<py::object>& user_meta,
        bool prune_previous_versions,
        bool throw_on_error);

    std::vector<VersionedItemOrError> batch_append(
        const std::vector<StreamId> &stream_ids,
        const std::vector<py::tuple> &items,
        const std::vector<py::object> &norms,
        const std::vector<py::object> &user_metas,
        bool prune_previous_versions,
        bool validate_index,
        bool upsert,
        bool throw_on_error);

    std::vector<std::pair<VersionedItem, TimeseriesDescriptor>> batch_restore_version(
        const std::vector<StreamId>& id,
        const std::vector<VersionQuery>& version_query);

    std::vector<std::variant<ReadResult, DataError>> batch_read(
        const std::vector<StreamId>& stream_ids,
        const std::vector<VersionQuery>& version_queries,
        std::vector<std::shared_ptr<ReadQuery>>& read_queries,
        const ReadOptions& read_options,
        std::any& handler_data);

    std::vector<VersionedItemOrError> batch_update(
        const std::vector<StreamId>& stream_ids,
        const std::vector<py::tuple>& items,
        const std::vector<py::object>& norms,
        const std::vector<py::object>& user_metas,
        const std::vector<UpdateQuery>& update_qeries,
        bool prune_previous_versions,
        bool upsert);

    ReadResult batch_read_and_join(
            const std::vector<StreamId>& stream_ids,
            const std::vector<VersionQuery>& version_queries,
            std::vector<std::shared_ptr<ReadQuery>>& read_queries,
            const ReadOptions& read_options,
            std::vector<std::shared_ptr<Clause>>&& clauses,
            std::any& handler_data);

    std::vector<std::variant<std::pair<VersionedItem, py::object>, DataError>> batch_read_metadata(
        const std::vector<StreamId>& stream_ids,
        const std::vector<VersionQuery>& version_queries,
        const ReadOptions& read_options);

    std::set<StreamId> list_streams(
        const std::optional<SnapshotId>& snap_name = std::nullopt,
        const std::optional<std::string> &regex = std::nullopt,
        const std::optional<std::string> &prefix = std::nullopt,
        const std::optional<bool>& use_symbol_list = std::nullopt,
        const std::optional<bool>& all_symbols = std::nullopt);

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

    std::unordered_map<VersionId, bool> get_all_tombstoned_versions(const StreamId &stream_id);

    std::vector<SliceAndKey> list_incompletes(const StreamId& stream_id);

    std::vector<AtomKey> get_version_history(const StreamId& stream_id);


private:
    void delete_snapshot_sync(const SnapshotId& snap_name, const VariantKey& snap_key);
};

void write_dataframe_to_file(
    const StreamId& stream_id,
    const std::string& path,
    const py::tuple& item,
    const py::object& norm,
    const py::object& user_meta);

ReadResult read_dataframe_from_file(
    const StreamId &stream_id,
    const std::string& path,
    const std::shared_ptr<ReadQuery>& read_query,
    const ReadOptions& read_options,
    std::any& handler_data);

struct ManualClockVersionStore : PythonVersionStore {
    ManualClockVersionStore(const std::shared_ptr<storage::Library>& library) :
            PythonVersionStore(library, util::ManualClock{}) {}
};

inline std::vector<std::variant<ReadResult, DataError>> frame_to_read_result(std::vector<ReadVersionOutput>&& keys_frame_and_descriptors) {
    std::vector<std::variant<ReadResult, DataError>> read_results;
    read_results.reserve(keys_frame_and_descriptors.size());
    for (auto& read_version_output : keys_frame_and_descriptors) {
        read_results.emplace_back(create_python_read_result(read_version_output.versioned_item_, OutputFormat::PANDAS, std::move(read_version_output.frame_and_descriptor_)));
    }
    return read_results;
}

} //namespace arcticdb::version_store
