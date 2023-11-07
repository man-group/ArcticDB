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
#include <arcticdb/util/timeouts.hpp>
#include <arcticdb/util/variant.hpp>
#include <pybind11/pybind11.h>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/async/async_store.hpp>
#include <arcticdb/version/version_map.hpp>
#include <arcticdb/version/snapshot.hpp>
#include <arcticdb/version/symbol_list.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/write_options.hpp>
#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/slicing.hpp>
#include <arcticdb/pipeline/read_pipeline.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/stream/append_map.hpp>
#include <arcticdb/version/version_core.hpp>
#include <arcticdb/version/local_versioned_engine.hpp>
#include <arcticdb/entity/read_result.hpp>
#include <arcticdb/version/version_log.hpp>

#include <folly/futures/Barrier.h>

#include <type_traits>
#include <iostream>

namespace arcticdb::version_store {

using namespace arcticdb::entity;
namespace as = arcticdb::stream;

/**
 * PythonVersionStore contains all the Pythonic cruft that isn't portable, as well as non-essential features that are
 * part of the backwards-compatibility with Arctic Python but that we think are random/a bad idea and aren't part of
 * the main product.
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
        const py::tuple &item,
        const py::object &norm,
        const py::object & user_meta,
        bool prune_previous_versions,
        bool allow_sparse,
        bool validate_index);

    VersionedItem write_versioned_composite_data(
        const StreamId& stream_id,
        const py::object &metastruct,
        const std::vector<StreamId> &sub_keys,  // TODO: make this optional?
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
        bool dynamic_schema);

    void append_incomplete(
        const StreamId& stream_id,
        const py::tuple &item,
        const py::object &norm,
        const py::object & user_meta) const;

    VersionedItem compact_incomplete(
            const StreamId& stream_id,
            bool append,
            bool convert_int_to_float,
            bool via_iteration = true,
            bool sparsify = false,
            const std::optional<py::object>& user_meta = std::nullopt,
            bool prune_previous_versions = false);

    void write_parallel(
        const StreamId& stream_id,
        const py::tuple &item,
        const py::object &norm,
        const py::object & user_meta) const;

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
        const VersionQuery& version_query);

    ColumnStats get_column_stats_info_version(
        const StreamId& stream_id,
        const VersionQuery& version_query);

    ReadResult read_dataframe_version(
        const StreamId &stream_id,
        const VersionQuery& version_query,
        ReadQuery& read_query,
        const ReadOptions& read_options);

    VersionedItem sort_merge(
            const StreamId& stream_id,
            const py::object& user_meta,
            bool append,
            bool convert_int_to_float,
            bool via_iteration,
            bool sparsify
            );

    ReadResult read_dataframe_merged(
        const StreamId& target_id,
        const std::vector<StreamId> &stream_ids,
        const VersionQuery& version_query,
        const ReadQuery &query,
        const ReadOptions& read_options);

    std::pair<VersionedItem, py::object> read_metadata(
        const StreamId& stream_id,
        const VersionQuery& version_query,
        const ReadOptions& read_options
    );

    std::vector<std::variant<DescriptorItem, DataError>> batch_read_descriptor(
        const std::vector<StreamId>& stream_ids,
        const std::vector<VersionQuery>& version_queries,
        const ReadOptions& read_options);

    DescriptorItem read_descriptor(
        const StreamId& stream_id,
        const VersionQuery& version_query,
        const ReadOptions& read_options);

    ReadResult read_index(
        const StreamId& stream_id,
        const VersionQuery& version_query);

    void delete_snapshot(
        const SnapshotId& snap_name);

    void delete_version(
        const StreamId& stream_id,
        const VersionId& version_id);

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

    void snapshot(
        const SnapshotId &snap_name,
        const py::object &user_meta,
        const std::vector<StreamId> &skip_symbols,
        std::map<StreamId, VersionId> &versions);

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
        const std::optional<bool>& iterate_on_failure,
        const std::optional<bool>& skip_snapshots);

    // Batch methods
    std::vector<std::variant<VersionedItem, DataError>> batch_write(
        const std::vector<StreamId> &stream_ids,
        const std::vector<py::tuple> &items,
        const std::vector<py::object> &norms,
        const std::vector<py::object> &user_metas,
        bool prune_previous_versions,
        bool validate_index,
        bool throw_on_error);

    std::vector<std::variant<VersionedItem, DataError>> batch_write_metadata(
        const std::vector<StreamId>& stream_ids,
        const std::vector<py::object>& user_meta,
        bool prune_previous_versions,
        bool throw_on_error);

    std::vector<std::variant<VersionedItem, DataError>> batch_append(
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
        std::vector<ReadQuery>& read_queries,
        const ReadOptions& read_options);

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

    void clear();
    bool empty();
    void force_delete_symbol(const StreamId& stream_id);

    void _clear_symbol_list_keys();
    void reload_symbol_list();
    void _compact_version_map(const StreamId& id);
    void compact_library(size_t batch_size);

    void fix_symbol_trees(const std::vector<StreamId>& symbols);

    /**
     * Main business logic of the DeleteTombstonedData background job. Delete tombstoned versions and snapshots.
     * @param limit_stream_id Test-specific. If non-empty, limit scope to tombstoned versions in the given stream.
     * @param min_age_sec Minimum age of index keys that can be deleted in unit of seconds.
     * @param stresser_sync Only used by stress tests to synchronise steps.
     */
    void delete_tombstones(const std::string& limit_stream_id, bool dry_run, uint64_t min_age_sec,
            folly::futures::Barrier* stresser_sync = nullptr, size_t batch_size = 2000);

    std::unordered_map<VersionId, bool> get_all_tombstoned_versions(const StreamId &stream_id);

    std::vector<SliceAndKey> list_incompletes(const StreamId& stream_id);

    std::vector<AtomKey> get_version_history(const StreamId& stream_id);

private:

    std::vector<VersionedItem> batch_write_index_keys_to_version_map(
        const std::vector<AtomKey>& index_keys,
        const std::vector<UpdateInfo>& stream_update_info_vector,
        bool prune_previous_versions);

    void delete_snapshot_sync(const SnapshotId& snap_name, const VariantKey& snap_key);
};

struct ManualClockVersionStore : PythonVersionStore {
    ManualClockVersionStore(const std::shared_ptr<storage::Library>& library) :
            PythonVersionStore(library, util::ManualClock{}) {}
};

inline std::vector<std::variant<ReadResult, DataError>> frame_to_read_result(std::vector<ReadVersionOutput>&& keys_frame_and_descriptors) {
    std::vector<std::variant<ReadResult, DataError>> read_results;
    read_results.reserve(keys_frame_and_descriptors.size());
    for (auto& read_version_output : keys_frame_and_descriptors) {
        read_results.emplace_back(ReadResult(
            read_version_output.versioned_item_,
            PythonOutputFrame{read_version_output.frame_and_descriptor_.frame_, read_version_output.frame_and_descriptor_.buffers_},
            read_version_output.frame_and_descriptor_.desc_.proto().normalization(),
            read_version_output.frame_and_descriptor_.desc_.proto().user_meta(),
            read_version_output.frame_and_descriptor_.desc_.proto().multi_key_meta(),
            std::vector<AtomKey>{}));
    }
    return read_results;
}

} //namespace arcticdb::version_store
