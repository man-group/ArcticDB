/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/util/error_code.hpp>
#include <pybind11/pybind11.h>
#include <arcticdb/entity/data_error.hpp>
#include <arcticdb/entity/protobuf_mappings.hpp>
#include <arcticdb/version/version_store_api.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/processing/query_planner.hpp>
#include <arcticdb/pipeline/value_set.hpp>
#include <arcticdb/util/pybind_mutex.hpp>
#include <arcticdb/entity/python_bindings_common.hpp>
#include <arcticdb/python/adapt_read_dataframe.hpp>

namespace arcticdb::version_store {
void register_python_version_store(py::module& version) {

    py::class_<PythonVersionStore>(version, "PythonVersionStore")
            .def(py::init([](const std::shared_ptr<storage::Library>& library, std::optional<std::string>) {
                     return PythonVersionStore(library);
                 }),
                 py::arg("library"),
                 py::arg("license_key") = std::nullopt)
            .def("write_partitioned_dataframe",
                 &PythonVersionStore::write_partitioned_dataframe,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Write a dataframe to the store")
            .def("delete_snapshot",
                 &PythonVersionStore::delete_snapshot,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Delete snapshot from store")
            .def("delete",
                 &PythonVersionStore::delete_all_versions,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Delete all versions of the given symbol")
            .def("delete_range",
                 &PythonVersionStore::delete_range,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Delete the date range from the symbol")
            .def("delete_version",
                 &PythonVersionStore::delete_version,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Delete specific version of the given symbol")
            .def("delete_versions",
                 &PythonVersionStore::delete_versions,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Delete specific versions of the given symbol")
            .def("batch_delete",
                 &PythonVersionStore::batch_delete,
                 py::arg("stream_ids"),
                 py::arg("version_ids"),
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Delete specific versions of the given symbols")
            .def("prune_previous_versions",
                 &PythonVersionStore::prune_previous_versions,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Delete all but the latest version of the given symbol")
            .def("sort_index",
                 &PythonVersionStore::sort_index,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Sort the index of a time series whose segments are internally sorted")
            .def("append",
                 &PythonVersionStore::append,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Append a dataframe to the most recent version")
            .def("merge",
                 &PythonVersionStore::merge,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Merge a dataframe into the most recent version")
            .def("append_incomplete",
                 &PythonVersionStore::append_incomplete,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Append a partial dataframe to the most recent version")
            .def("write_parallel",
                 &PythonVersionStore::write_parallel,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Append to a symbol in parallel")
            .def("write_metadata",
                 &PythonVersionStore::write_metadata,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Create a new version with new metadata and data from the last version")
            .def("create_column_stats_version",
                 &PythonVersionStore::create_column_stats_version,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Create column stats")
            .def("drop_column_stats_version",
                 &PythonVersionStore::drop_column_stats_version,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Drop column stats")
            .def(
                    "read_column_stats_version",
                    [&](PythonVersionStore& v, StreamId sid, const VersionQuery& version_query) {
                        auto handler_data = TypeHandlerRegistry::instance()->get_handler_data(OutputFormat::PANDAS);
                        return adapt_read_df(
                                v.read_column_stats_version(sid, version_query, handler_data), &handler_data
                        );
                    },
                    py::call_guard<SingleThreadMutexHolder>(),
                    "Read the column stats"
            )
            .def("get_column_stats_info_version",
                 &PythonVersionStore::get_column_stats_info_version,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Get info about column stats")
            .def("remove_incomplete",
                 &PythonVersionStore::remove_incomplete,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Delete incomplete segments")
            .def(
                    "remove_incompletes",
                    [&](PythonVersionStore& v,
                        const std::unordered_set<StreamId>& sids,
                        const std::string& common_prefix) { return v.remove_incompletes(sids, common_prefix); },
                    py::call_guard<SingleThreadMutexHolder>(),
                    "Remove several incomplete segments"
            )
            .def("compact_incomplete",
                 &PythonVersionStore::compact_incomplete,
                 py::arg("stream_id"),
                 py::arg("append"),
                 py::arg("convert_int_to_float"),
                 py::arg("via_iteration") = true,
                 py::arg("sparsify") = false,
                 py::arg("user_meta") = std::nullopt,
                 py::arg("prune_previous_versions") = false,
                 py::arg("validate_index") = false,
                 py::arg("delete_staged_data_on_failure") = false,
                 py::kw_only(),
                 py::arg("stage_results") = std::nullopt,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Compact incomplete segments")
            .def("sort_merge",
                 &PythonVersionStore::sort_merge,
                 py::arg("stream_id"),
                 py::arg("user_meta") = std::nullopt,
                 py::arg("append") = false,
                 py::arg("convert_int_to_float") = false,
                 py::arg("via_iteration") = true,
                 py::arg("sparsify") = false,
                 py::arg("prune_previous_versions") = false,
                 py::arg("delete_staged_data_on_failure") = false,
                 py::kw_only(),
                 py::arg("stage_results") = std::nullopt,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "sort_merge will sort and merge incomplete segments. The segments do not have to be ordered - "
                 "incomplete segments can contain interleaved time periods but the final result will be fully ordered")
            .def("compact_library",
                 &PythonVersionStore::compact_library,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Compact the whole library wherever necessary")
            .def("_compact_data_explain_plan",
                 &PythonVersionStore::compact_data_explain_plan,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Calculates what the new row-slice distribution would be after calling compact_data")
            .def("_compact_data",
                 &PythonVersionStore::compact_data,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Compact data segments")
            .def("is_symbol_fragmented",
                 &PythonVersionStore::is_symbol_fragmented,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Check if there are enough small data segments which can be compacted")
            .def("defragment_symbol_data",
                 &PythonVersionStore::defragment_symbol_data,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Compact small data segments into larger data segments")
            .def("get_incomplete_symbols",
                 &PythonVersionStore::get_incomplete_symbols,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Get all the symbols that have incomplete entries")
            .def("get_incomplete_refs",
                 &PythonVersionStore::get_incomplete_refs,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Get all the symbols that have incomplete entries")
            .def("get_active_incomplete_refs",
                 &PythonVersionStore::get_active_incomplete_refs,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Get all the symbols that have incomplete entries and some appended data")
            .def("update",
                 &PythonVersionStore::update,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Update the most recent version of a dataframe")
            .def("indexes_sorted",
                 &PythonVersionStore::indexes_sorted,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Returns the sorted indexes of a symbol")
            .def("verify_snapshot",
                 &PythonVersionStore::verify_snapshot,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Validate the snapshot name and raise if it fails")
            .def("snapshot",
                 &PythonVersionStore::snapshot,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Create a snapshot")
            .def("list_snapshots",
                 &PythonVersionStore::list_snapshots,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "List all snapshots")
            .def("add_to_snapshot",
                 &PythonVersionStore::add_to_snapshot,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Add an item to a snapshot")
            .def("remove_from_snapshot",
                 &PythonVersionStore::remove_from_snapshot,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Remove an item from a snapshot")
            .def("clear",
                 &PythonVersionStore::clear,
                 py::arg("continue_on_error") = true,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Delete everything. Don't use this unless you want to delete everything")
            .def("empty",
                 &PythonVersionStore::empty,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Deprecated - prefer is_empty_excluding_key_types. Returns True "
                 "if there are no keys other than those of the excluded types in "
                 "the library, and False otherwise")
            .def("is_empty_excluding_key_types",
                 &PythonVersionStore::is_empty_excluding_key_types,
                 py::arg("excluded_key_types"),
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Returns True if there are no keys other than those of the "
                 "excluded types in the library, and False otherwise")
            .def("force_delete_symbol",
                 &PythonVersionStore::force_delete_symbol,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Delete everything. Don't use this unless you want to delete everything")
            .def("_get_all_tombstoned_versions",
                 &PythonVersionStore::get_all_tombstoned_versions,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Get a list of all the versions for a symbol which are tombstoned")
            .def("delete_storage",
                 &PythonVersionStore::delete_storage,
                 py::arg("continue_on_error") = true,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Delete everything. Don't use this unless you want to delete everything")
            .def("write_versioned_dataframe",
                 &PythonVersionStore::write_versioned_dataframe,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Write the most recent version of this dataframe to the store")
            .def("_test_write_versioned_segment",
                 &PythonVersionStore::test_write_versioned_segment,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Write the most recent version of this segment to the store")
            .def("write_versioned_composite_data",
                 &PythonVersionStore::write_versioned_composite_data,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Allows the user to write multiple dataframes in a batch with one version entity")
            .def("write_dataframe_specific_version",
                 &PythonVersionStore::write_dataframe_specific_version,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Write a specific  version of this dataframe to the store")
            .def(
                    "read_dataframe_version",
                    [&](PythonVersionStore& v,
                        StreamId sid,
                        const VersionQuery& version_query,
                        const std::shared_ptr<ReadQuery>& read_query,
                        const ReadOptions& read_options) {
                        auto handler_data = std::make_shared<std::any>(
                                TypeHandlerRegistry::instance()->get_handler_data(read_options.output_format())
                        );
                        return adapt_read_df(
                                v.read_dataframe_version(sid, version_query, read_query, read_options, handler_data),
                                handler_data.get()
                        );
                    },
                    py::call_guard<SingleThreadMutexHolder>(),
                    "Read the specified version of the dataframe from the store"
            )
            .def("_read_modify_write",
                 &PythonVersionStore::read_modify_write,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Read, modify and write the specified version for the dataframe (experimental)")
            .def(
                    "read_index",
                    [&](PythonVersionStore& v, StreamId sid, const VersionQuery& version_query) {
                        constexpr OutputFormat output_format = OutputFormat::PANDAS;
                        auto handler_data = TypeHandlerRegistry::instance()->get_handler_data(output_format);
                        return adapt_read_df(
                                v.read_index(sid, version_query, output_format, handler_data), &handler_data
                        );
                    },
                    py::call_guard<SingleThreadMutexHolder>(),
                    "Read the most recent dataframe from the store"
            )
            .def("get_update_time",
                 &PythonVersionStore::get_update_time,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Get the most recent update time for the stream ids")
            .def("get_update_times",
                 &PythonVersionStore::get_update_times,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Get the most recent update time for a list of stream ids")
            .def("scan_object_sizes",
                 &PythonVersionStore::scan_object_sizes,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Scan the compressed sizes of all objects in the library.")
            .def("scan_object_sizes_by_stream",
                 &PythonVersionStore::scan_object_sizes_by_stream,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Scan the compressed sizes of all objects in the library, grouped by stream ID and KeyType.")
            .def("scan_object_sizes_for_stream",
                 &PythonVersionStore::scan_object_sizes_for_stream,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Scan the compressed sizes of the given symbol.")
            .def("find_version",
                 &PythonVersionStore::get_version_to_read,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Check if a specific stream has been written to previously")
            .def("list_streams",
                 &PythonVersionStore::list_streams,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "List all the stream ids that have been written")
            .def("compact_symbol_list",
                 &PythonVersionStore::compact_symbol_list,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Compacts the symbol list cache into a single key in the storage")
            .def("read_metadata",
                 &PythonVersionStore::read_metadata,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Get back the metadata and version info for a symbol.")
            .def("fix_symbol_trees",
                 &PythonVersionStore::fix_symbol_trees,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Regenerate symbol tree by adding indexes from snapshots")
            .def("flush_version_map",
                 &PythonVersionStore::flush_version_map,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Flush the version cache")
            .def("read_descriptor",
                 &PythonVersionStore::read_descriptor,
                 py::arg("stream_id"),
                 py::arg("version_query"),
                 py::arg("include_index_segment") = false,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Get back the descriptor for a symbol.")
            .def("batch_read_descriptor",
                 &PythonVersionStore::batch_read_descriptor,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Get back the descriptor of a list of symbols.")
            .def(
                    "restore_version",
                    [&](PythonVersionStore& v,
                        StreamId sid,
                        const VersionQuery& version_query,
                        const ReadOptions& read_options) {
                        auto [vit, tsd] = v.restore_version(sid, version_query);
                        const auto& tsd_proto = tsd.proto();
                        ReadResult res{
                                vit,
                                PandasOutputFrame{SegmentInMemory{tsd.as_stream_descriptor()}},
                                read_options.output_format(),
                                tsd_proto.normalization(),
                                tsd_proto.user_meta(),
                                tsd_proto.multi_key_meta(),
                        };
                        return adapt_read_df(std::move(res), nullptr);
                    },
                    py::call_guard<SingleThreadMutexHolder>(),
                    "Restore a previous version of a symbol."
            )
            .def("check_ref_key",
                 &PythonVersionStore::check_ref_key,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Fix reference keys.")
            .def("dump_versions",
                 &PythonVersionStore::dump_versions,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Dump version data.")
            .def("_set_validate_version_map",
                 &PythonVersionStore::_test_set_validate_version_map,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Validate the version map.")
            .def("_clear_symbol_list_keys",
                 &PythonVersionStore::_clear_symbol_list_keys,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Delete all ref keys of type SYMBOL_LIST.")
            .def("reload_symbol_list",
                 &PythonVersionStore::reload_symbol_list,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Regenerate symbol list for library.")
            .def("write_partitioned_dataframe",
                 &PythonVersionStore::write_partitioned_dataframe,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Write a dataframe and partition it into sub symbols using partition key")
            .def("fix_ref_key",
                 &PythonVersionStore::fix_ref_key,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Fix reference keys.")
            .def("remove_and_rewrite_version_keys",
                 &PythonVersionStore::remove_and_rewrite_version_keys,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Remove all version keys and rewrite all indexes - useful in case a version has been tombstoned but "
                 "not deleted")
            .def("force_release_lock",
                 &PythonVersionStore::force_release_lock,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Force release a lock.")
            .def(
                    "batch_read",
                    [&](PythonVersionStore& v,
                        const std::vector<StreamId>& stream_ids,
                        const std::vector<VersionQuery>& version_queries,
                        std::vector<std::shared_ptr<ReadQuery>>& read_queries,
                        const BatchReadOptions& batch_read_options) {
                        auto handler_data = std::make_shared<std::any>(
                                TypeHandlerRegistry::instance()->get_handler_data(batch_read_options.output_format())
                        );
                        return python_util::adapt_read_dfs(
                                v.batch_read(
                                        stream_ids, version_queries, read_queries, batch_read_options, handler_data
                                ),
                                handler_data.get()
                        );
                    },
                    py::call_guard<SingleThreadMutexHolder>(),
                    "Read a dataframe from the store"
            )
            .def(
                    "batch_read_and_join",
                    [&](PythonVersionStore& v,
                        std::vector<StreamId>
                                stream_ids,
                        std::vector<VersionQuery>
                                version_queries,
                        std::vector<std::shared_ptr<ReadQuery>>& read_queries,
                        const ReadOptions& read_options,
                        std::vector<ClauseVariant>
                                clauses) {
                        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                                !clauses.empty(), "batch_read_and_join called with no clauses"
                        );
                        clauses = plan_query(std::move(clauses));
                        std::vector<std::shared_ptr<Clause>> _clauses;
                        bool first_clause{true};
                        for (auto&& clause : clauses) {
                            util::variant_match(clause, [&](auto&& clause) {
                                if (first_clause) {
                                    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                                            clause->clause_info().multi_symbol_,
                                            "Single-symbol clause cannot be used to join multiple symbols together"
                                    );
                                    first_clause = false;
                                } else {
                                    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                                            !clause->clause_info().multi_symbol_,
                                            "Multi-symbol clause cannot be used on a single symbol"
                                    );
                                }
                                _clauses.emplace_back(std::make_shared<Clause>(*std::forward<decltype(clause)>(clause))
                                );
                            });
                        }
                        const OutputFormat output_format = read_options.output_format();
                        auto handler_data = std::make_shared<std::any>(
                                TypeHandlerRegistry::instance()->get_handler_data(output_format)
                        );
                        return adapt_read_df(
                                v.batch_read_and_join(
                                        std::make_shared<std::vector<StreamId>>(std::move(stream_ids)),
                                        std::make_shared<std::vector<VersionQuery>>(std::move(version_queries)),
                                        read_queries,
                                        read_options,
                                        std::move(_clauses),
                                        handler_data
                                ),
                                handler_data.get()
                        );
                    },
                    py::call_guard<SingleThreadMutexHolder>(),
                    "Join multiple symbols from the store"
            )
            .def("batch_write",
                 &PythonVersionStore::batch_write,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Batch write latest versions of multiple symbols.")
            .def("batch_read_metadata",
                 &PythonVersionStore::batch_read_metadata,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Batch read the metadata of a list of symbols for the latest version")
            .def("batch_write_metadata",
                 &PythonVersionStore::batch_write_metadata,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Batch write the metadata of a list of symbols")
            .def("batch_append",
                 &PythonVersionStore::batch_append,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Batch append to a list of symbols")
            .def("batch_update",
                 &PythonVersionStore::batch_update,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Batch update a list of symbols")
            .def(
                    "batch_restore_version",
                    [&](PythonVersionStore& v,
                        const std::vector<StreamId>& ids,
                        const std::vector<VersionQuery>& version_queries,
                        const ReadOptions& read_options) {
                        auto results = v.batch_restore_version(ids, version_queries);
                        std::vector<std::variant<ReadResult, DataError>> output;
                        output.reserve(results.size());
                        for (auto& [vit, tsd] : results) {
                            const auto& tsd_proto = tsd.proto();
                            ReadResult res{
                                    vit,
                                    PandasOutputFrame{SegmentInMemory{tsd.as_stream_descriptor()}},
                                    read_options.output_format(),
                                    tsd_proto.normalization(),
                                    tsd_proto.user_meta(),
                                    tsd_proto.multi_key_meta()
                            };
                            output.emplace_back(std::move(res));
                        }
                        return python_util::adapt_read_dfs(std::move(output), nullptr);
                    },
                    py::call_guard<SingleThreadMutexHolder>(),
                    "Batch restore a group of versions to the versions indicated"
            )
            .def(
                    "list_versions",
                    [](PythonVersionStore& v,
                       const std::optional<StreamId>& s_id,
                       const std::optional<SnapshotId>& snap_id,
                       bool latest_only,
                       bool skip_snapshots) { return v.list_versions(s_id, snap_id, latest_only, skip_snapshots); },
                    py::call_guard<SingleThreadMutexHolder>(),
                    "List all the version ids for this store."
            )
            .def("_compact_version_map",
                 &PythonVersionStore::_compact_version_map,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Compact the version map contents for a given symbol")
            .def("get_storage_lock",
                 &PythonVersionStore::get_storage_lock,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Get a coarse-grained storage lock in the library")
            .def("list_incompletes",
                 &PythonVersionStore::list_incompletes,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "List incomplete chunks for stream id")
            .def("_get_version_history",
                 &PythonVersionStore::get_version_history,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Returns a list of index and tombstone keys in chronological order")
            .def("latest_timestamp",
                 &PythonVersionStore::latest_timestamp,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "Returns latest timestamp of a symbol")
            .def("get_store_current_timestamp_for_tests",
                 &PythonVersionStore::get_store_current_timestamp_for_tests,
                 py::call_guard<SingleThreadMutexHolder>(),
                 "For testing purposes only")
            .def(
                    "trim",
                    [](ARCTICDB_UNUSED PythonVersionStore& v) { Allocator::instance()->trim(); },
                    py::call_guard<SingleThreadMutexHolder>(),
                    "Call trim on the native store's underlining memory allocator"
            )
            .def_static("reuse_storage_for_testing", [](PythonVersionStore& from, PythonVersionStore& to) {
                to._test_set_store(from._test_get_store());
            });
}
} // namespace arcticdb::version_store