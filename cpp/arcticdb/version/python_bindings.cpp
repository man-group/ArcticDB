/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/version/python_bindings.hpp>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>
#include <arcticdb/version/version_store_api.hpp>
#include <arcticdb/python/arctic_version.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <folly/Singleton.h>
#include <arcticdb/storage/test/test_s3_storage_common.hpp>
#include <arcticdb/storage/mongo/mongo_instance.hpp>
#include <arcticdb/processing/operation_types.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/execution_context.hpp>
#include <arcticdb/pipeline/value_set.hpp>
#include <arcticdb/python/adapt_read_dataframe.hpp>
#include <arcticdb/version/snapshot.hpp>

namespace arcticdb::version_store {

void register_bindings(py::module &m) {
    auto version = m.def_submodule("version_store", "Versioned storage implementation apis");

    py::register_exception<NoSuchVersionException>(version, "NoSuchVersionException");
    py::register_exception<StreamDescriptorMismatch>(version, "StreamDescriptorMismatch");

  /*  class PyFilter: public Filter {
    public:
        using Filter::Filter;

        [[nodiscard]] util::BitSet go(const SegmentInMemory &seg) const override {
            PYBIND11_OVERRIDE_PURE(
                util::BitSet ,
                Filter,
                go,
                seg
            );
        }

        virtual ~PyFilter() = default;
    };

    py::class_<FilterProcessor, std::shared_ptr<FilterProcessor>>(version, "Processor")
            .def(py::init<std::shared_ptr<Filter>, std::shared_ptr<FilterProcessor>, std::shared_ptr<FilterProcessor>>())
            .def("set_left", &FilterProcessor::set_left)
            .def("set_right", &FilterProcessor::set_right)
            .def("print", &FilterProcessor::print_tree);

    py::class_<Filter, PyFilter, std::shared_ptr<Filter>>(version, "Filter")
            .def("filter", &Filter::filter);

    py::class_<EqualsFilter, Filter, std::shared_ptr<EqualsFilter>>(version, "EqualsFilter")
        .def(py::init<std::string, Value>())
        .def("filter", &EqualsFilter::filter)
        ;

    py::class_<NotEqualsFilter, Filter, std::shared_ptr<NotEqualsFilter>>(version, "NotEqualsFilter")
        .def(py::init<std::string, Value>())
        .def("filter", &NotEqualsFilter::filter)
        ;

    py::class_<LessThanFilter, Filter, std::shared_ptr<LessThanFilter>>(version, "LessThanFilter")
        .def(py::init<std::string, Value>())
        .def("filter", &LessThanFilter::filter)
        ;

    py::class_<GreaterThanFilter, Filter, std::shared_ptr<GreaterThanFilter>>(version, "GreaterThanFilter")
        .def(py::init<std::string, Value>())
        .def("filter", &GreaterThanFilter::filter)
        ;

    py::class_<LessThanOrEqualFilter, Filter, std::shared_ptr<LessThanOrEqualFilter>>(version, "LessThanOrEqualFilter")
        .def(py::init<std::string, Value>())
        .def("filter", &LessThanOrEqualFilter::filter)
        ;

    py::class_<GreaterThanOrEqualFilter, Filter, std::shared_ptr<GreaterThanOrEqualFilter>>(version, "GreaterThanOrEqualFilter")
        .def(py::init<std::string, Value>())
        .def("filter", &GreaterThanOrEqualFilter::filter)
        ;

    py::class_<ListMembershipFilter, Filter, std::shared_ptr<ListMembershipFilter>>(version, "ListMembershipFilter")
        .def(py::init<std::string, ValueList>())
        .def("filter", &ListMembershipFilter::filter)
        ;

    py::class_<OrFilter, Filter, std::shared_ptr<OrFilter>>(version, "OrFilter")
        .def(py::init())
        ;

    py::class_<AndFilter, Filter, std::shared_ptr<AndFilter>>(version, "AndFilter")
            .def(py::init())
    ;
    */

    py::class_<AtomKey, std::shared_ptr<AtomKey>>(version, "AtomKey")
    .def(py::init())
    .def_property_readonly("id", &AtomKey::id)
    .def_property_readonly("version_id", &AtomKey::version_id)
    .def_property_readonly("creation_ts", &AtomKey::creation_ts)
    .def_property_readonly("content_hash", &AtomKey::content_hash)
    .def_property_readonly("start_index", &AtomKey::start_index)
    .def_property_readonly("end_index", &AtomKey::end_index)
    .def_property_readonly("type", [](const AtomKey& self) {return self.type();})
    ;

    py::class_<Value, std::shared_ptr<Value>>(version, "ValueType")
        .def(py::init())
        ;

    version.def("ValueBool", &construct_value<bool>);
    version.def("ValueUint8", &construct_value<uint8_t>);
    version.def("ValueUint16", &construct_value<uint16_t>);
    version.def("ValueUint32", &construct_value<uint32_t>);
    version.def("ValueUint64", &construct_value<uint64_t>);
    version.def("ValueInt8", &construct_value<int8_t>);
    version.def("ValueInt16", &construct_value<int16_t>);
    version.def("ValueInt32", &construct_value<int32_t>);
    version.def("ValueInt64", &construct_value<int64_t>);
    version.def("ValueFloat32", &construct_value<float>);
    version.def("ValueFloat64", &construct_value<double>);

    version.def("Value", &construct_value<uint8_t>);
    version.def("Value", &construct_value<uint16_t>);
    version.def("Value", &construct_value<uint32_t>);
    version.def("Value", &construct_value<uint64_t>);
    version.def("Value", &construct_value<int8_t>);
    version.def("Value", &construct_value<int16_t>);
    version.def("Value", &construct_value<int32_t>);
    version.def("Value", &construct_value<int64_t>);
    version.def("Value", &construct_value<float>);
    version.def("Value", &construct_value<double>);
    version.def("Value", &construct_string_value);

    py::class_<ValueSet, std::shared_ptr<ValueSet>>(version, "ValueSet")
    .def(py::init([](std::vector<std::string>&& value_list){
        return std::make_shared<ValueSet>(std::move(value_list));
    }))
    .def(py::init([](py::array value_list){
        return std::make_shared<ValueSet>(value_list);
    }))
    ;

   // py::class_<ValueList>(version, "ValueListType")
   //     .def(py::init<NativeTensor>());

  //  version.def("ValueList", &construct_value_list);

    py::class_<ClauseBuilder>(version, "ClauseBuilder")
            .def(py::init())
            .def("add_ProjectClause", &ClauseBuilder::add_ProjectClause)
            .def("add_FilterClause", &ClauseBuilder::add_FilterClause)
            .def("prepare_AggregationClause", &ClauseBuilder::prepare_AggregationClause)
            .def("add_MeanAggregationOperator", [&](ClauseBuilder& v,  std::string input_column, std::string output_column) {
                return v.add_MeanAggregationOperator(ColumnName(input_column), ColumnName(output_column));
            })
            .def("add_SumAggregationOperator", [&](ClauseBuilder& v,  std::string input_column, std::string output_column) {
                return v.add_SumAggregationOperator(ColumnName(input_column), ColumnName(output_column));
            })
            .def("add_MaxAggregationOperator", [&](ClauseBuilder& v,  std::string input_column, std::string output_column) {
              return v.add_MaxAggregationOperator(ColumnName(input_column), ColumnName(output_column));
            })
            .def("add_MinAggregationOperator", [&](ClauseBuilder& v,  std::string input_column, std::string output_column) {
                return v.add_MinAggregationOperator(ColumnName(input_column), ColumnName(output_column));
            })
        .def("finalize_AggregationClause", &ClauseBuilder::finalize_AggregationClause);

    py::class_<VersionQuery>(version, "PythonVersionStoreVersionQuery")
        .def(py::init())
        .def("set_snap_name", &VersionQuery::set_snap_name)
        .def("set_timestamp", &VersionQuery::set_timestamp)
        .def("set_version", &VersionQuery::set_version)
        .def("set_skip_compat", &VersionQuery::set_skip_compat)
        .def("set_iterate_on_failure", &VersionQuery::set_iterate_on_failure);

    py::class_<ReadOptions>(version, "PythonVersionStoreReadOptions")
        .def(py::init())
        .def("set_force_strings_to_object", &ReadOptions::set_force_strings_to_object)
        .def("set_dynamic_schema", &ReadOptions::set_dynamic_schema)
        .def("set_allow_sparse", &ReadOptions::set_allow_sparse)
        .def("set_incompletes", &ReadOptions::set_incompletes)
        .def("set_set_tz", &ReadOptions::set_set_tz)
        .def("set_optimise_string_memory", &ReadOptions::set_optimise_string_memory)
        .def_property_readonly("incompletes", &ReadOptions::get_incompletes);

    using FrameDataWrapper = arcticdb::pipelines::FrameDataWrapper;
    py::class_<FrameDataWrapper, std::shared_ptr<FrameDataWrapper>>(version, "FrameDataWrapper")
            .def_property_readonly("data", &FrameDataWrapper::data);

    using PythonOutputFrame = arcticdb::pipelines::PythonOutputFrame;
    py::class_<PythonOutputFrame>(version, "PythonOutputFrame")
        .def(py::init<const SegmentInMemory&>())
        .def_property_readonly("value", [](py::object & obj){
            auto& fd = obj.cast<PythonOutputFrame&>();
            return fd.arrays(obj);
        })
        .def_property_readonly("offset", [](PythonOutputFrame& self) {
            return self.frame().offset();
        })
        .def_property_readonly("names", &PythonOutputFrame::names, py::return_value_policy::reference)
        .def_property_readonly("index_columns", &PythonOutputFrame::index_columns, py::return_value_policy::reference);


    // TODO: add repr.
    py::class_<VersionedItem>(version, "VersionedItem")
        .def_property_readonly("symbol", &VersionedItem::symbol)
        .def_property_readonly("version", &VersionedItem::version);

    py::class_<pipelines::FrameSlice, std::shared_ptr<pipelines::FrameSlice>>(version, "FrameSlice")
        .def_property_readonly("col_range", &pipelines::FrameSlice::columns)
        .def_property_readonly("row_range", &pipelines::FrameSlice::rows);

    py::class_<pipelines::RowRange, std::shared_ptr<pipelines::RowRange>>(version, "RowRange")
        .def(py::init([](std::size_t start, std::size_t end){
            return RowRange(start, end);
        }))
        .def_property_readonly("start", &pipelines::RowRange::start)
        .def_property_readonly("end", &pipelines::RowRange::end)
        .def_property_readonly("diff", &pipelines::RowRange::diff);

    py::class_<pipelines::HeadRange, std::shared_ptr<pipelines::HeadRange>>(version, "HeadRange")
    .def(py::init([](int64_t n){
        return HeadRange{n};
    }));

    py::class_<pipelines::TailRange, std::shared_ptr<pipelines::TailRange>>(version, "TailRange")
    .def(py::init([](int64_t n){
        return TailRange{n};
    }));

    py::class_<pipelines::SignedRowRange, std::shared_ptr<pipelines::SignedRowRange>>(version, "SignedRowRange")
    .def(py::init([](int64_t start, int64_t end){
        return SignedRowRange{start, end};
    }));

    py::class_<pipelines::ColRange, std::shared_ptr<pipelines::ColRange>>(version, "ColRange")
        .def_property_readonly("start", &pipelines::ColRange::start)
        .def_property_readonly("end", &pipelines::ColRange::end)
        .def_property_readonly("diff", &pipelines::ColRange::diff);



    auto adapt_read_dfs = [](std::vector<ReadResult> && ret) -> py::list {
        py::list lst;
        for (auto &res: ret) {
            auto pynorm = python_util::pb_to_python(res.norm_meta);
            auto pyuser_meta = python_util::pb_to_python(res.user_meta);
            auto multi_key_meta = python_util::pb_to_python(res.multi_key_meta);
            lst.append(py::make_tuple(res.item, std::move(res.frame_data), pynorm, pyuser_meta, multi_key_meta, res.multi_keys));
        }
        return lst;
    };

    py::class_<IndexRange>(version, "IndexRange")
            .def(py::init([](timestamp start, timestamp end){
                return IndexRange(start, end);
            }))
            .def_property_readonly("start_ts",[](const IndexRange&self){
                return std::get<timestamp>(self.start_);
            })
            .def_property_readonly("end_ts",[](const IndexRange&self){
                return std::get<timestamp>(self.end_);
            });

    py::class_<ReadQuery>(version, "PythonVersionStoreReadQuery")
            .def(py::init())
            .def_readwrite("columns",&ReadQuery::columns)
            .def_readwrite("row_range",&ReadQuery::row_range)
            .def_readwrite("row_filter",&ReadQuery::row_filter)
            .def("set_clause_builder", &ReadQuery::set_clause_builder);

    py::enum_<OperationType>(version, "OperationType")
            .value("ABS", OperationType::ABS)
            .value("NEG", OperationType::NEG)
            .value("IDENTITY", OperationType::IDENTITY)
            .value("NOT", OperationType::NOT)
            .value("ADD", OperationType::ADD)
            .value("SUB", OperationType::SUB)
            .value("MUL", OperationType::MUL)
            .value("DIV", OperationType::DIV)
            .value("EQ", OperationType::EQ)
            .value("NE", OperationType::NE)
            .value("LT", OperationType::LT)
            .value("LE", OperationType::LE)
            .value("GT", OperationType::GT)
            .value("GE", OperationType::GE)
            .value("ISIN", OperationType::ISIN)
            .value("ISNOTIN", OperationType::ISNOTIN)
            .value("AND", OperationType::AND)
            .value("OR", OperationType::OR)
            .value("XOR", OperationType::XOR);

    py::class_<ColumnName>(version, "ColumnName")
            .def(py::init([](const std::string& name) {
                return ColumnName(name);
            }));

    py::class_<ValueName>(version, "ValueName")
            .def(py::init([](const std::string& name) {
                return ValueName(name);
            }));

    py::class_<ValueSetName>(version, "ValueSetName")
    .def(py::init([](const std::string& name) {
        return ValueSetName(name);
    }));

    py::class_<ExpressionName>(version, "ExpressionName")
            .def(py::init([](const std::string& name) {
                return ExpressionName(name);
            }));

    py::class_<ExpressionNode, std::shared_ptr<ExpressionNode>>(version, "ExpressionNode")
            .def(py::init([](VariantNode left, VariantNode right, OperationType operation_type) {
                return ExpressionNode(left, right, operation_type);
            }))
            .def(py::init([](VariantNode left, OperationType operation_type) {
                return ExpressionNode(left, operation_type);
            }));

    py::enum_<ExecutionContext::Optimisation>(version, "ExecutionContextOptimisation")
            .value("SPEED", ExecutionContext::Optimisation::SPEED)
            .value("MEMORY", ExecutionContext::Optimisation::MEMORY);

    py::class_<ExecutionContext, std::shared_ptr<ExecutionContext>>(version, "ExecutionContext")
            .def(py::init())
            .def("add_column", &ExecutionContext::add_column)
            .def("add_expression_node", &ExecutionContext::add_expression_node)
            .def("add_value", &ExecutionContext::add_value)
            .def("add_value_set", &ExecutionContext::add_value_set)
            .def_readwrite("root_node_name", &ExecutionContext::root_node_name_)
            .def_readwrite("optimisation", &ExecutionContext::optimisation_);

    py::class_<UpdateQuery>(version, "PythonVersionStoreUpdateQuery")
            .def(py::init())
            .def_readwrite("row_filter",&UpdateQuery::row_filter);

    py::class_<PythonVersionStore>(version, "PythonVersionStore")
        .def(py::init<std::shared_ptr<storage::Library>>())
        .def("write_partitioned_dataframe",
             &PythonVersionStore::write_partitioned_dataframe,
             "Write a dataframe to the store")
        .def("delete_snapshot",
             &PythonVersionStore::delete_snapshot,
             "Delete snapshot from store")
        .def("delete",
             &PythonVersionStore::delete_all_versions,
             "Delete all versions of the given symbol")
        .def("delete_range",
             &PythonVersionStore::delete_range,
             "Delete the date range from the symbol")
        .def("delete_version",
             &PythonVersionStore::delete_version,
             "Delete specific version of the given symbol")
         .def("prune_previous_versions",
              &PythonVersionStore::prune_previous_versions,
              "Delete all but the latest version of the given symbol")
        .def("sort_index",
             &PythonVersionStore::sort_index,
             "Sort the index of a time series whose segments are internally sorted")
        .def("append",
             &PythonVersionStore::append,
             "Append a dataframe to the most recent version")
        .def("append_incomplete",
             &PythonVersionStore::append_incomplete,
             "Append a partial dataframe to the most recent version")
         .def("write_parallel",
             &PythonVersionStore::write_parallel,
             "Append to a symbol in parallel")
         .def("write_metadata",
             &PythonVersionStore::write_metadata,
             "Create a new version with new metadata and data from the last version")
         .def("remove_incomplete",
             &PythonVersionStore::remove_incomplete,
             "Delete incomplete segments")
         .def("compact_incomplete",
             &PythonVersionStore::compact_incomplete,
             py::arg("stream_id"),
             py::arg("append"),
             py::arg("convert_int_to_float"),
             py::arg("via_iteration") = true,
             py::arg("sparsify") = false,
             py::arg("user_meta") = std::nullopt,
             "Compact incomplete segments")
         .def("sort_merge",
             &PythonVersionStore::sort_merge,
             py::arg("stream_id"),
             py::arg("user_meta") = std::nullopt,
             py::arg("append") = false,
             py::arg("convert_int_to_float") = false,
             py::arg("via_iteration") = true,
             py::arg("sparsify") = false,
             "sort_merge will sort and merge incomplete segments. The segments do not have to be ordered - incomplete segments can contain interleaved time periods but the final result will be fully ordered")
        .def("compact_library",
             &PythonVersionStore::compact_library,
             "Compact the whole library wherever necessary")
        .def("get_incomplete_symbols",
             &PythonVersionStore::get_incomplete_symbols,
             "Get all the symbols that have incomplete entries")
        .def("get_incomplete_refs",
             &PythonVersionStore::get_incomplete_refs,
             "Get all the symbols that have incomplete entries")
        .def("get_active_incomplete_refs",
             &PythonVersionStore::get_active_incomplete_refs,
             "Get all the symbols that have incomplete entries and some appended data")
        .def("push_incompletes_to_symbol_list",
             &PythonVersionStore::push_incompletes_to_symbol_list,
             "Push all the symbols that have incomplete entries to the symbol list")
        .def("update",
             &PythonVersionStore::update,
             "Update the most recent version of a dataframe")
        .def("snapshot",
             &PythonVersionStore::snapshot,
             "Create a snapshot")
        .def("list_snapshots",
             &PythonVersionStore::list_snapshots,
             "List all snapshots")
        .def("add_to_snapshot",
             &PythonVersionStore::add_to_snapshot,
             "Add an item to a snapshot")
        .def("remove_from_snapshot",
             &PythonVersionStore::remove_from_snapshot,
             "Remove an item from a snapshot")
        .def("clear",
             &PythonVersionStore::clear,
             "Delete everything. Don't use this unless you want to delete everything")
        .def("empty",
             &PythonVersionStore::empty,
             "Returns True if there are no keys of any type in the library, and False otherwise")
        .def("force_delete_symbol",
             &PythonVersionStore::force_delete_symbol,
             "Delete everything. Don't use this unless you want to delete everything")
        .def("_get_all_tombstoned_versions",
             &PythonVersionStore::get_all_tombstoned_versions,
             "Get a list of all the versions for a symbol which are tombstoned")
        .def("delete_storage",
             &PythonVersionStore::delete_storage,
             "Delete everything. Don't use this unless you want to delete everything")
        .def("write_versioned_dataframe",
             &PythonVersionStore::write_versioned_dataframe,
             "Write the most recent version of this dataframe to the store")
        .def("write_versioned_composite_data",
             &PythonVersionStore::write_versioned_composite_data,
             "Allows the user to write multiple dataframes in a batch with one version entity")
        .def("write_dataframe_specific_version",
            &PythonVersionStore::write_dataframe_specific_version,
             "Write a specific  version of this dataframe to the store")
        .def("read_dataframe_version",
             [&](PythonVersionStore& v,  StreamId sid, const VersionQuery& version_query, ReadQuery& read_query, const ReadOptions& read_options){
                return adapt_read_df(v.read_dataframe_version(sid, version_query, read_query, read_options));
              },
             "Read the specified version of the dataframe from the store")
        .def("read_index",
             [&](PythonVersionStore& v,  StreamId sid, const VersionQuery& version_query){
                 return adapt_read_df(v.read_index(sid, version_query));
             },
             "Read the most recent dataframe from the store")
        .def("read_latest_dataframe_merged",
             [&](PythonVersionStore& v, StreamId target_id, std::vector<StreamId> &sids, ReadQuery &query, const ReadOptions read_options){
                 return adapt_read_df(v.read_dataframe_merged(target_id, sids, VersionQuery{}, query, read_options));
             },
             "Read the most recent dataframe from the store")
         .def("get_update_time",
              &PythonVersionStore::get_update_time,
             "Get the most recent update time for the stream ids")
         .def("get_update_times",
              &PythonVersionStore::get_update_times,
             "Get the most recent update time for a list of stream ids")
         .def("scan_object_sizes",
              &PythonVersionStore::scan_object_sizes,
            "Scan the sizes of object")
        .def("find_version",
             &PythonVersionStore::get_version_to_read,
             "Check if a specific stream has been written to previously")
        .def("list_streams",
             &PythonVersionStore::list_streams,
             "List all the stream ids that have been written")
        .def("read_metadata",
             &PythonVersionStore::read_metadata,
             "Get back the metadata and version info for a symbol.")
         .def("fix_symbol_trees",
             &PythonVersionStore::fix_symbol_trees,
             "Regenerate symbol tree by adding indexes from snapshots")
         .def("flush_version_map",
             &PythonVersionStore::flush_version_map,
             "Flush the version cache")
        .def("read_descriptor",
             &PythonVersionStore::read_descriptor,
             "Get back the descriptor for a symbol.")
        .def("restore_version",
             [&](PythonVersionStore& v,  StreamId sid, const VersionQuery& version_query) {
                auto [vit, tsd] = v.restore_version(sid, version_query);
                ReadResult res{
                    vit,
                    PythonOutputFrame{
                            SegmentInMemory{StreamDescriptor{std::move(*tsd.mutable_stream_descriptor())}}
                            },
                        tsd.normalization(),
                        tsd.user_meta(),
                        tsd.multi_key_meta(),
                        std::vector<entity::AtomKey>{}
                };
                return adapt_read_df(std::move(res)); },
             "Restore a previous version of a symbol.")
        .def("check_ref_key",
             &PythonVersionStore::check_ref_key,
             "Fix reference keys.")
        .def("dump_versions",
             &PythonVersionStore::dump_versions,
             "Dump version data.")
        .def("_set_validate_version_map",
             &PythonVersionStore::_test_set_validate_version_map,
             "Validate the version map.")
        .def("_clear_symbol_list_keys",
             &PythonVersionStore::_clear_symbol_list_keys,
             "Delete all ref keys of type SYMBOL_LIST.")
        .def("_refresh_symbol_list",
             &PythonVersionStore::_refresh_symbol_list,
             "Regenerate symbol list for library.")

        .def("write_partitioned_dataframe",
             &PythonVersionStore::write_partitioned_dataframe,
             "Write a dataframe and partition it into sub symbols using partition key")
        .def("fix_ref_key",
             &PythonVersionStore::fix_ref_key,
             "Fix reference keys.")
        .def("remove_and_rewrite_version_keys",
             &PythonVersionStore::remove_and_rewrite_version_keys,
             "Remove all version keys and rewrite all indexes - useful in case a version has been tombstoned but not deleted")
        .def("force_release_lock",
             &PythonVersionStore::force_release_lock,
             "Force release a lock.")
        .def("batch_read",
             [&](PythonVersionStore& v, const std::vector<StreamId> &stream_ids,  const std::vector<VersionQuery>& version_queries, std::vector<ReadQuery> read_queries, const ReadOptions& read_options){
                 return adapt_read_dfs(v.batch_read(stream_ids, version_queries, read_queries, read_options));
             },
             "Read a dataframe from the store")
        .def("batch_read_keys",
             [&](PythonVersionStore& v, std::vector<AtomKey> atom_keys) {
                 return adapt_read_dfs(frame_to_read_result(v.batch_read_keys(atom_keys, {}, ReadOptions{})));
             },
             "Read a specific version of a dataframe from the store")
        .def("batch_write",
             &PythonVersionStore::batch_write,
             "Batch write latest versions of multiple symbols.")
        .def("batch_read_metadata",
             &PythonVersionStore::batch_read_metadata,
             "Batch read the metadata of a list of symbols for the latest version")
        .def("batch_write_metadata",
             &PythonVersionStore::batch_write_metadata,
             "Batch write the metadata of a list of symbols")
        .def("batch_append",
             &PythonVersionStore::batch_append,
             "Batch append to a list of symbols")
        .def("batch_restore_version",
             [&](PythonVersionStore& v, const std::vector<StreamId>& ids, const std::vector<VersionQuery>& version_queries){
                 auto results = v.batch_restore_version(ids, version_queries);
                 std::vector<py::object> output;
                 output.reserve(results.size());
                 for(auto& [vit, tsd] : results) {
                     ReadResult res{vit,
                                {SegmentInMemory{StreamDescriptor{std::move(*tsd.mutable_stream_descriptor())}}},
                                tsd.normalization(),
                                tsd.user_meta(),
                                tsd.multi_key_meta(),
                                {}};
                     output.emplace_back(adapt_read_df(std::move(res)));
                 }
                 return output;
             },
           "Batch restore a group of versions to the versions indicated")
        .def("list_versions",[](
                PythonVersionStore& v,
                const std::optional<StreamId> & s_id,
                const std::optional<SnapshotId> & snap_id,
                const std::optional<bool>& latest,
                const std::optional<bool>& iterate_on_failure,
                const std::optional<bool>& skip_snapshots
                ){
                 return v.list_versions(s_id, snap_id, latest, iterate_on_failure, skip_snapshots);
             },
             "List all the version ids for this store.")
        .def("_compact_version_map",
             &PythonVersionStore::_compact_version_map,
             "Compact the version map contents for a given symbol")
        .def("get_storage_lock",
             &PythonVersionStore::get_storage_lock,
             "Get a coarse-grained storage lock in the library")
        .def("list_incompletes",
             &PythonVersionStore::list_incompletes,
             "List incomplete chunks for stream id")
        .def("_get_version_history",
             &PythonVersionStore::get_version_history,
             "Returns a list of index and tombstone keys in chronological order")
        .def("latest_timestamp",
             &PythonVersionStore::latest_timestamp,
             "Returns latest timestamp of a symbol")
        ;

    m.def("get_version_string", &get_arcticdb_version_string);

    m.def("read_runtime_config", [](const py::object object) {
        auto config = RuntimeConfig{};
         python_util::pb_from_python(object, config);
         read_runtime_config(config);
    });

    py::class_<LocalVersionedEngine>(version, "VersionedEngine")
      .def(py::init<std::shared_ptr<storage::Library>>())
      .def("read_versioned_dataframe",
           &LocalVersionedEngine::read_dataframe_version_internal,
           "Read a dataframe from the store");
}

} //namespace arcticdb::version_store
