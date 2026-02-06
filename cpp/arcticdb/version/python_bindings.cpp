/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/error_code.hpp>
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/operators.h>
#include <arcticdb/entity/data_error.hpp>
#include <arcticdb/entity/protobuf_mappings.hpp>
#include <arcticdb/version/version_store_api.hpp>
#include <arcticdb/version/python_bindings_common.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/processing/operation_types.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/processing/query_planner.hpp>
#include <arcticdb/pipeline/value_set.hpp>
#include <arcticdb/python/adapt_read_dataframe.hpp>
#include <arcticdb/python/numpy_buffer_holder.hpp>
#include <arcticdb/version/schema_checks.hpp>
#include <arcticdb/util/pybind_mutex.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/entity/python_bindings_common.hpp>

namespace arcticdb::version_store {

static consteval timestamp one_day_in_nanoseconds() { return timestamp(24) * 60 * 60 * 1'000'000'000; }

template<typename T>
requires std::integral<T>
[[nodiscard]] static T python_mod(T a, T b) {
    return (a % b + b) % b;
}

/// @param ts in nanoseconds
[[nodiscard]] static timestamp start_of_day_nanoseconds(timestamp ts) {
    return ts - python_mod(ts, one_day_in_nanoseconds());
}

/// @param ts in nanoseconds
[[nodiscard]] static timestamp end_of_day_nanoseconds(timestamp ts) {
    const timestamp start_of_day = start_of_day_nanoseconds(ts);
    const bool is_midnnight = start_of_day == ts;
    if (is_midnnight) {
        return ts;
    }
    return start_of_day + one_day_in_nanoseconds();
}

[[nodiscard]] static std::pair<timestamp, timestamp> compute_first_last_dates(
        timestamp start, timestamp end, const timestamp rule, const ResampleBoundary closed_boundary_arg,
        const timestamp offset, const ResampleOrigin& origin
) {
    // Origin value formula from Pandas:
    // https://github.com/pandas-dev/pandas/blob/68d9dcab5b543adb3bfe5b83563c61a9b8afae77/pandas/core/resample.py#L2564
    auto [origin_ns, origin_adjusted_start] = util::variant_match(
            origin,
            [start](timestamp o) -> std::pair<timestamp, timestamp> { return {o, start}; },
            [&](const std::string& o) -> std::pair<timestamp, timestamp> {
                if (o == "epoch") {
                    return {0, start};
                } else if (o == "start") {
                    return {start, start};
                } else if (o == "start_day") {
                    return {start_of_day_nanoseconds(start), start};
                } else if (o == "end_day" || o == "end") {
                    const timestamp origin_last = o == "end" ? end : end_of_day_nanoseconds(end);
                    const timestamp bucket_count =
                            (origin_last - start) / rule + (closed_boundary_arg == ResampleBoundary::LEFT);
                    const timestamp origin_ns = origin_last - bucket_count * rule;
                    return {origin_ns, origin_ns};
                } else {
                    user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                            R"(Invalid origin value {}. Supported values are: "start", "start_day", "end", "end_day", "epoch" or timestamp in nanoseconds)",
                            o
                    );
                }
            }
    );
    origin_ns += offset;

    const timestamp ns_to_prev_offset_start = python_mod(origin_adjusted_start - origin_ns, rule);
    const timestamp ns_to_prev_offset_end = python_mod(end - origin_ns, rule);

    if (closed_boundary_arg == ResampleBoundary::RIGHT) {
        return {ns_to_prev_offset_start > 0 ? origin_adjusted_start - ns_to_prev_offset_start
                                            : origin_adjusted_start - rule,
                ns_to_prev_offset_end > 0 ? end + (rule - ns_to_prev_offset_end) : end};
    } else {
        return {ns_to_prev_offset_start > 0 ? origin_adjusted_start - ns_to_prev_offset_start : origin_adjusted_start,
                ns_to_prev_offset_end > 0 ? end + (rule - ns_to_prev_offset_end) : end + rule};
    }
}

std::vector<timestamp> generate_buckets(
        timestamp start, timestamp end, std::string_view rule, ResampleBoundary closed_boundary_arg, timestamp offset,
        const ResampleOrigin& origin
) {
    // e.g. Can happen if date range specified does not overlap with the time range covered by the symbol
    if (end < start) {
        return {};
    }
    const timestamp rule_ns = [](std::string_view rule) {
        py::gil_scoped_acquire acquire_gil;
        return python_util::pd_to_offset(rule);
    }(rule);
    const auto [start_with_offset, end_with_offset] =
            compute_first_last_dates(start, end, rule_ns, closed_boundary_arg, offset, origin);
    const auto bucket_boundary_count = (end_with_offset - start_with_offset) / rule_ns + 1;
    std::vector<timestamp> res;
    res.reserve(bucket_boundary_count);
    for (auto boundary = start_with_offset; boundary <= end_with_offset; boundary += rule_ns) {
        res.push_back(boundary);
    }
    return res;
}

template<ResampleBoundary closed_boundary>
void declare_resample_clause(py::module& version) {
    const char* class_name =
            closed_boundary == ResampleBoundary::LEFT ? "ResampleClauseLeftClosed" : "ResampleClauseRightClosed";
    py::class_<ResampleClause<closed_boundary>, std::shared_ptr<ResampleClause<closed_boundary>>>(version, class_name)
            .def(py::init([](std::string rule, ResampleBoundary label_boundary, timestamp offset, ResampleOrigin origin
                          ) {
                return ResampleClause<closed_boundary>(
                        std::move(rule), label_boundary, generate_buckets, offset, std::move(origin)
                );
            }))
            .def_property_readonly("rule", &ResampleClause<closed_boundary>::rule)
            .def("set_aggregations",
                 [](ResampleClause<closed_boundary>& self,
                    std::unordered_map<std::string, std::variant<std::string, std::pair<std::string, std::string>>>
                            aggregations) {
                     self.set_aggregations(python_util::named_aggregators_from_dict(std::move(aggregations)));
                 })
            .def("__str__", &ResampleClause<closed_boundary>::to_string);
}

void register_bindings(py::module& version, py::exception<arcticdb::ArcticException>& base_exception) {

    py::register_exception<StreamDescriptorMismatch>(version, "StreamDescriptorMismatch", base_exception.ptr());

    entity::apy::register_common_entity_bindings(version, arcticdb::BindingScope::GLOBAL);

    py::class_<Value, std::shared_ptr<Value>>(version, "ValueType").def(py::init());

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

    py::enum_<MergeAction>(version, "MergeAction")
            .value("DO_NOTHING", MergeAction::DO_NOTHING)
            .value("UPDATE", MergeAction::UPDATE)
            .value("INSERT", MergeAction::INSERT)
            .export_values();

    py::class_<ValueSet, std::shared_ptr<ValueSet>>(version, "ValueSet")
            .def(py::init([](std::vector<std::string>&& value_list) {
                return std::make_shared<ValueSet>(std::move(value_list));
            }))
            .def(py::init([](py::array value_list) { return std::make_shared<ValueSet>(value_list); }));

    py::class_<PreloadedIndexQuery>(version, "PreloadedIndexQuery").def(py::init<AtomKey, SegmentInMemory>());

    py::class_<VersionQuery>(version, "PythonVersionStoreVersionQuery")
            .def(py::init())
            .def("set_snap_name", &VersionQuery::set_snap_name)
            .def("set_timestamp", &VersionQuery::set_timestamp)
            .def("set_version", &VersionQuery::set_version)
            .def("set_schema_item", &VersionQuery::set_schema_item);

    py::enum_<OutputFormat>(version, "InternalOutputFormat")
            .value("PANDAS", OutputFormat::PANDAS)
            .value("ARROW", OutputFormat::ARROW);

    py::enum_<ArrowOutputStringFormat>(version, "InternalArrowOutputStringFormat")
            .value("CATEGORICAL", ArrowOutputStringFormat::CATEGORICAL)
            .value("LARGE_STRING", ArrowOutputStringFormat::LARGE_STRING)
            .value("SMALL_STRING", ArrowOutputStringFormat::SMALL_STRING);

    py::class_<ReadOptions>(version, "PythonVersionStoreReadOptions")
            .def(py::init())
            .def("set_force_strings_to_object", &ReadOptions::set_force_strings_to_object)
            .def("set_dynamic_schema", &ReadOptions::set_dynamic_schema)
            .def("set_allow_sparse", &ReadOptions::set_allow_sparse)
            .def("set_incompletes", &ReadOptions::set_incompletes)
            .def("set_set_tz", &ReadOptions::set_set_tz)
            .def("set_optimise_string_memory", &ReadOptions::set_optimise_string_memory)
            .def("set_output_format", &ReadOptions::set_output_format)
            .def("set_arrow_output_default_string_format", &ReadOptions::set_arrow_output_default_string_format)
            .def("set_arrow_output_per_column_string_format", &ReadOptions::set_arrow_output_per_column_string_format)
            .def_property_readonly("incompletes", &ReadOptions::get_incompletes)
            .def_property_readonly("output_format", &ReadOptions::output_format);

    py::class_<BatchReadOptions>(version, "PythonVersionStoreBatchReadOptions")
            .def(py::init([](bool batch_throw_on_error) { return BatchReadOptions(batch_throw_on_error); }))
            .def("set_read_options", &BatchReadOptions::set_read_options)
            .def("set_read_options_per_symbol", &BatchReadOptions::set_read_options_per_symbol)
            .def("set_output_format", &BatchReadOptions::set_output_format)
            .def("set_batch_throw_on_error", &BatchReadOptions::set_batch_throw_on_error)
            .def("at", &BatchReadOptions::at);

    version.def("write_dataframe_to_file", &write_dataframe_to_file);
    version.def(
            "read_dataframe_from_file",
            [](StreamId sid, std::string path, std::shared_ptr<ReadQuery>& read_query, const ReadOptions& read_options
            ) {
                auto handler_data = TypeHandlerRegistry::instance()->get_handler_data(read_options.output_format());
                return adapt_read_df(
                        read_dataframe_from_file(sid, path, read_query, read_options, handler_data), &handler_data
                );
            }
    );

    using PandasOutputFrame = arcticdb::pipelines::PandasOutputFrame;
    register_version_store_common_bindings(version, BindingScope::GLOBAL);

    py::class_<RecordBatchData>(version, "RecordBatchData")
            .def(py::init<>())
            .def("array", &RecordBatchData::array)
            .def("schema", &RecordBatchData::schema);

    py::enum_<VersionRequestType>(version, "VersionRequestType", R"pbdoc(
        Enum of possible version request types passed to as_of.
    )pbdoc")
            .value("SNAPSHOT", VersionRequestType::SNAPSHOT, R"pbdoc(
            Request the version of the symbol contained in the specified snapshot.
    )pbdoc")
            .value("TIMESTAMP", VersionRequestType::TIMESTAMP, R"pbdoc(
            Request the version of the symbol as it was at the specified timestamp.
    )pbdoc")
            .value("SPECIFIC", VersionRequestType::SPECIFIC, R"pbdoc(
            Request a specific version of the symbol.
    )pbdoc")
            .value("LATEST", VersionRequestType::LATEST, R"pbdoc(
            Request the latest undeleted version of the symbol.
    )pbdoc");

    py::class_<DataError, std::shared_ptr<DataError>>(version, "DataError", R"pbdoc(
        Return value for batch methods which fail in some way.

        Attributes
        ----------
        symbol: str
            Read or modified symbol.
        version_request_type: Optional[VersionRequestType]
            For operations that support as_of, the type of version query provided. `None` otherwise.
        version_request_data: Optional[Union[str, int]]
            For operations that support as_of, the value provided in the version query:
                None - Operation does not support as_of, or latest version was requested
                str - The name of the snapshot provided to as_of
                int - The specific version requested if version_request_type == VersionRequestType::SPECIFIC, or
                      nanoseconds since epoch if version_request_type == VersionRequestType::TIMESTAMP
        error_code: Optional[ErrorCode]
            For the most common error types, the ErrorCode is included here.
            e.g. ErrorCode.E_NO_SUCH_VERSION if the version requested has been deleted
            Please see the Error Messages section of the docs for more detail.
        error_category: Optional[ErrorCategory]
            If error_code is provided, the category is also provided.
            e.g.  ErrorCategory.MISSING_DATA if error_code is ErrorCode.E_NO_SUCH_VERSION
        exception_string: str
            The string associated with the exception that was originally raised.
    )pbdoc")
            .def_property_readonly("symbol", &DataError::symbol)
            .def_property_readonly("version_request_type", &DataError::version_request_type)
            .def_property_readonly("version_request_data", &DataError::version_request_data)
            .def_property_readonly("error_code", &DataError::error_code)
            .def_property_readonly("error_category", &DataError::error_category)
            .def_property_readonly("exception_string", &DataError::exception_string)
            .def("__str__", &DataError::to_string);

    py::class_<storage::KeyNotFoundInStageResultInfo, std::shared_ptr<storage::KeyNotFoundInStageResultInfo>>(
            version, "KeyNotFoundInStageResultInfo", R"pbdoc(
        Internal type. Information about a stage result that failed during staged data finalization, because a key that it refers
        to is not present in storage.

        Attributes
        ----------
        stage_result_index: int
            Index of the stage result that containing a key that was not found, an index in to the stage_results provided to the finalization
            method.
        missing_key: AtomKey
            The key that was in the stage result but missing in storage.
    )pbdoc"
    )
            .def(py::init([](uint64_t stage_result_index, const VariantKey& missing_key) {
                return storage::KeyNotFoundInStageResultInfo(stage_result_index, missing_key);
            }))
            .def_property_readonly("stage_result_index", &storage::KeyNotFoundInStageResultInfo::stage_result_index)
            .def_property_readonly("missing_key", &storage::KeyNotFoundInStageResultInfo::missing_key)
            .def("__repr__", &storage::KeyNotFoundInStageResultInfo::to_string)
            .def("__str__", &storage::KeyNotFoundInStageResultInfo::to_string)
            .def(py::self == py::self)
            .def(py::self != py::self);

    py::class_<DescriptorItem>(version, "DescriptorItem")
            .def_property_readonly("symbol", &DescriptorItem::symbol)
            .def_property_readonly("version", &DescriptorItem::version)
            .def_property_readonly("start_index", &DescriptorItem::start_index)
            .def_property_readonly("end_index", &DescriptorItem::end_index)
            .def_property_readonly("creation_ts", &DescriptorItem::creation_ts)
            .def_property_readonly("timeseries_descriptor", &DescriptorItem::timeseries_descriptor)
            .def_property_readonly("key", &DescriptorItem::key)
            .def_property_readonly("index_segment", &DescriptorItem::index_segment);

    py::class_<StageResult>(version, "StageResult", R"pbdoc(
        Result returned by the stage method containing information about staged segments.
        
        StageResult objects can be passed to finalization methods to specify which staged data to finalize.
        This enables selective finalization of staged data when multiple stage operations have been performed.
        
        Attributes
        ----------
        staged_segments : List[AtomKey]
)pbdoc")
            .def(py::init([]() { return StageResult({}); }))
            .def_property_readonly("staged_segments", [](const StageResult& self) { return self.staged_segments; })
            .def("__repr__", &StageResult::view)
            .def(py::pickle(
                    [](const StageResult& s) {
                        constexpr int serialization_version = 0;
                        return py::make_tuple(serialization_version, s.staged_segments);
                    },
                    [](py::tuple t) {
                        util::check(t.size() >= 1, "Invalid StageResult pickle object!");

                        [[maybe_unused]] const int serialization_version = t[0].cast<int>();
                        StageResult p(t[1].cast<std::vector<AtomKey>>());
                        return p;
                    }
            ));

    py::class_<pipelines::FrameSlice, std::shared_ptr<pipelines::FrameSlice>>(version, "FrameSlice")
            .def_property_readonly("col_range", &pipelines::FrameSlice::columns)
            .def_property_readonly("row_range", &pipelines::FrameSlice::rows);

    py::class_<pipelines::RowRange, std::shared_ptr<pipelines::RowRange>>(version, "RowRange")
            .def(py::init([](std::size_t start, std::size_t end) { return RowRange(start, end); }))
            .def_property_readonly("start", &pipelines::RowRange::start)
            .def_property_readonly("end", &pipelines::RowRange::end)
            .def_property_readonly("diff", &pipelines::RowRange::diff);

    py::class_<pipelines::SignedRowRange, std::shared_ptr<pipelines::SignedRowRange>>(version, "SignedRowRange")
            .def(py::init([](std::optional<int64_t> start, std::optional<int64_t> end) {
                return SignedRowRange{start, end};
            }));

    py::class_<pipelines::ColRange, std::shared_ptr<pipelines::ColRange>>(version, "ColRange")
            .def_property_readonly("start", &pipelines::ColRange::start)
            .def_property_readonly("end", &pipelines::ColRange::end)
            .def_property_readonly("diff", &pipelines::ColRange::diff);

    py::class_<IndexRange>(version, "IndexRange")
            .def(py::init([](timestamp start, timestamp end) { return IndexRange(start, end); }))
            .def_property_readonly("start_ts", [](const IndexRange& self) { return std::get<timestamp>(self.start_); })
            .def_property_readonly("end_ts", [](const IndexRange& self) { return std::get<timestamp>(self.end_); });

    py::class_<FilterClause, std::shared_ptr<FilterClause>>(version, "FilterClause")
            .def(py::init<std::unordered_set<std::string>, ExpressionContext, std::optional<PipelineOptimisation>>())
            .def("__str__", &FilterClause::to_string)
            .def("set_pipeline_optimisation", &FilterClause::set_pipeline_optimisation);

    py::class_<ProjectClause, std::shared_ptr<ProjectClause>>(version, "ProjectClause")
            .def(py::init<std::unordered_set<std::string>, std::string, ExpressionContext>())
            .def("__str__", &ProjectClause::to_string);

    py::class_<GroupByClause, std::shared_ptr<GroupByClause>>(version, "GroupByClause")
            .def(py::init<std::string>())
            .def_property_readonly("grouping_column", [](const GroupByClause& self) { return self.grouping_column_; })
            .def("__str__", &GroupByClause::to_string);

    py::class_<AggregationClause, std::shared_ptr<AggregationClause>>(version, "AggregationClause")
            .def(py::init(
                    [](const std::string& grouping_colum,
                       std::unordered_map<std::string, std::variant<std::string, std::pair<std::string, std::string>>>
                               aggregations) {
                        return AggregationClause(
                                grouping_colum, python_util::named_aggregators_from_dict(std::move(aggregations))
                        );
                    }
            ))
            .def("__str__", &AggregationClause::to_string);

    declare_resample_clause<ResampleBoundary::LEFT>(version);
    declare_resample_clause<ResampleBoundary::RIGHT>(version);

    py::enum_<ResampleBoundary>(version, "ResampleBoundary")
            .value("LEFT", ResampleBoundary::LEFT)
            .value("RIGHT", ResampleBoundary::RIGHT);

    py::enum_<RowRangeClause::RowRangeType>(version, "RowRangeType")
            .value("HEAD", RowRangeClause::RowRangeType::HEAD)
            .value("TAIL", RowRangeClause::RowRangeType::TAIL)
            .value("RANGE", RowRangeClause::RowRangeType::RANGE);

    py::enum_<JoinType>(version, "JoinType").value("OUTER", JoinType::OUTER).value("INNER", JoinType::INNER);

    py::class_<RowRangeClause, std::shared_ptr<RowRangeClause>>(version, "RowRangeClause")
            .def(py::init<RowRangeClause::RowRangeType, int64_t>())
            .def(py::init<std::optional<int64_t>, std::optional<int64_t>>())
            .def("__str__", &RowRangeClause::to_string);

    py::class_<DateRangeClause, std::shared_ptr<DateRangeClause>>(version, "DateRangeClause")
            .def(py::init<timestamp, timestamp>())
            .def_property_readonly("start", &DateRangeClause::start)
            .def_property_readonly("end", &DateRangeClause::end)
            .def("__str__", &DateRangeClause::to_string);

    py::class_<ConcatClause, std::shared_ptr<ConcatClause>>(version, "ConcatClause")
            .def(py::init<JoinType>())
            .def("__str__", &ConcatClause::to_string);

    py::class_<ReadQuery, std::shared_ptr<ReadQuery>>(version, "PythonVersionStoreReadQuery")
            .def(py::init())
            .def_readwrite("columns", &ReadQuery::columns)
            .def_readwrite("row_range", &ReadQuery::row_range)
            .def_readwrite("row_filter", &ReadQuery::row_filter)
            .def_readonly("needs_post_processing", &ReadQuery::needs_post_processing)
            // Unsurprisingly, pybind11 doesn't understand folly::poly, so use vector of variants here
            .def("add_clauses", [](ReadQuery& self, std::vector<ClauseVariant> clauses) {
                clauses = plan_query(std::move(clauses));
                std::vector<std::shared_ptr<Clause>> _clauses;
                self.needs_post_processing = false;
                for (auto&& clause : clauses) {
                    util::variant_match(clause, [&](auto&& clause) {
                        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                                !clause->clause_info().multi_symbol_,
                                "Multi-symbol clause cannot be used on a single symbol"
                        );
                        _clauses.emplace_back(std::make_shared<Clause>(*clause));
                    });
                }
                self.add_clauses(std::move(_clauses));
            });

    py::enum_<OperationType>(version, "OperationType")
            .value("ABS", OperationType::ABS)
            .value("NEG", OperationType::NEG)
            .value("ISNULL", OperationType::ISNULL)
            .value("NOTNULL", OperationType::NOTNULL)
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
            .value("REGEX_MATCH", OperationType::REGEX_MATCH)
            .value("AND", OperationType::AND)
            .value("OR", OperationType::OR)
            .value("XOR", OperationType::XOR)
            .value("TERNARY", OperationType::TERNARY);

    py::enum_<SortedValue>(version, "SortedValue")
            .value("UNKNOWN", SortedValue::UNKNOWN)
            .value("UNSORTED", SortedValue::UNSORTED)
            .value("ASCENDING", SortedValue::ASCENDING)
            .value("DESCENDING", SortedValue::DESCENDING);

    py::class_<ColumnStats>(version, "ColumnStats")
            .def(py::init<std::unordered_map<std::string, std::unordered_set<std::string>>>())
            .def("to_map", &ColumnStats::to_map);

    py::class_<ColumnName>(version, "ColumnName").def(py::init([](const std::string& name) {
        return ColumnName(name);
    }));

    py::class_<ValueName>(version, "ValueName").def(py::init([](const std::string& name) { return ValueName(name); }));

    py::class_<ValueSetName>(version, "ValueSetName").def(py::init([](const std::string& name) {
        return ValueSetName(name);
    }));

    py::class_<ExpressionName>(version, "ExpressionName").def(py::init([](const std::string& name) {
        return ExpressionName(name);
    }));

    py::class_<RegexName>(version, "RegexName").def(py::init([](const std::string& name) { return RegexName(name); }));

    py::class_<ExpressionNode, std::shared_ptr<ExpressionNode>>(version, "ExpressionNode")
            .def(py::init([](VariantNode condition, VariantNode left, VariantNode right, OperationType operation_type) {
                return ExpressionNode(condition, left, right, operation_type);
            }))
            .def(py::init([](VariantNode left, VariantNode right, OperationType operation_type) {
                return ExpressionNode(left, right, operation_type);
            }))
            .def(py::init([](VariantNode left, OperationType operation_type) {
                return ExpressionNode(left, operation_type);
            }));

    py::enum_<PipelineOptimisation>(version, "PipelineOptimisation")
            .value("SPEED", PipelineOptimisation::SPEED)
            .value("MEMORY", PipelineOptimisation::MEMORY);

    py::enum_<Slicing>(version, "Slicing")
            .value("NoSlicing", Slicing::NoSlicing)
            .value("RowSlicing", Slicing::RowSlicing);

    py::class_<ExpressionContext, std::shared_ptr<ExpressionContext>>(version, "ExpressionContext")
            .def(py::init())
            .def("add_expression_node", &ExpressionContext::add_expression_node)
            .def("add_value", &ExpressionContext::add_value)
            .def("add_value_set", &ExpressionContext::add_value_set)
            .def("add_regex", &ExpressionContext::add_regex)
            .def_readwrite("root_node_name", &ExpressionContext::root_node_name_);

    py::class_<UpdateQuery>(version, "PythonVersionStoreUpdateQuery")
            .def(py::init())
            .def_readwrite("row_filter", &UpdateQuery::row_filter);

    py::class_<KeySizesInfo>(version, "KeySizesInfo")
            .def(py::init())
            .def_readonly("count", &KeySizesInfo::count)
            .def_readonly("compressed_size", &KeySizesInfo::compressed_size)
            .doc() = "Count of keys and their compressed and uncompressed sizes in bytes.";

    py::class_<storage::ObjectSizes>(version, "ObjectSizes")
            .def_readonly("key_type", &storage::ObjectSizes::key_type_)
            .def_property_readonly("count", [](storage::ObjectSizes& self) { return self.count_.load(); })
            .def_property_readonly(
                    "compressed_size", [](storage::ObjectSizes& self) { return self.compressed_size_.load(); }
            )
            .def("__repr__", [](storage::ObjectSizes object_sizes) { return fmt::format("{}", object_sizes); })
            .doc() = "Count of keys and their uncompressed sizes in bytes for a given key type";

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
                        auto handler_data =
                                TypeHandlerRegistry::instance()->get_handler_data(read_options.output_format());
                        return adapt_read_df(
                                v.read_dataframe_version(sid, version_query, read_query, read_options, handler_data),
                                &handler_data
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
                        auto handler_data =
                                TypeHandlerRegistry::instance()->get_handler_data(batch_read_options.output_format());
                        return python_util::adapt_read_dfs(
                                v.batch_read(
                                        stream_ids, version_queries, read_queries, batch_read_options, handler_data
                                ),
                                &handler_data
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
                        auto handler_data = TypeHandlerRegistry::instance()->get_handler_data(output_format);
                        return adapt_read_df(
                                v.batch_read_and_join(
                                        std::make_shared<std::vector<StreamId>>(std::move(stream_ids)),
                                        std::make_shared<std::vector<VersionQuery>>(std::move(version_queries)),
                                        read_queries,
                                        read_options,
                                        std::move(_clauses),
                                        handler_data
                                ),
                                &handler_data
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

    py::class_<ManualClockVersionStore, PythonVersionStore>(version, "ManualClockVersionStore")
            .def(py::init<const std::shared_ptr<storage::Library>&>())
            .def_property_static(
                    "time",
                    [](const py::class_<ManualClockVersionStore>& /*self*/) { return util::ManualClock::time_.load(); },
                    [](const py::class_<ManualClockVersionStore>& /*self*/, entity::timestamp ts) {
                        util::ManualClock::time_ = ts;
                    }
            );

    py::class_<LocalVersionedEngine>(version, "VersionedEngine").def(py::init<std::shared_ptr<storage::Library>>());

    version.def("sorted_value_name", [](SortedValue sorted_value) {
        switch (sorted_value) {
        case SortedValue::UNKNOWN:
            return "UNKNOWN";
        case SortedValue::ASCENDING:
            return "ASCENDING";
        case SortedValue::DESCENDING:
            return "DESCENDING";
        case SortedValue::UNSORTED:
            return "UNSORTED";
        default:
            util::raise_rte("Unknown sorted value: {}", static_cast<uint8_t>(sorted_value));
        }
    });

    version.def("write_dataframe_to_file", &write_dataframe_to_file);

    version.def("read_dataframe_from_file", &read_dataframe_from_file);
}

} // namespace arcticdb::version_store
