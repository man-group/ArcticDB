/*
 Copyright 2026 Man Group Operations Limited

 Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

 As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
 be governed by the Apache License, version 2.0.
 */
#include <arcticdb/pipeline/column_stats_filter.hpp>
#include <arcticdb/pipeline/column_stats_dispatch.hpp>

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/index_fields.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/stream/stream_utils.hpp>

#include <iterator>
#include <unordered_set>

namespace arcticdb {

bool is_column_stats_enabled() { return ConfigsMap::instance()->get_int("ColumnStats.UseForQueries", 0) == 1; }

bool ColumnStatsQueryMetadata::should_try_column_stats_read() const {
    return is_column_stats_enabled() && filter_expression != nullptr;
}

StatsVariantData dispatch_unary_stats(const StatsVariantData& left, OperationType operation);

StatsVariantData evaluate_ast_node_against_stats(
        const ExpressionNode& node, const StatsRowIndices& row_indices, const ColumnStatsData& column_stats
) {
    return util::variant_match(
            node.kind_,
            [&](const ExpressionNode::Leaf& leaf) -> StatsVariantData {
                return util::variant_match(
                        leaf,
                        [&](const ColumnName& column_name) -> StatsVariantData {
                            return column_stats.values_for_column(column_name.value, row_indices);
                        },
                        [&](const std::shared_ptr<Value>& value) -> StatsVariantData { return value; },
                        [&](const std::shared_ptr<ValueSet>& value_set) -> StatsVariantData { return value_set; },
                        [&](const std::shared_ptr<util::RegexGeneric>&) -> StatsVariantData {
                            return std::vector(row_indices.size(), StatsComparison::UNKNOWN);
                        }
                );
            },
            [&](const ExpressionNode::Operation& op) -> StatsVariantData {
                if (is_binary_operation(op.operation_type_)) {
                    auto left = evaluate_ast_node_against_stats(*op.left_, row_indices, column_stats);
                    auto right = evaluate_ast_node_against_stats(*op.right_, row_indices, column_stats);
                    return dispatch_binary_stats(left, right, op.operation_type_);
                }
                if (is_unary_operation(op.operation_type_)) {
                    auto left = evaluate_ast_node_against_stats(*op.left_, row_indices, column_stats);
                    return dispatch_unary_stats(left, op.operation_type_);
                }
                return std::vector(row_indices.size(), StatsComparison::UNKNOWN);
            }
    );
}

StatsVariantData dispatch_binary_stats(
        const StatsVariantData& left, const StatsVariantData& right, OperationType operation
) {
    switch (operation) {
    case OperationType::GT:
        return column_stats_detail::visit_binary_comparator_stats<GreaterThanOperator>(left, right);
    case OperationType::GE:
        return column_stats_detail::visit_binary_comparator_stats<GreaterThanEqualsOperator>(left, right);
    case OperationType::LT:
        return column_stats_detail::visit_binary_comparator_stats<LessThanOperator>(left, right);
    case OperationType::LE:
        return column_stats_detail::visit_binary_comparator_stats<LessThanEqualsOperator>(left, right);
    case OperationType::EQ:
        return column_stats_detail::visit_binary_comparator_stats<EqualsOperator>(left, right);
    case OperationType::NE:
        return column_stats_detail::visit_binary_comparator_stats<NotEqualsOperator>(left, right);
    case OperationType::AND:
    case OperationType::OR:
    case OperationType::XOR:
        return column_stats_detail::visit_binary_boolean_stats(left, right, operation);
    case OperationType::ISIN:
    case OperationType::ISNOTIN:
        return column_stats_detail::visit_binary_membership_stats(left, right, operation);
    default: {
        // Not yet implemented: ADD SUB MUL DIV (binary operators) Monday: 11292578954
        size_t sz =
                std::max(column_stats_detail::stats_variant_size(left), column_stats_detail::stats_variant_size(right));
        return std::vector(sz, StatsComparison::UNKNOWN);
    }
    }
}

StatsVariantData dispatch_unary_stats(const StatsVariantData& left, OperationType operation) {
    switch (operation) {
    case OperationType::NOT:
    case OperationType::IDENTITY:
        return column_stats_detail::visit_unary_boolean_stats(left, operation);
    case OperationType::ISNULL:
    case OperationType::NOTNULL:
        return column_stats_detail::visit_unary_null_stats(left, operation);
    default:
        ARCTICDB_DEBUG(log::version(), "Unsupported unary operator for stats {}", operation);
        return util::variant_match(
                left,
                [](const std::vector<StatsComparison>& comparisons) -> StatsVariantData {
                    return std::vector(comparisons.size(), StatsComparison::UNKNOWN);
                },
                [](const std::vector<ColumnStatsValues>& values) -> StatsVariantData {
                    return std::vector(values.size(), StatsComparison::UNKNOWN);
                },
                [](const std::shared_ptr<Value>&) -> StatsVariantData {
                    util::raise_rte("Do not expect a Value in dispatch_unary_stats!");
                },
                [](const std::shared_ptr<ValueSet>&) -> StatsVariantData {
                    util::raise_rte("Do not expect a ValueSet in dispatch_unary_stats!");
                }
        );
    }
}

namespace {
std::vector<StatsMetadataForColumn> calculate_stats_metadata(
        const SegmentInMemory& segment, const TimeseriesDescriptor& tsd,
        arcticc::pb2::column_stats_pb2::ColumnStatsHeader header, const FieldCollection& fields
) {
    // Gather metadata about the statistics we're interested in
    std::vector<StatsMetadataForColumn> stats_metadata;
    stats_metadata.reserve(header.stats_by_column().size());
    for (const auto& [data_col_offset, entry_list] : header.stats_by_column()) {
        StatsMetadataForColumn stats_metadata_for_column;
        util::check(
                data_col_offset < tsd.fields().size(),
                "Expected data_col_offset < tsd.fields().size() but saw data_col_offset=[{}] tsd.fields().size()=[{}]",
                data_col_offset,
                tsd.fields().size()
        );
        stats_metadata_for_column.col_name = std::string{tsd.fields().at(data_col_offset).name()};
        for (const auto& entry : entry_list.entries()) {
            const auto entry_type = entry.type();
            const bool is_min_max = entry_type == arcticc::pb2::column_stats_pb2::MIN_V1 ||
                                    entry_type == arcticc::pb2::column_stats_pb2::MAX_V1;
            const bool is_count = entry_type == arcticc::pb2::column_stats_pb2::ISNULL_COUNT_V1;
            if (!is_min_max && !is_count) {
                log::version().warn(
                        "Unknown column stats type {} for column {}, skipping",
                        static_cast<int>(entry_type),
                        stats_metadata_for_column.col_name
                );
                continue;
            }
            const auto field_name = to_segment_column_name(stats_metadata_for_column.col_name, entry_type);
            const auto col_index = segment.column_index(field_name);
            if (!col_index.has_value()) {
                // Column was filtered out at decode time, or never present in this segment.
                continue;
            }
            // ISNULL_COUNT is always UINT64 and tracked separately; only MIN/MAX define the
            // column's value data type.
            if (is_min_max) {
                const auto entry_data_type = fields.at(*col_index).type().data_type();
                if (stats_metadata_for_column.data_type == DataType::UNKNOWN) {
                    stats_metadata_for_column.data_type = entry_data_type;
                } else {
                    util::check(
                            stats_metadata_for_column.data_type == entry_data_type,
                            "MIN/MAX stats columns for {} disagree on data type",
                            stats_metadata_for_column.col_name
                    );
                }
            }
            stats_metadata_for_column.entries.push_back({*col_index, entry_type});
        }
        if (!stats_metadata_for_column.entries.empty()) {
            stats_metadata.emplace_back(std::move(stats_metadata_for_column));
        }
    }
    return stats_metadata;
}

std::unordered_map<std::string, StatsForColumn> load_stats_by_column(
        const SegmentInMemory& segment, std::vector<StatsMetadataForColumn> stats_metadata, size_t first_kept,
        size_t last_kept_excl, size_t num_rows
) {
    using namespace arcticc::pb2::column_stats_pb2;
    std::unordered_map<std::string, StatsForColumn> stats_by_column;
    for (auto& stats_metadata_for_column : stats_metadata) {
        StatsForColumn stats_for_column;
        stats_for_column.mins.resize(num_rows);
        stats_for_column.maxes.resize(num_rows);
        stats_for_column.isnull_counts.resize(num_rows, 0);

        // data_type stays UNKNOWN when every row-slice this column's stats were computed over was
        // entirely null, leaving only ISNULL_COUNT
        // entries below - nothing to decode here, mins/maxes stay all-absent.
        if (stats_metadata_for_column.data_type != DataType::UNKNOWN) {
            details::visit_type(stats_metadata_for_column.data_type, [&]<typename T>(T) {
                using type_info = ScalarTypeInfo<T>;
                if constexpr (is_numeric_type(type_info::data_type) || is_time_type(type_info::data_type) ||
                              is_bool_type(type_info::data_type)) {
                    for (const auto& entry : stats_metadata_for_column.entries) {
                        if (entry.stat_type != MIN_V1 && entry.stat_type != MAX_V1) {
                            continue;
                        }
                        const auto& column = segment.column(static_cast<position_t>(entry.segment_col_idx));
                        const bool is_min = entry.stat_type == MIN_V1;
                        auto& dest = is_min ? stats_for_column.mins : stats_for_column.maxes;
                        for_each_enumerated<typename type_info::TDT>(
                                column,
                                [&](const ColumnData::Enumeration<typename type_info::RawType>& enumerating_it) {
                                    auto idx = static_cast<size_t>(enumerating_it.idx());
                                    if (idx >= first_kept && idx < last_kept_excl) {
                                        dest.at(idx - first_kept) = Value{enumerating_it.value(), type_info::data_type};
                                    }
                                }
                        );
                    }
                }
            });
        }

        // The isnull count is stored inline with min/max as a dense UINT64 column.
        using CountTDT = ScalarTagType<DataTypeTag<DataType::UINT64>>;
        for (const auto& entry : stats_metadata_for_column.entries) {
            if (entry.stat_type != ISNULL_COUNT_V1) {
                continue;
            }
            const auto& column = segment.column(static_cast<position_t>(entry.segment_col_idx));
            for_each_enumerated<CountTDT>(column, [&](const ColumnData::Enumeration<uint64_t>& enumerating_it) {
                auto idx = static_cast<size_t>(enumerating_it.idx());
                if (idx >= first_kept && idx < last_kept_excl) {
                    stats_for_column.isnull_counts.at(idx - first_kept) = enumerating_it.value();
                }
            });
        }

        stats_by_column.emplace(std::move(stats_metadata_for_column.col_name), std::move(stats_for_column));
    }
    return stats_by_column;
}

} // anonymous namespace

std::pair<size_t, size_t> ColumnStatsData::parse_row_ranges(
        const std::optional<pipelines::RowRange>& window, const size_t segment_row_count, const Column& start_row_col,
        const Column& end_row_col
) {
    // Build the row range lookup, skipping row slices outside the window.
    // Also construct the interval [first_kept, last_kept_excl) to quickly skip stats for row ranges
    // we are not interested in when we read the statistics themselves.
    size_t first_kept = segment_row_count;
    size_t last_kept_excl = 0;
    row_range_to_row_.reserve(segment_row_count);
    using RowTDT = ScalarTagType<DataTypeTag<DataType::UINT64>>;
    auto start_data = start_row_col.data();
    auto end_data = end_row_col.data();
    auto start_it = start_data.begin<RowTDT>();
    auto end_it = end_data.begin<RowTDT>();
    uint64_t prev_start = 0;
    for (size_t r = 0; r < segment_row_count; ++r, ++start_it, ++end_it) {
        const uint64_t start_row = *start_it;
        const uint64_t end_row = *end_it;
        if (r > 0) {
            util::check(
                    start_row > prev_start,
                    "Column stats segment start_row must be strictly monotonically increasing "
                    "(violated at row {})",
                    r
            );
        }
        prev_start = start_row;
        if (window.has_value()) {
            // Sorted on start_row, so once a slice starts at or after the window nothing later can
            // intersect it.
            if (start_row >= window->second) {
                break;
            }
            if (end_row <= window->first) {
                continue;
            }
        }
        if (first_kept == segment_row_count) {
            first_kept = r;
        }
        last_kept_excl = r + 1;
        // r - first_kept is the same convention load_stats_by_column uses to index the value vectors
        auto [_, inserted] = row_range_to_row_.emplace(pipelines::RowRange{start_row, end_row}, r - first_kept);
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                inserted, "Duplicate row range [{}, {}) in column stats segment at row {}", start_row, end_row, r
        );
        row_counts_.push_back(end_row - start_row);
    }
    return std::make_pair(first_kept, last_kept_excl);
}

ColumnStatsData::ColumnStatsData(
        SegmentInMemory&& segment, const TimeseriesDescriptor& tsd, const std::optional<pipelines::RowRange>& window
) {
    using namespace arcticc::pb2::column_stats_pb2;
    if (segment.row_count() == 0) {
        return;
    }

    ColumnStatsHeader header;
    auto* metadata = segment.metadata();
    util::check(metadata != nullptr, "Column stats segment has no metadata");
    bool unpacked = metadata->UnpackTo(&header);
    util::check(unpacked, "Could not unpack ColumnStatsHeader from column stats segment metadata");
    validate_column_stats_header_version(header, ColumnStatsHeaderVersionMismatchAction::Warn);

    segment.init_column_map();
    const auto& fields = segment.descriptor().fields();
    const auto segment_row_count = segment.row_count();

    const auto& start_row_col = segment.column(start_row_column_offset);
    const auto& end_row_col = segment.column(end_row_column_offset);
    if (start_row_col.is_sparse() || end_row_col.is_sparse() ||
        static_cast<size_t>(start_row_col.row_count()) != segment_row_count ||
        static_cast<size_t>(end_row_col.row_count()) != segment_row_count) {
        log::version().warn("Saw column stats row without start_row or end_row, discarding all column stats");
        return;
    }

    std::vector<StatsMetadataForColumn> stats_metadata = calculate_stats_metadata(segment, tsd, header, fields);

    auto [first_kept, last_kept_excl] = parse_row_ranges(window, segment_row_count, start_row_col, end_row_col);
    num_rows_ = last_kept_excl > first_kept ? last_kept_excl - first_kept : 0;
    if (num_rows_ == 0) {
        return;
    }

    stats_by_column_ = load_stats_by_column(segment, stats_metadata, first_kept, last_kept_excl, num_rows_);
}

std::optional<size_t> ColumnStatsData::find_row(const pipelines::RowRange& row_range) const {
    if (auto it = row_range_to_row_.find(row_range); it != row_range_to_row_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::vector<ColumnStatsValues> ColumnStatsData::values_for_column(
        const std::string& col_name, const std::vector<std::optional<size_t>>& row_indices
) const {
    std::vector<ColumnStatsValues> result(row_indices.size());
    if (num_rows_ == 0) {
        return result;
    }
    auto it = stats_by_column_.find(col_name);
    if (it == stats_by_column_.end()) {
        return result;
    }
    const auto& stats = it->second;
    for (size_t i = 0; i < row_indices.size(); ++i) {
        const auto& maybe_row = row_indices.at(i);
        if (!maybe_row.has_value()) {
            continue;
        }
        const size_t r = *maybe_row;
        const bool min_set = stats.mins.at(r).has_value();
        const bool max_set = stats.maxes.at(r).has_value();
        util::check(min_set == max_set, "MIN and MAX should both be present or both be absent");
        auto& result_entry = result.at(i);
        if (min_set) {
            result_entry.min = stats.mins.at(r);
            result_entry.max = stats.maxes.at(r);
            result_entry.isnull_stats = IsNullStats{stats.isnull_counts.at(r), row_counts_.at(r)};
        } else if (stats.isnull_counts.at(r) > 0) {
            result_entry.isnull_stats = IsNullStats{stats.isnull_counts.at(r), row_counts_.at(r)};
        } else {
            result_entry.column_absent = true;
        }
    }
    return result;
}

FilterQuery<index::IndexSegmentReader> create_column_stats_filter(
        ColumnStatsData&& column_stats_data, ExpressionContext&& expression_context
) {
    return [column_stats_data = std::move(column_stats_data), expression_context = std::move(expression_context)](
                   const index::IndexSegmentReader& isr, std::unique_ptr<util::BitSet>&& input
           ) mutable {
        using namespace pipelines::index;

        std::unique_ptr<util::BitSet> res;
        if (input) {
            res = std::move(input);
        } else {
            res = std::make_unique<util::BitSet>(static_cast<util::BitSetSizeType>(isr.size()));
            res->invert();
        }

        StatsRowIndices row_indices;
        row_indices.reserve(isr.size());
        [[maybe_unused]] size_t total_count = 0; // for debug logging only, unused in release build
        for (size_t row = 0; row < isr.size(); ++row) {
            if (!res->get_bit(row)) {
                // Don't bother - we already know we don't need to look at the segment
                row_indices.emplace_back(std::nullopt);
                continue;
            }
            total_count++;
            row_indices.emplace_back(column_stats_data.find_row(slice_row_range_at(isr, row)));
        }
        util::check(row_indices.size() == isr.size(), "Expected row_indices.size() == isr.size()");

        // Evaluate the AST
        StatsVariantData result =
                evaluate_ast_node_against_stats(*expression_context.root_, row_indices, column_stats_data);
        util::check(
                std::holds_alternative<std::vector<StatsComparison>>(result),
                "evaluate_ast_node_against_stats should evaluate to a vector<StatsComparison>"
        );

        // Convert to BitSet
        size_t pruned_count = 0;
        const auto& comparisons = std::get<std::vector<StatsComparison>>(result);
        util::check(comparisons.size() == isr.size(), "Expected comparisons.size() == isr.size()");
        for (size_t row = 0; row < isr.size(); ++row) {
            if (comparisons.at(row) == StatsComparison::NONE_MATCH) {
                res->set_bit(row, false);
                pruned_count++;
            }
        }

        log::version().debug("Column stats filter pruned {} of {} segments", pruned_count, total_count);
        return res;
    };
}

ColumnStatsQueryMetadata::ColumnStatsQueryMetadata(const std::vector<std::shared_ptr<Clause>>& clauses) {
    // We apply column stats filtering to a "prefix" of clauses that are eligible based on the rules below.
    // Column stats are not used for any clauses after this "prefix".
    // - FilterClauses contribute filter expressions and columns of interest
    // - DateRangeClauses contribute their range
    // - A RowRangeClause ends the prefix unless it is the first clause, because a leading RowRangeClause
    //   selects absolute row positions. A RowRangeClause anywhere else is over a changed dataset,
    //   so a filter after it must not drive pruning (that would change which rows are "first/last N").
    // - Anything else (Resample / GroupBy / Project) ends the prefix because those clauses
    // transform the data so stats computed on the original segments are no longer valid.
    for (auto&& [idx, clause] : folly::enumerate(clauses)) {
        auto& clause_type = folly::poly_type(*clause);
        if (clause_type == typeid(DateRangeClause)) {
            const auto& date_range_clause = folly::poly_cast<DateRangeClause>(*clause);
            util::check(
                    !date_range.has_value(),
                    "Expected at most one DateRangeClause in the column stats prefix (date ranges are merged in "
                    "plan_query)"
            );
            date_range = std::make_pair(date_range_clause.start(), date_range_clause.end());
            continue;
        }
        if (clause_type == typeid(RowRangeClause)) {
            if (idx == 0) {
                continue;
            }
            break;
        }
        if (clause_type != typeid(FilterClause)) {
            break;
        }
        const auto& filter = folly::poly_cast<FilterClause>(*clause);
        util::check(
                filter_expression == nullptr,
                "Expected at most one FilterClause in the column stats prefix (filters are merged in plan_query)"
        );
        filter_expression = filter.expression_context_;
        for (const auto& col : filter.clause_info().input_columns_) {
            columns_of_interest.insert(col);
        }
    }
}

SegmentInMemory partial_decode_column_stats_segment(
        Segment& column_stats_segment, const TimeseriesDescriptor& tsd,
        const std::unordered_set<std::string>& columns_of_interest
) {
    using namespace arcticc::pb2::column_stats_pb2;

    auto maybe_metadata = decode_metadata_from_segment(column_stats_segment);
    util::check(maybe_metadata.has_value(), "Column stats segment has no metadata");
    ColumnStatsHeader header;
    bool unpacked = maybe_metadata->UnpackTo(&header);
    util::check(unpacked, "Could not unpack ColumnStatsHeader from column stats segment metadata");
    validate_column_stats_header_version(header, ColumnStatsHeaderVersionMismatchAction::Warn);

    std::unordered_set<std::string> retain_field_names;
    retain_field_names.insert(start_row_column_name);
    retain_field_names.insert(end_row_column_name);
    for (const auto& [data_col_offset, entry_list] : header.stats_by_column()) {
        std::string col_name{tsd.fields().at(data_col_offset).name()};
        if (!columns_of_interest.contains(col_name)) {
            continue;
        }
        for (const auto& entry : entry_list.entries()) {
            retain_field_names.insert(to_segment_column_name(col_name, entry.type()));
        }
    }

    // Preserve the order so start_row lands at offset 0 and end_row at offset 1, matching the
    // start_row_column_offset / end_row_column_offset constants used downstream.
    StreamDescriptor partial_desc;
    partial_desc.set_index(column_stats_segment.descriptor().index());
    for (const auto& field : column_stats_segment.descriptor().fields()) {
        if (retain_field_names.contains(std::string{field.name()})) {
            partial_desc.add_field(field);
        }
    }
    util::check(
            partial_desc.fields().size() >= 2 && partial_desc.fields(0).name() == start_row_column_name &&
                    partial_desc.fields(1).name() == end_row_column_name,
            "Expected start_row/end_row at the front of the column stats segment"
    );

    SegmentInMemory partial(std::move(partial_desc), 0, AllocationType::DYNAMIC);
    decode_into_memory_segment(
            column_stats_segment, column_stats_segment.header(), partial, column_stats_segment.descriptor()
    );
    return partial;
}

namespace {
// Restrict which rows in the column stats segment we parse.
// This is a single RowRange spanning all the row ranges that survived build_row_read_query_filters,
// narrowed by a DateRangeClause if the query had one. Empty if nothing survived.
pipelines::RowRange row_window_of_interest(
        const index::IndexSegmentReader& isr, const util::BitSet& surviving,
        const std::optional<std::pair<timestamp, timestamp>>& date_range
) {
    using namespace pipelines;
    std::optional<RowRange> window;
    auto extend_to = [&window, &isr](size_t row) {
        const auto slice_row_range = slice_row_range_at(isr, row);
        if (window.has_value()) {
            window = RowRange{
                    std::min(window->first, slice_row_range.first), std::max(window->second, slice_row_range.second)
            };
        } else {
            window = slice_row_range;
        }
    };

    // A DateRangeClause prunes ranges_and_keys in structure_for_processing, which runs after
    // filter_index, so it is not yet reflected in the bitset.
    if (date_range.has_value() && isr.has_timestamp_index()) {
        using TsTDT = stream::TimeseriesIndex::TypeDescTag;
        const IndexRange date_filter{date_range->first, date_range->second};
        auto start_index_it = isr.column(index::Fields::start_index).begin<TsTDT>();
        auto end_index_it = isr.column(index::Fields::end_index).begin<TsTDT>();
        for (size_t row = 0; row < isr.size(); ++row) {
            if (!surviving.get_bit(row)) {
                continue;
            }
            const IndexRange slice_index_range{*(start_index_it + row), *(end_index_it + row)};
            if (is_slice_in_index_range(slice_index_range, date_filter, true)) {
                extend_to(row);
            }
        }
    } else {
        for (size_t row = 0; row < isr.size(); ++row) {
            if (surviving.get_bit(row)) {
                extend_to(row);
            }
        }
    }
    return window.value_or(RowRange{0, 0});
}
} // namespace

FilterQuery<index::IndexSegmentReader> create_column_stats_filter(
        std::shared_ptr<Segment> column_stats_compressed, const TimeseriesDescriptor& tsd,
        ColumnStatsQueryMetadata&& query_metadata
) {
    util::check(
            query_metadata.should_try_column_stats_read(),
            "Should not try to create column stats filter if !should_try_column_stats_read()"
    );
    return [column_stats_compressed = std::move(column_stats_compressed),
            tsd,
            query_metadata = std::move(query_metadata
            )](const index::IndexSegmentReader& isr, std::unique_ptr<util::BitSet>&& input) mutable {
        std::unique_ptr<util::BitSet> surviving;
        if (input) {
            surviving = std::move(input);
        } else {
            surviving = std::make_unique<util::BitSet>(static_cast<util::BitSetSizeType>(isr.size()));
            surviving->invert();
        }

        auto window = row_window_of_interest(isr, *surviving, query_metadata.date_range);
        SegmentInMemory partial_segment =
                partial_decode_column_stats_segment(*column_stats_compressed, tsd, query_metadata.columns_of_interest);
        ColumnStatsData column_stats{std::move(partial_segment), tsd, window};
        ExpressionContext expression_context = *query_metadata.filter_expression;
        auto filter = create_column_stats_filter(std::move(column_stats), std::move(expression_context));
        return filter(isr, std::move(surviving));
    };
}

} // namespace arcticdb
