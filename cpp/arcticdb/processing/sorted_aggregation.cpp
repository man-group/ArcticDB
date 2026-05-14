/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/column_algorithms.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/processing/aggregation_utils.hpp>
#include <arcticdb/processing/clause_utils.hpp>
#include <arcticdb/processing/sorted_aggregation.hpp>
#include <arcticdb/util/type_traits.hpp>
#include <folly/container/Enumerate.h>

namespace arcticdb {
template<typename InputTypeInfo, typename OutputTypeInfo>
requires util::instantiation_of<InputTypeInfo, ScalarTypeInfo> && util::instantiation_of<OutputTypeInfo, ScalarTypeInfo>
consteval bool is_aggregation_allowed(const AggregationOperator aggregation_operator) {
    return (is_numeric_type(InputTypeInfo::data_type) && is_numeric_type(OutputTypeInfo::data_type)) ||
           (is_sequence_type(InputTypeInfo::data_type) &&
            (is_sequence_type(OutputTypeInfo::data_type) || aggregation_operator == AggregationOperator::COUNT)) ||
           (is_bool_type(InputTypeInfo::data_type) &&
            (is_bool_type(OutputTypeInfo::data_type) || is_numeric_type(OutputTypeInfo::data_type)));
}

template<typename OutputTypeInfo>
requires util::instantiation_of<OutputTypeInfo, ScalarTypeInfo>
consteval bool is_aggregation_allowed(const AggregationOperator aggregation_operator) {
    return is_numeric_type(OutputTypeInfo::data_type) || is_bool_type(OutputTypeInfo::data_type) ||
           (is_sequence_type(OutputTypeInfo::data_type) &&
            (aggregation_operator == AggregationOperator::FIRST || aggregation_operator == AggregationOperator::LAST));
}

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
DataType SortedAggregator<aggregation_operator, closed_boundary>::generate_output_data_type(
        const DataType common_input_data_type
) const {
    DataType output_type{common_input_data_type};
    if constexpr (aggregation_operator == AggregationOperator::SUM) {
        // Deal with overflow as best we can
        if (is_unsigned_type(common_input_data_type) || is_bool_type(common_input_data_type)) {
            output_type = DataType::UINT64;
        } else if (is_signed_type(common_input_data_type)) {
            output_type = DataType::INT64;
        } else if (is_floating_point_type(common_input_data_type)) {
            output_type = DataType::FLOAT64;
        }
    } else if constexpr (aggregation_operator == AggregationOperator::MEAN) {
        if (!is_time_type(common_input_data_type)) {
            output_type = DataType::FLOAT64;
        }
    } else if constexpr (aggregation_operator == AggregationOperator::COUNT) {
        output_type = DataType::UINT64;
    }
    return output_type;
}

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
SortedAggregatorOutputColumnInfo SortedAggregator<aggregation_operator, closed_boundary>::generate_common_input_type(
        const std::span<const std::optional<ColumnWithStrings>> input_agg_columns
) const {
    SortedAggregatorOutputColumnInfo output_column_info;
    for (const auto& opt_input_agg_column : input_agg_columns) {
        if (opt_input_agg_column.has_value()) {
            auto input_data_type = opt_input_agg_column->column_->type().data_type();
            check_aggregator_supported_with_data_type(input_data_type);
            add_data_type_impl(input_data_type, output_column_info.data_type_);
        } else {
            output_column_info.maybe_sparse_ = true;
        }
    }
    return output_column_info;
}

template<ResampleBoundary closed_boundary>
std::pair<std::shared_ptr<Column>, ResampleMapping> generate_output_index_column(
        const std::vector<std::shared_ptr<Column>>& input_index_columns,
        const std::vector<timestamp>& bucket_boundaries, const TimestampRange& date_range,
        const ResampleBoundary label_boundary
) {
    constexpr auto data_type = DataType::NANOSECONDS_UTC64;
    using IndexTDT = ScalarTagType<DataTypeTag<data_type>>;

    // The output row count is bounded by both the number of buckets and the number of input rows
    size_t total_input_rows{0};
    for (const auto& col : input_index_columns) {
        total_input_rows += col->row_count();
    }
    const auto max_output_rows = std::min(bucket_boundaries.size() - 1, total_input_rows);
    // Below this rows/bucket threshold, element-by-element iteration benchmarked to beat galloping search.
    constexpr size_t linear_scan_threshold = 32;
    const size_t num_buckets = bucket_boundaries.size() > 1 ? bucket_boundaries.size() - 1 : 1;
    const bool use_linear_scan = (total_input_rows / num_buckets) < linear_scan_threshold;
    const auto max_index_column_bytes = max_output_rows * get_type_size(data_type);
    auto output_index_column = std::make_shared<Column>(
            TypeDescriptor(data_type, Dimension::Dim0),
            Sparsity::NOT_PERMITTED,
            ChunkedBuffer::presized_in_blocks(max_index_column_bytes)
    );
    auto output_index_column_data = output_index_column->data();
    auto output_index_column_it = output_index_column_data.template begin<IndexTDT>();
    size_t output_index_column_row_count{0};

    ResampleMapping mapping;
    mapping.reserve(max_output_rows + 1);

    // Largest value contained in the bucket that ends at `bucket_end_value`.
    // LEFT-closed [_, end) → end - 1; RIGHT-closed (_, end] → end.
    constexpr auto inclusive_bucket_end = [](timestamp bucket_end_value) {
        if constexpr (closed_boundary == ResampleBoundary::LEFT) {
            return bucket_end_value - 1;
        } else {
            return bucket_end_value;
        }
    };

    auto bucket_end_it = std::next(bucket_boundaries.cbegin());
    Bucket<closed_boundary> current_bucket{*std::prev(bucket_end_it), *bucket_end_it};
    bool current_bucket_added_to_index{false};
    bool reached_end_of_buckets{false};
    ResampleInputCursor close_cursor{input_index_columns.size(), 0};
    for (auto&& [col_idx, input_index_column] : folly::enumerate(input_index_columns)) {
        if (reached_end_of_buckets) {
            break;
        }
        auto index_column_data = input_index_column->data();
        const auto cend = index_column_data.template cend<IndexTDT, IteratorType::ENUMERATED>();
        auto it = index_column_data.template cbegin<IndexTDT, IteratorType::ENUMERATED>();
        // Skip rows before the date range start and rows before the current bucket
        it = exponential_upper_bound(
                it, cend, std::max(date_range.first - 1, inclusive_bucket_end(*std::prev(bucket_end_it)))
        );

        auto advance_it = [&]() {
            const auto advance_until = std::min(date_range.second, inclusive_bucket_end(*bucket_end_it));
            if (use_linear_scan) {
                while (ARCTICDB_LIKELY(it != cend && it->value() <= advance_until)) {
                    ++it;
                }
            } else {
                ++it;
                it = exponential_upper_bound(it, cend, advance_until);
            }
        };
        for (; it != cend && it->value() <= date_range.second; advance_it()) {
            if (ARCTICDB_LIKELY(!current_bucket.contains(it->value()))) {
                advance_boundary_past_value<closed_boundary>(bucket_boundaries, bucket_end_it, it->value());

                if (bucket_end_it == bucket_boundaries.end()) {
                    close_cursor = {col_idx, static_cast<size_t>(it->idx())};
                    reached_end_of_buckets = true;
                    break;
                }

                current_bucket.set_boundaries(*std::prev(bucket_end_it), *bucket_end_it);
                current_bucket_added_to_index = false;
            }

            if (ARCTICDB_LIKELY(!current_bucket_added_to_index)) {
                *output_index_column_it++ =
                        label_boundary == ResampleBoundary::LEFT ? *std::prev(bucket_end_it) : *bucket_end_it;
                ++output_index_column_row_count;
                mapping.push_back({col_idx, static_cast<size_t>(it->idx())});
                current_bucket_added_to_index = true;
            }
        }
        if (!reached_end_of_buckets && it != cend) {
            // Stopped because *it > date_range.second
            close_cursor = {col_idx, static_cast<size_t>(it->idx())};
            reached_end_of_buckets = true;
        }
    }
    mapping.push_back(close_cursor);
    const auto actual_index_column_bytes = output_index_column_row_count * get_type_size(data_type);
    output_index_column->buffer().trim(actual_index_column_bytes);
    output_index_column->set_row_data(output_index_column_row_count - 1);
    return {std::move(output_index_column), std::move(mapping)};
}

template std::pair<std::shared_ptr<Column>, ResampleMapping> generate_output_index_column<ResampleBoundary::LEFT>(
        const std::vector<std::shared_ptr<Column>>&, const std::vector<timestamp>&, const TimestampRange&,
        ResampleBoundary
);
template std::pair<std::shared_ptr<Column>, ResampleMapping> generate_output_index_column<ResampleBoundary::RIGHT>(
        const std::vector<std::shared_ptr<Column>>&, const std::vector<timestamp>&, const TimestampRange&,
        ResampleBoundary
);

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
std::optional<Column> SortedAggregator<aggregation_operator, closed_boundary>::generate_resampling_output_column(
        const std::span<const std::optional<ColumnWithStrings>> input_agg_columns, const ResampleMapping& mapping
) const {
    const SortedAggregatorOutputColumnInfo type_info = generate_common_input_type(input_agg_columns);
    if (!type_info.data_type_) {
        return std::nullopt;
    }
    const int64_t output_row_count = static_cast<int64_t>(mapping.size()) - 1;
    if (!type_info.maybe_sparse_) {
        return Column(
                make_scalar_type(generate_output_data_type(*type_info.data_type_)),
                output_row_count,
                AllocationType::PRESIZED,
                Sparsity::NOT_PERMITTED
        );
    }

    util::BitSet sparse_map(output_row_count);
    for (int64_t out_row = 0; out_row < output_row_count; ++out_row) {
        const auto& start = mapping[out_row];
        const auto& end = mapping[out_row + 1];
        const size_t last_contributing_exclusive = end.input_column_idx + (end.offset > 0 ? 1 : 0);
        for (size_t col_idx = start.input_column_idx;
             col_idx < last_contributing_exclusive && col_idx < input_agg_columns.size();
             ++col_idx) {
            if (input_agg_columns[col_idx].has_value()) {
                sparse_map.set(out_row);
                break;
            }
        }
    }
    const Sparsity sparsity = sparse_map.count() == sparse_map.size() ? Sparsity::NOT_PERMITTED : Sparsity::PERMITTED;
    const int64_t row_count = sparsity == Sparsity::PERMITTED ? sparse_map.count() : output_row_count;
    Column result(
            make_scalar_type(generate_output_data_type(*type_info.data_type_)),
            row_count,
            AllocationType::PRESIZED,
            sparsity
    );
    if (sparsity == Sparsity::PERMITTED) {
        result.set_sparse_map(std::move(sparse_map));
    }
    result.set_row_data(output_row_count - 1);
    return result;
}

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
std::optional<Column> SortedAggregator<aggregation_operator, closed_boundary>::aggregate(
        const std::vector<std::shared_ptr<Column>>& input_index_columns,
        const std::vector<std::optional<ColumnWithStrings>>& input_agg_columns, const ResampleMapping& mapping,
        StringPool& string_pool
) const {
    std::optional<Column> res = generate_resampling_output_column(input_agg_columns, mapping);
    if (!res) {
        return std::nullopt;
    }
    details::visit_type(res->type().data_type(), [&](auto output_type_desc_tag) {
        using output_type_info = ScalarTypeInfo<decltype(output_type_desc_tag)>;
        // Need this here to only generate valid get_bucket_aggregator code, exception will have been thrown earlier at
        // runtime
        if constexpr (is_aggregation_allowed<output_type_info>(aggregation_operator)) {
            auto bucket_aggregator = get_bucket_aggregator<output_type_info>();
            auto output_data = res->data();
            const auto run = [&]<IteratorDensity output_density>() {
                auto output_it = output_data.template begin<
                        typename output_type_info::TDT,
                        IteratorType::ENUMERATED,
                        output_density>();
                const auto output_end =
                        output_data
                                .template end<typename output_type_info::TDT, IteratorType::ENUMERATED, output_density>(
                                );
                if (output_it == output_end) {
                    return;
                }
                size_t start_col_idx = mapping[output_it->idx()].input_column_idx;
                size_t start_col_offset = mapping[output_it->idx()].offset;
                size_t end_col_idx = mapping[output_it->idx() + 1].input_column_idx;
                size_t end_col_offset = mapping[output_it->idx() + 1].offset;
                const auto advance_output = [&]() {
                    output_it->value() =
                            finalize_aggregator<output_type_info::data_type>(bucket_aggregator, string_pool);
                    ++output_it;
                    if (output_it != output_end) {
                        start_col_idx = mapping[output_it->idx()].input_column_idx;
                        start_col_offset = mapping[output_it->idx()].offset;
                        end_col_idx = mapping[output_it->idx() + 1].input_column_idx;
                        end_col_offset = mapping[output_it->idx() + 1].offset;
                    }
                };
                for (size_t col_idx = 0; col_idx < input_agg_columns.size() && output_it != output_end; ++col_idx) {
                    // Finalise any buckets whose exclusive end falls at or before the start of this column.
                    while (output_it != output_end &&
                           (col_idx > end_col_idx || (col_idx == end_col_idx && end_col_offset == 0))) {
                        advance_output();
                    }
                    if (output_it == output_end) {
                        break;
                    }
                    if (col_idx < start_col_idx) {
                        continue;
                    }
                    const auto& opt_input_agg_column = input_agg_columns[col_idx];
                    if (!opt_input_agg_column.has_value()) {
                        continue;
                    }
                    const auto& agg_column = *opt_input_agg_column;
                    const auto& input_index_column = input_index_columns[col_idx];
                    details::visit_type(
                            agg_column.column_->type().data_type(),
                            [&, &agg_column = agg_column](auto input_type_desc_tag) {
                                using input_type_info = ScalarTypeInfo<decltype(input_type_desc_tag)>;
                                if constexpr (is_aggregation_allowed<input_type_info, output_type_info>(
                                                      aggregation_operator
                                              )) {
                                    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                                            !agg_column.column_->is_sparse() &&
                                                    agg_column.column_->row_count() == input_index_column->row_count(),
                                            "Not implemented yet: Cannot aggregate sparse column '{}' during "
                                            "resampling.",
                                            get_input_column_name().value
                                    );
                                    auto agg_data = agg_column.column_->data();
                                    auto col_it = agg_data.template cbegin<
                                            typename input_type_info::TDT,
                                            IteratorType::ENUMERATED,
                                            IteratorDensity::DENSE>();
                                    const auto col_end = agg_data.template cend<
                                            typename input_type_info::TDT,
                                            IteratorType::ENUMERATED,
                                            IteratorDensity::DENSE>();
                                    for (; col_it != col_end && output_it != output_end; ++col_it) {
                                        const auto idx = static_cast<size_t>(col_it->idx());
                                        // After advance_output, the next bucket may not include this column.
                                        if (col_idx < start_col_idx) {
                                            break;
                                        }
                                        // Skip rows before the bucket's start (e.g., right-closed bucket excluding
                                        // its leftmost edge, or date_range trimming).
                                        if (col_idx == start_col_idx && idx < start_col_offset) {
                                            continue;
                                        }
                                        push_to_aggregator<input_type_info::data_type>(
                                                bucket_aggregator, col_it->value(), agg_column
                                        );
                                        if (col_idx == end_col_idx && idx + 1 == end_col_offset) {
                                            advance_output();
                                        }
                                    }
                                }
                            }
                    );
                }
                // The trailing bucket (whose exclusive end is past the last input column) is finalised here.
                if (output_it != output_end) {
                    advance_output();
                }
                util::check(output_it == output_end, "Resample aggregation finished without consuming all output rows");
            };
            if (res->is_sparse()) {
                run.template operator()<IteratorDensity::SPARSE>();
            } else {
                run.template operator()<IteratorDensity::DENSE>();
            }
        }
    });
    return res;
}

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
void SortedAggregator<aggregation_operator, closed_boundary>::check_aggregator_supported_with_data_type(
        DataType data_type
) const {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            (is_time_type(data_type) && aggregation_operator != AggregationOperator::SUM) ||
                    (is_numeric_type(data_type) && !is_time_type(data_type)) || is_bool_type(data_type) ||
                    (is_sequence_type(data_type) && (aggregation_operator == AggregationOperator::FIRST ||
                                                     aggregation_operator == AggregationOperator::LAST ||
                                                     aggregation_operator == AggregationOperator::COUNT)),
            "Resample: Unsupported aggregation type {} on column '{}' of type {}",
            aggregation_operator,
            get_input_column_name().value,
            data_type
    );
}

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
std::optional<Value> SortedAggregator<aggregation_operator, closed_boundary>::get_default_value(
        const DataType common_input_data_type
) const {
    if constexpr (aggregation_operator == AggregationOperator::SUM) {
        return details::visit_type(
                generate_output_data_type(common_input_data_type),
                [&](auto tag) -> std::optional<Value> {
                    using data_type_tag = decltype(tag);
                    return Value{typename data_type_tag::raw_type{0}, data_type_tag::data_type};
                }
        );
    } else {
        return {};
    }
}

template class SortedAggregator<AggregationOperator::SUM, ResampleBoundary::LEFT>;
template class SortedAggregator<AggregationOperator::SUM, ResampleBoundary::RIGHT>;
template class SortedAggregator<AggregationOperator::MIN, ResampleBoundary::LEFT>;
template class SortedAggregator<AggregationOperator::MIN, ResampleBoundary::RIGHT>;
template class SortedAggregator<AggregationOperator::MAX, ResampleBoundary::LEFT>;
template class SortedAggregator<AggregationOperator::MAX, ResampleBoundary::RIGHT>;
template class SortedAggregator<AggregationOperator::MEAN, ResampleBoundary::LEFT>;
template class SortedAggregator<AggregationOperator::MEAN, ResampleBoundary::RIGHT>;
template class SortedAggregator<AggregationOperator::FIRST, ResampleBoundary::LEFT>;
template class SortedAggregator<AggregationOperator::FIRST, ResampleBoundary::RIGHT>;
template class SortedAggregator<AggregationOperator::LAST, ResampleBoundary::LEFT>;
template class SortedAggregator<AggregationOperator::LAST, ResampleBoundary::RIGHT>;
template class SortedAggregator<AggregationOperator::COUNT, ResampleBoundary::LEFT>;
template class SortedAggregator<AggregationOperator::COUNT, ResampleBoundary::RIGHT>;

} // namespace arcticdb
