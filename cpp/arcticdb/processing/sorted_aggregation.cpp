/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/processing/aggregation_utils.hpp>
#include <arcticdb/processing/sorted_aggregation.hpp>

namespace arcticdb {

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
Column SortedAggregator<aggregation_operator, closed_boundary>::aggregate(const std::vector<std::shared_ptr<Column>>& input_index_columns,
                                                                          const std::vector<std::optional<ColumnWithStrings>>& input_agg_columns,
                                                                          const std::vector<timestamp>& bucket_boundaries,
                                                                          const Column& output_index_column,
                                                                          StringPool& string_pool) const {
    using IndexTDT = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
    auto common_input_type = generate_common_input_type(input_agg_columns);
    Column res(TypeDescriptor(generate_output_data_type(common_input_type), Dimension::Dim0), output_index_column.row_count(), true, false);
    details::visit_type(
        res.type().data_type(),
        [this,
        &input_index_columns,
        &input_agg_columns,
        &bucket_boundaries,
        &string_pool,
        &res](auto output_type_desc_tag) {
            using output_type_info = ScalarTypeInfo<decltype(output_type_desc_tag)>;
            auto output_data = res.data();
            auto output_it = output_data.begin<typename output_type_info::TDT>();
            auto output_end_it = output_data.end<typename output_type_info::TDT>();
            // Need this here to only generate valid get_bucket_aggregator code, exception will have been thrown earlier at runtime
            constexpr bool supported_aggregation_type_combo = is_numeric_type(output_type_info::data_type) ||
                                                              is_bool_type(output_type_info::data_type) ||
                                                              (is_sequence_type(output_type_info::data_type) &&
                                                               (aggregation_operator == AggregationOperator::FIRST ||
                                                                aggregation_operator == AggregationOperator::LAST));
            if constexpr (supported_aggregation_type_combo) {
                auto bucket_aggregator = get_bucket_aggregator<output_type_info>();
                bool reached_end_of_buckets{false};
                auto bucket_start_it = bucket_boundaries.cbegin();
                auto bucket_end_it = std::next(bucket_start_it);
                Bucket<closed_boundary> current_bucket(*bucket_start_it, *bucket_end_it);
                const auto bucket_boundaries_end = bucket_boundaries.cend();
                for (auto [idx, input_agg_column]: folly::enumerate(input_agg_columns)) {
                    // Always true right now due to earlier check
                    if (input_agg_column.has_value()) {
                        details::visit_type(
                            input_agg_column->column_->type().data_type(),
                            [this,
                            &output_it,
                            &bucket_aggregator,
                            &agg_column = *input_agg_column,
                            &input_index_column = input_index_columns.at(idx),
                            &bucket_boundaries_end,
                            &string_pool,
                            &bucket_start_it,
                            &bucket_end_it,
                            &current_bucket,
                            &reached_end_of_buckets](auto input_type_desc_tag) {
                                using input_type_info = ScalarTypeInfo<decltype(input_type_desc_tag)>;
                                // Again, only needed to generate valid code below, exception will have been thrown earlier at runtime
                                if constexpr ((is_numeric_type(input_type_info::data_type) && is_numeric_type(output_type_info::data_type)) ||
                                              (is_sequence_type(input_type_info::data_type) && (is_sequence_type(output_type_info::data_type) || aggregation_operator == AggregationOperator::COUNT)) ||
                                              (is_bool_type(input_type_info::data_type) && (is_bool_type(output_type_info::data_type) || is_numeric_type(output_type_info::data_type)))) {
                                    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                                            !agg_column.column_->is_sparse() && agg_column.column_->row_count() == input_index_column->row_count(),
                                            "Resample: Cannot aggregate column '{}' as it is sparse",
                                            get_input_column_name().value);
                                    auto index_data = input_index_column->data();
                                    const auto index_cend = index_data.template cend<IndexTDT>();
                                    auto agg_data = agg_column.column_->data();
                                    auto agg_it = agg_data.template cbegin<typename input_type_info::TDT>();
                                    for (auto index_it = index_data.template cbegin<IndexTDT>(); index_it != index_cend && !reached_end_of_buckets; ++index_it, ++agg_it) {
                                        if (ARCTICDB_LIKELY(current_bucket.contains(*index_it))) {
                                            push_to_aggregator<input_type_info::data_type>(bucket_aggregator, *agg_it, agg_column);
                                        } else if (ARCTICDB_LIKELY(index_value_past_end_of_bucket(*index_it, *bucket_end_it))) {
                                            *output_it++ = finalize_aggregator<output_type_info::data_type>(bucket_aggregator, string_pool);
                                            // The following code is equivalent to:
                                            // if constexpr (closed_boundary == ResampleBoundary::LEFT) {
                                            //     bucket_end_it = std::upper_bound(bucket_end_it, bucket_boundaries_end, *index_it);
                                            // } else {
                                            //     bucket_end_it = std::upper_bound(bucket_end_it, bucket_boundaries_end, *index_it, std::less_equal{});
                                            // }
                                            // bucket_start_it = std::prev(bucket_end_it);
                                            // reached_end_of_buckets = bucket_end_it == bucket_boundaries_end;
                                            // The above code will be more performant when the vast majority of buckets are empty
                                            // See comment in  ResampleClause::advance_bucket_past_value for mathematical and experimental bounds
                                            ++bucket_start_it;
                                            if (ARCTICDB_UNLIKELY(++bucket_end_it == bucket_boundaries_end)) {
                                                reached_end_of_buckets = true;
                                            } else {
                                                while (ARCTICDB_UNLIKELY(index_value_past_end_of_bucket(*index_it, *bucket_end_it))) {
                                                    ++bucket_start_it;
                                                    if (ARCTICDB_UNLIKELY(++bucket_end_it == bucket_boundaries_end)) {
                                                        reached_end_of_buckets = true;
                                                        break;
                                                    }
                                                }
                                            }
                                            if (ARCTICDB_LIKELY(!reached_end_of_buckets)) {
                                                current_bucket.set_boundaries(*bucket_start_it, *bucket_end_it);
                                                if (ARCTICDB_LIKELY(current_bucket.contains(*index_it))) {
                                                    push_to_aggregator<input_type_info::data_type>(bucket_aggregator, *agg_it, agg_column);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        );
                    }
                }
                // We were in the middle of aggregating a bucket when we ran out of index values
                if (output_it != output_end_it) {
                    *output_it++ = finalize_aggregator<output_type_info::data_type>(bucket_aggregator, string_pool);
                }
            }
        }
    );
    return res;
}

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
DataType SortedAggregator<aggregation_operator, closed_boundary>::generate_common_input_type(
        const std::vector<std::optional<ColumnWithStrings>>& input_agg_columns
        ) const {
    std::optional<DataType> common_input_type;
    for (const auto& opt_input_agg_column: input_agg_columns) {
        if (opt_input_agg_column.has_value()) {
            auto input_data_type = opt_input_agg_column->column_->type().data_type();
            check_aggregator_supported_with_data_type(input_data_type);
            add_data_type_impl(input_data_type, common_input_type);
        } else {
            // Column is missing from this row-slice due to dynamic schema, currently unsupported
            schema::raise<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>("Resample: Cannot aggregate column '{}' as it is missing from some row slices",
                                                                get_input_column_name().value);
        }
    }
    // Column is missing from all row-slices due to dynamic schema, currently unsupported
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(common_input_type.has_value(),
                                                        "Resample: Cannot aggregate column '{}' as it is missing from some row slices",
                                                        get_input_column_name().value);
    return *common_input_type;
}

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
void SortedAggregator<aggregation_operator, closed_boundary>::check_aggregator_supported_with_data_type(DataType data_type) const {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            (is_time_type(data_type) && aggregation_operator != AggregationOperator::SUM) ||
            (is_numeric_type(data_type)  && !is_time_type(data_type)) ||
            is_bool_type(data_type) ||
            (is_sequence_type(data_type) &&
             (aggregation_operator == AggregationOperator::FIRST ||
              aggregation_operator == AggregationOperator::LAST ||
              aggregation_operator == AggregationOperator::COUNT)),
            "Resample: Unsupported aggregation type {} on column '{}' of type {}",
            aggregation_operator, get_input_column_name().value, data_type);
}

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
DataType SortedAggregator<aggregation_operator, closed_boundary>::generate_output_data_type(DataType common_input_data_type) const {
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
bool SortedAggregator<aggregation_operator, closed_boundary>::index_value_past_end_of_bucket(timestamp index_value, timestamp bucket_end) const {
    if constexpr (closed_boundary == ResampleBoundary::LEFT) {
        return index_value >= bucket_end;
    } else {
        // closed_boundary == ResampleBoundary::RIGHT
        return index_value > bucket_end;
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

}