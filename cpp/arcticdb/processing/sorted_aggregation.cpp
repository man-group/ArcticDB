/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/processing/aggregation_utils.hpp>
#include <arcticdb/processing/sorted_aggregation.hpp>
#include <arcticdb/util/type_traits.hpp>
#include <folly/container/Enumerate.h>

namespace arcticdb {
template<typename InputTypeInfo, typename OutputTypeInfo>
requires util::instantiation_of<InputTypeInfo, ScalarTypeInfo> && util::instantiation_of<OutputTypeInfo, ScalarTypeInfo>
consteval bool is_aggregation_allowed(const AggregationOperator aggregation_operator) {
    return (is_numeric_type(InputTypeInfo::data_type) && is_numeric_type(OutputTypeInfo::data_type)) ||
           (is_sequence_type(InputTypeInfo::data_type) && (is_sequence_type(OutputTypeInfo::data_type) || aggregation_operator == AggregationOperator::COUNT)) ||
           (is_bool_type(InputTypeInfo::data_type) && (is_bool_type(OutputTypeInfo::data_type) || is_numeric_type(OutputTypeInfo::data_type)));
}

template<typename OutputTypeInfo>
requires util::instantiation_of<OutputTypeInfo, ScalarTypeInfo>
consteval bool is_aggregation_allowed(const AggregationOperator aggregation_operator) {
    return is_numeric_type(OutputTypeInfo::data_type) ||
           is_bool_type(OutputTypeInfo::data_type) ||
           (is_sequence_type(OutputTypeInfo::data_type) &&
            (aggregation_operator == AggregationOperator::FIRST ||
             aggregation_operator == AggregationOperator::LAST));
}

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
DataType SortedAggregator<aggregation_operator, closed_boundary>::generate_output_data_type(const DataType common_input_data_type) const {
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
typename SortedAggregator<aggregation_operator, closed_boundary>::OutputColumnInfo SortedAggregator<aggregation_operator, closed_boundary>::generate_common_input_type(
    const std::span<const std::optional<ColumnWithStrings>> input_agg_columns
) const {
    OutputColumnInfo output_column_info;
    for (const auto& opt_input_agg_column: input_agg_columns) {
        if (opt_input_agg_column.has_value()) {
            auto input_data_type = opt_input_agg_column->column_->type().data_type();
            check_aggregator_supported_with_data_type(input_data_type);
            add_data_type_impl(input_data_type, output_column_info.data_type_);
        } else {
            output_column_info.is_sparse_ = true;
        }
    }
    return output_column_info;
}

template<ResampleBoundary closed_boundary>
bool value_past_bucket_start(const timestamp bucket_start, const timestamp value) {
    if constexpr(closed_boundary == ResampleBoundary::LEFT) {
        return value >= bucket_start;
    }
    return value > bucket_start;
}

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
std::optional<Column> SortedAggregator<aggregation_operator, closed_boundary>::generate_resampling_output_column(
    [[maybe_unused]] const std::span<const std::shared_ptr<Column>> input_index_columns,
    const std::span<const std::optional<ColumnWithStrings>> input_agg_columns,
    const Column& output_index_column,
    [[maybe_unused]] const ResampleBoundary label
) const {
    using IndexTDT = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
    const OutputColumnInfo type_info = generate_common_input_type(input_agg_columns);

    std::cout<<fmt::format("Label: {}", label == ResampleBoundary::LEFT ? "LEFT" : "RIGHT")<<std::endl;
    std::cout<<fmt::format("Closed: {}", closed == ResampleBoundary::LEFT ? "LEFT" : "RIGHT")<<std::endl;
    std::cout<<fmt::format("Output index column has {} rows", output_index_column.row_count())<<std::endl;
    std::cout<<"Index content\n"<<std::flush;
    Column::for_each<IndexTDT>(output_index_column, [](auto val){ std::cout << val << " "; });
    std::cout<<std::endl;

    if (!type_info.data_type_) {
        return std::nullopt;
    }

    if (!type_info.is_sparse_) {
        std::cout<<fmt::format("Creating a dense column of size {}\n", output_index_column.row_count())<<std::flush;
        return Column(
            make_scalar_type(generate_output_data_type(*type_info.data_type_)),
            output_index_column.row_count(),
            AllocationType::PRESIZED,
            Sparsity::NOT_PERMITTED
        );
    }
    auto output_index_at = [&output_index_column](const int64_t idx) {
        return *(output_index_column.begin<IndexTDT>() + idx);
    };
    util::BitSet sparse_map(output_index_column.row_count());
    std::cout<<fmt::format("Initialized sparse map of size: {}\n", sparse_map.size())<<std::flush;
    int64_t output_row = 0, output_row_prev = 0;

    for (size_t col_index = 0; const std::shared_ptr<Column>& input_index_column : input_index_columns) {
        const timestamp first_index_value = *input_index_column->begin<IndexTDT>();
        const timestamp last_index_value = *(input_index_column->begin<IndexTDT>() + (input_index_column->row_count() - 1));
        std::cout<<fmt::format("Processing column: {}, [{};{}]. Rowcount {}", col_index, first_index_value, last_index_value, input_index_column->row_count())<<std::endl;

        std::cout<<"Skip to find the first bucket containing the column"<<std::endl;
        while (output_row < output_index_column.row_count() && value_past_bucket_start<closed>(output_index_at(output_row), first_index_value)) {
            ++output_row;
        }
        output_row_prev = output_row = std::max(int64_t{0}, output_row - (label == ResampleBoundary::LEFT));
        std::cout<<fmt::format("Skipped to output row {}, value {}. First index value: {}", output_row, output_index_at(output_row), first_index_value)<<std::endl;

        std::cout<<fmt::format("Find which output buckets does the column span")<<std::endl;
        while (output_row < output_index_column.row_count() && value_past_bucket_start<closed>(output_index_at(output_row), last_index_value)) {
            ++output_row;
        }
        output_row = std::max(int64_t{0}, output_row - (label == ResampleBoundary::LEFT));
        std::cout<<fmt::format("Column index: [{};{}] spans [{};{}] row indexes [{};{}]", first_index_value, last_index_value, output_index_at(output_row_prev), output_index_at(output_row), output_row_prev, output_row)<<std::endl;
        if (input_agg_columns[col_index]) {
            const util::BitSet::size_type interval_end = std::min(static_cast<util::BitSet::size_type>(output_row), sparse_map.size() - 1);
            std::cout<<fmt::format("Column {} exists filling sparse map : [{};{}](inclusive)", col_index, output_row_prev, interval_end)<<std::endl;
            sparse_map.set_range(output_row_prev, interval_end);
        }
        output_row_prev = output_row;
        ++col_index;
        std::cout<<std::endl;
    }
    const Sparsity sparsity = sparse_map.count() == sparse_map.size() ? Sparsity::NOT_PERMITTED : Sparsity::PERMITTED;
    const int64_t row_count = sparsity == Sparsity::PERMITTED ? sparse_map.count() : output_index_column.row_count();
    std::cout<<fmt::format("Creating {} column. Sparse map size: {} sparse map count: {}", sparsity == Sparsity::NOT_PERMITTED ? "Dense" : "Sparse", sparse_map.size(), sparse_map.count())<<std::endl;
    Column result(make_scalar_type(generate_output_data_type(*type_info.data_type_)), row_count, AllocationType::PRESIZED, sparsity);
    if (sparsity == Sparsity::PERMITTED) {
        result.set_sparse_map(std::move(sparse_map));
    }
    result.set_row_data(output_index_column.row_count() - 1);

    std::cout<<std::flush;
    return result;

}

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
std::optional<Column> SortedAggregator<aggregation_operator, closed_boundary>::aggregate(const std::vector<std::shared_ptr<Column>>& input_index_columns,
                                                                          const std::vector<std::optional<ColumnWithStrings>>& input_agg_columns,
                                                                          const std::vector<timestamp>& bucket_boundaries,
                                                                          const Column& output_index_column,
                                                                          StringPool& string_pool,
                                                                          const ResampleBoundary label) const {
    std::cout<<std::endl;
    using IndexTDT = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
    std::optional<Column> res = generate_resampling_output_column(input_index_columns, input_agg_columns, output_index_column, label);
    if (!res) {
        return std::nullopt;
    }
    constexpr char left_bracket = closed == ResampleBoundary::LEFT ? '[' : '(';
    constexpr char right_bracket = closed == ResampleBoundary::RIGHT ? ']' : ')';
    //std::cout<<"Buckets: \n";
    //for (size_t i = 0; i < bucket_boundaries.size() - 1; ++i) {
    //    std::cout<<fmt::format("{}{};{}{}\n", left_bracket, bucket_boundaries[i], bucket_boundaries[i + 1], right_bracket);
    //}

    details::visit_type(
        res->type().data_type(),
        [&](auto output_type_desc_tag) {
            using output_type_info = ScalarTypeInfo<decltype(output_type_desc_tag)>;
            auto output_data = res->data();
            auto output_it = output_data.begin<typename output_type_info::TDT>();
            auto output_end_it = output_data.end<typename output_type_info::TDT>();
            // Need this here to only generate valid get_bucket_aggregator code, exception will have been thrown earlier at runtime
            if constexpr (is_aggregation_allowed<output_type_info>(aggregation_operator)) {
                auto bucket_aggregator = get_bucket_aggregator<output_type_info>();
                bool reached_end_of_buckets{false};
                auto bucket_start_it = bucket_boundaries.cbegin();
                auto bucket_end_it = std::next(bucket_start_it);
                Bucket<closed_boundary> current_bucket(*bucket_start_it, *bucket_end_it);
                bool bucket_has_values{false};
                const auto bucket_boundaries_end = bucket_boundaries.cend();
                for (auto [idx, input_agg_column]: folly::enumerate(input_agg_columns)) {
                    if (input_agg_column.has_value()) {
                        details::visit_type(
                            input_agg_column->column_->type().data_type(),
                            [&, &agg_column = *input_agg_column, &input_index_column = input_index_columns.at(idx)](auto input_type_desc_tag) {
                                using input_type_info = ScalarTypeInfo<decltype(input_type_desc_tag)>;
                                // Again, only needed to generate valid code below, exception will have been thrown earlier at runtime
                                if constexpr (is_aggregation_allowed<input_type_info, output_type_info>(aggregation_operator)) {
                                    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                                            !agg_column.column_->is_sparse() && agg_column.column_->row_count() == input_index_column->row_count(),
                                            "Resample: Cannot aggregate column '{}' as it is sparse",
                                            get_input_column_name().value);
                                    auto index_data = input_index_column->data();
                                    const auto index_cend = index_data.template cend<IndexTDT>();
                                    auto agg_data = agg_column.column_->data();
                                    auto agg_it = agg_data.template cbegin<typename input_type_info::TDT>();
                                    for (auto index_it = index_data.template cbegin<IndexTDT>(); index_it != index_cend && !reached_end_of_buckets; ++index_it, ++agg_it) {
                                        std::cout << fmt::format(
                                            "Processing index: {}. Current bucket: {}{};{}{}. Data: {}", *index_it,
                                            left_bracket, current_bucket.start(), current_bucket.end(), right_bracket,
                                            *agg_it) << std::endl;
                                        if (ARCTICDB_LIKELY(current_bucket.contains(*index_it))) {
                                            std::cout << fmt::format("Current bucket: {}{};{}{} contains {} pushing",
                                                                     left_bracket, current_bucket.start(),
                                                                     current_bucket.end(), right_bracket,
                                                                     *index_it, *agg_it) << std::endl;
                                            push_to_aggregator<input_type_info::data_type>(bucket_aggregator, *agg_it, agg_column);
                                            bucket_has_values = true;
                                        } else if (ARCTICDB_LIKELY(index_value_past_end_of_bucket(*index_it, current_bucket.end())) && output_it != output_end_it) {
                                            if (bucket_has_values) {
                                                std::cout<<fmt::format("Index {} past end of bucket {}{};{}{} finalizing",
                                                    *index_it, left_bracket, current_bucket.start(), current_bucket.end(), right_bracket)<<std::endl;
                                                *output_it = finalize_aggregator<output_type_info::data_type>(bucket_aggregator, string_pool);
                                                ++output_it;
                                            }
                                            // The following code is equivalent to:
                                            // if constexpr (closed_boundary == ResampleBoundary::LEFT) {
                                            //     bucket_end_it = std::upper_bound(bucket_end_it, bucket_boundaries_end, *index_it);
                                            // } else {
                                            //     bucket_end_it = std::upper_bound(bucket_end_it, bucket_boundaries_end, *index_it, std::less_equal{});
                                            // }
                                            // bucket_start_it = std::prev(bucket_end_it);
                                            // reached_end_of_buckets = bucket_end_it == bucket_boundaries_end;
                                            // The above code will be more performant when the vast majority of buckets are empty
                                            // See comment in  ResampleClause::advance_boundary_past_value for mathematical and experimental bounds
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
                                                bucket_has_values = false;
                                                current_bucket.set_boundaries(*bucket_start_it, *bucket_end_it);
                                                if (ARCTICDB_LIKELY(current_bucket.contains(*index_it))) {
                                                    push_to_aggregator<input_type_info::data_type>(bucket_aggregator, *agg_it, agg_column);
                                                    bucket_has_values = true;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        );
                    } else {
                        const Column& input_index = *input_index_columns[idx];
                        const timestamp last_index_value = *input_index.scalar_at<IndexTDT::DataTypeTag::raw_type>(input_index.row_count() - 1);
                        // Check how many buckets must be skipped.
                        bool moved_to_new_bucket{};
                        while (bucket_end_it != bucket_boundaries_end && index_value_past_end_of_bucket(last_index_value, *bucket_end_it)) {
                            ++bucket_start_it;
                            ++bucket_end_it;
                            moved_to_new_bucket = true;
                        }
                        if (bucket_end_it == bucket_boundaries_end) {
                            reached_end_of_buckets = true;
                        } else {
                            if (moved_to_new_bucket) {
                                std::cout<<fmt::format("Missing column has moved to a new bucket: {} {}\n", *bucket_start_it, *bucket_end_it);
                                if (bucket_has_values) {
                                    std::cout<<fmt::format("Column preceding empty column had values to finalize\n");
                                    *output_it = finalize_aggregator<output_type_info::data_type>(bucket_aggregator, string_pool);
                                    ++output_it;
                                }
                                bucket_has_values = false;
                            }
                            current_bucket.set_boundaries(*bucket_start_it, *bucket_end_it);

                        }
                    }
                }
                // We were in the middle of aggregating a bucket when we ran out of index values
                if (output_it != output_end_it) {
                    std::cout<<fmt::format("Reached end of input index values. Finalize aggregator\n");
                    *output_it = finalize_aggregator<output_type_info::data_type>(bucket_aggregator, string_pool);
                    ++output_it;
                }
            }
        }
    );
    return res;
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
std::optional<Value> SortedAggregator<aggregation_operator, closed_boundary>::get_default_value(const DataType common_input_data_type) const {
    if constexpr (aggregation_operator == AggregationOperator::SUM) {
        return details::visit_type(generate_output_data_type(common_input_data_type), [&](auto tag) -> std::optional<Value> {
            using data_type_tag = decltype(tag);
            return Value{typename data_type_tag::raw_type{0}, data_type_tag::data_type};
        });
    } else {
        return {};
    }
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
