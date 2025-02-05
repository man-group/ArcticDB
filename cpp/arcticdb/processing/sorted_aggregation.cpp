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

namespace arcticdb {

template<AggregationOperator aggregation_operator, DataType input_data_type, typename Aggregator, typename T>
void push_to_aggregator(Aggregator& bucket_aggregator, T value, ARCTICDB_UNUSED const ColumnWithStrings& column_with_strings) {
    if constexpr(is_time_type(input_data_type) && aggregation_operator == AggregationOperator::COUNT) {
        bucket_aggregator.template push<timestamp, true>(value);
    } else if constexpr (is_numeric_type(input_data_type) || is_bool_type(input_data_type)) {
        bucket_aggregator.push(value);
    } else if constexpr (is_sequence_type(input_data_type)) {
        bucket_aggregator.push(column_with_strings.string_at_offset(value));
    }
}

template<AggregationOperator aggregation_operator, typename scalar_type_info>
requires arcticdb::util::is_instantiation_of_v<scalar_type_info, ScalarTypeInfo>
[[nodiscard]] auto get_bucket_aggregator() {
    if constexpr (aggregation_operator == AggregationOperator::SUM) {
        if constexpr (is_bool_type(scalar_type_info::data_type)) {
            // Sum of bool column is just the count of true values
            return SumAggregatorSorted<uint64_t>();
        } else {
            return SumAggregatorSorted<typename scalar_type_info::RawType>();
        }
    } else if constexpr (aggregation_operator == AggregationOperator::MEAN) {
        if constexpr (is_time_type(scalar_type_info::data_type)) {
            return MeanAggregatorSorted<typename scalar_type_info::RawType, true>();
        } else {
            return MeanAggregatorSorted<typename scalar_type_info::RawType>();
        }
    } else if constexpr (aggregation_operator == AggregationOperator::MIN) {
        if constexpr (is_time_type(scalar_type_info::data_type)) {
            return MinAggregatorSorted<typename scalar_type_info::RawType, true>();
        } else {
            return MinAggregatorSorted<typename scalar_type_info::RawType>();
        }
    } else if constexpr (aggregation_operator == AggregationOperator::MAX) {
        if constexpr (is_time_type(scalar_type_info::data_type)) {
            return MaxAggregatorSorted<typename scalar_type_info::RawType, true>();
        } else {
            return MaxAggregatorSorted<typename scalar_type_info::RawType>();
        }
    } else if constexpr (aggregation_operator == AggregationOperator::FIRST) {
        if constexpr (is_time_type(scalar_type_info::data_type)) {
            return FirstAggregatorSorted<typename scalar_type_info::RawType, true>();
        } else if constexpr (is_numeric_type(scalar_type_info::data_type) || is_bool_type(scalar_type_info::data_type)) {
            return FirstAggregatorSorted<typename scalar_type_info::RawType>();
        } else if constexpr (is_sequence_type(scalar_type_info::data_type)) {
            return FirstAggregatorSorted<std::optional<std::string_view>>();
        }
    } else if constexpr (aggregation_operator == AggregationOperator::LAST) {
        if constexpr (is_time_type(scalar_type_info::data_type)) {
            return LastAggregatorSorted<typename scalar_type_info::RawType, true>();
        } else if constexpr (is_numeric_type(scalar_type_info::data_type) || is_bool_type(scalar_type_info::data_type)) {
            return LastAggregatorSorted<typename scalar_type_info::RawType>();
        } else if constexpr (is_sequence_type(scalar_type_info::data_type)) {
            return LastAggregatorSorted<std::optional<std::string_view>>();
        }
    } else if constexpr (aggregation_operator == AggregationOperator::COUNT) {
        return CountAggregatorSorted();
    }
}

template<AggregationOperator aggregation_operator, typename output_type_info>
requires arcticdb::util::is_instantiation_of_v<output_type_info, ScalarTypeInfo>
consteval bool is_output_type_allowed() {
    return is_numeric_type(output_type_info::data_type) ||
        is_bool_type(output_type_info::data_type) ||
        (is_sequence_type(output_type_info::data_type) && (aggregation_operator == AggregationOperator::FIRST || aggregation_operator == AggregationOperator::LAST));
}

template<AggregationOperator aggregation_operator, typename input_type_info, typename output_type_info>
requires arcticdb::util::is_instantiation_of_v<input_type_info, ScalarTypeInfo> && arcticdb::util::is_instantiation_of_v<output_type_info, ScalarTypeInfo>
consteval bool are_input_output_operation_allowed() {
    return (is_numeric_type(input_type_info::data_type) && is_numeric_type(output_type_info::data_type)) ||
    (is_sequence_type(input_type_info::data_type) && (is_sequence_type(output_type_info::data_type) || aggregation_operator == AggregationOperator::COUNT)) ||
    (is_bool_type(input_type_info::data_type) && (is_bool_type(output_type_info::data_type) || is_numeric_type(output_type_info::data_type)));
}

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
SortedAggregator<aggregation_operator, closed_boundary>::SortedAggregator(ColumnName input_column_name, ColumnName output_column_name)
        : input_column_name_(std::move(input_column_name))
        , output_column_name_(std::move(output_column_name))
{}

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
ColumnName SortedAggregator<aggregation_operator, closed_boundary>::get_input_column_name() const { return input_column_name_; }

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
ColumnName SortedAggregator<aggregation_operator, closed_boundary>::get_output_column_name() const { return output_column_name_; }

template<AggregationOperator aggregation_operator, DataType output_data_type, typename Aggregator>
auto finalize_aggregator(
    Aggregator& bucket_aggregator,
    [[maybe_unused]] StringPool& string_pool
) {
    if constexpr (is_numeric_type(output_data_type) || is_bool_type(output_data_type) || aggregation_operator == AggregationOperator::COUNT) {
        return bucket_aggregator.finalize();
    } else if constexpr (is_sequence_type(output_data_type)) {
        auto opt_string_view = bucket_aggregator.finalize();
        if (ARCTICDB_LIKELY(opt_string_view.has_value())) {
            return string_pool.get(*opt_string_view).offset();
        } else {
            return string_none;
        }
    }
}

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
[[nodiscard]] Column SortedAggregator<aggregation_operator, closed_boundary>::aggregate_static_schema(
    std::span<const std::shared_ptr<Column>> input_index_columns,
    std::span<const std::optional<ColumnWithStrings>> input_agg_columns,
    std::span<const timestamp> bucket_boundaries,
    const Column& output_index_column,
    StringPool& string_pool,
    DataType common_input_type
) const {
    using IndexTDT = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
    Column res(
        TypeDescriptor(generate_output_data_type(common_input_type), Dimension::Dim0),
        output_index_column.row_count(),
        AllocationType::PRESIZED,
        Sparsity::NOT_PERMITTED
    );
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
            if constexpr (is_output_type_allowed<aggregation_operator, output_type_info>()) {
                auto bucket_aggregator = get_bucket_aggregator<aggregation_operator, output_type_info>();
                bool reached_end_of_buckets{false};
                auto bucket_start_it = bucket_boundaries.begin();
                auto bucket_end_it = std::next(bucket_start_it);
                Bucket<closed_boundary> current_bucket(*bucket_start_it, *bucket_end_it);
                const auto bucket_boundaries_end = bucket_boundaries.end();
                for (auto [idx, input_agg_column]: folly::enumerate(input_agg_columns)) {
                    // Always true right now due to earlier check
                    if (input_agg_column.has_value()) {
                        details::visit_type(
                            input_agg_column->column_->type().data_type(),
                            [this,
                            &output_it,
                            &output_end_it,
                            &bucket_aggregator,
                            &agg_column = *input_agg_column,
                            &input_index_column = input_index_columns[idx],
                            &bucket_boundaries_end,
                            &string_pool,
                            &bucket_start_it,
                            &bucket_end_it,
                            &current_bucket,
                            &reached_end_of_buckets](auto input_type_desc_tag) {
                                using input_type_info = ScalarTypeInfo<decltype(input_type_desc_tag)>;
                                // Again, only needed to generate valid code below, exception will have been thrown earlier at runtime
                                if constexpr (are_input_output_operation_allowed<aggregation_operator, input_type_info, output_type_info>()) {
                                    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                                            !agg_column.column_->is_sparse() && agg_column.column_->row_count() == input_index_column->row_count(),
                                            "Resample: Cannot aggregate column '{}' as it is sparse",
                                            get_input_column_name().value);
                                    auto index_data = input_index_column->data();
                                    const auto index_cend = index_data.cend<IndexTDT>();
                                    auto agg_data = agg_column.column_->data();
                                    auto agg_it = agg_data.cbegin<typename input_type_info::TDT>();
                                    bool bucket_has_values = false;
                                    for (auto index_it = index_data.cbegin<IndexTDT>(); index_it != index_cend && !reached_end_of_buckets; ++index_it, ++agg_it) {
                                        if (ARCTICDB_LIKELY(current_bucket.contains(*index_it))) {
                                            push_to_aggregator<aggregation_operator, input_type_info::data_type>(bucket_aggregator, *agg_it, agg_column);
                                            bucket_has_values = true;
                                        } else if (ARCTICDB_LIKELY(index_value_past_end_of_bucket(*index_it, *bucket_end_it)) && output_it != output_end_it) {
                                            if (bucket_has_values) {
                                                *output_it++ = finalize_aggregator<aggregation_operator, output_type_info::data_type>(bucket_aggregator, string_pool);
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
                                                    push_to_aggregator<aggregation_operator, input_type_info::data_type>(bucket_aggregator, *agg_it, agg_column);
                                                    bucket_has_values = true;
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
                    *output_it++ = finalize_aggregator<aggregation_operator, output_type_info::data_type>(bucket_aggregator, string_pool);
                }
            }
        }
    );
    return res;
}

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
[[nodiscard]] Column SortedAggregator<aggregation_operator, closed_boundary>::aggregate_dynamic_schema(
    [[maybe_unused]] std::span<const std::shared_ptr<Column>> input_index_columns,
    [[maybe_unused]] std::span<const std::optional<ColumnWithStrings>> input_agg_columns,
    [[maybe_unused]] std::span<const timestamp> bucket_boundaries,
    [[maybe_unused]] const Column& output_index_column,
    [[maybe_unused]] StringPool& string_pool
) const {
    return {};
}

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
Column SortedAggregator<aggregation_operator, closed_boundary>::aggregate(const std::vector<std::shared_ptr<Column>>& input_index_columns,
                                                                          const std::vector<std::optional<ColumnWithStrings>>& input_agg_columns,
                                                                          const std::vector<timestamp>& bucket_boundaries,
                                                                          const Column& output_index_column,
                                                                          StringPool& string_pool) const {
    if (const std::optional<DataType> common_input_type = generate_common_input_type(input_agg_columns)) {
        return aggregate_static_schema(
            input_index_columns,
            input_agg_columns,
            bucket_boundaries,
            output_index_column,
            string_pool,
            *common_input_type);
    }
    return aggregate_dynamic_schema(input_index_columns,
        input_agg_columns,
        bucket_boundaries,
        output_index_column,
        string_pool);

}

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
std::optional<DataType> SortedAggregator<aggregation_operator, closed_boundary>::generate_common_input_type(
        const std::vector<std::optional<ColumnWithStrings>>& input_agg_columns
) const {
    std::optional<DataType> common_input_type;
    for (const auto& opt_input_agg_column: input_agg_columns) {
        if (opt_input_agg_column.has_value()) {
            const auto input_data_type = opt_input_agg_column->column_->type().data_type();
            check_aggregator_supported_with_data_type(input_data_type);
            add_data_type_impl(input_data_type, common_input_type);
        }
    }
    return common_input_type;
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

template<ResampleBoundary closed_boundary>
Bucket<closed_boundary>::Bucket(timestamp start, timestamp end):
        start_(start), end_(end){}

template<ResampleBoundary closed_boundary>
void Bucket<closed_boundary>::set_boundaries(timestamp start, timestamp end) {
    start_ = start;
    end_ = end;
}

template<ResampleBoundary closed_boundary>
[[nodiscard]] bool Bucket<closed_boundary>::contains(timestamp ts) const {
    if constexpr (closed_boundary == ResampleBoundary::LEFT) {
        return ts >= start_ && ts < end_;
    } else {
        // closed_boundary == ResampleBoundary::RIGHT
        return ts > start_ && ts <= end_;
    }
}

template class Bucket<ResampleBoundary::LEFT>;
template class Bucket<ResampleBoundary::RIGHT>;
}

template<>
struct fmt::formatter<arcticdb::AggregationOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const arcticdb::AggregationOperator& agg, FormatContext &ctx) const {
        switch(agg) {
            case arcticdb::AggregationOperator::SUM:
                return fmt::format_to(ctx.out(), "SUM");
            case arcticdb::AggregationOperator::MEAN:
                return fmt::format_to(ctx.out(), "MEAN");
            case arcticdb::AggregationOperator::MIN:
                return fmt::format_to(ctx.out(), "MIN");
            case arcticdb::AggregationOperator::MAX:
                return fmt::format_to(ctx.out(), "MAX");
            case arcticdb::AggregationOperator::FIRST:
                return fmt::format_to(ctx.out(), "FIRST");
            case arcticdb::AggregationOperator::LAST:
                return fmt::format_to(ctx.out(), "LAST");
            case arcticdb::AggregationOperator::COUNT:
            default:
                return fmt::format_to(ctx.out(), "COUNT");
        }
    }
};