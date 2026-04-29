/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <memory>
#include <optional>
#include <vector>

#include <folly/Poly.h>

#include <arcticdb/column_store/column.hpp>
#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/column_store/string_pool.hpp>

namespace arcticdb {

enum class ResampleBoundary { LEFT, RIGHT };

// Identifies a row position within a sequence of sorted-row-slice index columns. `input_column_idx` indexes into the
// outer `input_index_columns` vector; `offset` is the row offset within that column.
struct ResampleInputCursor {
    size_t input_column_idx;
    size_t offset;
};

// Mapping from output row to input range. For output row i, the input values feeding it are
// `[mapping[i], mapping[i+1])` interpreted as a contiguous range across the input index columns. The vector has length
// N+1 where N is the number of non-empty buckets (= output rows). The trailing cursor is the one-past-the-end position.
using ResampleMapping = std::vector<ResampleInputCursor>;

struct ISortedAggregator {
    template<class Base>
    struct Interface : Base {
        [[nodiscard]] ColumnName get_input_column_name() const { return folly::poly_call<0>(*this); };
        [[nodiscard]] ColumnName get_output_column_name() const { return folly::poly_call<1>(*this); };
        [[nodiscard]] std::optional<Column> aggregate(
                const std::vector<std::shared_ptr<Column>>& input_index_columns,
                const std::vector<std::optional<ColumnWithStrings>>& input_agg_columns, const ResampleMapping& mapping,
                StringPool& string_pool
        ) const {
            return folly::poly_call<2>(*this, input_index_columns, input_agg_columns, mapping, string_pool);
        }
        void check_aggregator_supported_with_data_type(DataType data_type) const {
            folly::poly_call<3>(*this, data_type);
        };
        [[nodiscard]] DataType generate_output_data_type(DataType common_input_data_type) const {
            return folly::poly_call<4>(*this, common_input_data_type);
        };
        [[nodiscard]] std::optional<Value> get_default_value(DataType common_input_data_type) const {
            return folly::poly_call<5>(*this, common_input_data_type);
        }
    };

    template<class T>
    using Members = folly::PolyMembers<
            &T::get_input_column_name, &T::get_output_column_name, &T::aggregate,
            &T::check_aggregator_supported_with_data_type, &T::generate_output_data_type, &T::get_default_value>;
};

using SortedAggregatorInterface = folly::Poly<ISortedAggregator>;

template<ResampleBoundary closed_boundary>
class Bucket {
  public:
    Bucket(timestamp start, timestamp end) : start_(start), end_(end) {}

    void set_boundaries(timestamp start, timestamp end) {
        start_ = start;
        end_ = end;
    }

    [[nodiscard]] bool contains(timestamp ts) const {
        if constexpr (closed_boundary == ResampleBoundary::LEFT) {
            return ts >= start_ && ts < end_;
        } else {
            // closed_boundary == ResampleBoundary::RIGHT
            return ts > start_ && ts <= end_;
        }
    }
    [[nodiscard]] timestamp start() const { return start_; }

    [[nodiscard]] timestamp end() const { return end_; }

  private:
    timestamp start_;
    timestamp end_;
};

template<ResampleBoundary closed_boundary>
std::pair<std::shared_ptr<Column>, ResampleMapping> generate_output_index_column(
        const std::vector<std::shared_ptr<Column>>& input_index_columns,
        const std::vector<timestamp>& bucket_boundaries, const TimestampRange& date_range,
        ResampleBoundary label_boundary
);

enum class AggregationOperator { SUM, MEAN, MIN, MAX, FIRST, LAST, COUNT };

template<typename T>
class SumAggregatorSorted {
  public:
    void push(T value) {
        if constexpr (std::is_floating_point_v<T>) {
            if (ARCTICDB_LIKELY(!std::isnan(value))) {
                sum_ += value;
            }
        } else {
            sum_ += value;
        }
    }

    T finalize() {
        T res{sum_};
        sum_ = 0;
        return res;
    }

  private:
    T sum_{0};
};

template<typename T, bool TimeType = false>
class MeanAggregatorSorted {
  public:
    void push(T value) {
        if constexpr (std::is_floating_point_v<T>) {
            if (ARCTICDB_LIKELY(!std::isnan(value))) {
                sum_ += value;
                ++count_;
            }
        } else if constexpr (std::is_same_v<T, timestamp> && TimeType) {
            if (ARCTICDB_LIKELY(value != NaT)) {
                sum_ += value;
                ++count_;
            }
        } else {
            sum_ += value;
            ++count_;
        }
    }

    std::conditional_t<TimeType, timestamp, double> finalize() {
        if constexpr (std::is_same_v<T, timestamp> && TimeType) {
            timestamp res;
            if (ARCTICDB_LIKELY(count_ > 0)) {
                res = static_cast<timestamp>(sum_ / static_cast<double>(count_));
                sum_ = 0;
                count_ = 0;
            } else {
                res = NaT;
            }
            return res;
        } else {
            double res;
            if (ARCTICDB_LIKELY(count_ > 0)) {
                res = sum_ / static_cast<double>(count_);
                sum_ = 0;
                count_ = 0;
            } else {
                res = std::numeric_limits<double>::quiet_NaN();
            }
            return res;
        }
    }

  private:
    double sum_{0};
    uint64_t count_{0};
};

template<typename T, bool TimeType = false>
class MinAggregatorSorted {
  public:
    MinAggregatorSorted() {
        if constexpr (!std::is_floating_point_v<T> && !TimeType) {
            min_ = std::numeric_limits<T>::max();
        }
    }

    void push(T value) {
        if constexpr (std::is_floating_point_v<T>) {
            if (ARCTICDB_LIKELY(!std::isnan(value))) {
                min_ = std::min(min_.value_or(std::numeric_limits<T>::infinity()), value);
            }
        } else if constexpr (std::is_same_v<T, timestamp> && TimeType) {
            if (ARCTICDB_LIKELY(value != NaT)) {
                min_ = std::min(min_.value_or(std::numeric_limits<T>::max()), value);
            }
        } else {
            min_ = std::min(min_, value);
        }
    }

    T finalize() {
        T res;
        if constexpr (std::is_floating_point_v<T>) {
            res = min_.value_or(std::numeric_limits<T>::quiet_NaN());
            min_.reset();
        } else if constexpr (std::is_same_v<T, timestamp> && TimeType) {
            res = min_.value_or(NaT);
            min_.reset();
        } else {
            res = min_;
            min_ = std::numeric_limits<T>::max();
        }
        return res;
    }

  private:
    // Floats and timestamps need a special case for when only nan/nat values are pushed
    std::conditional_t<std::is_floating_point_v<T> || TimeType, std::optional<T>, T> min_;
};

template<typename T, bool TimeType = false>
class MaxAggregatorSorted {
  public:
    MaxAggregatorSorted() {
        if constexpr (!std::is_floating_point_v<T> && !TimeType) {
            max_ = std::numeric_limits<T>::lowest();
        }
    }

    void push(T value) {
        if constexpr (std::is_floating_point_v<T>) {
            if (ARCTICDB_LIKELY(!std::isnan(value))) {
                max_ = std::max(max_.value_or(-std::numeric_limits<T>::infinity()), value);
            }
        } else if constexpr (std::is_same_v<T, timestamp> && TimeType) {
            if (ARCTICDB_LIKELY(value != NaT)) {
                max_ = std::max(max_.value_or(std::numeric_limits<T>::lowest()), value);
            }
        } else {
            max_ = std::max(max_, value);
        }
    }

    T finalize() {
        T res;
        if constexpr (std::is_floating_point_v<T>) {
            res = max_.value_or(std::numeric_limits<T>::quiet_NaN());
            max_.reset();
        } else if constexpr (std::is_same_v<T, timestamp> && TimeType) {
            res = max_.value_or(NaT);
            max_.reset();
        } else {
            res = max_;
            max_ = std::numeric_limits<T>::min();
        }
        return res;
    }

  private:
    // Floats and timestamps need a special case for when only nan/nat values are pushed
    std::conditional_t<std::is_floating_point_v<T> || TimeType, std::optional<T>, T> max_;
};

template<typename T, bool TimeType = false>
class FirstAggregatorSorted {
  public:
    void push(T value) {
        if constexpr (std::is_same_v<T, std::optional<std::string_view>>) {
            if (ARCTICDB_UNLIKELY(!first_.has_value() || !(*first_).has_value())) {
                first_ = value;
            }
        } else if constexpr (std::is_floating_point_v<T>) {
            if (ARCTICDB_UNLIKELY(!first_.has_value() || std::isnan(*first_))) {
                first_ = value;
            }
        } else if constexpr (std::is_same_v<T, timestamp> && TimeType) {
            if (ARCTICDB_UNLIKELY(!first_.has_value() || *first_ == NaT)) {
                first_ = value;
            }
        } else {
            if (ARCTICDB_UNLIKELY(!first_.has_value())) {
                first_ = value;
            }
        }
    }

    T finalize() {
        T res;
        if constexpr (std::is_floating_point_v<T>) {
            res = first_.value_or(std::numeric_limits<T>::quiet_NaN());
        } else if constexpr (std::is_same_v<T, timestamp> && TimeType) {
            res = first_.value_or(NaT);
        } else {
            ARCTICDB_DEBUG_CHECK(
                    ErrorCode::E_ASSERTION_FAILURE,
                    first_.has_value(),
                    "FirstBucketAggregator::finalize called with no values pushed"
            );
            res = *first_;
        }
        first_.reset();
        return res;
    }

  private:
    std::optional<T> first_;
};

template<typename T, bool TimeType = false>
class LastAggregatorSorted {
  public:
    void push(T value) {
        if constexpr (std::is_same_v<T, std::optional<std::string_view>>) {
            if (ARCTICDB_LIKELY(!last_.has_value() || value.has_value())) {
                last_ = value;
            }
        } else if constexpr (std::is_floating_point_v<T>) {
            if (ARCTICDB_LIKELY(!last_.has_value() || !std::isnan(value))) {
                last_ = value;
            }
        } else if constexpr (std::is_same_v<T, timestamp> && TimeType) {
            if (ARCTICDB_LIKELY(!last_.has_value() || value != NaT)) {
                last_ = value;
            }
        } else {
            last_ = value;
        }
    }

    T finalize() {
        T res;
        if constexpr (std::is_floating_point_v<T>) {
            res = last_.value_or(std::numeric_limits<T>::quiet_NaN());
        } else if constexpr (std::is_same_v<T, timestamp> && TimeType) {
            res = last_.value_or(NaT);
        } else {
            ARCTICDB_DEBUG_CHECK(
                    ErrorCode::E_ASSERTION_FAILURE,
                    last_.has_value(),
                    "LastBucketAggregator::finalize called with no values pushed"
            );
            res = *last_;
        }
        last_.reset();
        return res;
    }

  private:
    std::optional<T> last_;
};

class CountAggregatorSorted {
  public:
    template<typename T, bool TimeType = false>
    void push(T value) {
        if constexpr (std::is_same_v<T, std::optional<std::string_view>>) {
            if (ARCTICDB_LIKELY(value.has_value())) {
                ++count_;
            }
        } else if constexpr (std::is_floating_point_v<T>) {
            if (ARCTICDB_LIKELY(!std::isnan(value))) {
                ++count_;
            }
        } else if constexpr (std::is_same_v<T, timestamp> && TimeType) {
            if (ARCTICDB_LIKELY(value != NaT)) {
                ++count_;
            }
        } else {
            ++count_;
        }
    }

    uint64_t finalize() {
        uint64_t res{count_};
        count_ = 0;
        return res;
    }

  private:
    uint64_t count_{0};
};

struct SortedAggregatorOutputColumnInfo {
    std::optional<DataType> data_type_{};
    bool maybe_sparse_{};
};

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
class SortedAggregator {
  public:
    static constexpr ResampleBoundary closed = closed_boundary;

    explicit SortedAggregator(ColumnName input_column_name, ColumnName output_column_name) :
        input_column_name_(std::move(input_column_name)),
        output_column_name_(std::move(output_column_name)) {}
    ARCTICDB_MOVE_COPY_DEFAULT(SortedAggregator)

    [[nodiscard]] ColumnName get_input_column_name() const { return input_column_name_; }
    [[nodiscard]] ColumnName get_output_column_name() const { return output_column_name_; }

    [[nodiscard]] std::optional<Column> aggregate(
            const std::vector<std::shared_ptr<Column>>& input_index_columns,
            const std::vector<std::optional<ColumnWithStrings>>& input_agg_columns, const ResampleMapping& mapping,
            StringPool& string_pool
    ) const;

    void check_aggregator_supported_with_data_type(DataType data_type) const;
    [[nodiscard]] std::optional<Value> get_default_value(DataType common_input_data_type) const;
    [[nodiscard]] DataType generate_output_data_type(const DataType common_input_data_type) const;
    [[nodiscard]] std::optional<Column> generate_resampling_output_column(
            std::span<const std::optional<ColumnWithStrings>> input_agg_columns, const ResampleMapping& mapping
    ) const;

  private:
    [[nodiscard]] SortedAggregatorOutputColumnInfo generate_common_input_type(std::span<
                                                                              const std::optional<ColumnWithStrings>>)
            const;

    template<DataType input_data_type, typename Aggregator, typename T>
    void push_to_aggregator(
            Aggregator& bucket_aggregator, T value, ARCTICDB_UNUSED const ColumnWithStrings& column_with_strings
    ) const {
        if constexpr (is_time_type(input_data_type) && aggregation_operator == AggregationOperator::COUNT) {
            bucket_aggregator.template push<timestamp, true>(value);
        } else if constexpr (is_numeric_type(input_data_type) || is_bool_type(input_data_type)) {
            bucket_aggregator.push(value);
        } else if constexpr (is_sequence_type(input_data_type)) {
            bucket_aggregator.push(column_with_strings.string_at_offset(value));
        } else {
            static_assert(sizeof(T) == 0, "Unsupported aggregation.");
        }
    }

    template<DataType output_data_type, typename Aggregator>
    [[nodiscard]] auto finalize_aggregator(Aggregator& bucket_aggregator, ARCTICDB_UNUSED StringPool& string_pool)
            const {
        if constexpr (is_numeric_type(output_data_type) || is_bool_type(output_data_type) ||
                      aggregation_operator == AggregationOperator::COUNT) {
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

    template<typename scalar_type_info>
    [[nodiscard]] auto get_bucket_aggregator() const {
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
            } else if constexpr (is_numeric_type(scalar_type_info::data_type) ||
                                 is_bool_type(scalar_type_info::data_type)) {
                return FirstAggregatorSorted<typename scalar_type_info::RawType>();
            } else if constexpr (is_sequence_type(scalar_type_info::data_type)) {
                return FirstAggregatorSorted<std::optional<std::string_view>>();
            }
        } else if constexpr (aggregation_operator == AggregationOperator::LAST) {
            if constexpr (is_time_type(scalar_type_info::data_type)) {
                return LastAggregatorSorted<typename scalar_type_info::RawType, true>();
            } else if constexpr (is_numeric_type(scalar_type_info::data_type) ||
                                 is_bool_type(scalar_type_info::data_type)) {
                return LastAggregatorSorted<typename scalar_type_info::RawType>();
            } else if constexpr (is_sequence_type(scalar_type_info::data_type)) {
                return LastAggregatorSorted<std::optional<std::string_view>>();
            }
        } else if constexpr (aggregation_operator == AggregationOperator::COUNT) {
            return CountAggregatorSorted();
        }
    }

    ColumnName input_column_name_;
    ColumnName output_column_name_;
};

} // namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::AggregationOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::AggregationOperator& agg, FormatContext& ctx) const {
        switch (agg) {
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
} // namespace fmt