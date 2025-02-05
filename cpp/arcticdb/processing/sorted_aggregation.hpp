/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <memory>
#include <optional>
#include <vector>
#include <span>

#include <folly/Poly.h>

#include <arcticdb/column_store/column.hpp>
#include <arcticdb/processing/expression_node.hpp>

namespace arcticdb {

enum class ResampleBoundary {
    LEFT,
    RIGHT
};

struct ISortedAggregator {
    template<class Base>
    struct Interface : Base {
        [[nodiscard]] ColumnName get_input_column_name() const { return folly::poly_call<0>(*this); };
        [[nodiscard]] ColumnName get_output_column_name() const { return folly::poly_call<1>(*this); };
        [[nodiscard]] Column aggregate(const std::vector<std::shared_ptr<Column>>& input_index_columns,
                                       const std::vector<std::optional<ColumnWithStrings>>& input_agg_columns,
                                       const std::vector<timestamp>& bucket_boundaries,
                                       const Column& output_index_column,
                                       StringPool& string_pool) const {
            return folly::poly_call<2>(*this, input_index_columns, input_agg_columns, bucket_boundaries, output_index_column, string_pool);
        }
    };

    template<class T>
    using Members = folly::PolyMembers<&T::get_input_column_name, &T::get_output_column_name, &T::aggregate>;
};

using SortedAggregatorInterface = folly::Poly<ISortedAggregator>;

template<ResampleBoundary closed_boundary>
class Bucket {
public:
    Bucket(timestamp start, timestamp end);
    void set_boundaries(timestamp start, timestamp end);
    [[nodiscard]] bool contains(timestamp ts) const;
private:
    timestamp start_;
    timestamp end_;
};

enum class AggregationOperator {
    SUM,
    MEAN,
    MIN,
    MAX,
    FIRST,
    LAST,
    COUNT
};

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
    std::conditional_t<std::is_floating_point_v<T> || TimeType, std::optional<T>,T> min_;
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
    std::conditional_t<std::is_floating_point_v<T> || TimeType, std::optional<T>,T> max_;
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
        } else if constexpr(std::is_same_v<T, timestamp> && TimeType) {
            res = first_.value_or(NaT);
        } else {
            debug::check<ErrorCode::E_ASSERTION_FAILURE>(first_.has_value(), "FirstBucketAggregator::finalize called with no values pushed");
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
        } else if constexpr(std::is_same_v<T, timestamp> && TimeType) {
            res = last_.value_or(NaT);
        } else {
            debug::check<ErrorCode::E_ASSERTION_FAILURE>(last_.has_value(), "LastBucketAggregator::finalize called with no values pushed");
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
    template<typename T, bool TimeType=false>
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

template<AggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
class SortedAggregator
{
public:

    explicit SortedAggregator(ColumnName input_column_name, ColumnName output_column_name);
    ARCTICDB_MOVE_COPY_DEFAULT(SortedAggregator)

    [[nodiscard]] ColumnName get_input_column_name() const;
    [[nodiscard]] ColumnName get_output_column_name() const;

    [[nodiscard]] Column aggregate(const std::vector<std::shared_ptr<Column>>& input_index_columns,
                                   const std::vector<std::optional<ColumnWithStrings>>& input_agg_columns,
                                   const std::vector<timestamp>& bucket_boundaries,
                                   const Column& output_index_column,
                                   StringPool& string_pool) const;
private:
    [[nodiscard]] Column aggregate_static_schema(
        std::span<const std::shared_ptr<Column>> input_index_columns,
        std::span<const std::optional<ColumnWithStrings>> input_agg_columns,
        std::span<const timestamp> bucket_boundaries,
        const Column& output_index_column,
        StringPool& string_pool,
        DataType common_input_type) const;
    [[nodiscard]] Column aggregate_dynamic_schema(
        std::span<const std::shared_ptr<Column>> input_index_columns,
        std::span<const std::optional<ColumnWithStrings>> input_agg_columns,
        std::span<const timestamp> bucket_boundaries,
        const Column& output_index_column,
        StringPool& string_pool) const;
    [[nodiscard]] std::optional<DataType> generate_common_input_type(const std::vector<std::optional<ColumnWithStrings>>& input_agg_columns) const;
    void check_aggregator_supported_with_data_type(DataType data_type) const;
    [[nodiscard]] DataType generate_output_data_type(DataType common_input_data_type) const;
    [[nodiscard]] bool index_value_past_end_of_bucket(timestamp index_value, timestamp bucket_end) const;

    ColumnName input_column_name_;
    ColumnName output_column_name_;
};

} // namespace arcticdb

template<>
struct fmt::formatter<arcticdb::AggregationOperator>;