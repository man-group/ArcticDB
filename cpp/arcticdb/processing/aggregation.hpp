/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/processing/bucketizer.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/pipeline/value_set.hpp>
#include <arcticdb/processing/aggregation_interface.hpp>

#include <folly/Poly.h>

namespace arcticdb {



void add_data_type_impl(DataType data_type, std::optional<DataType>& current_data_type);

struct MinMaxAggregatorData {
    std::optional<Value> min_;
    std::optional<Value> max_;

    MinMaxAggregatorData() = default;
    ARCTICDB_MOVE_COPY_DEFAULT(MinMaxAggregatorData)

    void aggregate(const ColumnWithStrings& input_column);
    SegmentInMemory finalize(const std::vector<ColumnName>& output_column_names) const;
};

struct MinMaxAggregator {
    ColumnName column_name_;
    ColumnName output_column_name_min_;
    ColumnName output_column_name_max_;

    explicit MinMaxAggregator(ColumnName column_name, ColumnName output_column_name_min, ColumnName output_column_name_max) :
            column_name_(std::move(column_name)),
            output_column_name_min_(std::move(output_column_name_min)),
            output_column_name_max_(std::move(output_column_name_max)){
    }
    ARCTICDB_MOVE_COPY_DEFAULT(MinMaxAggregator)

    [[nodiscard]] ColumnName get_input_column_name() const { return column_name_; }
    [[nodiscard]] std::vector<ColumnName> get_output_column_names() const { return {output_column_name_min_, output_column_name_max_}; }
    [[nodiscard]] ColumnStatsAggregatorData get_aggregator_data() const { return MinMaxAggregatorData(); }
};

struct SumAggregatorData {
    std::vector<uint8_t> aggregated_;
    std::optional<DataType> data_type_;

    SumAggregatorData() = default;
    // Warn on copies as aggregated_ could be a large buffer
    SumAggregatorData(const SumAggregatorData& other):
    aggregated_(other.aggregated_),
    data_type_(other.data_type_) {
        log::version().warn("Copying potentially large buffer in SumAggregatorData");
    }
    SumAggregatorData& operator=(const SumAggregatorData& other) {
        aggregated_ = other.aggregated_;
        data_type_ = other.data_type_;
        log::version().warn("Copying potentially large buffer in SumAggregatorData");
        return *this;
    }
    ARCTICDB_MOVE(SumAggregatorData)

    void add_data_type(DataType data_type) {
        add_data_type_impl(data_type, data_type_);
    }
    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values);
    SegmentInMemory finalize(const ColumnName& output_column_name,  bool dynamic_schema, size_t unique_values);
};

struct MaxAggregatorData {
    std::vector<uint8_t> aggregated_;
    std::optional<DataType> data_type_;

    MaxAggregatorData() = default;
    // Warn on copies as aggregated_ could be a large buffer
    MaxAggregatorData(const MaxAggregatorData& other):
            aggregated_(other.aggregated_),
            data_type_(other.data_type_) {
        log::version().warn("Copying potentially large buffer in MaxAggregatorData");
    }
    MaxAggregatorData& operator=(const MaxAggregatorData& other) {
        aggregated_ = other.aggregated_;
        data_type_ = other.data_type_;
        log::version().warn("Copying potentially large buffer in MaxAggregatorData");
        return *this;
    }
    ARCTICDB_MOVE(MaxAggregatorData)

    void add_data_type(DataType data_type) {
        add_data_type_impl(data_type, data_type_);
    }

    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values);
    SegmentInMemory finalize(const ColumnName& output_column_name, bool dynamic_schema, size_t unique_values);
};

struct MinAggregatorData {
    std::vector<uint8_t> aggregated_;
    std::optional<DataType> data_type_;

    MinAggregatorData() = default;
    // Warn on copies as aggregated_ could be a large buffer
    MinAggregatorData(const MinAggregatorData& other):
            aggregated_(other.aggregated_),
            data_type_(other.data_type_) {
        log::version().warn("Copying potentially large buffer in MinAggregatorData");
    }
    MinAggregatorData& operator=(const MinAggregatorData& other) {
        aggregated_ = other.aggregated_;
        data_type_ = other.data_type_;
        log::version().warn("Copying potentially large buffer in MinAggregatorData");
        return *this;
    }
    ARCTICDB_MOVE(MinAggregatorData)

    void add_data_type(DataType data_type) {
        add_data_type_impl(data_type, data_type_);
    }

    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values);
    SegmentInMemory finalize(const ColumnName& output_column_name, bool dynamic_schema, size_t unique_values);
};

struct MeanAggregatorData {
    CountAndTotals data_;

    MeanAggregatorData() = default;
    // Warn on copies as data_ could be a large buffer
    MeanAggregatorData(const MeanAggregatorData& other):
            data_(other.data_) {
        log::version().warn("Copying potentially large buffer in MeanAggregatorData");
    }
    MeanAggregatorData& operator=(const MeanAggregatorData& other) {
        data_ = other.data_;
        log::version().warn("Copying potentially large buffer in MeanAggregatorData");
        return *this;
    }
    ARCTICDB_MOVE(MeanAggregatorData)

    // Mean values are always doubles so this is a no-op
    void add_data_type(ARCTICDB_UNUSED DataType data_type) {}
    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values);
    SegmentInMemory finalize(const ColumnName& output_column_name,  bool dynamic_schema, size_t unique_values);
};

struct CountAggregatorData {
    std::vector<uint64_t> aggregated_;

    CountAggregatorData() = default;
    // Warn on copies as aggregated_ could be a large buffer
    CountAggregatorData(const CountAggregatorData& other):
            aggregated_(other.aggregated_) {
        log::version().warn("Copying potentially large buffer in CountAggregatorData");
    }
    CountAggregatorData& operator=(const CountAggregatorData& other) {
        aggregated_ = other.aggregated_;
        log::version().warn("Copying potentially large buffer in CountAggregatorData");
        return *this;
    }
    ARCTICDB_MOVE(CountAggregatorData)

    // Count values are always integers so this is a no-op
    void add_data_type(ARCTICDB_UNUSED DataType data_type) {}
    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values);
    SegmentInMemory finalize(const ColumnName& output_column_name,  bool dynamic_schema, size_t unique_values);
};

template <class AggregatorData>
class GenericAggregator
{
public:

    explicit GenericAggregator(ColumnName input_column_name, ColumnName output_column_name)
        : input_column_name_(std::move(input_column_name))
        , output_column_name_(std::move(output_column_name))
    {
    }

    ARCTICDB_MOVE_COPY_DEFAULT(GenericAggregator);

    [[nodiscard]] ColumnName get_input_column_name() const { return input_column_name_; }
    [[nodiscard]] ColumnName get_output_column_name() const { return output_column_name_; }
    [[nodiscard]] AggregatorData get_aggregator_data() const { return AggregatorData(); }

private:

    ColumnName input_column_name_;
    ColumnName output_column_name_;
};

using SumAggregator = GenericAggregator<SumAggregatorData>;
using MinAggregator = GenericAggregator<MinAggregatorData>;
using MaxAggregator = GenericAggregator<MaxAggregatorData>;
using MeanAggregator = GenericAggregator<MeanAggregatorData>;
using CountAggregator = GenericAggregator<CountAggregatorData>;

} //namespace arcticdb
