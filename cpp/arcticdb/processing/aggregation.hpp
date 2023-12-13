/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <unordered_map>

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/processing/expression_node.hpp>

namespace arcticdb {

class MinMaxAggregatorData
{
public:

    MinMaxAggregatorData() = default;
    ARCTICDB_MOVE_COPY_DEFAULT(MinMaxAggregatorData)

    void aggregate(const ColumnWithStrings& input_column);
    SegmentInMemory finalize(const std::vector<ColumnName>& output_column_names) const;

private:

    std::optional<Value> min_;
    std::optional<Value> max_;
};

class MinMaxAggregator
{
public:

    explicit MinMaxAggregator(ColumnName column_name, ColumnName output_column_name_min, ColumnName output_column_name_max)
        : column_name_(std::move(column_name))
        , output_column_name_min_(std::move(output_column_name_min))
        , output_column_name_max_(std::move(output_column_name_max))
    {}
    ARCTICDB_MOVE_COPY_DEFAULT(MinMaxAggregator)

    [[nodiscard]] ColumnName get_input_column_name() const { return column_name_; }
    [[nodiscard]] std::vector<ColumnName> get_output_column_names() const { return {output_column_name_min_, output_column_name_max_}; }
    [[nodiscard]] MinMaxAggregatorData get_aggregator_data() const { return MinMaxAggregatorData(); }

private:

    ColumnName column_name_;
    ColumnName output_column_name_min_;
    ColumnName output_column_name_max_;
};

class AggregatorDataBase
{
public:

    AggregatorDataBase() = default;
    // Warn on copies as inheriting classes may embed large buffers
    AggregatorDataBase(const AggregatorDataBase&);
    AggregatorDataBase& operator=(const AggregatorDataBase&);

    ARCTICDB_MOVE(AggregatorDataBase);
};

class SumAggregatorData : private AggregatorDataBase
{
public:

    void add_data_type(DataType data_type);
    void set_string_offset_map(const std::unordered_map<entity::position_t, entity::position_t>&) {}
    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values);
    SegmentInMemory finalize(const ColumnName& output_column_name,  bool dynamic_schema, size_t unique_values);

private:

    std::vector<uint8_t> aggregated_;
    std::optional<DataType> data_type_;
};

class MaxAggregatorData : private AggregatorDataBase
{
public:

    void add_data_type(DataType data_type);
    void set_string_offset_map(const std::unordered_map<entity::position_t, entity::position_t>&) {}
    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values);
    SegmentInMemory finalize(const ColumnName& output_column_name, bool dynamic_schema, size_t unique_values);

private:

    std::vector<uint8_t> aggregated_;
    std::optional<DataType> data_type_;
};

class MinAggregatorData : private AggregatorDataBase
{
public:

    void add_data_type(DataType data_type);
    void set_string_offset_map(const std::unordered_map<entity::position_t, entity::position_t>&) {}
    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values);
    SegmentInMemory finalize(const ColumnName& output_column_name, bool dynamic_schema, size_t unique_values);

private:

    std::vector<uint8_t> aggregated_;
    std::optional<DataType> data_type_;
};

class MeanAggregatorData : private AggregatorDataBase
{
public:

    // Mean values are always doubles so this is a no-op
    void add_data_type(DataType) {}
    void set_string_offset_map(const std::unordered_map<entity::position_t, entity::position_t>&) {}
    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values);
    SegmentInMemory finalize(const ColumnName& output_column_name,  bool dynamic_schema, size_t unique_values);

private:

    struct Fraction
    {
        double numerator_{0.0};
        uint64_t denominator_{0};

        double to_double() const;
    };

    std::vector<Fraction> fractions_;
};

class CountAggregatorData : private AggregatorDataBase
{
public:

    // Count values are always integers so this is a no-op
    void add_data_type(DataType) {}
    void set_string_offset_map(const std::unordered_map<entity::position_t, entity::position_t>&) {}
    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values);
    SegmentInMemory finalize(const ColumnName& output_column_name,  bool dynamic_schema, size_t unique_values);

private:

    std::vector<uint64_t> aggregated_;
};

class FirstAggregatorData : private AggregatorDataBase
{
public:

    void add_data_type(DataType data_type);
    // Needs to be called before finalize - only used for strings columns case
    void set_string_offset_map(const std::unordered_map<entity::position_t, entity::position_t>& offset_map);
    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values);
    SegmentInMemory finalize(const ColumnName& output_column_name, bool dynamic_schema, size_t unique_values);

private:

    std::vector<uint8_t> aggregated_;
    std::optional<DataType> data_type_;

    std::unordered_map<entity::position_t, entity::position_t> str_offset_mapping_;
    std::unordered_set<size_t> groups_cache_;
};

class LastAggregatorData : private AggregatorDataBase
{
public:

    void add_data_type(DataType data_type);
    // Needs to be called before finalize - only used for strings columns case
    void set_string_offset_map(const std::unordered_map<entity::position_t, entity::position_t>& offset_map);
    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values);
    SegmentInMemory finalize(const ColumnName& output_column_name, bool dynamic_schema, size_t unique_values);

private:

    std::vector<uint8_t> aggregated_;
    std::optional<DataType> data_type_;

    std::unordered_map<entity::position_t, entity::position_t> str_offset_mapping_;
    std::unordered_set<size_t> groups_cache_;
};

template <class AggregatorData>
class GroupingAggregatorImpl
{
public:

    explicit GroupingAggregatorImpl(ColumnName input_column_name, ColumnName output_column_name)
        : input_column_name_(std::move(input_column_name))
        , output_column_name_(std::move(output_column_name))
    {
    }

    ARCTICDB_MOVE_COPY_DEFAULT(GroupingAggregatorImpl);

    [[nodiscard]] ColumnName get_input_column_name() const { return input_column_name_; }
    [[nodiscard]] ColumnName get_output_column_name() const { return output_column_name_; }
    [[nodiscard]] AggregatorData get_aggregator_data() const { return AggregatorData(); }

private:

    ColumnName input_column_name_;
    ColumnName output_column_name_;
};

using SumAggregator = GroupingAggregatorImpl<SumAggregatorData>;
using MinAggregator = GroupingAggregatorImpl<MinAggregatorData>;
using MaxAggregator = GroupingAggregatorImpl<MaxAggregatorData>;
using MeanAggregator = GroupingAggregatorImpl<MeanAggregatorData>;
using CountAggregator = GroupingAggregatorImpl<CountAggregatorData>;
using FirstAggregator = GroupingAggregatorImpl<FirstAggregatorData>;
using LastAggregator = GroupingAggregatorImpl<LastAggregatorData>;

} //namespace arcticdb
