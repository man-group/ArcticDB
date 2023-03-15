/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/processing_segment.hpp>
#include <arcticdb/processing/bucketizer.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/pipeline/value_set.hpp>
#include <arcticdb/processing/aggregation_interface.hpp>

#include <folly/Poly.h>

namespace arcticdb {

template<typename T, typename T2=void>
struct OutputType;

template <typename InputType>
struct OutputType <InputType, typename std::enable_if_t<is_floating_point_type(InputType::DataTypeTag::data_type)>> {
    using type = ScalarTagType<DataTypeTag<DataType::FLOAT64>>;
};

template <typename InputType>
struct OutputType <InputType, typename std::enable_if_t<is_unsigned_type(InputType::DataTypeTag::data_type)>> {
    using type = ScalarTagType<DataTypeTag<DataType::UINT64>>;
};

template <typename InputType>
struct OutputType<InputType, typename std::enable_if_t<is_signed_type(InputType::DataTypeTag::data_type) && is_integer_type(InputType::DataTypeTag::data_type)>> {
    using type = ScalarTagType<DataTypeTag<DataType::INT64>>;
};

template<>
struct OutputType<DataTypeTag<DataType::BOOL8>, void> {
    using type = ScalarTagType<DataTypeTag<DataType::BOOL8>>;
};

template<>
struct OutputType<DataTypeTag<DataType::MICROS_UTC64>, void> {
    using type = ScalarTagType<DataTypeTag<DataType::MICROS_UTC64>>;
};

struct Sum {
    std::vector<uint8_t> aggregated_;
    ColumnName input_column_name_;
    ColumnName output_column_name_;
    DataType data_type_ = {};

    Sum(ColumnName input_column_name, ColumnName output_column_name) :
        input_column_name_(std::move(input_column_name)),
        output_column_name_(std::move(output_column_name)) {
    }

    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values);

    std::optional<DataType> finalize(SegmentInMemory& seg, bool dynamic_schema, size_t unique_values);

    [[nodiscard]] ColumnName get_input_column_name() const { return input_column_name_; }

    [[nodiscard]] ColumnName get_output_column_name() const { return output_column_name_; }

    [[nodiscard]] Sum construct() const { return {input_column_name_, output_column_name_}; }

    void set_data_type(DataType data_type) { data_type_ = data_type; }

};

template <typename T>
struct MaybeValue {
    bool written_ = false;
    T value_ = std::numeric_limits<T>::lowest();
};

enum class Extremum {
    max, min
};

struct MaxOrMin {
    std::vector<uint8_t> aggregated_;
    ColumnName input_column_name_;
    ColumnName output_column_name_;
    DataType data_type_ = {};
    Extremum extremum_;

    MaxOrMin(ColumnName input_column_name, ColumnName output_column_name, Extremum extremum) :
        input_column_name_(std::move(input_column_name)),
        output_column_name_(std::move(output_column_name)),
        extremum_(extremum){
    }

    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values);

    std::optional<DataType> finalize(SegmentInMemory& seg, bool dynamic_schema, size_t unique_values);

    [[nodiscard]] ColumnName get_input_column_name() const { return input_column_name_; }

    [[nodiscard]] ColumnName get_output_column_name() const { return output_column_name_; }

    [[nodiscard]] MaxOrMin construct() const { return {input_column_name_, output_column_name_, extremum_}; }

    void set_data_type(DataType data_type) { data_type_ = data_type; }

};

struct Mean {
    ColumnName input_;
    CountAndTotals data_;
    ColumnName input_column_name_;
    ColumnName output_column_name_;
    DataType data_type_ = {};

    explicit Mean(ColumnName input_column_name, ColumnName output_column_name) :
        input_column_name_(std::move(input_column_name)),
        output_column_name_(std::move(output_column_name)) {
    }

    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values);

    std::optional<DataType> finalize(SegmentInMemory& seg, bool dynamic_schema, size_t unique_values);

    [[nodiscard]] ColumnName get_input_column_name() const { return input_column_name_; }

    [[nodiscard]] ColumnName get_output_column_name() const { return output_column_name_; }

    [[nodiscard]] Mean construct() const { return Mean(input_column_name_, output_column_name_); }

    void set_data_type(DataType data_type) { data_type_ = data_type; }

};

} //namespace arcticdb