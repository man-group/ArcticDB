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
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/pipeline/value.hpp>
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

struct MinMaxAggregatorData {
    std::optional<Value> min_;
    std::optional<Value> max_;
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
    void add_data_type(DataType data_type) {
        if (data_type_.has_value()) {
            auto common_type = has_valid_common_type(entity::TypeDescriptor(*data_type_, 0),
                                                     entity::TypeDescriptor(data_type, 0));
            schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                    common_type.has_value(),
                    "Cannot perform sum aggregation on column, incompatible types present: {} and {}",
                    entity::TypeDescriptor(*data_type_, 0), entity::TypeDescriptor(data_type, 0));
            data_type_ = common_type->data_type();
        } else {
            data_type_ = data_type;
        }
    }
    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values);
    SegmentInMemory finalize(const ColumnName& output_column_name,  bool dynamic_schema, size_t unique_values);
};

struct SumAggregator {
    ColumnName input_column_name_;
    ColumnName output_column_name_;

    explicit SumAggregator(ColumnName input_column_name, ColumnName output_column_name) :
        input_column_name_(std::move(input_column_name)),
        output_column_name_(std::move(output_column_name)) {
    }
    ARCTICDB_MOVE_COPY_DEFAULT(SumAggregator)

    [[nodiscard]] ColumnName get_input_column_name() const { return input_column_name_; }
    [[nodiscard]] ColumnName get_output_column_name() const { return output_column_name_; }
    [[nodiscard]] GroupingAggregatorData get_aggregator_data() const { return SumAggregatorData(); }
};

template <typename T>
struct MaybeValue {
    bool written_ = false;
    T value_ = std::numeric_limits<T>::lowest();
};

enum class Extremum {
    MAX,
    MIN
};

template<Extremum T>
struct MaxOrMinAggregatorData {
    std::vector<uint8_t> aggregated_;
    std::optional<DataType> data_type_;
    void add_data_type(DataType data_type) {
        if (data_type_.has_value()) {
            auto common_type = has_valid_common_type(entity::TypeDescriptor(*data_type_, 0),
                                                     entity::TypeDescriptor(data_type, 0));
            schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                    common_type.has_value(),
                    "Cannot perform sum aggregation on column, incompatible types present: {} and {}",
                    entity::TypeDescriptor(*data_type_, 0), entity::TypeDescriptor(data_type, 0));
            data_type_ = common_type->data_type();
        } else {
            data_type_ = data_type;
        }
    }
    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values) {
        if(data_type_.has_value() && input_column.has_value()) {
            entity::details::visit_type(*data_type_, [&input_column, unique_values, &groups, that=this] (auto global_type_desc_tag) {
                using GlobalInputType = decltype(global_type_desc_tag);
                if constexpr(!is_sequence_type(GlobalInputType::DataTypeTag::data_type)) {
                    using GlobalTypeDescriptorTag =  typename OutputType<GlobalInputType>::type;
                    using GlobalRawType = typename GlobalTypeDescriptorTag::DataTypeTag::raw_type;
                    auto prev_size = that->aggregated_.size() / sizeof(MaybeValue<GlobalRawType>);
                    that->aggregated_.resize(sizeof(MaybeValue<GlobalRawType>) * unique_values);
                    auto col_data = input_column->column_->data();
                    auto out_ptr = reinterpret_cast<MaybeValue<GlobalRawType>*>(that->aggregated_.data());
                    std::fill(out_ptr + prev_size, out_ptr + unique_values, MaybeValue<GlobalRawType>{});
                    entity::details::visit_type(input_column->column_->type().data_type(), [&groups, &out_ptr, &col_data, that=that] (auto type_desc_tag) {
                        using InputType = decltype(type_desc_tag);
                        if constexpr(!is_sequence_type(InputType::DataTypeTag::data_type)) {
                            using TypeDescriptorTag =  typename OutputType<InputType>::type;
                            using RawType = typename TypeDescriptorTag::DataTypeTag::raw_type;
                            auto groups_pos = 0;
                            while (auto block = col_data.next<TypeDescriptorTag>()) {
                                auto ptr = reinterpret_cast<const RawType *>(block.value().data());
                                for (auto i = 0u; i < block.value().row_count(); ++i, ++ptr) {
                                    auto& val = out_ptr[groups[groups_pos]];
                                    if constexpr(std::is_floating_point_v<RawType>) {
                                        const auto& curr = GlobalRawType(*ptr);
                                        if (!val.written_ || std::isnan(static_cast<RawType>(val.value_))) {
                                            val.value_ = curr;
                                            val.written_ = true;
                                        } else if (!std::isnan(static_cast<RawType>(curr))) {
                                            if constexpr(T == Extremum::MAX) {
                                                val.value_ = std::max(val.value_, curr);
                                            } else {
                                                val.value_ = std::min(val.value_, curr);
                                            }
                                        }
                                    } else {
                                        if constexpr(T == Extremum::MAX) {
                                            val.value_ = std::max(val.value_, GlobalRawType(*ptr));
                                        } else {
                                            // If unwritten, MaybeValue begins life with MIN_INT. That's always going to "win" when
                                            // passed into `std::min`!
                                            val.value_ = !val.written_ ? GlobalRawType(*ptr) : std::min(val.value_, GlobalRawType(*ptr));
                                        }
                                        val.written_ = true;
                                    }
                                    ++groups_pos;
                                }
                            }
                        } else {
                            util::raise_rte("String aggregations not currently supported");
                        }
                    });
                }
            });
        }
    }
    [[nodiscard]] SegmentInMemory finalize(const ColumnName& output_column_name, bool dynamic_schema, size_t unique_values) {
        SegmentInMemory res;
        if(!aggregated_.empty()) {
            if(dynamic_schema) {
                entity::details::visit_type(*data_type_, [that=this, &res, &output_column_name, unique_values] (auto type_desc_tag) {
                    using RawType = typename decltype(type_desc_tag)::DataTypeTag::raw_type;
                    auto prev_size = that->aggregated_.size() / sizeof(MaybeValue<RawType>);
                    auto new_size = sizeof(MaybeValue<RawType>) * unique_values;
                    that->aggregated_.resize(new_size);
                    auto in_ptr =  reinterpret_cast<MaybeValue<RawType>*>(that->aggregated_.data());
                    std::fill(in_ptr + prev_size, in_ptr + unique_values, MaybeValue<RawType>{});
                    auto col = std::make_shared<Column>(make_scalar_type(DataType::FLOAT64), unique_values, true, false);
                    auto out_ptr = reinterpret_cast<double*>(col->ptr());
                    for(auto i = 0u; i < unique_values; ++i, ++in_ptr, ++out_ptr) {
                        *out_ptr = in_ptr->written_ ? static_cast<double>(in_ptr->value_) : std::numeric_limits<double>::quiet_NaN();                }

                    col->set_row_data(unique_values - 1);
                    res.add_column(scalar_field_proto(DataType::FLOAT64, output_column_name.value), col);
                });
            } else {
                entity::details::visit_type(*data_type_, [that=this, &res, output_column_name, unique_values] (auto type_desc_tag) {
                    using RawType = typename decltype(type_desc_tag)::DataTypeTag::raw_type;
                    auto col = std::make_shared<Column>(make_scalar_type(that->data_type_.value()), unique_values, true, false);
                    const auto* in_ptr =  reinterpret_cast<const MaybeValue<RawType>*>(that->aggregated_.data());
                    auto out_ptr = reinterpret_cast<RawType*>(col->ptr());
                    for(auto i = 0u; i < unique_values; ++i, ++in_ptr, ++out_ptr) {
                        *out_ptr = in_ptr->value_;
                    }
                    col->set_row_data(unique_values - 1);
                    res.add_column(scalar_field_proto(that->data_type_.value(), output_column_name.value), col);
                });
            }
        }
        return res;
    }
};

struct MaxOrMinAggregator {
    ColumnName input_column_name_;
    ColumnName output_column_name_;
    Extremum extremum_;

    explicit MaxOrMinAggregator(ColumnName input_column_name, ColumnName output_column_name, Extremum extremum) :
        input_column_name_(std::move(input_column_name)),
        output_column_name_(std::move(output_column_name)),
        extremum_(extremum){
    }
    ARCTICDB_MOVE_COPY_DEFAULT(MaxOrMinAggregator)

    [[nodiscard]] ColumnName get_input_column_name() const { return input_column_name_; }
    [[nodiscard]] ColumnName get_output_column_name() const { return output_column_name_; }
    [[nodiscard]] GroupingAggregatorData get_aggregator_data() const {
        switch(extremum_) {
            case Extremum::MAX:
                return MaxOrMinAggregatorData<Extremum::MAX>();
            case Extremum::MIN:
                return MaxOrMinAggregatorData<Extremum::MIN>();
            default:
                internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected Extremum enum value in MaxOrMinAggregator");
        }
    }
};

struct MeanAggregatorData {
    CountAndTotals data_;
    // Mean values are always doubles so this is a no-op
    void add_data_type(ARCTICDB_UNUSED DataType data_type) {}
    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values);
    SegmentInMemory finalize(const ColumnName& output_column_name,  bool dynamic_schema, size_t unique_values);
};

struct MeanAggregator {
    ColumnName input_column_name_;
    ColumnName output_column_name_;

    explicit MeanAggregator(ColumnName input_column_name, ColumnName output_column_name) :
            input_column_name_(std::move(input_column_name)),
            output_column_name_(std::move(output_column_name)) {
    }
    ARCTICDB_MOVE_COPY_DEFAULT(MeanAggregator)

    [[nodiscard]] ColumnName get_input_column_name() const { return input_column_name_; }
    [[nodiscard]] ColumnName get_output_column_name() const { return output_column_name_; }
    [[nodiscard]] GroupingAggregatorData get_aggregator_data() const { return MeanAggregatorData(); }
};

} //namespace arcticdb