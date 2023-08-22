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
struct OutputType<DataTypeTag<DataType::NANOSECONDS_UTC64>, void> {
    using type = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
};

template<>
struct OutputType<DataTypeTag<DataType::EMPTYVAL>, void> {
    using type = ScalarTagType<DataTypeTag<DataType::EMPTYVAL>>;
};

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

    MaxOrMinAggregatorData() = default;
    // Warn on copies as aggregated_ could be a large buffer
    MaxOrMinAggregatorData(const MaxOrMinAggregatorData& other):
            aggregated_(other.aggregated_),
            data_type_(other.data_type_) {
        log::version().warn("Copying potentially large buffer in MaxOrMinAggregatorData");
    }
    MaxOrMinAggregatorData& operator=(const MaxOrMinAggregatorData& other) {
        aggregated_ = other.aggregated_;
        data_type_ = other.data_type_;
        log::version().warn("Copying potentially large buffer in MaxOrMinAggregatorData");
        return *this;
    }
    ARCTICDB_MOVE(MaxOrMinAggregatorData)

    void add_data_type(DataType data_type) {
        add_data_type_impl(data_type, data_type_);
    }
    void aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values) {
        if(data_type_.has_value() && *data_type_ != DataType::EMPTYVAL && input_column.has_value()) {
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
                    entity::details::visit_type(input_column->column_->type().data_type(), [&groups, &out_ptr, &col_data] (auto type_desc_tag) {
                        using ColumnTagType = std::decay_t<decltype(type_desc_tag)>;
                        using ColumnType =  typename ColumnTagType::raw_type;
                        if constexpr(!is_sequence_type(ColumnTagType::data_type)) {
                            auto groups_pos = 0;
                            while (auto block = col_data.next<TypeDescriptorTag<ColumnTagType, DimensionTag<entity::Dimension::Dim0>>>()) {
                                auto ptr = reinterpret_cast<const ColumnType *>(block.value().data());
                                for (auto i = 0u; i < block.value().row_count(); ++i, ++ptr) {
                                    auto& val = out_ptr[groups[groups_pos]];
                                    if constexpr(std::is_floating_point_v<ColumnType>) {
                                        const auto& curr = GlobalRawType(*ptr);
                                        if (!val.written_ || std::isnan(static_cast<ColumnType>(val.value_))) {
                                            val.value_ = curr;
                                            val.written_ = true;
                                        } else if (!std::isnan(static_cast<ColumnType>(curr))) {
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
                    res.add_column(scalar_field(DataType::FLOAT64, output_column_name.value), col);
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
                    res.add_column(scalar_field(that->data_type_.value(), output_column_name.value), col);
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