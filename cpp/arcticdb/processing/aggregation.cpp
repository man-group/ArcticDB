/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/aggregation.hpp>

#include <cmath>

namespace arcticdb {

void add_data_type_impl(DataType data_type, std::optional<DataType>& current_data_type) {
    if (current_data_type.has_value()) {
        auto common_type = has_valid_common_type(entity::TypeDescriptor(*current_data_type, 0),
                                                 entity::TypeDescriptor(data_type, 0));
        schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                common_type.has_value(),
                "Cannot perform aggregation on column, incompatible types present: {} and {}",
                entity::TypeDescriptor(*current_data_type, 0), entity::TypeDescriptor(data_type, 0));
        current_data_type = common_type->data_type();
    } else {
        current_data_type = data_type;
    }
}

namespace {
    inline util::BitMagic::enumerator::value_type deref(util::BitMagic::enumerator iter) {
        return *iter;
    }

    inline std::size_t deref(std::size_t index) {
        return index;
    }
}

void MinMaxAggregatorData::aggregate(const ColumnWithStrings& input_column) {
    entity::details::visit_type(input_column.column_->type().data_type(), [&input_column, that=this] (auto type_desc_tag) {
        using InputType = decltype(type_desc_tag);
        if constexpr(!is_sequence_type(InputType::DataTypeTag::data_type)) {
            using DescriptorType = std::decay_t<decltype(type_desc_tag)>;
            using RawType = typename DescriptorType::raw_type;
            auto col_data = input_column.column_->data();
            while (auto block = col_data.next<ScalarTagType<DescriptorType>>()) {
                auto ptr = reinterpret_cast<const RawType *>(block.value().data());
                for (auto i = 0u; i < block.value().row_count(); ++i, ++ptr) {
                    const auto& curr = RawType(*ptr);
                    if (UNLIKELY(!that->min_.has_value())) {
                        that->min_ = std::make_optional<Value>(curr, DescriptorType::DataTypeTag::data_type);
                        that->max_ = std::make_optional<Value>(curr, DescriptorType::DataTypeTag::data_type);
                    } else {
                        that->min_->set(std::min(that->min_->get<RawType>(), curr));
                        that->max_->set(std::max(that->max_->get<RawType>(), curr));
                    }
                }
            }
        } else {
            schema::raise<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                    "Minmax column stat generation not supported with string types");
        }
    });
}

SegmentInMemory MinMaxAggregatorData::finalize(const std::vector<ColumnName>& output_column_names) const {
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            output_column_names.size() == 2,
            "Expected 2 output column names in MinMaxAggregatorData::finalize, but got {}",
            output_column_names.size());
    SegmentInMemory seg;
    if (min_.has_value()) {
        entity::details::visit_type(min_->data_type_, [&output_column_names, &seg, that = this](auto type_desc_tag) {
            using RawType = typename decltype(type_desc_tag)::DataTypeTag::raw_type;
            auto min_col = std::make_shared<Column>(make_scalar_type(that->min_->data_type_), true);
            min_col->template push_back<RawType>(that->min_->get<RawType>());

            auto max_col = std::make_shared<Column>(make_scalar_type(that->max_->data_type_), true);
            max_col->template push_back<RawType>(that->max_->get<RawType>());

            seg.add_column(scalar_field(min_col->type().data_type(), output_column_names[0].value), min_col);
            seg.add_column(scalar_field(max_col->type().data_type(), output_column_names[1].value), max_col);
        });
    }
    return seg;
}

// TODO: move this in anonymous namespace
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


void SumAggregatorData::aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values) {
    // If data_type_ has no value, it means there is no data for this aggregation
    // For sums, we want this to display as zero rather than NaN
    if (!data_type_.has_value() || *data_type_ == DataType::EMPTYVAL) {
        data_type_ = DataType::FLOAT64;
    }
    entity::details::visit_type(*data_type_, [&input_column, unique_values, &groups, that=this] (auto global_type_desc_tag) {
        using GlobalInputType = decltype(global_type_desc_tag);
        if constexpr(!is_sequence_type(GlobalInputType::DataTypeTag::data_type)) {
            using GlobalTypeDescriptorTag =  typename OutputType<GlobalInputType>::type;
            using GlobalRawType = typename GlobalTypeDescriptorTag::DataTypeTag::raw_type;
            that->aggregated_.resize(sizeof(GlobalRawType)* unique_values);
            auto out_ptr = reinterpret_cast<GlobalRawType*>(that->aggregated_.data());
            if (input_column.has_value()) {
                entity::details::visit_type(input_column->column_->type().data_type(), [&input_column, &groups, &out_ptr] (auto type_desc_tag) {
                    using ColumnTagType = std::decay_t<decltype(type_desc_tag)>;
                    using ColumnType =  typename ColumnTagType::raw_type;
                    if constexpr(!is_sequence_type(ColumnTagType::data_type)) {
                        auto col_data = input_column->column_->data();
                        auto lambda = [&col_data, &out_ptr, &groups](auto iter) {
                            while (auto block = col_data.next<TypeDescriptorTag<ColumnTagType, DimensionTag<entity::Dimension::Dim0>>>()) {
                                auto ptr = reinterpret_cast<const ColumnType *>(block.value().data());
                                for (auto i = 0u; i < block.value().row_count(); ++i, ++ptr, ++iter) {
                                    out_ptr[groups[deref(iter)]] += GlobalRawType(*ptr);
                                }
                            }
                        };

                        if (input_column->column_->is_sparse()) {
                            lambda(col_data.bit_vector()->first());
                        }
                        else {
                            lambda(std::size_t(0));
                        }
                    } else {
                        util::raise_rte("String aggregations not currently supported");
                    }
                });
            }
        }
    });
}

SegmentInMemory SumAggregatorData::finalize(const ColumnName& output_column_name, bool, size_t unique_values) {
    SegmentInMemory res;
    if(!aggregated_.empty()) {
        entity::details::visit_type(*data_type_, [that=this, &res, &output_column_name, unique_values] (auto type_desc_tag) {
            using RawType = typename decltype(type_desc_tag)::DataTypeTag::raw_type;
            that->aggregated_.resize(sizeof(RawType)* unique_values);
            auto col = std::make_shared<Column>(make_scalar_type(that->data_type_.value()), unique_values, true, false);
            memcpy(col->ptr(), that->aggregated_.data(), that->aggregated_.size());
            res.add_column(scalar_field(that->data_type_.value(), output_column_name.value), col);
            col->set_row_data(unique_values - 1);
        });
    }
    return res;
}

/********************
 * MinMaxAggregator *
 ********************/

namespace min_max_aggregation
{


    template <typename T>
    struct MaybeValue {
        bool written_ = false;
        T value_ = std::numeric_limits<T>::lowest();
    };

    enum class Extremum {
        MAX,
        MIN
    };

    template <Extremum T>
    inline void aggregate(
        const std::optional<ColumnWithStrings>& input_column,
        const std::vector<size_t>& groups,
        size_t unique_values,
        std::vector<uint8_t>& aggregated_,
        std::optional<DataType>& data_type_
    ) {
        if(data_type_.has_value() && *data_type_ != DataType::EMPTYVAL && input_column.has_value()) {
            entity::details::visit_type(*data_type_, [&aggregated_, &data_type_, &input_column, unique_values, &groups] (auto global_type_desc_tag) {
                using GlobalInputType = decltype(global_type_desc_tag);
                if constexpr(!is_sequence_type(GlobalInputType::DataTypeTag::data_type)) {
                    using GlobalTypeDescriptorTag =  typename OutputType<GlobalInputType>::type;
                    using GlobalRawType = typename GlobalTypeDescriptorTag::DataTypeTag::raw_type;
                    auto prev_size = aggregated_.size() / sizeof(MaybeValue<GlobalRawType>);
                    aggregated_.resize(sizeof(MaybeValue<GlobalRawType>) * unique_values);
                    auto col_data = input_column->column_->data();
                    auto out_ptr = reinterpret_cast<MaybeValue<GlobalRawType>*>(aggregated_.data());
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

    inline SegmentInMemory finalize(
            const ColumnName& output_column_name,
            bool dynamic_schema,
            size_t unique_values,
            std::vector<uint8_t>& aggregated_,
            std::optional<DataType>& data_type_
    ) {
        SegmentInMemory res;
        if(!aggregated_.empty()) {
            if(dynamic_schema) {
                entity::details::visit_type(*data_type_, [&aggregated_, &data_type_, &res, &output_column_name, unique_values] (auto type_desc_tag) {
                    using RawType = typename decltype(type_desc_tag)::DataTypeTag::raw_type;
                    auto prev_size = aggregated_.size() / sizeof(MaybeValue<RawType>);
                    auto new_size = sizeof(MaybeValue<RawType>) * unique_values;
                    aggregated_.resize(new_size);
                    auto in_ptr =  reinterpret_cast<MaybeValue<RawType>*>(aggregated_.data());
                    std::fill(in_ptr + prev_size, in_ptr + unique_values, MaybeValue<RawType>{});
                    auto col = std::make_shared<Column>(make_scalar_type(DataType::FLOAT64), unique_values, true, false);
                    auto out_ptr = reinterpret_cast<double*>(col->ptr());
                    for(auto i = 0u; i < unique_values; ++i, ++in_ptr, ++out_ptr) {
                        *out_ptr = in_ptr->written_ ? static_cast<double>(in_ptr->value_) : std::numeric_limits<double>::quiet_NaN();                }

                    col->set_row_data(unique_values - 1);
                    res.add_column(scalar_field(DataType::FLOAT64, output_column_name.value), col);
                });
            } else {
                entity::details::visit_type(*data_type_, [&aggregated_, &data_type_, &res, output_column_name, unique_values] (auto type_desc_tag) {
                    using RawType = typename decltype(type_desc_tag)::DataTypeTag::raw_type;
                    auto col = std::make_shared<Column>(make_scalar_type(data_type_.value()), unique_values, true, false);
                    const auto* in_ptr =  reinterpret_cast<const MaybeValue<RawType>*>(aggregated_.data());
                    auto out_ptr = reinterpret_cast<RawType*>(col->ptr());
                    for(auto i = 0u; i < unique_values; ++i, ++in_ptr, ++out_ptr) {
                        *out_ptr = in_ptr->value_;
                    }
                    col->set_row_data(unique_values - 1);
                    res.add_column(scalar_field(data_type_.value(), output_column_name.value), col);
                });
            }
        }
        return res;
    }

}

void MaxAggregatorData::aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values)
{
    min_max_aggregation::aggregate<min_max_aggregation::Extremum::MAX>(input_column, groups, unique_values, aggregated_, data_type_);
}

SegmentInMemory MaxAggregatorData::finalize(const ColumnName& output_column_name, bool dynamic_schema, size_t unique_values)
{
    return min_max_aggregation::finalize(output_column_name, dynamic_schema, unique_values, aggregated_, data_type_);
}

void MinAggregatorData::aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values)
{
    min_max_aggregation::aggregate<min_max_aggregation::Extremum::MIN>(input_column, groups, unique_values, aggregated_, data_type_);
}

SegmentInMemory MinAggregatorData::finalize(const ColumnName& output_column_name, bool dynamic_schema, size_t unique_values)
{
    return min_max_aggregation::finalize(output_column_name, dynamic_schema, unique_values, aggregated_, data_type_);
}

/**********************
 * MeanAggregatorData *
 **********************/

void MeanAggregatorData::aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values) {
    if(input_column.has_value()) {
        input_column->column_->type().visit_tag([&] (auto type_desc_tag) {
            using TypeDescriptorTag =  decltype(type_desc_tag);
            using RawType = typename TypeDescriptorTag::DataTypeTag::raw_type;

            data_.fractions_.resize(unique_values);

            auto col_data = input_column->column_->data();
            while (auto block = col_data.next<TypeDescriptorTag>()) {
                auto ptr = reinterpret_cast<const RawType *>(block.value().data());
                for (auto i = 0u; i < block.value().row_count(); ++i) {
                    auto group = groups[i];
                    auto& fraction = data_.fractions_[group];
                    fraction.numerator_ += double(*ptr++);
                    ++fraction.denominator_;
                }
            }
        });
    }
}

SegmentInMemory MeanAggregatorData::finalize(const ColumnName& output_column_name,  bool, size_t unique_values) {
    SegmentInMemory res;
    if(!data_.fractions_.empty()) {
        data_.fractions_.resize(unique_values);
        auto pos = res.add_column(scalar_field(DataType::FLOAT64, output_column_name.value), data_.fractions_.size(), true);
        auto& column = res.column(pos);
        auto ptr = reinterpret_cast<double*>(column.ptr());
        column.set_row_data(data_.fractions_.size() - 1);

        for (const auto& fraction : folly::enumerate(data_.fractions_)) {
            ptr[fraction.index] = fraction->to_double();
        }
    }
    return res;
}

void CountAggregatorData::aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values) {
    if(input_column.has_value()) {
        input_column->column_->type().visit_tag([&] (auto type_desc_tag) {
            using TypeDescriptorTag =  decltype(type_desc_tag);
            using RawType = typename TypeDescriptorTag::DataTypeTag::raw_type;

            aggregated_.resize(unique_values);

            if constexpr (is_floating_point_type(TypeDescriptorTag::DataTypeTag::data_type)) {
                auto col_data = input_column->column_->data();
                while (auto block = col_data.next<TypeDescriptorTag>()) {
                    auto ptr = reinterpret_cast<const RawType *>(block.value().data());
                    for (auto i = 0u; i < block.value().row_count(); ++i) {
                        if (!std::isnan(static_cast<double>(ptr[i]))) {
                            auto group = groups[i];
                            auto& val = aggregated_[group];
                            ++val;
                        }
                    }
                }
            } else {
                for (auto group: groups) {
                    auto& val = aggregated_[group];
                    ++val;
                }
            }
        });
    }
}

SegmentInMemory CountAggregatorData::finalize(const ColumnName& output_column_name,  bool, size_t unique_values) {
    SegmentInMemory res;
    if(!aggregated_.empty()) {
        aggregated_.resize(unique_values);
        auto pos = res.add_column(scalar_field(DataType::UINT64, output_column_name.value), unique_values, true);
        auto& column = res.column(pos);
        auto ptr = reinterpret_cast<uint64_t*>(column.ptr());
        column.set_row_data(unique_values - 1);
        memcpy(ptr, aggregated_.data(), sizeof(uint64_t)*unique_values);
    }
    return res;
}

} //namespace arcticdb
