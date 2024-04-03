/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/aggregation.hpp>

#include <cmath>

namespace arcticdb
{

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

void MinMaxAggregatorData::aggregate(const ColumnWithStrings& input_column) {
    details::visit_type(input_column.column_->type().data_type(), [&] (auto col_tag) {
        using type_info = ScalarTypeInfo<decltype(col_tag)>;
        using RawType = typename type_info::RawType;
        if constexpr(!is_sequence_type(type_info::data_type)) {
            Column::for_each<typename type_info::TDT>(*input_column.column_, [this](auto value) {
                const auto& curr = static_cast<RawType>(value);
                if (ARCTICDB_UNLIKELY(!min_.has_value())) {
                    min_ = std::make_optional<Value>(curr, type_info::data_type);
                    max_ = std::make_optional<Value>(curr, type_info::data_type);
                } else {
                    min_->set(std::min(min_->get<RawType>(), curr));
                    max_->set(std::max(max_->get<RawType>(), curr));
                }
            });
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
        details::visit_type(min_->data_type_, [&output_column_names, &seg, that = this](auto col_tag) {
            using RawType = typename ScalarTypeInfo<decltype(col_tag)>::RawType;
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

namespace
{
    inline util::BitMagic::enumerator::value_type deref(util::BitMagic::enumerator iter) {
        return *iter;
    }

    inline std::size_t deref(std::size_t index) {
        return index;
    }

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

    template<>
    struct OutputType<DataTypeTag<DataType::ASCII_FIXED64>, void> {
        using type = ScalarTagType<DataTypeTag<DataType::ASCII_FIXED64>>;
    };

    template<>
    struct OutputType<DataTypeTag<DataType::UTF_DYNAMIC64>, void> {
        using type = ScalarTagType<DataTypeTag<DataType::UTF_DYNAMIC64>>;
    };

    template<>
    struct OutputType<DataTypeTag<DataType::ASCII_DYNAMIC64>, void> {
        using type = ScalarTagType<DataTypeTag<DataType::ASCII_DYNAMIC64>>;
    };

    template<>
    struct OutputType<DataTypeTag<DataType::UTF_FIXED64>, void> {
        using type = ScalarTagType<DataTypeTag<DataType::UTF_FIXED64>>;
    };

    template<>
    struct OutputType<DataTypeTag<DataType::BOOL_OBJECT8>, void> {
        using type = ScalarTagType<DataTypeTag<DataType::BOOL_OBJECT8>>;
    };
}

/**********************
 * AggregatorDataBase *
 **********************/

AggregatorDataBase::AggregatorDataBase(const AggregatorDataBase&)
{
    log::version().warn("Copying potentially large buffer in AggregatorData");
}

AggregatorDataBase& AggregatorDataBase::operator=(const AggregatorDataBase&)
{
    log::version().warn("Copying potentially large buffer in AggregatorData");
    return *this;
}

/*********************
 * SumAggregatorData *
 *********************/

void SumAggregatorData::add_data_type(DataType data_type) {
    add_data_type_impl(data_type, data_type_);
}

void SumAggregatorData::aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values) {
    // If data_type_ has no value, it means there is no data for this aggregation
    // For sums, we want this to display as zero rather than NaN
    if (!data_type_.has_value() || *data_type_ == DataType::EMPTYVAL) {
        data_type_ = DataType::FLOAT64;
    }
    details::visit_type(*data_type_, [&input_column, unique_values, &groups, this] (auto global_tag) {
        using global_type_info = ScalarTypeInfo<decltype(global_tag)>;
        using RawType = typename global_type_info::RawType;
        if constexpr(!is_sequence_type(global_type_info::data_type)) {
            aggregated_.resize(sizeof(RawType) * unique_values);
            auto out_ptr = reinterpret_cast<RawType*>(aggregated_.data());
            if (input_column.has_value()) {
                details::visit_type(input_column->column_->type().data_type(), [&input_column, &groups, &out_ptr] (auto col_tag) {
                    using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
                    if constexpr(!is_sequence_type(col_type_info::data_type)) {
                        Column::for_each_enumerated<typename col_type_info::TDT>(*input_column->column_, [&out_ptr, &groups](auto enumerating_it) {
                            if constexpr (is_bool_type(global_type_info::data_type)) {
                                out_ptr[groups[enumerating_it.idx()]] |= RawType(enumerating_it.value());
                            } else if constexpr (is_floating_point_type(col_type_info::data_type)) {
                                if (ARCTICDB_LIKELY(!std::isnan(enumerating_it.value()))) {
                                    out_ptr[groups[enumerating_it.idx()]] += RawType(enumerating_it.value());
                                }
                            } else {
                                out_ptr[groups[enumerating_it.idx()]] += RawType(enumerating_it.value());
                            }
                        });
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
        details::visit_type(*data_type_, [that=this, &res, &output_column_name, unique_values] (auto col_tag) {
            using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
            that->aggregated_.resize(sizeof(typename col_type_info::RawType)* unique_values);
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

namespace
{
    enum class Extremum
    {
        MAX,
        MIN
    };

    template <typename T, Extremum E>
    struct MaybeValue
    {
        bool written_ = false;
        T value_ = init_value();

    private:

        static constexpr T init_value()
        {
            if constexpr (E == Extremum::MAX)
                return std::numeric_limits<T>::lowest();
            else
                return std::numeric_limits<T>::max();
        }
    };

    template <Extremum T>
    inline void aggregate_impl(
        const std::optional<ColumnWithStrings>& input_column,
        const std::vector<size_t>& groups,
        size_t unique_values,
        std::vector<uint8_t>& aggregated_,
        std::optional<DataType>& data_type_
    ) {
        if(data_type_.has_value() && *data_type_ != DataType::EMPTYVAL && input_column.has_value()) {
            details::visit_type(*data_type_, [&aggregated_, &input_column, unique_values, &groups] (auto global_tag) {
                using global_type_info = ScalarTypeInfo<decltype(global_tag)>;
                using GlobalRawType = typename global_type_info::RawType;
                if constexpr(!is_sequence_type(global_type_info::data_type)) {
                    using MaybeValueType = MaybeValue<GlobalRawType, T>;
                    auto prev_size = aggregated_.size() / sizeof(MaybeValueType);
                    aggregated_.resize(sizeof(MaybeValueType) * unique_values);
                    auto out_ptr = reinterpret_cast<MaybeValueType*>(aggregated_.data());
                    std::fill(out_ptr + prev_size, out_ptr + unique_values, MaybeValueType{});
                    details::visit_type(input_column->column_->type().data_type(), [&input_column, &groups, &out_ptr] (auto col_tag) {
                        using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
                        using ColRawType = typename col_type_info::RawType;
                        if constexpr(!is_sequence_type(col_type_info::data_type)) {
                            Column::for_each_enumerated<typename col_type_info::TDT>(*input_column->column_, [&groups, &out_ptr](auto enumerating_it) {
                                auto& val = out_ptr[groups[enumerating_it.idx()]];
                                if constexpr(std::is_floating_point_v<ColRawType>) {
                                    const auto& curr = GlobalRawType(enumerating_it.value());
                                    if (!val.written_ || std::isnan(static_cast<ColRawType>(val.value_))) {
                                        val.value_ = curr;
                                        val.written_ = true;
                                    } else if (!std::isnan(static_cast<ColRawType>(curr))) {
                                        if constexpr(T == Extremum::MAX) {
                                            val.value_ = std::max(val.value_, curr);
                                        } else {
                                            val.value_ = std::min(val.value_, curr);
                                        }
                                    }
                                } else {
                                    if constexpr(T == Extremum::MAX) {
                                        val.value_ = std::max(val.value_, GlobalRawType(enumerating_it.value()));
                                    } else {
                                        val.value_ = std::min(val.value_, GlobalRawType(enumerating_it.value()));
                                    }
                                    val.written_ = true;
                                }
                            });
                        } else {
                            util::raise_rte("String aggregations not currently supported");
                        }
                    });
                }
            });
        }
    }

    template <Extremum T>
    inline SegmentInMemory finalize_impl(
            const ColumnName& output_column_name,
            bool dynamic_schema,
            size_t unique_values,
            std::vector<uint8_t>& aggregated_,
            std::optional<DataType>& data_type_
    ) {
        SegmentInMemory res;
        if(!aggregated_.empty()) {
            constexpr auto dynamic_schema_data_type = DataType::FLOAT64;
            using DynamicSchemaTDT = ScalarTagType<DataTypeTag<dynamic_schema_data_type>>;
            auto col = std::make_shared<Column>(make_scalar_type(dynamic_schema ? dynamic_schema_data_type: data_type_.value()), unique_values, true, false);
            auto column_data = col->data();
            col->set_row_data(unique_values - 1);
            res.add_column(scalar_field(dynamic_schema ? dynamic_schema_data_type : data_type_.value(), output_column_name.value), col);
            details::visit_type(*data_type_, [&aggregated_, &column_data, unique_values, dynamic_schema] (auto col_tag) {
                using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
                using MaybeValueType = MaybeValue<typename col_type_info::RawType, T>;
                if(dynamic_schema) {
                    auto prev_size = aggregated_.size() / sizeof(MaybeValueType);
                    auto new_size = sizeof(MaybeValueType) * unique_values;
                    aggregated_.resize(new_size);
                    auto in_ptr = reinterpret_cast<MaybeValueType *>(aggregated_.data());
                    std::fill(in_ptr + prev_size, in_ptr + unique_values, MaybeValueType{});
                    for (auto it = column_data.begin<DynamicSchemaTDT>(); it != column_data.end<DynamicSchemaTDT>(); ++it, ++in_ptr) {
                        *it = in_ptr->written_ ? static_cast<double>(in_ptr->value_)
                                               : std::numeric_limits<double>::quiet_NaN();
                    }
                } else {
                    auto in_ptr = reinterpret_cast<MaybeValueType*>(aggregated_.data());
                    for (auto it = column_data.begin<typename col_type_info::TDT>(); it != column_data.end<typename col_type_info::TDT>(); ++it, ++in_ptr) {
                        *it = in_ptr->value_;
                    }
                }
            });
        }
        return res;
    }
}

/*********************
 * MaxAggregatorData *
 *********************/

void MaxAggregatorData::add_data_type(DataType data_type)
{
    add_data_type_impl(data_type, data_type_);
}

void MaxAggregatorData::aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values)
{
    aggregate_impl<Extremum::MAX>(input_column, groups, unique_values, aggregated_, data_type_);
}

SegmentInMemory MaxAggregatorData::finalize(const ColumnName& output_column_name, bool dynamic_schema, size_t unique_values)
{
    return finalize_impl<Extremum::MAX>(output_column_name, dynamic_schema, unique_values, aggregated_, data_type_);
}

/*********************
 * MinAggregatorData *
 *********************/

void MinAggregatorData::add_data_type(DataType data_type)
{
    add_data_type_impl(data_type, data_type_);
}

void MinAggregatorData::aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values)
{
    aggregate_impl<Extremum::MIN>(input_column, groups, unique_values, aggregated_, data_type_);
}

SegmentInMemory MinAggregatorData::finalize(const ColumnName& output_column_name, bool dynamic_schema, size_t unique_values)
{
    return finalize_impl<Extremum::MIN>(output_column_name, dynamic_schema, unique_values, aggregated_, data_type_);
}

/**********************
 * MeanAggregatorData *
 **********************/

void MeanAggregatorData::aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values) {
    if(input_column.has_value()) {
        fractions_.resize(unique_values);
        details::visit_type(input_column->column_->type().data_type(), [&input_column, &groups, this] (auto col_tag) {
            using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
            if constexpr(!is_sequence_type(col_type_info::data_type)) {
                Column::for_each_enumerated<typename col_type_info::TDT>(*input_column->column_, [&groups, this](auto enumerating_it) {
                    auto& fraction = fractions_[groups[enumerating_it.idx()]];
                    if constexpr ((is_floating_point_type(col_type_info ::data_type))) {
                        if (ARCTICDB_LIKELY(!std::isnan(enumerating_it.value()))) {
                            fraction.numerator_ += double(enumerating_it.value());
                            ++fraction.denominator_;
                        }
                    } else {
                        fraction.numerator_ += double(enumerating_it.value());
                        ++fraction.denominator_;
                    }
                });
            } else {
                util::raise_rte("String aggregations not currently supported");
            }
        });
    }
}

SegmentInMemory MeanAggregatorData::finalize(const ColumnName& output_column_name,  bool, size_t unique_values) {
    SegmentInMemory res;
    if(!fractions_.empty()) {
        fractions_.resize(unique_values);
        auto col = std::make_shared<Column>(make_scalar_type(DataType::FLOAT64), fractions_.size(), true, false);
        auto column_data = col->data();
        std::transform(fractions_.cbegin(), fractions_.cend(), column_data.begin<ScalarTagType<DataTypeTag<DataType::FLOAT64>>>(), [](auto fraction) {
            return fraction.to_double();
        });
        col->set_row_data(fractions_.size() - 1);
        res.add_column(scalar_field(DataType::FLOAT64, output_column_name.value), col);
    }
    return res;
}

double MeanAggregatorData::Fraction::to_double() const
{
    return denominator_ == 0 ? std::numeric_limits<double>::quiet_NaN(): numerator_ / static_cast<double>(denominator_);
}

/***********************
 * CountAggregatorData *
 ***********************/

void CountAggregatorData::aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values) {
    if(input_column.has_value()) {
        aggregated_.resize(unique_values);
        details::visit_type(input_column->column_->type().data_type(), [&input_column, &groups, this] (auto col_tag) {
            using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
            Column::for_each_enumerated<typename col_type_info::TDT>(*input_column->column_, [&groups, this](auto enumerating_it) {
                if constexpr (is_floating_point_type(col_type_info::data_type)) {
                    if (ARCTICDB_LIKELY(!std::isnan(enumerating_it.value()))) {
                        auto& val = aggregated_[groups[enumerating_it.idx()]];
                        ++val;
                    }
                } else {
                    auto& val = aggregated_[groups[enumerating_it.idx()]];
                    ++val;
                }
            });
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

/***********************
 * FirstAggregatorData *
 ***********************/

void FirstAggregatorData::add_data_type(DataType data_type) {
    add_data_type_impl(data_type, data_type_);
}

void FirstAggregatorData::aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values) {
    if(data_type_.has_value() && *data_type_ != DataType::EMPTYVAL && input_column.has_value()) {
        details::visit_type(*data_type_, [&input_column, unique_values, &groups, this] (auto global_tag) {
            using GlobalInputType = decltype(global_tag);
            using GlobalTypeDescriptorTag =  typename OutputType<GlobalInputType>::type;
            using GlobalRawType = typename GlobalTypeDescriptorTag::DataTypeTag::raw_type;
            aggregated_.resize(sizeof(GlobalRawType)* unique_values);
            auto col_data = input_column->column_->data();
            auto out_ptr = reinterpret_cast<GlobalRawType*>(aggregated_.data());
            details::visit_type(input_column->column_->type().data_type(), [this, &groups, &out_ptr, &col_data] (auto col_tag) {
                using ColumnTagType = std::decay_t<decltype(col_tag)>;
                using ColumnType =  typename ColumnTagType::raw_type;
                auto groups_pos = 0;
                while (auto block = col_data.next<TypeDescriptorTag<ColumnTagType, DimensionTag<entity::Dimension::Dim0>>>()) {
                    auto ptr = reinterpret_cast<const ColumnType *>(block.value().data());
                    for (auto i = 0u; i < block.value().row_count(); ++i, ++ptr, ++groups_pos) {
                        auto& val = out_ptr[groups[groups_pos]];
                        bool is_first_group_el = (groups_cache_.find(groups[groups_pos]) == groups_cache_.end());
                        if constexpr(std::is_floating_point_v<ColumnType>) {
                            if (is_first_group_el || std::isnan(static_cast<ColumnType>(val))) {
                                groups_cache_.insert(groups[groups_pos]);
                                val = GlobalRawType(*ptr);
                            }
                        } else {
                            if (is_first_group_el) {
                                groups_cache_.insert(groups[groups_pos]);
                                val = GlobalRawType(*ptr);
                            }
                        }
                    }
                }
            });
        });
    }
}

SegmentInMemory FirstAggregatorData::finalize(const ColumnName& output_column_name, bool, size_t unique_values) {
    SegmentInMemory res;
    if(!aggregated_.empty()) {
        details::visit_type(*data_type_, [that=this, &res, &output_column_name, unique_values] (auto col_tag) {
            using RawType = typename decltype(col_tag)::DataTypeTag::raw_type;
            that->aggregated_.resize(sizeof(RawType)* unique_values);
            auto col = std::make_shared<Column>(make_scalar_type(that->data_type_.value()), unique_values, true, false);
            memcpy(col->ptr(), that->aggregated_.data(), that->aggregated_.size());
            res.add_column(scalar_field(that->data_type_.value(), output_column_name.value), col);
            col->set_row_data(unique_values - 1);
        });
    }
    return res;
}

/***********************
 * LastAggregatorData *
 ***********************/

void LastAggregatorData::add_data_type(DataType data_type) {
    add_data_type_impl(data_type, data_type_);
}

void LastAggregatorData::aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values) {
    if(data_type_.has_value() && *data_type_ != DataType::EMPTYVAL && input_column.has_value()) {
        details::visit_type(*data_type_, [&input_column, unique_values, &groups, this] (auto global_tag) {
            using GlobalInputType = decltype(global_tag);
            using GlobalTypeDescriptorTag =  typename OutputType<GlobalInputType>::type;
            using GlobalRawType = typename GlobalTypeDescriptorTag::DataTypeTag::raw_type;
            aggregated_.resize(sizeof(GlobalRawType)* unique_values);
            auto col_data = input_column->column_->data();
            auto out_ptr = reinterpret_cast<GlobalRawType*>(aggregated_.data());
            details::visit_type(input_column->column_->type().data_type(), [&groups, &out_ptr, &col_data, this] (auto col_tag) {
                using ColumnTagType = std::decay_t<decltype(col_tag)>;
                using ColumnType =  typename ColumnTagType::raw_type;
                auto groups_pos = 0;
                while (auto block = col_data.next<TypeDescriptorTag<ColumnTagType, DimensionTag<entity::Dimension::Dim0>>>()) {
                    auto ptr = reinterpret_cast<const ColumnType *>(block.value().data());
                    for (auto i = 0u; i < block.value().row_count(); ++i, ++ptr, ++groups_pos) {
                        auto& val = out_ptr[groups[groups_pos]];
                        if constexpr(std::is_floating_point_v<ColumnType>) {
                            bool is_first_group_el = (groups_cache_.find(groups[groups_pos]) == groups_cache_.end());
                            const auto& curr = GlobalRawType(*ptr);
                            if (is_first_group_el || !std::isnan(static_cast<ColumnType>(curr))) {
                                groups_cache_.insert(groups[groups_pos]);
                                val = curr;
                            }
                        } else {
                            val = GlobalRawType(*ptr);
                        }
                    }
                }
            });
        });
    }
}

SegmentInMemory LastAggregatorData::finalize(const ColumnName& output_column_name, bool, size_t unique_values) {
    SegmentInMemory res;
    if(!aggregated_.empty()) {
        details::visit_type(*data_type_, [that=this, &res, &output_column_name, unique_values] (auto col_tag) {
            using RawType = typename decltype(col_tag)::DataTypeTag::raw_type;
            that->aggregated_.resize(sizeof(RawType)* unique_values);
            auto col = std::make_shared<Column>(make_scalar_type(that->data_type_.value()), unique_values, true, false);
            memcpy(col->ptr(), that->aggregated_.data(), that->aggregated_.size());
            res.add_column(scalar_field(that->data_type_.value(), output_column_name.value), col);
            col->set_row_data(unique_values - 1);
        });
    }
    return res;
}

template<SortedAggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
Column SortedAggregator<aggregation_operator, closed_boundary>::aggregate(const std::vector<std::shared_ptr<Column>>& input_index_columns,
                                                                          const std::vector<std::optional<ColumnWithStrings>>& input_agg_columns,
                                                                          const std::vector<timestamp>& bucket_boundaries,
                                                                          const Column& output_index_column,
                                                                          StringPool& string_pool) const {
    std::optional<Column> res;
    std::optional<DataType> common_input_type;
    for (const auto& opt_input_agg_column: input_agg_columns) {
        if (opt_input_agg_column.has_value()) {
            auto input_data_type = opt_input_agg_column->column_->type().data_type();
            schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(is_numeric_type(input_data_type) || is_bool_type(input_data_type) ||
                                                                (is_sequence_type(input_data_type) &&
                                                                 (aggregation_operator == SortedAggregationOperator::FIRST ||
                                                                  aggregation_operator == SortedAggregationOperator::LAST ||
                                                                  aggregation_operator == SortedAggregationOperator::COUNT)),
                                                                "Resample: Unsupported aggregation type {} on column '{}' of type {}",
                                                                aggregation_operator, get_input_column_name().value, input_data_type);
            add_data_type_impl(input_data_type, common_input_type);
        } else {
            // Column is missing from this row-slice due to dynamic schema, currently unsupported
            schema::raise<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>("Resample: Cannot aggregate column '{}' as it is missing from some row slices",
                                                                get_input_column_name().value);
        }
    }
    if (common_input_type.has_value()) {
        DataType output_type{*common_input_type};
        if constexpr (aggregation_operator == SortedAggregationOperator::SUM) {
            schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(!is_time_type(*common_input_type),
                                                                "Resample: Unsupported aggregation type {} on column '{}' of type {}",
                                                                aggregation_operator, get_input_column_name().value, *common_input_type);
            // Deal with overflow as best we can
            if (is_unsigned_type(*common_input_type) || is_bool_type(*common_input_type)) {
                output_type = DataType::UINT64;
            } else if (is_signed_type(*common_input_type)) {
                output_type = DataType::INT64;
            } else if (is_floating_point_type(*common_input_type)) {
                output_type = DataType::FLOAT64;
            }
        } else if constexpr (aggregation_operator == SortedAggregationOperator::MEAN) {
            if (!is_time_type(*common_input_type)) {
                output_type = DataType::FLOAT64;
            }
        } else if constexpr (aggregation_operator == SortedAggregationOperator::COUNT) {
            output_type = DataType::UINT64;
        }

        res.emplace(TypeDescriptor(output_type, Dimension::Dim0), output_index_column.row_count(), true, false);
        details::visit_type(
            res->type().data_type(),
            [this,
            &input_index_columns,
            &input_agg_columns,
            &bucket_boundaries,
            &string_pool,
            &res](auto output_type_desc_tag) {
                using output_type_info = ScalarTypeInfo<decltype(output_type_desc_tag)>;
                auto output_data = res->data();
                auto output_it = output_data.begin<typename output_type_info::TDT>();
                auto output_end_it = output_data.end<typename output_type_info::TDT>();
                using IndexTDT = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
                // Need this here to only generate valid get_bucket_aggregator code
                constexpr bool supported_aggregation_type_combo = is_numeric_type(output_type_info::data_type) ||
                                                                  is_bool_type(output_type_info::data_type) ||
                                                                  (is_sequence_type(output_type_info::data_type) &&
                                                                  (aggregation_operator == SortedAggregationOperator::FIRST ||
                                                                   aggregation_operator == SortedAggregationOperator::LAST));
                if constexpr (supported_aggregation_type_combo) {
                    auto bucket_aggregator = get_bucket_aggregator<output_type_info>();
                    bool reached_end_of_buckets{false};
                    auto bucket_start_it = bucket_boundaries.cbegin();
                    auto bucket_end_it = std::next(bucket_start_it);
                    const auto bucket_boundaries_end = bucket_boundaries.cend();
                    for (auto [idx, input_agg_column]: folly::enumerate(input_agg_columns)) {
                        // Always true right now due to check at the top of this function
                        if (input_agg_column.has_value()) {
                            details::visit_type(
                                input_agg_column->column_->type().data_type(),
                                [this,
                                &output_it,
                                &bucket_aggregator,
                                &agg_column = *input_agg_column,
                                &input_index_column = input_index_columns.at(idx),
                                &bucket_boundaries_end,
                                &string_pool,
                                &bucket_start_it,
                                &bucket_end_it,
                                &reached_end_of_buckets](auto input_type_desc_tag) {
                                    using input_type_info = ScalarTypeInfo<decltype(input_type_desc_tag)>;
                                    if constexpr ((is_numeric_type(input_type_info::data_type) && is_numeric_type(output_type_info::data_type)) ||
                                                  (is_sequence_type(input_type_info::data_type) && (is_sequence_type(output_type_info::data_type) || aggregation_operator == SortedAggregationOperator::COUNT)) ||
                                                  (is_bool_type(input_type_info::data_type) && (is_bool_type(output_type_info::data_type) || is_numeric_type(output_type_info::data_type)))) {
                                        schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                                                !agg_column.column_->is_sparse() && agg_column.column_->row_count() == input_index_column->row_count(),
                                                "Resample: Cannot aggregate column '{}' as it is sparse",
                                                get_input_column_name().value);
                                        auto index_data = input_index_column->data();
                                        const auto index_cend = index_data.template cend<IndexTDT>();
                                        auto agg_data = agg_column.column_->data();
                                        auto agg_it = agg_data.template cbegin<typename input_type_info::TDT>();
                                        for (auto index_it = index_data.template cbegin<IndexTDT>();
                                             index_it != index_cend && !reached_end_of_buckets;
                                             ++index_it, ++agg_it) {
                                            if (ARCTICDB_LIKELY(index_value_in_bucket(*index_it, *bucket_start_it, *bucket_end_it))) {
                                                if constexpr(is_time_type(input_type_info::data_type) && aggregation_operator == SortedAggregationOperator::COUNT) {
                                                    bucket_aggregator.template push<typename input_type_info::RawType, true>(*agg_it);
                                                } else if constexpr (is_numeric_type(input_type_info::data_type) || is_bool_type(input_type_info::data_type)) {
                                                    bucket_aggregator.push(*agg_it);
                                                } else if constexpr (is_sequence_type(input_type_info::data_type)) {
                                                    bucket_aggregator.push(agg_column.string_at_offset(*agg_it));
                                                }
                                            } else if (ARCTICDB_LIKELY(index_value_past_end_of_bucket(*index_it, *bucket_end_it))) {
                                                if constexpr (is_numeric_type(output_type_info::data_type) ||
                                                              is_bool_type(output_type_info::data_type) ||
                                                              aggregation_operator ==
                                                              SortedAggregationOperator::COUNT) {
                                                    *output_it++ = bucket_aggregator.finalize();
                                                } else if constexpr (is_sequence_type(output_type_info::data_type)) {
                                                    auto opt_string_view = bucket_aggregator.finalize();
                                                    if (ARCTICDB_LIKELY(opt_string_view.has_value())) {
                                                        *output_it++ = string_pool.get(*opt_string_view).offset();
                                                    } else {
                                                        *output_it++ = string_none;
                                                    }
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
                                                // See comment in  ResampleClause::advance_bucket_past_value for mathematical and experimental bounds
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
                                                if (ARCTICDB_LIKELY(!reached_end_of_buckets && index_value_in_bucket(*index_it, *bucket_start_it, *bucket_end_it))) {
                                                    if constexpr(is_time_type(input_type_info::data_type) && aggregation_operator == SortedAggregationOperator::COUNT) {
                                                        bucket_aggregator.template push<typename input_type_info::RawType, true>(*agg_it);
                                                    } else if constexpr (is_numeric_type(input_type_info::data_type) || is_bool_type(input_type_info::data_type)) {
                                                        bucket_aggregator.push(*agg_it);
                                                    } else if constexpr (is_sequence_type(input_type_info::data_type)) {
                                                        bucket_aggregator.push(agg_column.string_at_offset(*agg_it));
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
                        if constexpr (is_numeric_type(output_type_info::data_type) ||
                                      is_bool_type(output_type_info::data_type) ||
                                      aggregation_operator == SortedAggregationOperator::COUNT) {
                            *output_it++ = bucket_aggregator.finalize();
                        } else if constexpr (is_sequence_type(output_type_info::data_type)) {
                            auto opt_string_view = bucket_aggregator.finalize();
                            if (opt_string_view.has_value()) {
                                *output_it++ = string_pool.get(*opt_string_view).offset();
                            } else {
                                *output_it++ = string_none;
                            }
                        }
                    }
                }
            }
        );
    }
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(res.has_value(),
                                                    "Should not be able to reach end of SortedAggregator without a column to return");
    return std::move(*res);
}

template<SortedAggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
bool SortedAggregator<aggregation_operator, closed_boundary>::index_value_past_end_of_bucket(timestamp index_value, timestamp bucket_end) const {
    if constexpr (closed_boundary == ResampleBoundary::LEFT) {
        return index_value >= bucket_end;
    } else {
        // closed_boundary == ResampleBoundary::RIGHT
        return index_value > bucket_end;
    }
}

template<SortedAggregationOperator aggregation_operator, ResampleBoundary closed_boundary>
bool SortedAggregator<aggregation_operator, closed_boundary>::index_value_in_bucket(timestamp index_value, timestamp bucket_start, timestamp bucket_end) const {
    if constexpr (closed_boundary == ResampleBoundary::LEFT) {
        return index_value >= bucket_start && index_value < bucket_end;
    } else {
        // closed_boundary == ResampleBoundary::RIGHT
        return index_value > bucket_start && index_value <= bucket_end;
    }
}

template class SortedAggregator<SortedAggregationOperator::SUM, ResampleBoundary::LEFT>;
template class SortedAggregator<SortedAggregationOperator::SUM, ResampleBoundary::RIGHT>;
template class SortedAggregator<SortedAggregationOperator::MIN, ResampleBoundary::LEFT>;
template class SortedAggregator<SortedAggregationOperator::MIN, ResampleBoundary::RIGHT>;
template class SortedAggregator<SortedAggregationOperator::MAX, ResampleBoundary::LEFT>;
template class SortedAggregator<SortedAggregationOperator::MAX, ResampleBoundary::RIGHT>;
template class SortedAggregator<SortedAggregationOperator::MEAN, ResampleBoundary::LEFT>;
template class SortedAggregator<SortedAggregationOperator::MEAN, ResampleBoundary::RIGHT>;
template class SortedAggregator<SortedAggregationOperator::FIRST, ResampleBoundary::LEFT>;
template class SortedAggregator<SortedAggregationOperator::FIRST, ResampleBoundary::RIGHT>;
template class SortedAggregator<SortedAggregationOperator::LAST, ResampleBoundary::LEFT>;
template class SortedAggregator<SortedAggregationOperator::LAST, ResampleBoundary::RIGHT>;
template class SortedAggregator<SortedAggregationOperator::COUNT, ResampleBoundary::LEFT>;
template class SortedAggregator<SortedAggregationOperator::COUNT, ResampleBoundary::RIGHT>;

} //namespace arcticdb
