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

void MinMaxAggregatorData::aggregate(const ColumnWithStrings& input_column) {
    details::visit_type(input_column.column_->type().data_type(), [&] (auto col_tag) {
        using type_info = ScalarTypeInfo<decltype(col_tag)>;
        if constexpr(!is_sequence_type(type_info::data_type)) {
            Column::for_each<typename type_info::TDT>(*input_column.column_, [this](auto value) {
                const auto& curr = static_cast<typename type_info::RawType>(value);
                if (ARCTICDB_UNLIKELY(!min_.has_value())) {
                    min_ = std::make_optional<Value>(curr, type_info::data_type);
                    max_ = std::make_optional<Value>(curr, type_info::data_type);
                } else {
                    min_->set(std::min(min_->get<typename type_info::RawType>(), curr));
                    max_->set(std::max(max_->get<typename type_info::RawType>(), curr));
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
        using GlobalInputType = decltype(global_tag);
        if constexpr(!is_sequence_type(GlobalInputType::DataTypeTag::data_type)) {
            using GlobalTypeDescriptorTag =  typename OutputType<GlobalInputType>::type;
            using GlobalRawType = typename GlobalTypeDescriptorTag::DataTypeTag::raw_type;
            aggregated_.resize(sizeof(GlobalRawType)* unique_values);
            auto out_ptr = reinterpret_cast<GlobalRawType*>(aggregated_.data());
            if (input_column.has_value()) {
                details::visit_type(input_column->column_->type().data_type(), [&input_column, &groups, &out_ptr] (auto col_tag) {
                    using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
                    if constexpr(!is_sequence_type(col_type_info::data_type)) {
                        Column::for_each_enumerated<typename col_type_info::TDT>(*input_column->column_, [&out_ptr, &groups](auto enumerating_it) {
                            if constexpr (std::is_same_v<GlobalRawType, bool>) {
                                out_ptr[groups[enumerating_it.idx()]] |= GlobalRawType(enumerating_it.value());
                            } else {
                                out_ptr[groups[enumerating_it.idx()]] += GlobalRawType(enumerating_it.value());
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
                using GlobalInputType = decltype(global_tag);
                if constexpr(!is_sequence_type(GlobalInputType::DataTypeTag::data_type)) {
                    using GlobalTypeDescriptorTag =  typename OutputType<GlobalInputType>::type;
                    using GlobalRawType = typename GlobalTypeDescriptorTag::DataTypeTag::raw_type;
                    using MaybeValueType = MaybeValue<GlobalRawType, T>;
                    auto prev_size = aggregated_.size() / sizeof(MaybeValueType);
                    aggregated_.resize(sizeof(MaybeValueType) * unique_values);
                    auto out_ptr = reinterpret_cast<MaybeValueType*>(aggregated_.data());
                    std::fill(out_ptr + prev_size, out_ptr + unique_values, MaybeValueType{});
                    details::visit_type(input_column->column_->type().data_type(), [&input_column, &groups, &out_ptr] (auto col_tag) {
                        using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
                        if constexpr(!is_sequence_type(col_type_info::data_type)) {
                            Column::for_each_enumerated<typename col_type_info::TDT>(*input_column->column_, [&groups, &out_ptr](auto enumerating_it) {
                                auto& val = out_ptr[groups[enumerating_it.idx()]];
                                if constexpr(std::is_floating_point_v<typename col_type_info::RawType>) {
                                    const auto& curr = GlobalRawType(enumerating_it.value());
                                    if (!val.written_ || std::isnan(static_cast<typename col_type_info::RawType>(val.value_))) {
                                        val.value_ = curr;
                                        val.written_ = true;
                                    } else if (!std::isnan(static_cast<typename col_type_info::RawType>(curr))) {
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
            if(dynamic_schema) {
                details::visit_type(*data_type_, [&aggregated_, &res, &output_column_name, unique_values] (auto col_tag) {
                    using RawType = typename decltype(col_tag)::DataTypeTag::raw_type;
                    using MaybeValueType = MaybeValue<RawType, T>;
                    auto prev_size = aggregated_.size() / sizeof(MaybeValueType);
                    auto new_size = sizeof(MaybeValueType) * unique_values;
                    aggregated_.resize(new_size);
                    auto in_ptr =  reinterpret_cast<MaybeValueType*>(aggregated_.data());
                    std::fill(in_ptr + prev_size, in_ptr + unique_values, MaybeValueType{});
                    auto col = std::make_shared<Column>(make_scalar_type(DataType::FLOAT64), unique_values, true, false);
                    auto out_ptr = reinterpret_cast<double*>(col->ptr());
                    for(auto i = 0u; i < unique_values; ++i, ++in_ptr, ++out_ptr) {
                        *out_ptr = in_ptr->written_ ? static_cast<double>(in_ptr->value_) : std::numeric_limits<double>::quiet_NaN();                }

                    col->set_row_data(unique_values - 1);
                    res.add_column(scalar_field(DataType::FLOAT64, output_column_name.value), col);
                });
            } else {
                details::visit_type(*data_type_, [&aggregated_, &data_type_, &res, output_column_name, unique_values] (auto col_tag) {
                    using RawType = typename decltype(col_tag)::DataTypeTag::raw_type;
                    auto col = std::make_shared<Column>(make_scalar_type(data_type_.value()), unique_values, true, false);
                    const auto* in_ptr =  reinterpret_cast<const MaybeValue<RawType, T>*>(aggregated_.data());
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
                    fraction.numerator_ += double(enumerating_it.value());
                    ++fraction.denominator_;
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
        auto pos = res.add_column(scalar_field(DataType::FLOAT64, output_column_name.value), fractions_.size(), true);
        auto& column = res.column(pos);
        auto ptr = reinterpret_cast<double*>(column.ptr());
        column.set_row_data(fractions_.size() - 1);

        for (auto idx = 0u; idx < fractions_.size(); ++idx) {
            ptr[idx] = fractions_[idx].to_double();
        }
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
                    if (!std::isnan(static_cast<double>(enumerating_it.value()))) {
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

void FirstAggregatorData::set_string_offset_map(const std::unordered_map<entity::position_t, entity::position_t>& offset_map) {
    str_offset_mapping_ = offset_map;
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
        details::visit_type(*data_type_, [this, &res, &output_column_name, unique_values] (auto col_tag) {
            using InputType = decltype(col_tag);
            using RawType = typename decltype(col_tag)::DataTypeTag::raw_type;
            aggregated_.resize(sizeof(RawType)* unique_values);
            auto col = std::make_shared<Column>(make_scalar_type(data_type_.value()), unique_values, true, false);
            memcpy(col->ptr(), aggregated_.data(), aggregated_.size());
            // In case of strings, we need to set the output column to the correct offset pointing to StringPool which contains the actual string
            // This is done using `str_offset_mapping_` set in `AggregationClause::process`
            if constexpr(is_sequence_type(InputType::DataTypeTag::data_type)) {
                auto col_ptr = reinterpret_cast<RawType*>(col->ptr());
                for (auto i = 0u; i < unique_values; ++i, ++col_ptr) {
                    if(str_offset_mapping_.find(*col_ptr) != str_offset_mapping_.end()) {
                        *col_ptr = str_offset_mapping_[*col_ptr];
                    }
                }
            }
            res.add_column(scalar_field(data_type_.value(), output_column_name.value), col);
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

void LastAggregatorData::set_string_offset_map(const std::unordered_map<entity::position_t, entity::position_t>& offset_map) {
    str_offset_mapping_ = offset_map;
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
        details::visit_type(*data_type_, [this, &res, &output_column_name, unique_values] (auto col_tag) {
            using InputType = decltype(col_tag);
            using RawType = typename decltype(col_tag)::DataTypeTag::raw_type;
            aggregated_.resize(sizeof(RawType)* unique_values);
            auto col = std::make_shared<Column>(make_scalar_type(data_type_.value()), unique_values, true, false);
            memcpy(col->ptr(), aggregated_.data(), aggregated_.size());
            // In case of strings, we need to set the output column to the correct offset pointing to StringPool which contains the actual string
            // This is done using `str_offset_mapping_` set in `AggregationClause::process`
            if constexpr(is_sequence_type(InputType::DataTypeTag::data_type)) {
                auto col_ptr = reinterpret_cast<RawType*>(col->ptr());
                for (auto i = 0u; i < unique_values; ++i, ++col_ptr) {
                    if(str_offset_mapping_.find(*col_ptr) != str_offset_mapping_.end()) {
                        *col_ptr = str_offset_mapping_[*col_ptr];
                    }
                }
            }
            res.add_column(scalar_field(data_type_.value(), output_column_name.value), col);
            col->set_row_data(unique_values - 1);
        });
    }
    return res;
}

} //namespace arcticdb
