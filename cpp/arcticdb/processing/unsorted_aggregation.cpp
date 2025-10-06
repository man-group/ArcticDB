/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/column_algorithms.hpp>
#include <arcticdb/processing/unsorted_aggregation.hpp>
#include <arcticdb/processing/aggregation_utils.hpp>
#include <arcticdb/entity/types.hpp>

#include <cmath>

namespace arcticdb {

namespace ranges = std::ranges;

void MinMaxAggregatorData::aggregate(const ColumnWithStrings& input_column) {
    details::visit_type(input_column.column_->type().data_type(), [&](auto col_tag) {
        using type_info = ScalarTypeInfo<decltype(col_tag)>;
        using RawType = typename type_info::RawType;
        if constexpr (!is_sequence_type(type_info::data_type)) {
            arcticdb::for_each<typename type_info::TDT>(*input_column.column_, [this](auto value) {
                const auto& curr = static_cast<RawType>(value);
                if (ARCTICDB_UNLIKELY(!min_.has_value())) {
                    min_ = Value{curr, type_info::data_type};
                    max_ = Value{curr, type_info::data_type};
                } else {
                    min_->set(std::min(min_->get<RawType>(), curr));
                    max_->set(std::max(max_->get<RawType>(), curr));
                }
            });
        } else {
            schema::raise<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                    "Minmax column stat generation not supported with string types"
            );
        }
    });
}

SegmentInMemory MinMaxAggregatorData::finalize(const std::vector<ColumnName>& output_column_names) const {
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            output_column_names.size() == 2,
            "Expected 2 output column names in MinMaxAggregatorData::finalize, but got {}",
            output_column_names.size()
    );
    SegmentInMemory seg;
    if (min_.has_value()) {
        details::visit_type(min_->data_type(), [&output_column_names, &seg, this](auto col_tag) {
            using RawType = typename ScalarTypeInfo<decltype(col_tag)>::RawType;
            auto min_col = std::make_shared<Column>(make_scalar_type(min_->data_type()), Sparsity::PERMITTED);
            min_col->push_back<RawType>(min_->get<RawType>());

            auto max_col = std::make_shared<Column>(make_scalar_type(max_->data_type()), Sparsity::PERMITTED);
            max_col->push_back<RawType>(max_->get<RawType>());

            seg.add_column(scalar_field(min_col->type().data_type(), output_column_names[0].value), min_col);
            seg.add_column(scalar_field(max_col->type().data_type(), output_column_names[1].value), max_col);
        });
    }
    return seg;
}

namespace {

template<typename T, typename T2 = void>
struct OutputType;

template<typename InputType>
requires(is_floating_point_type(InputType::DataTypeTag::data_type))
struct OutputType<InputType> {
    using type = ScalarTagType<DataTypeTag<DataType::FLOAT64>>;
};

template<typename InputType>
requires(is_unsigned_type(InputType::DataTypeTag::data_type))
struct OutputType<InputType> {
    using type = ScalarTagType<DataTypeTag<DataType::UINT64>>;
};

template<typename InputType>
requires(is_signed_type(InputType::DataTypeTag::data_type) && is_integer_type(InputType::DataTypeTag::data_type))
struct OutputType<InputType> {
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
struct OutputType<DataTypeTag<DataType::UTF_DYNAMIC32>, void> {
    using type = ScalarTagType<DataTypeTag<DataType::UTF_DYNAMIC32>>;
};

template<>
struct OutputType<DataTypeTag<DataType::UTF_FIXED64>, void> {
    using type = ScalarTagType<DataTypeTag<DataType::UTF_FIXED64>>;
};

template<>
struct OutputType<DataTypeTag<DataType::BOOL_OBJECT8>, void> {
    using type = ScalarTagType<DataTypeTag<DataType::BOOL_OBJECT8>>;
};
} // namespace

/**********************
 * AggregatorDataBase *
 **********************/

AggregatorDataBase::AggregatorDataBase(const AggregatorDataBase&) {
    log::version().warn("Copying potentially large buffer in AggregatorData");
}

AggregatorDataBase& AggregatorDataBase::operator=(const AggregatorDataBase&) {
    log::version().warn("Copying potentially large buffer in AggregatorData");
    return *this;
}

/*********************
 * SumAggregatorData *
 *********************/

void SumAggregatorData::add_data_type(DataType data_type) { add_data_type_impl(data_type, common_input_type_); }

DataType SumAggregatorData::get_output_data_type() {
    if (output_type_.has_value()) {
        return *output_type_;
    }
    // On the first call to this method, common_input_type_ will be a type capable of representing all the values in all
    // the input columns This may be too small to hold the result, as summing 2 values of the same type cannot
    // necessarily be represented by that type For safety, use the widest type available for the 3 numeric flavours
    // (unsigned int, signed int, float) to have the best chance of avoiding overflow
    if (!common_input_type_.has_value() || *common_input_type_ == DataType::EMPTYVAL) {
        // If data_type_ has no value or is empty type, it means there is no data for this aggregation
        // For sums, we want this to display as zero rather than NaN
        output_type_ = DataType::FLOAT64;
    } else if (is_unsigned_type(*common_input_type_) || is_bool_type(*common_input_type_)) {
        output_type_ = DataType::UINT64;
    } else if (is_signed_type(*common_input_type_)) {
        output_type_ = DataType::INT64;
    } else if (is_floating_point_type(*common_input_type_)) {
        output_type_ = DataType::FLOAT64;
    } else {
        // Unsupported data type
        schema::raise<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                "Sum aggregation not supported with type {}", *common_input_type_
        );
    }
    return *output_type_;
}

std::optional<Value> SumAggregatorData::get_default_value() {
    return details::visit_type(get_output_data_type(), []<typename TD>(TD) -> std::optional<Value> {
        return Value{typename TD::raw_type{0}, TD::data_type};
    });
}

void SumAggregatorData::aggregate(
        const ColumnWithStrings& input_column, const std::vector<size_t>& groups, size_t unique_values
) {
    details::visit_type(get_output_data_type(), [&input_column, unique_values, &groups, this](auto global_tag) {
        using global_type_info = ScalarTypeInfo<decltype(global_tag)>;
        using RawType = typename global_type_info::RawType;
        // Output type for sum aggregation cannot be bool. If the input is bool the output is uint64 and the result
        // is the count of true values. The constexpr is here to prevent compiler warnings.
        if constexpr (!is_sequence_type(global_type_info::data_type) && !is_bool_type(global_type_info::data_type)) {
            aggregated_.resize(sizeof(RawType) * unique_values);
            auto out = std::span{reinterpret_cast<RawType*>(aggregated_.data()), unique_values};
            details::visit_type(input_column.column_->type().data_type(), [&input_column, &groups, &out](auto col_tag) {
                using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
                if constexpr (!is_sequence_type(col_type_info::data_type)) {
                    arcticdb::for_each_enumerated<typename col_type_info::TDT>(
                            *input_column.column_,
                            [&out, &groups](auto enumerating_it) {
                                if constexpr (is_floating_point_type(col_type_info::data_type)) {
                                    if (ARCTICDB_LIKELY(!std::isnan(enumerating_it.value()))) {
                                        out[groups[enumerating_it.idx()]] += RawType(enumerating_it.value());
                                    }
                                } else {
                                    out[groups[enumerating_it.idx()]] += RawType(enumerating_it.value());
                                }
                            }
                    );
                } else {
                    util::raise_rte("String aggregations not currently supported");
                }
            });
        }
    });
}

SegmentInMemory SumAggregatorData::finalize(const ColumnName& output_column_name, bool, size_t unique_values) {
    SegmentInMemory res;
    if (!aggregated_.empty()) {
        details::visit_type(get_output_data_type(), [this, &res, &output_column_name, unique_values](auto col_tag) {
            using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
            aggregated_.resize(sizeof(typename col_type_info::RawType) * unique_values);
            auto col = std::make_shared<Column>(
                    make_scalar_type(output_type_.value()),
                    unique_values,
                    AllocationType::PRESIZED,
                    Sparsity::NOT_PERMITTED
            );
            memcpy(col->ptr(), aggregated_.data(), aggregated_.size());
            col->set_row_data(unique_values - 1);
            res.add_column(scalar_field(output_type_.value(), output_column_name.value), std::move(col));
        });
    }
    return res;
}

/********************
 * MinMaxAggregator *
 ********************/

namespace {
enum class Extremum { MAX, MIN };

std::shared_ptr<Column> create_output_column(TypeDescriptor td, util::BitMagic&& sparse_map, size_t unique_values) {
    const size_t num_set_rows = sparse_map.count();
    const Sparsity sparsity = num_set_rows == sparse_map.size() ? Sparsity::NOT_PERMITTED : Sparsity::PERMITTED;
    auto col = std::make_shared<Column>(td, num_set_rows, AllocationType::PRESIZED, sparsity);
    if (sparsity == Sparsity::PERMITTED) {
        col->set_sparse_map(std::move(sparse_map));
    }
    col->set_row_data(unique_values - 1);
    return col;
}

template<Extremum E, typename ColType>
requires(std::floating_point<ColType> || std::integral<ColType>) && (E == Extremum::MAX || E == Extremum::MIN)
consteval ColType default_value_for_extremum() {
    if constexpr (E == Extremum::MAX) {
        return std::numeric_limits<ColType>::lowest();
    } else {
        return std::numeric_limits<ColType>::max();
    }
}

template<Extremum E, typename T>
requires(std::floating_point<T> || std::integral<T>) && (E == Extremum::MAX || E == Extremum::MIN)
T apply_extremum(const T& left, const T& right) {
    if constexpr (E == Extremum::MAX) {
        return std::max(left, right);
    } else {
        return std::min(left, right);
    }
}

template<Extremum T>
void aggregate_impl(
        const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& row_to_group,
        size_t unique_values, std::vector<uint8_t>& aggregated, std::optional<DataType>& data_type,
        util::BitMagic& sparse_map
) {
    if (data_type.has_value() && *data_type != DataType::EMPTYVAL && input_column.has_value()) {
        details::visit_type(*data_type, [&](auto global_tag) {
            using global_type_info = ScalarTypeInfo<decltype(global_tag)>;
            using GlobalRawType = typename global_type_info::RawType;
            if constexpr (!is_sequence_type(global_type_info::data_type)) {
                auto prev_size = aggregated.size() / sizeof(GlobalRawType);
                aggregated.resize(sizeof(GlobalRawType) * unique_values);
                sparse_map.resize(unique_values);
                std::span<GlobalRawType> out{reinterpret_cast<GlobalRawType*>(aggregated.data()), unique_values};
                constexpr GlobalRawType default_value = default_value_for_extremum<T, GlobalRawType>();
                std::ranges::fill(out.subspan(prev_size), default_value);
                details::visit_type(input_column->column_->type().data_type(), [&](auto col_tag) {
                    using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
                    using ColRawType = typename col_type_info::RawType;
                    if constexpr (!is_sequence_type(col_type_info::data_type)) {
                        arcticdb::for_each_enumerated<typename col_type_info::TDT>(
                                *input_column->column_,
                                [&](auto row) {
                                    auto& group_entry = out[row_to_group[row.idx()]];
                                    const auto& current_value = GlobalRawType(row.value());
                                    if constexpr (std::is_floating_point_v<ColRawType>) {
                                        if (!sparse_map[row_to_group[row.idx()]] ||
                                            std::isnan(static_cast<ColRawType>(group_entry))) {
                                            group_entry = current_value;
                                            sparse_map.set(row_to_group[row.idx()]);
                                        } else if (!std::isnan(static_cast<ColRawType>(current_value))) {
                                            group_entry = apply_extremum<T>(group_entry, current_value);
                                        }
                                    } else {
                                        group_entry = apply_extremum<T>(group_entry, current_value);
                                        sparse_map.set(row_to_group[row.idx()]);
                                    }
                                }
                        );
                    } else {
                        util::raise_rte("String aggregations not currently supported");
                    }
                });
            }
        });
    }
}

template<Extremum T>
SegmentInMemory finalize_impl(
        const ColumnName& output_column_name, size_t unique_values, std::vector<uint8_t>& aggregated,
        std::optional<DataType>& data_type, util::BitMagic&& sparse_map
) {
    SegmentInMemory res;
    if (!aggregated.empty()) {
        const TypeDescriptor column_type = make_scalar_type(data_type.value());
        sparse_map.resize(unique_values);
        std::shared_ptr<Column> col = create_output_column(column_type, std::move(sparse_map), unique_values);
        details::visit_type(*data_type, [&](auto col_tag) {
            using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
            using RawType = typename col_type_info::RawType;
            const std::span<const RawType> group_values{
                    reinterpret_cast<const RawType*>(aggregated.data()), aggregated.size() / sizeof(RawType)
            };
            arcticdb::for_each_enumerated<typename col_type_info::TDT>(*col, [&](auto row) {
                row.value() = group_values[row.idx()];
            });
        });
        res.add_column(scalar_field(col->type().data_type(), output_column_name.value), std::move(col));
    }
    return res;
}
} // namespace

/*********************
 * MaxAggregatorData *
 *********************/

void MaxAggregatorData::add_data_type(DataType data_type) { add_data_type_impl(data_type, data_type_); }

DataType MaxAggregatorData::get_output_data_type() {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            is_numeric_type(*data_type_) || is_bool_type(*data_type_) || is_empty_type(*data_type_),
            "Max aggregation not supported with type {}",
            *data_type_
    );
    return *data_type_;
}

void MaxAggregatorData::aggregate(
        const ColumnWithStrings& input_column, const std::vector<size_t>& groups, size_t unique_values
) {
    aggregate_impl<Extremum::MAX>(input_column, groups, unique_values, aggregated_, data_type_, sparse_map_);
}

SegmentInMemory MaxAggregatorData::finalize(const ColumnName& output_column_name, bool, size_t unique_values) {
    return finalize_impl<Extremum::MAX>(
            output_column_name, unique_values, aggregated_, data_type_, std::move(sparse_map_)
    );
}

std::optional<Value> MaxAggregatorData::get_default_value() { return {}; }

/*********************
 * MinAggregatorData *
 *********************/

void MinAggregatorData::add_data_type(DataType data_type) { add_data_type_impl(data_type, data_type_); }

DataType MinAggregatorData::get_output_data_type() {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            is_numeric_type(*data_type_) || is_bool_type(*data_type_) || is_empty_type(*data_type_),
            "Min aggregation not supported with type {}",
            *data_type_
    );
    return *data_type_;
}

void MinAggregatorData::aggregate(
        const ColumnWithStrings& input_column, const std::vector<size_t>& groups, size_t unique_values
) {
    aggregate_impl<Extremum::MIN>(input_column, groups, unique_values, aggregated_, data_type_, sparse_map_);
}

SegmentInMemory MinAggregatorData::finalize(const ColumnName& output_column_name, bool, size_t unique_values) {
    return finalize_impl<Extremum::MIN>(
            output_column_name, unique_values, aggregated_, data_type_, std::move(sparse_map_)
    );
}

std::optional<Value> MinAggregatorData::get_default_value() { return {}; }

/**********************
 * MeanAggregatorData *
 **********************/

void MeanAggregatorData::add_data_type(DataType data_type) {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            is_numeric_type(data_type) || is_bool_type(data_type) || is_empty_type(data_type),
            "Mean aggregation not supported with type {}",
            data_type
    );
    add_data_type_impl(data_type, data_type_);
}

DataType MeanAggregatorData::get_output_data_type() {
    if (data_type_ && is_time_type(*data_type_)) {
        return *data_type_;
    }
    return DataType::FLOAT64;
}

void MeanAggregatorData::aggregate(
        const ColumnWithStrings& input_column, const std::vector<size_t>& groups, size_t unique_values
) {
    fractions_.resize(unique_values);
    sparse_map_.resize(unique_values);
    util::BitSet::bulk_insert_iterator inserter(sparse_map_);
    details::visit_type(
            input_column.column_->type().data_type(),
            [&input_column, &groups, &inserter, this](auto col_tag) {
                using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
                if constexpr (is_sequence_type(col_type_info::data_type)) {
                    util::raise_rte("String aggregations not currently supported");
                } else if constexpr (is_empty_type(col_type_info::data_type)) {
                    return;
                }
                arcticdb::for_each_enumerated<typename col_type_info::TDT>(
                        *input_column.column_,
                        [&groups, &inserter, this](auto enumerating_it) {
                            auto& fraction = fractions_[groups[enumerating_it.idx()]];
                            if constexpr ((is_floating_point_type(col_type_info ::data_type))) {
                                if (ARCTICDB_LIKELY(!std::isnan(enumerating_it.value()))) {
                                    fraction.numerator_ += static_cast<double>(enumerating_it.value());
                                    ++fraction.denominator_;
                                    inserter = groups[enumerating_it.idx()];
                                }
                            } else {
                                fraction.numerator_ += static_cast<double>(enumerating_it.value());
                                ++fraction.denominator_;
                                inserter = groups[enumerating_it.idx()];
                            }
                        }
                );
            }
    );
    inserter.flush();
}

SegmentInMemory MeanAggregatorData::finalize(const ColumnName& output_column_name, bool, size_t unique_values) {
    SegmentInMemory res;
    if (!fractions_.empty()) {
        fractions_.resize(unique_values);
        sparse_map_.resize(unique_values);
        auto col =
                create_output_column(make_scalar_type(get_output_data_type()), std::move(sparse_map_), unique_values);
        // TODO: Empty type needs more thought. Currently we emit a fully sparse column which will be populated by
        // `copy_frame_data_to_buffer` but this might not be the right approach. As of this PR (11.09.2025) the empty
        // type is feature flagged and not used so we don't worry too much about optimizing it.
        if (data_type_ && *data_type_ == DataType::EMPTYVAL) [[unlikely]] {
            auto empty_bitset = util::BitSet(unique_values);
            col->set_sparse_map(std::move(empty_bitset));
        } else {
            details::visit_type(col->type().data_type(), [&, this]<typename TypeTag>(TypeTag) {
                using OutputDataTypeTag =
                        std::conditional_t<is_time_type(TypeTag::data_type), TypeTag, DataTypeTag<DataType::FLOAT64>>;
                using OutputTypeDescriptor = typename ScalarTypeInfo<OutputDataTypeTag>::TDT;
                arcticdb::for_each_enumerated<OutputTypeDescriptor>(*col, [&](auto row) {
                    row.value() = static_cast<typename OutputDataTypeTag::raw_type>(fractions_[row.idx()].to_double());
                });
            });
        }
        res.add_column(scalar_field(get_output_data_type(), output_column_name.value), std::move(col));
    }
    return res;
}

double MeanAggregatorData::Fraction::to_double() const {
    return denominator_ == 0 ? std::numeric_limits<double>::quiet_NaN()
                             : numerator_ / static_cast<double>(denominator_);
}

std::optional<Value> MeanAggregatorData::get_default_value() { return {}; }
/***********************
 * CountAggregatorData *
 ***********************/

void CountAggregatorData::aggregate(
        const ColumnWithStrings& input_column, const std::vector<size_t>& groups, size_t unique_values
) {
    aggregated_.resize(unique_values);
    sparse_map_.resize(unique_values);
    util::BitSet::bulk_insert_iterator inserter(sparse_map_);
    details::visit_type(
            input_column.column_->type().data_type(),
            [&input_column, &groups, &inserter, this](auto col_tag) {
                using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
                arcticdb::for_each_enumerated<typename col_type_info::TDT>(
                        *input_column.column_,
                        [&groups, &inserter, this](auto enumerating_it) {
                            if constexpr (is_floating_point_type(col_type_info::data_type)) {
                                if (ARCTICDB_LIKELY(!std::isnan(enumerating_it.value()))) {
                                    auto& val = aggregated_[groups[enumerating_it.idx()]];
                                    ++val;
                                    inserter = groups[enumerating_it.idx()];
                                }
                            } else {
                                auto& val = aggregated_[groups[enumerating_it.idx()]];
                                ++val;
                                inserter = groups[enumerating_it.idx()];
                            }
                        }
                );
            }
    );
    inserter.flush();
}

SegmentInMemory CountAggregatorData::finalize(const ColumnName& output_column_name, bool, size_t unique_values) {
    SegmentInMemory res;
    if (!aggregated_.empty()) {
        aggregated_.resize(unique_values);
        sparse_map_.resize(unique_values);
        auto col =
                create_output_column(make_scalar_type(get_output_data_type()), std::move(sparse_map_), unique_values);
        if (!col->opt_sparse_map().has_value()) {
            // If all values are set we use memcpy for efficiency
            auto ptr = reinterpret_cast<uint64_t*>(col->ptr());
            memcpy(ptr, aggregated_.data(), sizeof(uint64_t) * unique_values);
        } else {
            using OutputTypeDescriptor = typename ScalarTypeInfo<DataTypeTag<DataType::UINT64>>::TDT;
            arcticdb::for_each_enumerated<OutputTypeDescriptor>(*col, [&](auto row) {
                row.value() = aggregated_[row.idx()];
            });
        }
        res.add_column(scalar_field(get_output_data_type(), output_column_name.value), std::move(col));
    }
    return res;
}

std::optional<Value> CountAggregatorData::get_default_value() { return {}; }

/***********************
 * FirstAggregatorData *
 ***********************/

void FirstAggregatorData::add_data_type(DataType data_type) { add_data_type_impl(data_type, data_type_); }

void FirstAggregatorData::aggregate(
        const ColumnWithStrings& input_column, const std::vector<size_t>& groups, size_t unique_values
) {
    if (data_type_.has_value() && *data_type_ != DataType::EMPTYVAL) {
        details::visit_type(*data_type_, [&input_column, unique_values, &groups, this](auto global_tag) {
            using GlobalInputType = decltype(global_tag);
            using GlobalTypeDescriptorTag = typename OutputType<GlobalInputType>::type;
            using GlobalRawType = typename GlobalTypeDescriptorTag::DataTypeTag::raw_type;
            aggregated_.resize(sizeof(GlobalRawType) * unique_values);
            sparse_map_.resize(unique_values);
            util::BitSet::bulk_insert_iterator inserter(sparse_map_);
            auto col_data = input_column.column_->data();
            auto out_ptr = reinterpret_cast<GlobalRawType*>(aggregated_.data());
            details::visit_type(
                    input_column.column_->type().data_type(),
                    [this, &groups, &out_ptr, &col_data, &inserter](auto col_tag) {
                        using ColumnTagType = std::decay_t<decltype(col_tag)>;
                        using ColumnType = typename ColumnTagType::raw_type;
                        auto groups_pos = 0;
                        while (auto block = col_data.next<
                                            TypeDescriptorTag<ColumnTagType, DimensionTag<entity::Dimension::Dim0>>>()
                        ) {
                            auto ptr = reinterpret_cast<const ColumnType*>(block.value().data());
                            for (auto i = 0u; i < block.value().row_count(); ++i, ++ptr, ++groups_pos) {
                                auto& val = out_ptr[groups[groups_pos]];
                                bool is_first_group_el = (!groups_cache_.contains(groups[groups_pos]));
                                if constexpr (std::is_floating_point_v<ColumnType>) {
                                    if (is_first_group_el || std::isnan(static_cast<ColumnType>(val))) {
                                        groups_cache_.insert(groups[groups_pos]);
                                        val = GlobalRawType(*ptr);
                                        inserter = groups[groups_pos];
                                    }
                                } else {
                                    if (is_first_group_el) {
                                        groups_cache_.insert(groups[groups_pos]);
                                        val = GlobalRawType(*ptr);
                                        inserter = groups[groups_pos];
                                    }
                                }
                            }
                        }
                    }
            );
            inserter.flush();
        });
    }
}

SegmentInMemory FirstAggregatorData::finalize(const ColumnName& output_column_name, bool, size_t unique_values) {
    SegmentInMemory res;
    if (!aggregated_.empty()) {
        details::visit_type(*data_type_, [this, &res, &output_column_name, unique_values](auto col_tag) {
            using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
            using RawType = typename col_type_info::RawType;
            aggregated_.resize(sizeof(RawType) * unique_values);
            sparse_map_.resize(unique_values);
            auto col =
                    create_output_column(make_scalar_type(data_type_.value()), std::move(sparse_map_), unique_values);
            if (!col->opt_sparse_map().has_value()) {
                memcpy(col->ptr(), aggregated_.data(), aggregated_.size());
            } else {
                const std::span<const RawType> group_values{
                        reinterpret_cast<const RawType*>(aggregated_.data()), aggregated_.size() / sizeof(RawType)
                };
                arcticdb::for_each_enumerated<typename col_type_info::TDT>(*col, [&](auto row) {
                    row.value() = group_values[row.idx()];
                });
            }
            res.add_column(scalar_field(data_type_.value(), output_column_name.value), col);
        });
    }
    return res;
}

std::optional<Value> FirstAggregatorData::get_default_value() { return {}; }

/***********************
 * LastAggregatorData *
 ***********************/

void LastAggregatorData::add_data_type(DataType data_type) { add_data_type_impl(data_type, data_type_); }

void LastAggregatorData::aggregate(
        const ColumnWithStrings& input_column, const std::vector<size_t>& groups, size_t unique_values
) {
    if (data_type_.has_value() && *data_type_ != DataType::EMPTYVAL) {
        details::visit_type(*data_type_, [&input_column, unique_values, &groups, this](auto global_tag) {
            using GlobalInputType = decltype(global_tag);
            using GlobalTypeDescriptorTag = typename OutputType<GlobalInputType>::type;
            using GlobalRawType = typename GlobalTypeDescriptorTag::DataTypeTag::raw_type;
            aggregated_.resize(sizeof(GlobalRawType) * unique_values);
            sparse_map_.resize(unique_values);
            util::BitSet::bulk_insert_iterator inserter(sparse_map_);
            auto col_data = input_column.column_->data();
            auto out_ptr = reinterpret_cast<GlobalRawType*>(aggregated_.data());
            details::visit_type(
                    input_column.column_->type().data_type(),
                    [&groups, &out_ptr, &col_data, &inserter, this](auto col_tag) {
                        using ColumnTagType = std::decay_t<decltype(col_tag)>;
                        using ColumnType = typename ColumnTagType::raw_type;
                        auto groups_pos = 0;
                        while (auto block = col_data.next<
                                            TypeDescriptorTag<ColumnTagType, DimensionTag<entity::Dimension::Dim0>>>()
                        ) {
                            auto ptr = reinterpret_cast<const ColumnType*>(block.value().data());
                            for (auto i = 0u; i < block.value().row_count(); ++i, ++ptr, ++groups_pos) {
                                auto& val = out_ptr[groups[groups_pos]];
                                if constexpr (std::is_floating_point_v<ColumnType>) {
                                    bool is_first_group_el =
                                            (groups_cache_.find(groups[groups_pos]) == groups_cache_.end());
                                    const auto& curr = GlobalRawType(*ptr);
                                    if (is_first_group_el || !std::isnan(static_cast<ColumnType>(curr))) {
                                        groups_cache_.insert(groups[groups_pos]);
                                        val = curr;
                                        inserter = groups[groups_pos];
                                    }
                                } else {
                                    val = GlobalRawType(*ptr);
                                    inserter = groups[groups_pos];
                                }
                            }
                        }
                    }
            );
            inserter.flush();
        });
    }
}

SegmentInMemory LastAggregatorData::finalize(const ColumnName& output_column_name, bool, size_t unique_values) {
    SegmentInMemory res;
    if (!aggregated_.empty()) {
        details::visit_type(*data_type_, [&res, &output_column_name, unique_values, this](auto col_tag) {
            using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
            using RawType = typename col_type_info::RawType;
            aggregated_.resize(sizeof(RawType) * unique_values);
            sparse_map_.resize(unique_values);
            auto col =
                    create_output_column(make_scalar_type(data_type_.value()), std::move(sparse_map_), unique_values);
            if (!col->opt_sparse_map().has_value()) {
                memcpy(col->ptr(), aggregated_.data(), aggregated_.size());
            } else {
                const std::span<const RawType> group_values{
                        reinterpret_cast<const RawType*>(aggregated_.data()), aggregated_.size() / sizeof(RawType)
                };
                arcticdb::for_each_enumerated<typename col_type_info::TDT>(*col, [&](auto row) {
                    row.value() = group_values[row.idx()];
                });
            }
            res.add_column(scalar_field(data_type_.value(), output_column_name.value), col);
        });
    }
    return res;
}

std::optional<Value> LastAggregatorData::get_default_value() { return {}; }

} // namespace arcticdb
