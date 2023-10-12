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
                        auto groups_pos = 0;
                        while (auto block = col_data.next<TypeDescriptorTag<ColumnTagType, DimensionTag<entity::Dimension::Dim0>>>()) {
                            auto ptr = reinterpret_cast<const ColumnType *>(block.value().data());
                            for (auto i = 0u; i < block.value().row_count(); ++i, ++ptr) {
                                out_ptr[groups[groups_pos++]] += GlobalRawType(*ptr);
                            }
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
} //namespace arcticdb
