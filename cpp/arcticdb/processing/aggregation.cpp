/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/aggregation.hpp>

#include <cmath>

namespace arcticdb {
void Sum::aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values) {
    entity::details::visit_type(data_type_, [&input_column, unique_values, &groups, that=this] (auto global_type_desc_tag) {
        using GlobalInputType = decltype(global_type_desc_tag);
        if constexpr(!is_sequence_type(GlobalInputType::DataTypeTag::data_type)) {
            using GlobalTypeDescriptorTag =  typename OutputType<GlobalInputType>::type;
            using GlobalRawType = typename GlobalTypeDescriptorTag::DataTypeTag::raw_type;
            that->aggregated_.resize(sizeof(GlobalRawType)* unique_values);
            auto out_ptr = reinterpret_cast<GlobalRawType*>(that->aggregated_.data());
            if(input_column.has_value()) {
                entity::details::visit_type(input_column->column_->type().data_type(), [&input_column, unique_values, &groups, &out_ptr, that=that] (auto type_desc_tag) {
                    using InputType = decltype(type_desc_tag);
                    if constexpr(!is_sequence_type(InputType::DataTypeTag::data_type)) {
                        using TypeDescriptorTag =  typename OutputType<InputType>::type;
                        using RawType = typename TypeDescriptorTag::DataTypeTag::raw_type;
                        auto col_data = input_column->column_->data();
                        auto groups_pos = 0;
                        while (auto block = col_data.next<TypeDescriptorTag>()) {
                            auto ptr = reinterpret_cast<const RawType *>(block.value().data());
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

std::optional<DataType> Sum::finalize(SegmentInMemory& seg, bool, size_t unique_values) {
    if(!aggregated_.empty()) {
        entity::details::visit_type(data_type_, [that=this, &seg, unique_values] (auto type_desc_tag) {
            using RawType = typename decltype(type_desc_tag)::DataTypeTag::raw_type;
            that->aggregated_.resize(sizeof(RawType)* unique_values);
            auto col = std::make_shared<Column>(make_scalar_type(that->data_type_), unique_values, true, false);
            memcpy(col->ptr(), that->aggregated_.data(), that->aggregated_.size());
            seg.add_column(scalar_field(that->data_type_, that->get_output_column_name().value), col);
            col->set_row_data(unique_values);
        });
        return data_type_;
    } else {
        return std::nullopt;
    }
}

void MaxOrMin::aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values) {
    if(input_column.has_value()) {
        entity::details::visit_type(data_type_, [&input_column, unique_values, &groups, that=this] (auto global_type_desc_tag) {
            using GlobalInputType = decltype(global_type_desc_tag);
            if constexpr(!is_sequence_type(GlobalInputType::DataTypeTag::data_type)) {
                using GlobalTypeDescriptorTag =  typename OutputType<GlobalInputType>::type;
                using GlobalRawType = typename GlobalTypeDescriptorTag::DataTypeTag::raw_type;
                auto prev_size = that->aggregated_.size() / sizeof(MaybeValue<GlobalRawType>);
                that->aggregated_.resize(sizeof(MaybeValue<GlobalRawType>) * unique_values);
                auto col_data = input_column->column_->data();
                auto out_ptr = reinterpret_cast<MaybeValue<GlobalRawType>*>(that->aggregated_.data());
                std::fill(out_ptr + prev_size, out_ptr + unique_values, MaybeValue<GlobalRawType>{});
                entity::details::visit_type(input_column->column_->type().data_type(), [&input_column, unique_values, &groups, &out_ptr, &col_data, that=that] (auto type_desc_tag) {
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
                                        if (that->extremum_ == Extremum::max) {
                                            val.value_ = std::max(val.value_, curr);
                                        } else {
                                            val.value_ = std::min(val.value_, curr);
                                        }
                                    }
                                } else {
                                    if (that->extremum_ == Extremum::max) {
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

std::optional<DataType> MaxOrMin::finalize(SegmentInMemory& seg, bool dynamic_schema, size_t unique_values) {
    if(!aggregated_.empty()) {
        if(dynamic_schema) {
            entity::details::visit_type(data_type_, [that=this, &seg, unique_values] (auto type_desc_tag) {
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

                col->set_row_data(unique_values);
                seg.add_column(scalar_field(DataType::FLOAT64, that->get_output_column_name().value), col);
            });
            return DataType::FLOAT64;
        } else {
            entity::details::visit_type(data_type_, [that=this, &seg, unique_values] (auto type_desc_tag) {
                using RawType = typename decltype(type_desc_tag)::DataTypeTag::raw_type;
                auto col = std::make_shared<Column>(make_scalar_type(that->data_type_), unique_values, true, false);
                const auto* in_ptr =  reinterpret_cast<const MaybeValue<RawType>*>(that->aggregated_.data());
                auto out_ptr = reinterpret_cast<RawType*>(col->ptr());
                for(auto i = 0u; i < unique_values; ++i, ++in_ptr, ++out_ptr) {
                    *out_ptr = in_ptr->value_;
                }
                col->set_row_data(unique_values);
                seg.add_column(scalar_field(that->data_type_, that->get_output_column_name().value), col);
            });
            return data_type_;
        }
    } else {
        return std::nullopt;
    }
}

void Mean::aggregate(const std::optional<ColumnWithStrings>& input_column, const std::vector<size_t>& groups, size_t unique_values) {
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

std::optional<DataType> Mean::finalize(SegmentInMemory& seg, bool, size_t unique_values) {
    if(!data_.fractions_.empty()) {
        data_.fractions_.resize(unique_values);
        auto pos = seg.add_column(scalar_field(arcticdb::entity::DataType::FLOAT64, get_output_column_name().value), data_.fractions_.size(), true);
        auto& column = seg.column(pos);
        auto ptr = reinterpret_cast<double*>(column.ptr());
        column.set_row_data(data_.fractions_.size());

        for (const auto& fraction : folly::enumerate(data_.fractions_)) {
            ptr[fraction.index] = fraction->to_double();
        }
        return arcticdb::entity::DataType::FLOAT64;
    } else {
        return std::nullopt;
    }
}
} //namespace arcticdb
