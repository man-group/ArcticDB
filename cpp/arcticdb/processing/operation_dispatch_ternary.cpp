/*
 * Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/operation_dispatch_ternary.hpp>
#include <arcticdb/processing/ternary_utils.hpp>

namespace arcticdb {

template<bool arguments_reversed = false>
inline std::string ternary_operation_column_name(std::string_view left, std::string_view right) {
    if constexpr (arguments_reversed) {
        return fmt::format("(COND ? {} : {})", right, left);
    } else {
        return fmt::format("(COND ? {} : {})", left, right);
    }
}

template<bool arguments_reversed = false>
inline std::string ternary_operation_with_types_to_string(
        std::string_view left,
        const TypeDescriptor& type_left,
        std::string_view right,
        const TypeDescriptor& type_right) {
    if constexpr (arguments_reversed) {
        return fmt::format("{} ({}) : {} ({})", right, get_user_friendly_type_string(type_right), left, get_user_friendly_type_string(type_left));
    } else {
        return fmt::format("{} ({}) : {} ({})", left, get_user_friendly_type_string(type_left), right, get_user_friendly_type_string(type_right));
    }
}

VariantData ternary_operator(const util::BitSet& condition, const util::BitSet& left, const util::BitSet& right) {
    util::BitSet output_bitset;
    auto output_size = condition.size();
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(left.size() == output_size && right.size() == output_size, "Mismatching bitset sizes");
    output_bitset = right;
    auto end_bit = condition.end();
    for (auto set_bit = condition.first(); set_bit < end_bit; ++set_bit) {
        output_bitset[*set_bit] = left[*set_bit];
    }
    return VariantData{std::move(output_bitset)};
}

template<bool arguments_reversed = false>
VariantData ternary_operator(const util::BitSet& condition, const util::BitSet& input_bitset, bool value) {
    util::BitSet output_bitset;
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(input_bitset.size() == condition.size(), "Mismatching bitset sizes");
    if constexpr (arguments_reversed) {
        if (value) {
            output_bitset = condition | input_bitset;
        } else {
            output_bitset = ~condition & input_bitset;
        }
    } else {
        if (value) {
            output_bitset = ~condition | input_bitset;
        } else {
            output_bitset = condition & input_bitset;
        }
    }
    output_bitset.resize(condition.size());
    return VariantData{std::move(output_bitset)};
}

VariantData ternary_operator(const util::BitSet& condition, const ColumnWithStrings& left, const ColumnWithStrings& right) {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            !is_empty_type(left.column_->type().data_type()) && !is_empty_type(right.column_->type().data_type()),
            "Empty column provided to ternary operator");
    std::unique_ptr<Column> output_column;
    std::shared_ptr<StringPool> string_pool;

    details::visit_type(left.column_->type().data_type(), [&](auto left_tag) {
        using left_type_info = ScalarTypeInfo<decltype(left_tag)>;
        details::visit_type(right.column_->type().data_type(), [&](auto right_tag) {
            using right_type_info = ScalarTypeInfo<decltype(right_tag)>;
            if constexpr(is_sequence_type(left_type_info::data_type) && is_sequence_type(right_type_info::data_type)) {
                if constexpr(left_type_info::data_type == right_type_info::data_type && is_dynamic_string_type(left_type_info::data_type)) {
                    output_column = std::make_unique<Column>(make_scalar_type(DataType::UTF_DYNAMIC64), Sparsity::PERMITTED);
                    string_pool = std::make_shared<StringPool>();
                    ternary_transform<typename left_type_info::TDT, typename right_type_info::TDT, typename left_type_info::TDT>(
                            condition,
                            *(left.column_),
                            *(right.column_),
                            *output_column,
                            [&string_pool, &left, &right](bool cond, auto left_val, auto right_val) -> typename left_type_info::RawType {
                                auto string_at_offset = cond ? left.string_at_offset((left_val)) : right.string_at_offset(right_val);
                                if (string_at_offset.has_value()) {
                                    auto offset_string = string_pool->get(*string_at_offset);
                                    return offset_string.offset();
                                } else {
                                    return cond ? left_val : right_val;
                                }
                            });
                } else {
                    // Fixed width string columns
                    schema::raise<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                            "Ternary operator does not support fixed width string columns '{}' and '{}'",
                            left.column_name_,
                            right.column_name_);
                }
            } else if constexpr (is_numeric_type(left_type_info::data_type) && is_numeric_type(right_type_info::data_type)) {
                using TargetType = typename ternary_operation_promoted_type<typename left_type_info::RawType, typename right_type_info::RawType>::type;
                constexpr auto output_data_type = data_type_from_raw_type<TargetType>();
                output_column = std::make_unique<Column>(make_scalar_type(output_data_type), Sparsity::PERMITTED);
                ternary_transform<typename left_type_info::TDT, typename right_type_info::TDT, ScalarTagType<DataTypeTag<output_data_type>>>(
                        condition,
                        *(left.column_),
                        *(right.column_),
                        *output_column,
                        [](bool condition, auto left_val, auto right_val) { return condition ? left_val : right_val; });
            } else if constexpr (is_bool_type(left_type_info::data_type) && is_bool_type(right_type_info::data_type)) {
                output_column = std::make_unique<Column>(make_scalar_type(DataType::BOOL8), Sparsity::PERMITTED);
                ternary_transform<typename left_type_info::TDT, typename right_type_info::TDT, typename left_type_info::TDT>(
                        condition,
                        *(left.column_),
                        *(right.column_),
                        *output_column,
                        [](bool condition, auto left_val, auto right_val) { return condition ? left_val : right_val; });
            } else {
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Invalid ternary operator arguments {}",
                                                                      ternary_operation_with_types_to_string(
                                                                              left.column_name_,
                                                                              left.column_->type(),
                                                                              right.column_name_,
                                                                              right.column_->type()));
            }
        });
    });
    return {ColumnWithStrings(std::move(output_column), string_pool, ternary_operation_column_name(left.column_name_, right.column_name_))};
}

template<bool arguments_reversed = false>
VariantData ternary_operator(const util::BitSet& condition, const ColumnWithStrings& col, const Value& val) {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            !is_empty_type(col.column_->type().data_type()),
            "Empty column provided to ternary operator");
    std::unique_ptr<Column> output_column;
    std::shared_ptr<StringPool> string_pool;
    std::string value_string;

    details::visit_type(col.column_->type().data_type(), [&](auto col_tag) {
        using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
        details::visit_type(val.type().data_type(), [&](auto val_tag) {
            using val_type_info = ScalarTypeInfo<decltype(val_tag)>;
            if constexpr(is_sequence_type(col_type_info::data_type) && is_sequence_type(val_type_info::data_type)) {
                if constexpr(is_dynamic_string_type(col_type_info::data_type)) {
                    output_column = std::make_unique<Column>(make_scalar_type(DataType::UTF_DYNAMIC64), Sparsity::PERMITTED);
                    string_pool = std::make_shared<StringPool>();
                    auto value_string = std::string(*val.str_data(), val.len());

                    // TODO: Remove code duplication with ternary
                    using output_tdt = ScalarTagType<DataTypeTag<DataType::UTF_DYNAMIC64>>;
                    initialise_output_column<arguments_reversed>(condition, *col.column_, *output_column);
                    auto output_data = output_column->data();
                    if (output_column->is_sparse()) {
                        auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
                        for (auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>(); output_it != output_end_it; ++output_it) {
                            auto idx = output_it->idx();
                            std::optional<std::string_view> string_at_offset;
                            if constexpr (arguments_reversed) {
                                string_at_offset = condition.get_bit(idx) ? value_string : col.string_at_offset(*col.column_->scalar_at<int64_t>(idx));
                            } else {
                                string_at_offset = condition.get_bit(idx) ? col.string_at_offset(*col.column_->scalar_at<int64_t>(idx)) : value_string;
                            }
                            if (string_at_offset.has_value()) {
                                auto offset_string = string_pool->get(*string_at_offset);
                                output_it->value() = offset_string.offset();
                            } else {
                                // string_at_offset will only be valueless if the condition was true and so was
                                // selected from the column
                                output_it->value() = *col.column_->scalar_at<int64_t>(idx);
                            }
                        }
                    } else {
                        auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED>();
                        for (auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED>(); output_it != output_end_it; ++output_it) {
                            auto idx = output_it->idx();
                            std::optional<std::string_view> string_at_offset;
                            if constexpr (arguments_reversed) {
                                string_at_offset = condition.get_bit(idx) ? value_string : col.string_at_offset(*col.column_->scalar_at<int64_t>(idx));
                            } else {
                                string_at_offset = condition.get_bit(idx) ? col.string_at_offset(*col.column_->scalar_at<int64_t>(idx)) : value_string;
                            }
                            if (string_at_offset.has_value()) {
                                auto offset_string = string_pool->get(*string_at_offset);
                                output_it->value() = offset_string.offset();
                            } else {
                                // string_at_offset will only be valueless if the condition was true and so was
                                // selected from the column
                                output_it->value() = *col.column_->scalar_at<int64_t>(idx);
                            }
                        }
                    }
                } else {
                    // Fixed width string column
                    schema::raise<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                            "Ternary operator does not support fixed width string columns '{}'",
                            col.column_name_);
                }
            } else if constexpr (is_numeric_type(col_type_info::data_type) && is_numeric_type(val_type_info::data_type)) {
                using TargetType = typename ternary_operation_promoted_type<typename col_type_info::RawType, typename val_type_info::RawType>::type;
                constexpr auto output_data_type = data_type_from_raw_type<TargetType>();
                output_column = std::make_unique<Column>(make_scalar_type(output_data_type), Sparsity::PERMITTED);
                auto value = static_cast<TargetType>(val.get<typename val_type_info::RawType>());
                value_string = fmt::format("{}", value);
                ternary<typename col_type_info::TDT, ScalarTagType<DataTypeTag<output_data_type>>, TargetType, arguments_reversed>(
                        condition,
                        *(col.column_),
                        value,
                        *output_column);
            } else if constexpr (is_bool_type(col_type_info::data_type) && is_bool_type(val_type_info::data_type)) {
                using TargetType = bool;
                constexpr auto output_data_type = data_type_from_raw_type<TargetType>();
                output_column = std::make_unique<Column>(make_scalar_type(DataType::BOOL8), Sparsity::PERMITTED);
                auto value = static_cast<TargetType>(val.get<typename val_type_info::RawType>());
                value_string = fmt::format("{}", value);
                ternary<typename col_type_info::TDT, ScalarTagType<DataTypeTag<output_data_type>>, TargetType, arguments_reversed>(
                        condition,
                        *(col.column_),
                        value,
                        *output_column);
            } else {
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Invalid ternary operator arguments {}",
                                                                      ternary_operation_with_types_to_string<arguments_reversed>(
                                                                              col.column_name_,
                                                                              col.column_->type(),
                                                                              val.to_string<typename val_type_info::RawType>(),
                                                                              val.type()));
            }
        });
    });
    return {ColumnWithStrings(std::move(output_column), string_pool, ternary_operation_column_name<arguments_reversed>(col.column_name_, value_string))};
}

template<bool arguments_reversed = false>
VariantData ternary_operator(const util::BitSet& condition, const ColumnWithStrings& col, EmptyResult) {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            !is_empty_type(col.column_->type().data_type()),
            "Empty column provided to ternary operator");
    std::unique_ptr<Column> output_column;
    std::shared_ptr<StringPool> string_pool;
    std::string value_string;

    details::visit_type(col.column_->type().data_type(), [&](auto col_tag) {
        using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
        if constexpr(is_dynamic_string_type(col_type_info::data_type)) {
            output_column = std::make_unique<Column>(make_scalar_type(DataType::UTF_DYNAMIC64), Sparsity::PERMITTED);
            string_pool = std::make_shared<StringPool>();

            size_t output_physical_rows;
            size_t output_logical_rows = condition.size();
            util::BitSet output_sparse_map;
            if (col.column_->is_sparse()) {
                if constexpr (arguments_reversed) {
                    output_sparse_map = ~condition & col.column_->sparse_map();
                } else {
                    output_sparse_map = condition & col.column_->sparse_map();
                }
                output_sparse_map.resize(output_logical_rows);
                output_physical_rows = output_sparse_map.count();
                // Input column is sparse, but output column is dense
                if (output_physical_rows != output_logical_rows) {
                    output_column->set_sparse_map(std::move(output_sparse_map));
                }
            } else {
                if constexpr (arguments_reversed) {
                    output_physical_rows = output_logical_rows - condition.count();
                } else {
                    output_physical_rows = condition.count();
                }
                if (output_physical_rows != output_logical_rows) {
                    if constexpr (arguments_reversed) {
                        output_sparse_map = ~condition;
                    } else {
                        output_sparse_map = condition;
                    }
                    output_sparse_map.resize(output_logical_rows);
                    output_column->set_sparse_map(std::move(output_sparse_map));
                }
            }
            if (output_physical_rows > 0) {
                output_column->allocate_data(output_physical_rows * get_type_size(output_column->type().data_type()));
            }
            output_column->set_row_data(output_logical_rows - 1);
            auto output_data = output_column->data();
            using input_tdt = typename col_type_info::TDT;
            if (output_column->is_sparse()) {
                auto output_end_it = output_data.end<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
                for (auto output_it = output_data.begin<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>(); output_it != output_end_it; ++output_it) {
                    auto idx = output_it->idx();
                    auto string_at_offset = col.string_at_offset(*col.column_->scalar_at<int64_t>(idx));
                    if (string_at_offset.has_value()) {
                        auto offset_string = string_pool->get(*string_at_offset);
                        output_it->value() = offset_string.offset();
                    } else {
                        // string_at_offset will only be valueless if the condition was true and so was
                        // selected from the column
                        output_it->value() = *col.column_->scalar_at<int64_t>(idx);
                    }
                }
            } else {
                auto output_end_it = output_data.end<input_tdt, IteratorType::ENUMERATED>();
                for (auto output_it = output_data.begin<input_tdt, IteratorType::ENUMERATED>(); output_it != output_end_it; ++output_it) {
                    auto idx = output_it->idx();
                    auto string_at_offset = col.string_at_offset(*col.column_->scalar_at<int64_t>(idx));
                    if (string_at_offset.has_value()) {
                        auto offset_string = string_pool->get(*string_at_offset);
                        output_it->value() = offset_string.offset();
                    } else {
                        // string_at_offset will only be valueless if the condition was true and so was
                        // selected from the column
                        output_it->value() = *col.column_->scalar_at<int64_t>(idx);
                    }
                }
            }
        } else if constexpr (is_numeric_type(col_type_info::data_type) || is_bool_type(col_type_info::data_type)) {
            output_column = std::make_unique<Column>(col.column_->type(), Sparsity::PERMITTED);
            ternary<typename col_type_info::TDT, arguments_reversed>(
                    condition,
                    *col.column_,
                    *output_column);
        } else {
            user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Invalid ternary operator arguments {}",
                                                                  ternary_operation_with_types_to_string<arguments_reversed>(
                                                                          col.column_name_,
                                                                          col.column_->type(),
                                                                          "",
                                                                          {}));
        }
    });
    return {ColumnWithStrings(std::move(output_column), string_pool, ternary_operation_column_name<arguments_reversed>(col.column_name_, value_string))};
}

VariantData ternary_operator(const util::BitSet& condition, const Value& left, const Value& right) {
    std::unique_ptr<Column> output_column;
    std::shared_ptr<StringPool> string_pool;
    std::string left_string;
    std::string right_string;

    details::visit_type(left.type().data_type(), [&](auto left_tag) {
        using left_type_info = ScalarTypeInfo<decltype(left_tag)>;
        details::visit_type(right.type().data_type(), [&](auto right_tag) {
            using right_type_info = ScalarTypeInfo<decltype(right_tag)>;
            if constexpr(is_sequence_type(left_type_info::data_type) && is_sequence_type(right_type_info::data_type)) {
                if constexpr(left_type_info::data_type == right_type_info::data_type && is_dynamic_string_type(left_type_info::data_type)) {
                    output_column = std::make_unique<Column>(make_scalar_type(left_type_info::data_type), Sparsity::PERMITTED);
                    string_pool = std::make_shared<StringPool>();
                    auto left_string = std::string(*left.str_data(), left.len());
                    auto right_string = std::string(*right.str_data(), right.len());
                    // Put both possible strings in the pool for performance, it's possible one will be redundant if condition is all true or all false
                    auto left_offset = string_pool->get(left_string, false).offset();
                    auto right_offset = string_pool->get(right_string, false).offset();
                    // TODO: Use ColumnDataIterator and more efficient bitset access
                    for (size_t idx = 0; idx < condition.size(); ++idx) {
                        output_column->push_back(condition[idx] ? left_offset : right_offset);
                    }
                } else {
                    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexepcted fixed-width string value in ternary operator");
                }
            } else if constexpr (is_numeric_type(left_type_info::data_type) && is_numeric_type(right_type_info::data_type)) {
                using TargetType = typename ternary_operation_promoted_type<typename left_type_info::RawType, typename right_type_info::RawType>::type;
                constexpr auto output_data_type = data_type_from_raw_type<TargetType>();
                output_column = std::make_unique<Column>(make_scalar_type(output_data_type), Sparsity::PERMITTED);
                auto left_value = static_cast<TargetType>(left.get<typename left_type_info::RawType>());
                auto right_value = static_cast<TargetType>(right.get<typename right_type_info::RawType>());
                left_string = fmt::format("{}", left_value);
                right_string = fmt::format("{}", right_value);
                // TODO: Use ColumnDataIterator and more efficient bitset access
                for (size_t idx = 0; idx < condition.size(); ++idx) {
                    output_column->push_back(condition[idx] ? left_value : right_value);
                }
            } else if constexpr (is_bool_type(left_type_info::data_type) && is_bool_type(right_type_info::data_type)) {
                output_column = std::make_unique<Column>(make_scalar_type(DataType::BOOL8), Sparsity::PERMITTED);
                auto left_value = left.get<bool>();
                auto right_value = right.get<bool>();
                left_string = fmt::format("{}", left_value);
                right_string = fmt::format("{}", right_value);
                // TODO: Use ColumnDataIterator and more efficient bitset access
                for (size_t idx = 0; idx < condition.size(); ++idx) {
                    output_column->push_back(condition[idx] ? left_value : right_value);
                }
            } else {
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Invalid ternary operator arguments {}",
                                                                      ternary_operation_with_types_to_string(
                                                                              left.to_string<typename right_type_info::RawType>(),
                                                                              left.type(),
                                                                              right.to_string<typename right_type_info::RawType>(),
                                                                              right.type()));
            }
        });
    });
    return {ColumnWithStrings(std::move(output_column), string_pool, ternary_operation_column_name(left_string, right_string))};
}

template<bool arguments_reversed = false>
VariantData ternary_operator(const util::BitSet& condition, const Value& val, EmptyResult) {
    std::unique_ptr<Column> output_column;
    std::shared_ptr<StringPool> string_pool;
    std::string value_string;

    details::visit_type(val.type().data_type(), [&](auto val_tag) {
        using val_type_info = ScalarTypeInfo<decltype(val_tag)>;
        if constexpr(is_dynamic_string_type(val_type_info::data_type)) {
            output_column = std::make_unique<Column>(val.type(), Sparsity::PERMITTED);
            string_pool = std::make_shared<StringPool>();
            auto value_string = std::string(*val.str_data(), val.len());
            auto offset_string = string_pool->get(value_string);
            ternary<typename val_type_info::TDT, arguments_reversed>(
                    condition,
                    offset_string.offset(),
                    *output_column);
        } else if constexpr (is_numeric_type(val_type_info::data_type) || is_bool_type(val_type_info::data_type)) {
            using TargetType = val_type_info::RawType;
            output_column = std::make_unique<Column>(val.type(), Sparsity::PERMITTED);
            auto value = static_cast<TargetType>(val.get<typename val_type_info::RawType>());
            value_string = fmt::format("{}", value);
            ternary<typename val_type_info::TDT, arguments_reversed>(
                    condition,
                    value,
                    *output_column);
        } else {
            user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Invalid ternary operator arguments {}",
                                                                  ternary_operation_with_types_to_string<arguments_reversed>(
                                                                          val.to_string<typename val_type_info::RawType>(),
                                                                          val.type(),
                                                                          "",
                                                                          {}));
        }
    });
    return {ColumnWithStrings(std::move(output_column), string_pool, ternary_operation_column_name<arguments_reversed>(value_string, ""))};
}

VariantData ternary_operator(const util::BitSet& condition, bool left, bool right) {
    util::BitSet output_bitset;
    output_bitset.resize(condition.size());
    util::BitSet::bulk_insert_iterator inserter(output_bitset);
    for (size_t idx = 0; idx < condition.size(); ++idx) {
        if (condition[idx] ? left : right) {
            inserter = idx;
        }
    }
    inserter.flush();
    return VariantData{std::move(output_bitset)};
}

VariantData visit_ternary_operator(const VariantData& condition, const VariantData& left, const VariantData& right) {
    if (std::holds_alternative<FullResult>(condition)) {
        return left;
    } else if (std::holds_alternative<EmptyResult>(condition)) {
        return right;
    }
    auto transformed_condition = transform_to_bitset(condition);
    // transformed_condition is a bitset. transform_to_bitset throws if provided a Value or ValueSet. If it is a column,
    // it throws if the column type is not bool, and converts bool columns to a bitset, and full/empty results were
    // handled above
    auto c = std::get<util::BitSet>(std::move(transformed_condition));
    return std::visit(util::overload{
            [&c](const util::BitSet &l, const util::BitSet &r) -> VariantData {
                auto result = ternary_operator(c, l, r);
                return transform_to_placeholder(result);
            },
            [&c](const util::BitSet &l, const ColumnWithStrings &r) -> VariantData {
                auto bitset = std::get<util::BitSet>(transform_to_bitset(r));
                auto result = ternary_operator(c, l, bitset);
                return transform_to_placeholder(result);
            },
            [&c](const ColumnWithStrings &l, const util::BitSet &r) -> VariantData {
                auto bitset = std::get<util::BitSet>(transform_to_bitset(l));
                auto result = ternary_operator(c, bitset, r);
                return transform_to_placeholder(result);
            },
            [&c](const util::BitSet &l, const std::shared_ptr<Value> &r) -> VariantData {
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(is_bool_type(r->data_type_),
                                                                      "Ternary operator expected bool value, received {}",
                                                                      get_user_friendly_type_string(r->type()));
                auto result = ternary_operator(c, l, r->get<bool>());
                return transform_to_placeholder(result);
            },
            [&c](const std::shared_ptr<Value> &l, const util::BitSet &r) -> VariantData {
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(is_bool_type(l->data_type_),
                                                                      "Ternary operator expected bool value, received {}",
                                                                      get_user_friendly_type_string(l->type()));
                auto result = ternary_operator<true>(c, r, l->get<bool>());
                return transform_to_placeholder(result);
            },
            [&c](const util::BitSet &l, FullResult) -> VariantData {
                auto result = ternary_operator(c, l, true);
                return transform_to_placeholder(result);
            },
            [&c](FullResult, const util::BitSet &r) -> VariantData {
                auto result = ternary_operator<true>(c, r, true);
                return transform_to_placeholder(result);
            },
            [&c](const util::BitSet &l, EmptyResult) -> VariantData {
                auto result = ternary_operator(c, l, false);
                return transform_to_placeholder(result);
            },
            [&c](EmptyResult, const util::BitSet &r) -> VariantData {
                auto result = ternary_operator<true>(c, r, false);
                return transform_to_placeholder(result);
            },
            [&c](const ColumnWithStrings &l, const ColumnWithStrings &r) -> VariantData {
                auto result = ternary_operator(c, l, r);
                return transform_to_placeholder(result);
            },
            [&c](const ColumnWithStrings &l, const std::shared_ptr<Value> &r) -> VariantData {
                auto result = ternary_operator(c, l, *r);
                return transform_to_placeholder(result);
            },
            [&c](const std::shared_ptr<Value> &l, const ColumnWithStrings &r) -> VariantData {
                auto result = ternary_operator<true>(c, r, *l);
                return transform_to_placeholder(result);
            },
            [&c](const ColumnWithStrings &l, FullResult) -> VariantData {
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                        is_bool_type(l.column_->type().data_type()),
                        "Ternary operator cannot combine column '{}' of type {} with a FullResult. This can be caused by dynamic schema when a row-slice has a necessary column missing.",
                        l.column_name_,
                        get_user_friendly_type_string(l.column_->type()));
                auto bitset = std::get<util::BitSet>(transform_to_bitset(l));
                auto result = ternary_operator(c, bitset, true);
                return transform_to_placeholder(result);
            },
            [&c](FullResult, const ColumnWithStrings &r) -> VariantData {
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                        is_bool_type(r.column_->type().data_type()),
                        "Ternary operator cannot combine column '{}' of type {} with a FullResult. This can be caused by dynamic schema when a row-slice has a necessary column missing.",
                        r.column_name_,
                        get_user_friendly_type_string(r.column_->type()));
                auto bitset = std::get<util::BitSet>(transform_to_bitset(r));
                auto result = ternary_operator<true>(c, bitset, true);
                return transform_to_placeholder(result);
            },
            [&c](const ColumnWithStrings& l, const EmptyResult& r) -> VariantData {
                auto result = ternary_operator(c, l, r);
                return transform_to_placeholder(result);
            },
            [&c](const EmptyResult& l, const ColumnWithStrings& r) -> VariantData {
                auto result = ternary_operator<true>(c, r, l);
                return transform_to_placeholder(result);
            },
            [&c](const std::shared_ptr<Value> &l, const std::shared_ptr<Value> &r) -> VariantData {
                auto result = ternary_operator(c, *l, *r);
                return transform_to_placeholder(result);
            },
            [&c](const std::shared_ptr<Value> &l, FullResult) -> VariantData {
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(is_bool_type(l->data_type_),
                                                                      "Ternary operator expected bool value, received {}",
                                                                      get_user_friendly_type_string(l->type()));
                auto value = l->get<bool>();
                auto result = ternary_operator(c, value, true);
                return transform_to_placeholder(result);
            },
            [&c](FullResult, const std::shared_ptr<Value> &r) -> VariantData {
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(is_bool_type(r->data_type_),
                                                                      "Ternary operator expected bool value, received {}",
                                                                      get_user_friendly_type_string(r->type()));
                auto value = r->get<bool>();
                auto result = ternary_operator(c, true, value);
                return transform_to_placeholder(result);
            },
            [&c](const std::shared_ptr<Value>& l, const EmptyResult& r) -> VariantData {
                auto result = ternary_operator(c, *l, r);
                return transform_to_placeholder(result);
            },
            [&c](const EmptyResult& l, const std::shared_ptr<Value> &r) -> VariantData {
                auto result = ternary_operator<true>(c, *r, l);
                return transform_to_placeholder(result);
            },
            [](FullResult, FullResult) -> VariantData {
                return FullResult{};
            },
            [&c](FullResult, EmptyResult) -> VariantData {
                return c;
            },
            [&c](EmptyResult, FullResult) -> VariantData {
                auto res = c;
                res.flip();
                res.resize(c.size());
                return res;
            },
            [](EmptyResult, EmptyResult) -> VariantData {
                return EmptyResult{};
            },
            [](const auto &, const auto &) -> VariantData {
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Invalid input types to ternary operator");
                return EmptyResult{};
            }
    }, left, right);
}

VariantData dispatch_ternary(const VariantData& condition, const VariantData& left, const VariantData& right, OperationType operation) {
    switch(operation) {
        case OperationType::TERNARY:
            return visit_ternary_operator(condition, left, right);
        default:
            util::raise_rte("Unknown operation {}", int(operation));
    }
}

}
