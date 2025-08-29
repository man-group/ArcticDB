/*
 * Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/pipeline/value_set.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/processing/operation_dispatch.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/entity/type_conversion.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <variant>
#include <memory>
#include <type_traits>

namespace arcticdb {

VariantData binary_boolean(const util::BitSet& left, const util::BitSet& right, OperationType operation);

VariantData binary_boolean(const util::BitSet& left, EmptyResult, OperationType operation);

VariantData binary_boolean(const util::BitSet& left, FullResult, OperationType operation);

VariantData binary_boolean(EmptyResult, FullResult, OperationType operation);

VariantData binary_boolean(FullResult, FullResult, OperationType operation);

VariantData binary_boolean(EmptyResult, EmptyResult, OperationType operation);

// Note that we can have fewer of these and reverse the parameters because all the operations are
// commutative, however if that were to change we would need the full set
VariantData visit_binary_boolean(const VariantData& left, const VariantData& right, OperationType operation);

template <typename Func>
inline std::string binary_operation_column_name(std::string_view left_column, Func&& func, std::string_view right_column) {
    return fmt::format("({} {} {})", left_column, func, right_column);
}

template <typename Func>
inline std::string binary_operation_with_types_to_string(std::string_view left, const TypeDescriptor& type_left, Func&& func,
                                                         std::string_view right, const TypeDescriptor& type_right,
                                                         bool arguments_reversed = false) {
    if (arguments_reversed) {
        return fmt::format("{} ({}) {} {} ({})", right, get_user_friendly_type_string(type_right), func, left, get_user_friendly_type_string(type_left));
    }
    return fmt::format("{} ({}) {} {} ({})", left, get_user_friendly_type_string(type_left), func, right, get_user_friendly_type_string(type_right));
}

template <typename Func>
VariantData binary_membership(const ColumnWithStrings& column_with_strings, ValueSet& value_set, Func&& func) {
    if (is_empty_type(column_with_strings.column_->type().data_type())) {
        if constexpr(std::is_same_v<std::remove_reference_t<Func>, IsInOperator>) {
            return EmptyResult{};
        } else if constexpr(std::is_same_v<std::remove_reference_t<Func>, IsNotInOperator>) {
            return FullResult{};
        }
    }
    // If the value set is empty, we can short-circuit
    if (value_set.empty()) {
        if constexpr(std::is_same_v<std::remove_reference_t<Func>, IsNotInOperator>) {
            return FullResult{};
        } else {
            return EmptyResult{};
        }
    }

    util::BitSet output_bitset;
    constexpr auto sparse_missing_value_output = std::is_same_v<std::remove_reference_t<Func>, IsNotInOperator>;
    details::visit_type(column_with_strings.column_->type().data_type(),[&, sparse_missing_value_output] (auto col_tag) {
        using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
        details::visit_type(value_set.base_type().data_type(), [&] (auto val_set_tag) {
            using val_set_type_info = ScalarTypeInfo<decltype(val_set_tag)>;
            if constexpr(is_sequence_type(col_type_info::data_type) && is_sequence_type(val_set_type_info::data_type)) {
                std::shared_ptr<std::unordered_set<std::string>> typed_value_set;
                if constexpr(is_fixed_string_type(col_type_info::data_type)) {
                    auto width = column_with_strings.get_fixed_width_string_size();
                    if (width.has_value()) {
                        typed_value_set = value_set.get_fixed_width_string_set(*width);
                    }
                } else {
                    typed_value_set = value_set.get_set<std::string>();
                }
                auto offset_set = column_with_strings.string_pool_->get_offsets_for_column(typed_value_set, *column_with_strings.column_);
                Column::transform<typename col_type_info::TDT>(
                        *column_with_strings.column_,
                        output_bitset,
                        sparse_missing_value_output,
                        [&func, &offset_set](auto input_value) -> bool {
                    auto offset = static_cast<entity::position_t>(input_value);
                    return func(offset, offset_set);
                });
            } else if constexpr (is_bool_type(col_type_info::data_type) && is_bool_type(val_set_type_info::data_type)) {
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Binary membership '{}' not implemented for bools", func);
            } else if constexpr (is_numeric_type(col_type_info::data_type) && is_numeric_type(val_set_type_info::data_type)) {
                using WideType = typename binary_operation_promoted_type<typename col_type_info::RawType,typename val_set_type_info::RawType, std::remove_reference_t<Func>>::type;
                auto typed_value_set = value_set.get_set<WideType>();
                Column::transform<typename col_type_info::TDT>(
                        *column_with_strings.column_,
                        output_bitset,
                        sparse_missing_value_output,
                        [&func, &typed_value_set](auto input_value) -> bool {
                    if constexpr (MembershipOperator::needs_uint64_special_handling<typename col_type_info::RawType, typename val_set_type_info::RawType>) {
                        // Avoid narrowing conversion on *input_it:
                        return func(input_value, *typed_value_set, UInt64SpecialHandlingTag{});
                    } else {
                        return func(static_cast<WideType>(input_value), *typed_value_set);
                    }
                });
            } else {
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Cannot check membership '{}' of {} {} in set of {}",
                                func,
                                column_with_strings.column_name_,
                                get_user_friendly_type_string(column_with_strings.column_->type()),
                                get_user_friendly_type_string(value_set.base_type()));
            }
        });
    });

    log::version().debug("Filtered column of size {} down to {} bits", column_with_strings.column_->last_row() + 1, output_bitset.count());

    return {std::move(output_bitset)};
}

template<typename Func>
VariantData visit_binary_membership(const VariantData &left, const VariantData &right, Func &&func) {
    if (std::holds_alternative<EmptyResult>(left))
        return EmptyResult{};

    return std::visit(util::overload {
        [&] (const ColumnWithStrings& l, const std::shared_ptr<ValueSet>& r) ->VariantData  {
            return transform_to_placeholder(binary_membership(l, *r, std::forward<decltype(func)>(func)));
            },
            [](const auto &, const auto&) -> VariantData {
            user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Binary membership operations must be Column/ValueSet");
            return EmptyResult{};
        }
        }, left, right);
}

template <typename Func>
VariantData binary_comparator(const ColumnWithStrings& left, const ColumnWithStrings& right, Func&& func) {
    if (is_empty_type(left.column_->type().data_type()) || is_empty_type(right.column_->type().data_type())) {
        return EmptyResult{};
    }
    util::BitSet output_bitset;
    constexpr auto sparse_missing_value_output = std::is_same_v<std::remove_reference_t<Func>, NotEqualsOperator>;

    details::visit_type(left.column_->type().data_type(), [&, sparse_missing_value_output](auto left_tag) {
        using left_type_info = ScalarTypeInfo<decltype(left_tag)>;
        details::visit_type(right.column_->type().data_type(), [&](auto right_tag) {
            using right_type_info = ScalarTypeInfo<decltype(right_tag)>;
            if constexpr(is_sequence_type(left_type_info::data_type) && is_sequence_type(right_type_info::data_type)) {
                bool strip_fixed_width_trailing_nulls{false};
                // If one or both columns are fixed width strings, we need to strip trailing null characters to get intuitive results
                if constexpr (is_fixed_string_type(left_type_info::data_type) || is_fixed_string_type(right_type_info::data_type)) {
                    strip_fixed_width_trailing_nulls = true;
                }
                Column::transform<typename left_type_info::TDT, typename right_type_info::TDT>(
                        *left.column_,
                        *right.column_,
                        output_bitset,
                        sparse_missing_value_output,
                        [&func, &left, &right, strip_fixed_width_trailing_nulls] (auto left_value, auto right_value) -> bool {
                    return func(left.string_at_offset(left_value, strip_fixed_width_trailing_nulls),
                                right.string_at_offset(right_value, strip_fixed_width_trailing_nulls));
                });
            } else if constexpr ((is_numeric_type(left_type_info::data_type) && is_numeric_type(right_type_info::data_type)) ||
                                 (is_bool_type(left_type_info::data_type) && is_bool_type(right_type_info::data_type))) {
                using comp = typename arcticdb::Comparable<typename left_type_info::RawType, typename right_type_info::RawType>;
                Column::transform<typename left_type_info::TDT, typename right_type_info::TDT>(
                        *left.column_,
                        *right.column_,
                        output_bitset,
                        sparse_missing_value_output,
                        [&func] (auto left_value, auto right_value) -> bool {
                    return func(static_cast<typename comp::left_type>(left_value), static_cast<typename comp::right_type>(right_value));
                });
            } else {
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Invalid comparison {}",
                                binary_operation_with_types_to_string(
                                        left.column_name_,
                                        left.column_->type(),
                                        func,
                                        right.column_name_,
                                        right.column_->type()));
            }
        });
    });
    ARCTICDB_DEBUG(log::version(), "Filtered column of size {} down to {} bits", std::max(left.column_->last_row(), right.column_->last_row()) + 1, output_bitset.count());

    return VariantData{std::move(output_bitset)};
}

template <typename Func, bool arguments_reversed = false>
VariantData binary_comparator(const ColumnWithStrings& column_with_strings, const Value& val, Func&& func) {
    if (is_empty_type(column_with_strings.column_->type().data_type())) {
        return EmptyResult{};
    }
    util::BitSet output_bitset;
    constexpr auto sparse_missing_value_output = std::is_same_v<std::remove_reference_t<Func>, NotEqualsOperator>;

    details::visit_type(column_with_strings.column_->type().data_type(), [&, sparse_missing_value_output](auto col_tag) {
        using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
        details::visit_type(val.data_type(), [&](auto val_tag) {
            using val_type_info = ScalarTypeInfo<decltype(val_tag)>;
            if constexpr(is_sequence_type(col_type_info::data_type) && is_sequence_type(val_type_info::data_type)) {
                std::optional<std::string> utf32_string;
                std::string value_string;
                if constexpr(is_fixed_string_type(col_type_info::data_type)) {
                    auto width = column_with_strings.get_fixed_width_string_size();
                    if (width.has_value()) {
                        utf32_string = ascii_to_padded_utf32(std::string_view(*val.str_data(), val.len()), *width);
                        if (utf32_string.has_value()) {
                            value_string = *utf32_string;
                        }
                    }
                } else {
                    value_string = std::string(*val.str_data(), val.len());
                }
                auto value_offset = column_with_strings.string_pool_->get_offset_for_column(value_string, *column_with_strings.column_);
                Column::transform<typename col_type_info::TDT>(
                        *column_with_strings.column_,
                        output_bitset,
                        sparse_missing_value_output,
                        [&func, value_offset](auto input_value) -> bool {
                    auto offset = static_cast<entity::position_t>(input_value);
                    if constexpr (arguments_reversed) {
                        return func(offset, value_offset);
                    } else {
                        return func(value_offset, offset);
                    }
                });
            } else if constexpr ((is_numeric_type(col_type_info::data_type) && is_numeric_type(val_type_info::data_type)) ||
                                 (is_bool_type(col_type_info::data_type) && is_bool_type(val_type_info::data_type))) {
                using ColType = typename col_type_info::RawType;
                using ValType = typename val_type_info::RawType;
                using comp = std::conditional_t<arguments_reversed,
                                                typename arcticdb::Comparable<ValType, ColType>,
                                                typename arcticdb::Comparable<ColType, ValType>>;
                auto value = static_cast<typename comp::right_type>(val.get<ValType>());
                Column::transform<typename col_type_info::TDT>(
                        *column_with_strings.column_,
                        output_bitset,
                        sparse_missing_value_output,
                        [&func, value](auto input_value) -> bool {
                    if constexpr (arguments_reversed) {
                        return func(value, static_cast<typename comp::left_type>(input_value));
                    } else {
                        return func(static_cast<typename comp::left_type>(input_value), value);
                    }
                });

            } else {
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Invalid comparison {}",
                                binary_operation_with_types_to_string(
                                        column_with_strings.column_name_,
                                        column_with_strings.column_->type(),
                                        func,
                                        val.to_string<typename val_type_info::RawType>(),
                                        val.descriptor(),
                                        arguments_reversed));
            }
        });
    });
    ARCTICDB_DEBUG(log::version(), "Filtered column of size {} down to {} bits", column_with_strings.column_->last_row() + 1, output_bitset.count());

    return VariantData{std::move(output_bitset)};
}

template <typename Func>
VariantData binary_comparator(const ColumnWithStrings& column_with_strings, const util::RegexGeneric& regex_generic, Func&& func) {
    if (is_empty_type(column_with_strings.column_->type().data_type())) {
        return EmptyResult{};
    }
    if constexpr(std::is_same_v<std::remove_reference_t<Func>, RegexMatchOperator>) {
        util::BitSet output_bitset;
        details::visit_type(column_with_strings.column_->type().data_type(), [&](auto col_tag) {
            using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
            if constexpr(is_sequence_type(col_type_info::data_type)) {
                auto offset_set = column_with_strings.string_pool_->get_regex_match_offsets_for_column(regex_generic, *column_with_strings.column_);
                Column::transform<typename col_type_info::TDT>(
                        *column_with_strings.column_,
                        output_bitset,
                        false,
                        [&offset_set, &func](auto input_value) {
                    auto offset = static_cast<entity::position_t>(input_value);
                    return func(offset, offset_set);
                });
            } else {
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Cannot perform regex_match with pattern {} on column {} as it has non-string type {}",
                    regex_generic.text(), column_with_strings.column_name_, get_user_friendly_type_string(column_with_strings.column_->type()));
            }
        });
        ARCTICDB_DEBUG(log::version(), "Filtered column of size {} down to {} bits", column_with_strings.column_->last_row() + 1, output_bitset.count());
        return VariantData{std::move(output_bitset)};
    } else {
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Invalid operator {} for regex match", func);
        return EmptyResult{};
    }
}

template<typename Func>
VariantData visit_binary_comparator(const VariantData& left, const VariantData& right, Func&& func) {
    if(std::holds_alternative<EmptyResult>(left) || std::holds_alternative<EmptyResult>(right))
        return EmptyResult{};

    return std::visit(util::overload {
        [&func] (const ColumnWithStrings& l, const std::shared_ptr<Value>& r) ->VariantData  {
            auto result = binary_comparator<decltype(func)>(l, *r, std::forward<decltype(func)>(func));
            return transform_to_placeholder(result);
        },
        [&func] (const ColumnWithStrings& l, const ColumnWithStrings& r)  ->VariantData {
            auto result = binary_comparator<decltype(func)>(l, r, std::forward<decltype(func)>(func));
            return transform_to_placeholder(result);
        },
        [&func](const std::shared_ptr<Value>& l, const ColumnWithStrings& r) ->VariantData {
            auto result =  binary_comparator<decltype(func), true>(r, *l, std::forward<decltype(func)>(func));
            return transform_to_placeholder(result);
        },
        [&func](const ColumnWithStrings& l, const std::shared_ptr<util::RegexGeneric>& r) ->VariantData {
            auto result =  binary_comparator<decltype(func)>(l, *r, std::forward<decltype(func)>(func));
            return transform_to_placeholder(result);
        },
        [] ([[maybe_unused]] const std::shared_ptr<Value>& l, [[maybe_unused]] const std::shared_ptr<Value>& r) ->VariantData  {
            user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Two value inputs not accepted to binary comparators");
            return EmptyResult{};
        },
        [](const auto &, const auto&) -> VariantData {
            user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Bitset/ValueSet inputs not accepted to binary comparators");
            return EmptyResult{};
        }
    }, left, right);
}

template <typename Func>
VariantData binary_operator(const Value& left, const Value& right, Func&& func) {
    auto output_value = std::make_unique<Value>();

    details::visit_type(left.data_type(), [&](auto left_tag) {
        using left_type_info = ScalarTypeInfo<decltype(left_tag)>;
        details::visit_type(right.data_type(), [&](auto right_tag) {
            using right_type_info = ScalarTypeInfo<decltype(right_tag)>;
            if constexpr(!is_numeric_type(left_type_info::data_type) || !is_numeric_type(right_type_info::data_type)) {
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Non-numeric type provided to binary operation: {}",
                                binary_operation_with_types_to_string(
                                    left.to_string<typename left_type_info::RawType>(),
                                    left.descriptor(),
                                    func,
                                    right.to_string<typename right_type_info::RawType>(),
                                    right.descriptor()));
            }
            auto right_value = right.get<typename right_type_info::RawType>();
            auto left_value = left.get<typename left_type_info::RawType>();
            using TargetType = typename binary_operation_promoted_type<typename left_type_info::RawType, typename right_type_info::RawType, std::remove_reference_t<Func>>::type;
            *output_value = Value{TargetType{func.apply(left_value, right_value)}, data_type_from_raw_type<TargetType>()};
        });
    });
    return VariantData(std::move(output_value));
}

template <typename Func>
VariantData binary_operator(const ColumnWithStrings& left, const ColumnWithStrings& right, Func&& func) {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            !is_empty_type(left.column_->type().data_type()) && !is_empty_type(right.column_->type().data_type()),
            "Empty column provided to binary operator");
    std::unique_ptr<Column> output_column;

    details::visit_type(left.column_->type().data_type(), [&](auto left_tag) {
        using left_type_info = ScalarTypeInfo<decltype(left_tag)>;
        details::visit_type(right.column_->type().data_type(), [&](auto right_tag) {
            using right_type_info = ScalarTypeInfo<decltype(right_tag)>;
            if constexpr(!is_numeric_type(left_type_info::data_type) || !is_numeric_type(right_type_info::data_type)) {
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Non-numeric column provided to binary operation: {}",
                                binary_operation_with_types_to_string(
                                        left.column_name_,
                                        left.column_->type(),
                                        func,
                                        right.column_name_,
                                        right.column_->type()));
            }
            using TargetType = typename binary_operation_promoted_type<typename left_type_info::RawType, typename right_type_info::RawType, std::remove_reference_t<decltype(func)>>::type;
            constexpr auto output_data_type = data_type_from_raw_type<TargetType>();
            output_column = std::make_unique<Column>(make_scalar_type(output_data_type), Sparsity::PERMITTED);
            Column::transform<typename left_type_info::TDT, typename right_type_info::TDT, ScalarTagType<DataTypeTag<output_data_type>>>(
                    *(left.column_),
                    *(right.column_),
                    *output_column,
                    [&func] (auto left_value, auto right_value) -> TargetType {
                        return func.apply(left_value, right_value);
                    });
        });
    });
    return VariantData(ColumnWithStrings(std::move(output_column), binary_operation_column_name(left.column_name_, func, right.column_name_)));
}

template <typename Func, bool arguments_reversed = false>
VariantData binary_operator(const ColumnWithStrings& col, const Value& val, Func&& func) {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            !is_empty_type(col.column_->type().data_type()),
            "Empty column provided to binary operator");
    std::unique_ptr<Column> output_column;
    std::string column_name;

    details::visit_type(col.column_->type().data_type(), [&](auto col_tag) {
        using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
        details::visit_type(val.data_type(), [&](auto val_tag) {
            using val_type_info = ScalarTypeInfo<decltype(val_tag)>;
            if constexpr(!is_numeric_type(col_type_info::data_type) || !is_numeric_type(val_type_info::data_type)) {
                std::string error_message;
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Non-numeric type provided to binary operation: {}",
                                binary_operation_with_types_to_string(
                                        col.column_name_,
                                        col.column_->type(),
                                        func,
                                        val.to_string<typename val_type_info::RawType>(),
                                        val.descriptor(),
                                        arguments_reversed));
            }
            const auto& raw_value = val.get<typename val_type_info::RawType>();
            using TargetType = typename binary_operation_promoted_type<typename col_type_info::RawType, typename val_type_info::RawType, std::remove_reference_t<decltype(func)>>::type;
            if constexpr(arguments_reversed) {
                column_name = binary_operation_column_name(fmt::format("{}", raw_value), func, col.column_name_);
                constexpr auto output_data_type = data_type_from_raw_type<TargetType>();
                output_column = std::make_unique<Column>(make_scalar_type(output_data_type), Sparsity::PERMITTED);
                Column::transform<typename col_type_info::TDT, ScalarTagType<DataTypeTag<output_data_type>>>(
                        *(col.column_),
                        *output_column,
                        [&func, raw_value](auto input_value) -> TargetType {
                            return func.apply(raw_value, input_value);
                });
            } else {
                column_name = binary_operation_column_name(col.column_name_, func, fmt::format("{}", raw_value));
                constexpr auto output_data_type = data_type_from_raw_type<TargetType>();
                output_column = std::make_unique<Column>(make_scalar_type(output_data_type), Sparsity::PERMITTED);
                Column::transform<typename col_type_info::TDT, ScalarTagType<DataTypeTag<output_data_type>>>(
                        *(col.column_),
                        *output_column,
                        [&func, raw_value](auto input_value) -> TargetType {
                            return func.apply(input_value, raw_value);
                });
            }
        });
    });
    return {ColumnWithStrings(std::move(output_column), column_name)};
}

template<typename Func>
VariantData visit_binary_operator(const VariantData& left, const VariantData& right, Func&& func) {
    if(std::holds_alternative<EmptyResult>(left) || std::holds_alternative<EmptyResult>(right))
        return EmptyResult{};

    return std::visit(util::overload {
        [&] (const ColumnWithStrings& l, const std::shared_ptr<Value>& r) ->VariantData  {
            return binary_operator<decltype(func)>(l, *r, std::forward<decltype(func)>(func));
            },
            [&] (const ColumnWithStrings& l, const ColumnWithStrings& r)  ->VariantData {
            return binary_operator<decltype(func)>(l, r, std::forward<decltype(func)>(func));
            },
            [&](const std::shared_ptr<Value>& l, const ColumnWithStrings& r) ->VariantData {
            return binary_operator<decltype(func), true>(r, *l, std::forward<decltype(func)>(func));
            },
            [&] (const std::shared_ptr<Value>& l, const std::shared_ptr<Value>& r) -> VariantData {
            return binary_operator<decltype(func)>(*l, *r, std::forward<decltype(func)>(func));
            },
            [](const auto &, const auto&) -> VariantData {
            user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Bitset/ValueSet inputs not accepted to binary operators");
            return EmptyResult{};
            }
        }, left, right);
}

VariantData dispatch_binary(const VariantData& left, const VariantData& right, OperationType operation);

// instantiated in operation_dispatch_binary_operator.cpp to reduce compilation memory use
extern template
VariantData visit_binary_operator<arcticdb::PlusOperator>(const VariantData&, const VariantData&, PlusOperator&&);
extern template
VariantData visit_binary_operator<arcticdb::MinusOperator>(const VariantData&, const VariantData&, MinusOperator&&);
extern template
VariantData visit_binary_operator<arcticdb::TimesOperator>(const VariantData&, const VariantData&, TimesOperator&&);
extern template
VariantData visit_binary_operator<arcticdb::DivideOperator>(const VariantData&, const VariantData&, DivideOperator&&);

// instantiated in operation_dispatch_binary_comparator.cpp to reduce compilation memory use
extern template
VariantData visit_binary_comparator<EqualsOperator>(const VariantData&, const VariantData&, EqualsOperator&&);
extern template
VariantData visit_binary_comparator<NotEqualsOperator>(const VariantData&, const VariantData&, NotEqualsOperator&&);
extern template
VariantData visit_binary_comparator<LessThanOperator>(const VariantData&, const VariantData&, LessThanOperator&&);
extern template
VariantData visit_binary_comparator<LessThanEqualsOperator>(const VariantData&, const VariantData&, LessThanEqualsOperator&&);
extern template
VariantData visit_binary_comparator<GreaterThanOperator>(const VariantData&, const VariantData&, GreaterThanOperator&&);
extern template
VariantData visit_binary_comparator<GreaterThanEqualsOperator>(const VariantData&, const VariantData&, GreaterThanEqualsOperator&&);

}
