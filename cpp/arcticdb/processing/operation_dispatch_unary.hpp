/*
 * Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/column_algorithms.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/operation_dispatch.hpp>
#include <variant>
#include <memory>
#include <type_traits>

namespace arcticdb {

VariantData unary_boolean(const util::BitSet& bitset, OperationType operation);

VariantData unary_boolean(EmptyResult, OperationType operation);

VariantData unary_boolean(FullResult, OperationType operation);

VariantData visit_unary_boolean(const VariantData& left, OperationType operation);

template<typename Func>
inline std::string unary_operation_to_string(Func&& func, std::string_view operand_str) {
    return fmt::format("{}({})", func, operand_str);
}

template<typename Func>
VariantData unary_operator(const Value& val, Func&& func) {
    auto output = std::make_unique<Value>();
    details::visit_type(val.data_type(), [&](auto val_tag) {
        using type_info = ScalarTypeInfo<decltype(val_tag)>;
        if constexpr (!is_numeric_type(type_info::data_type)) {
            user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    "Cannot perform unary operation {} ({})",
                    unary_operation_to_string(func, val.to_string<typename type_info::RawType>()),
                    get_user_friendly_type_string(val.descriptor())
            );
        }
        auto value = val.get<typename type_info::RawType>();
        using TargetType =
                typename unary_operation_promoted_type<typename type_info::RawType, std::remove_reference_t<Func>>::
                        type;
        *output = Value{TargetType{func.apply(value)}, data_type_from_raw_type<TargetType>()};
    });

    return {std::move(output)};
}

template<typename Func>
VariantData unary_operator(const ColumnWithStrings& col, Func&& func) {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            !is_empty_type(col.column_->type().data_type()),
            "Empty column provided to unary operation {} ({})",
            unary_operation_to_string(func, col.column_name_),
            get_user_friendly_type_string(col.column_->type())
    );
    std::unique_ptr<Column> output_column;

    details::visit_type(col.column_->type().data_type(), [&](auto col_tag) {
        using type_info = ScalarTypeInfo<decltype(col_tag)>;
        if constexpr (is_numeric_type(type_info::data_type)) {
            using TargetType =
                    typename unary_operation_promoted_type<typename type_info::RawType, std::remove_reference_t<Func>>::
                            type;
            constexpr auto output_data_type = data_type_from_raw_type<TargetType>();
            output_column = std::make_unique<Column>(make_scalar_type(output_data_type), Sparsity::PERMITTED);
            arcticdb::transform<typename type_info::TDT, ScalarTagType<DataTypeTag<output_data_type>>>(
                    *(col.column_),
                    *output_column,
                    [&func](auto input_value) -> TargetType { return func.apply(input_value); }
            );
        } else {
            user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    "Cannot perform unary operation {} ({})",
                    unary_operation_to_string(func, col.column_name_),
                    get_user_friendly_type_string(col.column_->type())
            );
        }
    });
    return {ColumnWithStrings(std::move(output_column), unary_operation_to_string(func, col.column_name_))};
}

template<typename Func>
VariantData visit_unary_operator(const VariantData& left, Func&& func) {
    return std::visit(
            util::overload{
                    [&](const ColumnWithStrings& l) -> VariantData {
                        return unary_operator(l, std::forward<Func>(func));
                    },
                    [&](const std::shared_ptr<Value>& l) -> VariantData {
                        return unary_operator(*l, std::forward<Func>(func));
                    },
                    [](EmptyResult l) -> VariantData { return l; },
                    [](const auto&) -> VariantData {
                        user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                                "Bitset/ValueSet inputs not accepted to unary operators"
                        );
                        return EmptyResult{};
                    }
            },
            left
    );
}

template<typename Func>
VariantData unary_comparator(const ColumnWithStrings& col, Func&& func) {
    if (is_empty_type(col.column_->type().data_type()) || is_integer_type(col.column_->type().data_type())) {
        if constexpr (std::is_same_v<std::remove_reference_t<Func>, IsNullOperator>) {
            return is_empty_type(col.column_->type().data_type()) ? VariantData(FullResult{})
                                                                  : VariantData(EmptyResult{});
        } else if constexpr (std::is_same_v<std::remove_reference_t<Func>, NotNullOperator>) {
            return is_empty_type(col.column_->type().data_type()) ? VariantData(EmptyResult{})
                                                                  : VariantData(FullResult{});
        } else {
            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected operator passed to unary_comparator");
        }
    }

    util::BitSet output_bitset;
    constexpr auto sparse_missing_value_output = std::is_same_v<std::remove_reference_t<Func>, IsNullOperator>;
    details::visit_type(col.column_->type().data_type(), [&, sparse_missing_value_output](auto col_tag) {
        using type_info = ScalarTypeInfo<decltype(col_tag)>;
        // Non-explicit lambda capture due to a bug in LLVM: https://github.com/llvm/llvm-project/issues/34798
        arcticdb::transform<typename type_info::TDT>(
                *(col.column_),
                output_bitset,
                sparse_missing_value_output,
                [&](auto input_value) -> bool {
                    if constexpr (is_floating_point_type(type_info::data_type)) {
                        return func.apply(input_value);
                    } else if constexpr (is_sequence_type(type_info::data_type)) {
                        return func.template apply<StringTypeTag>(input_value);
                    } else if constexpr (is_time_type(type_info::data_type)) {
                        return func.template apply<TimeTypeTag>(input_value);
                    } else {
                        // This line should not be reached with if the column is of int type because we have an early
                        // exit above
                        // https://github.com/man-group/ArcticDB/blob/bc554c9d42c7714bab645a167c4df843bc2672c6/cpp/arcticdb/processing/operation_dispatch_unary.hpp#L117
                        // both null and not null are allowed with integers and return respectively EmptyResult and
                        // FullResult. We must keep the exception though as otherwise not all control paths of this
                        // function will return a value and this won't compile.
                        user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                                "Cannot perform null check: {} ({})",
                                unary_operation_to_string(func, col.column_name_),
                                get_user_friendly_type_string(col.column_->type())
                        );
                    }
                }
        );
    });
    return VariantData{std::move(output_bitset)};
}

template<typename Func>
VariantData visit_unary_comparator(const VariantData& left, Func&& func) {
    return std::visit(
            util::overload{
                    [&](const ColumnWithStrings& l) -> VariantData {
                        return transform_to_placeholder(unary_comparator(l, std::forward<decltype(func)>(func)));
                    },
                    [](EmptyResult) -> VariantData {
                        if constexpr (std::is_same_v<std::remove_reference_t<Func>, IsNullOperator>) {
                            return FullResult{};
                        } else if constexpr (std::is_same_v<std::remove_reference_t<Func>, NotNullOperator>) {
                            return EmptyResult{};
                        } else {
                            internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                                    "Unexpected operator passed to visit unary_comparator"
                            );
                        }
                    },
                    [](const auto&) -> VariantData {
                        user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                                "Bitset/ValueSet inputs not accepted to unary comparators"
                        );
                        return EmptyResult{};
                    }
            },
            left
    );
}

VariantData dispatch_unary(const VariantData& left, OperationType operation);

} // namespace arcticdb
