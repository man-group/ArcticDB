/*
 * Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstdint>
#include <variant>
#include <memory>
#include <type_traits>

#include <folly/futures/Future.h>

#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/pipeline/value_set.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/operation_dispatch.hpp>
#include <arcticdb/entity/type_conversion.hpp>

namespace arcticdb {

VariantData unary_boolean(const std::shared_ptr<util::BitSet>& bitset, OperationType operation);

VariantData unary_boolean(EmptyResult, OperationType operation);

VariantData unary_boolean(FullResult, OperationType operation);

VariantData visit_unary_boolean(const VariantData& left, OperationType operation);

template <typename Func>
VariantData unary_operator(const Value& val, Func&& func) {
    auto output = std::make_unique<Value>();
    output->data_type_ = val.data_type_;

    details::visit_type(val.type().data_type(), [&](auto val_desc_tag) {
        using TDT = ScalarTagType<decltype(val_desc_tag)>;
        using RawType = typename TDT::DataTypeTag::raw_type;
        if constexpr (!is_numeric_type(TDT::DataTypeTag::data_type)) {
            util::raise_rte("Cannot perform arithmetic on {}", val.type());
        }
        auto value = *reinterpret_cast<const RawType*>(val.data_);
        using TargetType = typename unary_arithmetic_promoted_type<RawType, std::remove_reference_t<Func>>::type;
        output->data_type_ = data_type_from_raw_type<TargetType>();
        *reinterpret_cast<TargetType*>(output->data_) = func.apply(value);
    });

    return {std::move(output)};
}

template <typename Func>
VariantData unary_operator(const Column& col, Func&& func) {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            !is_empty_type(col.type().data_type()),
            "Empty column provided to unary operator");
    std::unique_ptr<Column> output;

    details::visit_type(col.type().data_type(), [&](auto col_desc_tag) {
        using TDT = ScalarTagType<decltype(col_desc_tag)>;
        using RawType = typename TDT::DataTypeTag::raw_type;
        if constexpr (!is_numeric_type(TDT::DataTypeTag::data_type)) {
            util::raise_rte("Cannot perform arithmetic on {}", col.type());
        }
        using TargetType = typename unary_arithmetic_promoted_type<RawType, std::remove_reference_t<Func>>::type;
        auto output_data_type = data_type_from_raw_type<TargetType>();
        output = std::make_unique<Column>(make_scalar_type(output_data_type), col.is_sparse());
        auto col_data = col.data();
        while(auto block = col_data.next<TDT>()) {
            const auto row_count = block->row_count();
            const auto nbytes = sizeof(TargetType) * row_count;
            auto in_ptr = reinterpret_cast<const RawType*>(block->data());
            auto out_ptr = reinterpret_cast<TargetType*>(output->allocate_data(nbytes));
            for(auto idx = 0u; idx < row_count; ++idx) {
                *out_ptr++ = func.apply(*in_ptr++);
            }
            output->advance_data(nbytes);
        }
        output->set_row_data(col.row_count() - 1);
    });
    return {ColumnWithStrings(std::move(output))};
}

template<typename Func>
VariantData visit_unary_operator(const VariantData& left, Func&& func) {
    return std::visit(util::overload{
        [&] (const ColumnWithStrings& l) -> VariantData {
            return unary_operator(*(l.column_), std::forward<Func>(func));
            },
        [&] (const std::shared_ptr<Value>& l) -> VariantData {
            return unary_operator(*l, std::forward<Func>(func));
        },
        [] (EmptyResult l) -> VariantData {
            return l;
        },
        [](const auto&) -> VariantData {
            util::raise_rte("Bitset/ValueSet inputs not accepted to unary operators");
        }
    }, left);
}

template <typename Func>
VariantData unary_comparator(const Column& col, Func&& func) {
    if (is_empty_type(col.type().data_type()) || is_integer_type(col.type().data_type())) {
        if constexpr (std::is_same_v<Func, IsNullOperator&&>) {
            return is_empty_type(col.type().data_type()) ? VariantData(FullResult{}) : VariantData(EmptyResult{});
        } else if constexpr (std::is_same_v<Func, NotNullOperator&&>) {
            return is_empty_type(col.type().data_type()) ? VariantData(EmptyResult{}) : VariantData(FullResult{});
        } else {
            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected operator passed to unary_comparator");
        }
    }

    auto output = std::make_shared<util::BitSet>(static_cast<util::BitSetSizeType>(col.row_count()));
    details::visit_type(col.type().data_type(), [&](auto col_desc_tag) {
        using TDT = ScalarTagType<decltype(col_desc_tag)>;
        using RawType = typename TDT::DataTypeTag::raw_type;
        auto column_data = col.data();
        util::BitSet::bulk_insert_iterator inserter(*output);
        auto pos = 0u;
        while (auto block = column_data.next<TDT>()) {
            auto ptr = reinterpret_cast<const RawType *>(block->data());
            const auto row_count = block->row_count();
            for (auto i = 0u; i < row_count; ++i, ++pos) {
                if constexpr (is_floating_point_type(TDT::DataTypeTag::data_type)) {
                    if (func.apply(*ptr++))
                        inserter = pos;
                } else if constexpr (is_sequence_type(TDT::DataTypeTag::data_type)) {
                    if (func.template apply<StringTypeTag>(*ptr++))
                        inserter = pos;
                } else if constexpr (is_time_type(TDT::DataTypeTag::data_type)) {
                    if (func.template apply<TimeTypeTag>(*ptr++))
                        inserter = pos;
                } else {
                    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Cannot perform null checks on {}", col.type());
                }
            }
        }
        inserter.flush();
    });
    return VariantData{std::move(output)};
}

template<typename Func>
VariantData visit_unary_comparator(const VariantData& left, Func&& func) {
    return std::visit(util::overload{
            [&] (const ColumnWithStrings& l) -> VariantData {
                return transform_to_placeholder(unary_comparator<decltype(func)>(*(l.column_), std::forward<decltype(func)>(func)));
            },
            [] (EmptyResult) -> VariantData {
                if constexpr (std::is_same_v<Func, IsNullOperator>) {
                    return FullResult{};
                } else if constexpr (std::is_same_v<Func, NotNullOperator>) {
                    return EmptyResult{};
                } else {
                    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected operator passed to unary_comparator");
                }
            },
            [](const auto&) -> VariantData {
                util::raise_rte("Bitset/ValueSet inputs not accepted to unary comparators");
            }
    }, left);
}

VariantData dispatch_unary(const VariantData& left, OperationType operation);

}
