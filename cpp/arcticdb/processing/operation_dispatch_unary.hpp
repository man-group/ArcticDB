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
        using DataTagType =  decltype(val_desc_tag);
        if constexpr (!is_numeric_type(DataTagType::data_type)) {
            util::raise_rte("Cannot perform arithmetic on {}", val.type());
        }
        using DT = TypeDescriptorTag<decltype(val_desc_tag), DimensionTag<Dimension::Dim0>>;
        using RawType = typename DT::DataTypeTag::raw_type;
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
        using ColumnTagType =  decltype(col_desc_tag);
        if constexpr (!is_numeric_type(ColumnTagType::data_type)) {
            util::raise_rte("Cannot perform arithmetic on {}", col.type());
        }
        using DT = TypeDescriptorTag<decltype(col_desc_tag), DimensionTag<Dimension::Dim0>>;
        using RawType = typename DT::DataTypeTag::raw_type;
        using TargetType = typename unary_arithmetic_promoted_type<RawType, std::remove_reference_t<Func>>::type;
        auto output_data_type = data_type_from_raw_type<TargetType>();
        output = std::make_unique<Column>(make_scalar_type(output_data_type), col.is_sparse());
        auto data = col.data();
        while(auto opt_block = data.next<DT>()) {
            auto block = opt_block.value();
            const auto nbytes = sizeof(TargetType) * block.row_count();
            auto ptr = reinterpret_cast<TargetType*>(output->allocate_data(nbytes));
            for(auto idx = 0u; idx < block.row_count(); ++idx)
                *ptr++ = func.apply(block[idx]);

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

VariantData dispatch_unary(const VariantData& left, OperationType operation);

}
