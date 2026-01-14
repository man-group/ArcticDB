/*
 * Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/operation_types.hpp>
#include <arcticdb/util/bitset.hpp>

namespace arcticdb {

VariantData ternary_operator(const util::BitSet& condition, const util::BitSet& left, const util::BitSet& right);

template<bool arguments_reversed = false>
VariantData ternary_operator(const util::BitSet& condition, const util::BitSet& input_bitset, bool value);

VariantData ternary_operator(
        const util::BitSet& condition, const ColumnWithStrings& left, const ColumnWithStrings& right
);

template<bool arguments_reversed = false>
VariantData ternary_operator(const util::BitSet& condition, const ColumnWithStrings& col, const Value& val);

template<bool arguments_reversed = false>
VariantData ternary_operator(const util::BitSet& condition, const ColumnWithStrings& col, EmptyResult);

VariantData ternary_operator(const util::BitSet& condition, const Value& left, const Value& right);

template<bool arguments_reversed = false>
VariantData ternary_operator(const util::BitSet& condition, const Value& val, EmptyResult);

VariantData ternary_operator(const util::BitSet& condition, bool left, bool right);

VariantData dispatch_ternary(
        const VariantData& condition, const VariantData& left, const VariantData& right, OperationType operation
);

} // namespace arcticdb
