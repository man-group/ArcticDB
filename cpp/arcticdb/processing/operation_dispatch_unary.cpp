/*
 * Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/operation_dispatch_unary.hpp>

namespace arcticdb {

VariantData unary_boolean(const util::BitSet& bitset, OperationType operation) {
    switch (operation) {
    case OperationType::IDENTITY:
        return bitset;
    case OperationType::NOT: {
        auto res = ~bitset;
        res.resize(bitset.size());
        return res;
    }
    default:
        util::raise_rte("Unexpected operator in unary_boolean {}", int(operation));
    }
}

VariantData unary_boolean(EmptyResult, OperationType operation) {
    switch (operation) {
    case OperationType::IDENTITY:
        return EmptyResult{};
    case OperationType::NOT:
        return FullResult{};
    default:
        util::raise_rte("Unexpected operator in unary_boolean {}", int(operation));
    }
}

VariantData unary_boolean(FullResult, OperationType operation) {
    switch (operation) {
    case OperationType::IDENTITY:
        return FullResult{};
    case OperationType::NOT:
        return EmptyResult{};
    default:
        util::raise_rte("Unexpected operator in unary_boolean {}", int(operation));
    }
}

VariantData visit_unary_boolean(const VariantData& left, OperationType operation) {
    auto data = transform_to_bitset(left);
    return std::visit(
            util::overload{
                    [operation](const util::BitSet& d) -> VariantData {
                        return transform_to_placeholder(unary_boolean(d, operation));
                    },
                    [operation](EmptyResult d) { return transform_to_placeholder(unary_boolean(d, operation)); },
                    [operation](FullResult d) { return transform_to_placeholder(unary_boolean(d, operation)); },
                    [](const auto&) -> VariantData {
                        util::raise_rte("Value/ValueSet/non-bool column inputs not accepted to unary boolean");
                    }
            },
            data
    );
}

VariantData dispatch_unary(const VariantData& left, OperationType operation) {
    switch (operation) {
    case OperationType::ABS:
        return visit_unary_operator(left, AbsOperator());
    case OperationType::NEG:
        return visit_unary_operator(left, NegOperator());
    case OperationType::ISNULL:
        return visit_unary_comparator(left, IsNullOperator());
    case OperationType::NOTNULL:
        return visit_unary_comparator(left, NotNullOperator());
    case OperationType::IDENTITY:
    case OperationType::NOT:
        return visit_unary_boolean(left, operation);
    default:
        util::raise_rte("Unknown operation {}", int(operation));
    }
}

} // namespace arcticdb
