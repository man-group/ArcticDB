/*
 * Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/operation_dispatch_binary.hpp>

namespace arcticdb {

VariantData binary_boolean(const std::shared_ptr<util::BitSet>& left, const std::shared_ptr<util::BitSet>& right, OperationType operation) {
    util::check(left->size() == right->size(), "BitSets of different lengths ({} and {}) in binary comparator", left->size(), right->size());
    switch(operation) {
        case OperationType::AND:
            return std::make_shared<util::BitSet>(*left & *right);
        case OperationType::OR:
            return std::make_shared<util::BitSet>(*left | *right);
        case OperationType::XOR:
            return std::make_shared<util::BitSet>(*left ^ *right);
        default:
            util::raise_rte("Unexpected operator in binary_boolean {}", int(operation));
    }
}

VariantData binary_boolean(const std::shared_ptr<util::BitSet>& left, EmptyResult, OperationType operation) {
    switch(operation) {
        case OperationType::AND:
            return EmptyResult{};
        case OperationType::OR:
            return left;
        case OperationType::XOR:
            return left;
        default:
            util::raise_rte("Unexpected operator in binary_boolean {}", int(operation));
    }
}

VariantData binary_boolean(const std::shared_ptr<util::BitSet>& left, FullResult, OperationType operation) {
    switch(operation) {
        case OperationType::AND:
            return left;
        case OperationType::OR:
            return FullResult{};
        case OperationType::XOR: {
            return std::make_shared<util::BitSet>(~(*left));
        }
        default:
            util::raise_rte("Unexpected operator in binary_boolean {}", int(operation));
    }
}

VariantData binary_boolean(EmptyResult, FullResult, OperationType operation) {
    switch(operation) {
        case OperationType::AND:
            return EmptyResult{};
        case OperationType::OR:
            return FullResult{};
        case OperationType::XOR:
            return FullResult{};
        default:
            util::raise_rte("Unexpected operator in binary_boolean {}", int(operation));
    }
}

VariantData binary_boolean(FullResult, FullResult, OperationType operation) {
    switch(operation) {
        case OperationType::AND:
            return FullResult{};
        case OperationType::OR:
            return FullResult{};
        case OperationType::XOR:
            return EmptyResult{};
        default:
            util::raise_rte("Unexpected operator in binary_boolean {}", int(operation));
    }
}

VariantData binary_boolean(EmptyResult, EmptyResult, OperationType operation) {
    switch(operation) {
        case OperationType::AND:
        case OperationType::OR:
        case OperationType::XOR:
            return EmptyResult{};
        default:
            util::raise_rte("Unexpected operator in binary_boolean {}", int(operation));
    }
}

// Note that we can have fewer of these and reverse the parameters because all the operations are
// commutative, however if that were to change we would need the full set
VariantData visit_binary_boolean(const VariantData& left, const VariantData& right, OperationType operation) {
    auto left_transformed = transform_to_bitset(left);
    auto right_transformed = transform_to_bitset(right);
    return std::visit(util::overload {
            [&operation] (const std::shared_ptr<util::BitSet>& l, const std::shared_ptr<util::BitSet>& r) {
                return transform_to_placeholder(binary_boolean(l, r, operation));
            },
            [&operation] (const std::shared_ptr<util::BitSet>& l, EmptyResult r) {
                return transform_to_placeholder(binary_boolean(l, r, operation));
            },
            [&operation] (const std::shared_ptr<util::BitSet>& l, FullResult r) {
                return transform_to_placeholder(binary_boolean(l, r, operation));
            },
            [&operation] (EmptyResult l, const std::shared_ptr<util::BitSet>& r) {
                return binary_boolean(r, l, operation);
            },
            [&operation] (FullResult l, const std::shared_ptr<util::BitSet>& r) {
                return transform_to_placeholder(binary_boolean(r, l, operation));
            },
            [&operation] (FullResult l, EmptyResult r) {
                return binary_boolean(r, l, operation);
            },
            [&operation] (EmptyResult l, FullResult r) {
                return binary_boolean(l, r, operation);
            },
            [&operation] (FullResult l, FullResult r) {
                return binary_boolean(l, r, operation);
            },
            [&operation] (EmptyResult l, EmptyResult r) {
                return binary_boolean(r, l, operation);
            },
            [](const auto &, const auto&) -> VariantData {
                util::raise_rte("Value/ValueSet/non-bool column inputs not accepted to binary boolean");
            }
    }, left_transformed, right_transformed);
}

VariantData dispatch_binary(const VariantData& left, const VariantData& right, OperationType operation) {
    switch(operation) {
        case OperationType::ADD:
            return visit_binary_operator(left, right, PlusOperator{});
        case OperationType::SUB:
            return visit_binary_operator(left, right, MinusOperator{});
        case OperationType::MUL:
            return visit_binary_operator(left, right, TimesOperator{});
        case OperationType::DIV:
            return visit_binary_operator(left, right, DivideOperator{});
        case OperationType::EQ:
            return visit_binary_comparator(left, right, EqualsOperator{});
        case OperationType::NE:
            return visit_binary_comparator(left, right, NotEqualsOperator{});
        case OperationType::LT:
            return visit_binary_comparator(left, right, LessThanOperator{});
        case OperationType::LE:
            return visit_binary_comparator(left, right, LessThanEqualsOperator{});
        case OperationType::GT:
            return visit_binary_comparator(left, right, GreaterThanOperator{});
        case OperationType::GE:
            return visit_binary_comparator(left, right, GreaterThanEqualsOperator{});
        case OperationType::ISIN:
            return visit_binary_membership(left, right, IsInOperator{});
        case OperationType::ISNOTIN:
            return visit_binary_membership(left, right, IsNotInOperator{});
        case OperationType::AND:
        case OperationType::OR:
        case OperationType::XOR:
            return visit_binary_boolean(left, right, operation);
        default:
            util::raise_rte("Unknown operation {}", int(operation));
    }
}

}
