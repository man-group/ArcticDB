/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <variant>
#include <memory>
#include <stack>
#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/pipeline/processors.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <folly/Poly.h>

namespace arcticdb {

struct PlusOperator {
    template<typename T, typename U, typename V>
    static V go(T t, U u) {
        return t + u;
    }
};

struct MinusOperator {
    template<typename T, typename U, typename V>
    static V go(T t, U u) {
        return t - u;
    }
};

struct TimesOperator {
    template<typename T, typename U, typename V>
    static V go(T t, U u) {
        if constexpr (std::is_same_v<V, bool>) {
            return t && u;
        } else {
            return t * u;
        }
    }
};

struct IIterable {
    template<class Base>
    struct Interface : Base {
        bool finished() const { return folly::poly_call<0>(*this); }
        uint8_t* value() const { return folly::poly_call<1>(*this); }
        void next() { folly::poly_call<2>(*this); }
    };

    template<class T>
    using Members = folly::PolyMembers<&T::finished, &T::value, &T::next>;
};

using Iterable = folly::Poly<IIterable>;

template<typename T, typename U, typename V, typename Operator>
struct CombiningIterator {
    CombiningIterator(Iterable l_pos, Iterable r_pos_) : l_pos_(l_pos), r_pos_(r_pos_) {}

    bool finished() const { return l_pos_.finished() || r_pos_.finished(); }

    uint8_t* value() const {
        auto left_val = *reinterpret_cast<const T*>(l_pos_.value());
        auto right_val = *reinterpret_cast<const U*>(r_pos_.value());
        value_ = Operator::template go<T, U, V>(left_val, right_val);
        return reinterpret_cast<uint8_t*>(&value_);
    }

    void next() {
        l_pos_.next();
        r_pos_.next();
    }

    mutable V value_;
    Iterable l_pos_;
    Iterable r_pos_;
};

struct IIterableContainer {
    template<class Base>
    struct Interface : Base {
        Iterable get_iterator() const { return folly::poly_call<0>(*this); }
        TypeDescriptor type() const { return folly::poly_call<1>(*this); }
    };

    template<class T>
    using Members = folly::PolyMembers<&T::get_iterator, &T::type>;
};

using IterableContainer = folly::Poly<IIterableContainer&>;

template<typename OperatorType>
struct ColumnExpression {
    TypeDescriptor type() { return type_; }

    ColumnExpression(const IterableContainer& left, const IterableContainer& right) : left_(left), right_(right) {
        auto promoted_type = has_valid_common_type(left_->type(), right_->type());
        util::check(
                promoted_type,
                "Cannot promote from type {} and type {} in column expression",
                left_->type(),
                right_->type()
        );
        type_ = promoted_type.value();
    }

    Iterable get_iterator() {
        return details::visit_type(left_->type().data_type, [&](auto left_dtt) {
            using left_raw_type = typename decltype(left_dtt)::raw_type;

            return details::visit_type(right_->type().data_type, [&](auto right_dtt) {
                using right_raw_type = typename decltype(right_dtt)::raw_type;

                return details::visit_type(type().data_type, [&](auto target_dtt) {
                    using target_raw_type = typename decltype(target_dtt)::raw_type;

                    return Iterable{CombiningIterator<left_raw_type, right_raw_type, target_raw_type, OperatorType>(
                            left_->get_iterator(), right_->get_iterator()
                    )};
                });
            });
        });
    }

    IterableContainer left_;
    IterableContainer right_;
    TypeDescriptor type_;
};

struct StreamFilter {
    void go(std::shared_ptr<pipelines::PipelineContext> /*context*/) {}
};

struct StreamAggregation {
    void go(std::shared_ptr<pipelines::PipelineContext> /*context*/) {}
};

struct StreamProjection {
    void go(std::shared_ptr<pipelines::PipelineContext> /*context*/) {}
};

using Operation = std::variant<StreamFilter, StreamAggregation, StreamProjection>;

struct ProcessingNode {
    Operation operation_;
    std::shared_ptr<ProcessingNode> left_;
    std::shared_ptr<ProcessingNode> right_;
};

void postorder_traverse(std::shared_ptr<ProcessingNode> root, std::shared_ptr<pipelines::PipelineContext> context) {
    if (!root)
        return;

    std::stack<std::shared_ptr<ProcessingNode>> stack, result;
    stack.push(root);

    while (!stack.empty() == false) {
        auto node = stack.top();
        stack.pop();
        result.push(node);

        if (node->right_)
            stack.push(node->right_);
        if (node->left_)
            stack.push(node->left_);
    }

    while (!result.empty()) {
        auto curr = result.top();
        result.pop();
        util::variant_match(curr->operation_, [&context](auto& op) { op.go(context); });
    }
}

} // namespace arcticdb