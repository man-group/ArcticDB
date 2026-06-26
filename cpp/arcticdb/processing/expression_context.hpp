/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/pipeline/value_set.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/variant.hpp>

namespace arcticdb {

// ColumnName refers to a real data column and must never be prefixed. The other leaf kinds name
// entries in an ExpressionContext's maps, so they are prefixed to match a merge of expression
// contexts that has `prefix`.
inline VariantNode prefix_internal_names(const VariantNode& node, const std::string& prefix) {
    return util::variant_match(
            node,
            [](std::monostate empty) -> VariantNode { return empty; },
            [](const ColumnName& column_name) -> VariantNode { return column_name; },
            [&](const ValueName& value_name) -> VariantNode { return ValueName{prefix + value_name.value}; },
            [&](const ValueSetName& value_set_name) -> VariantNode {
                return ValueSetName{prefix + value_set_name.value};
            },
            [&](const ExpressionName& expression_name) -> VariantNode {
                return ExpressionName{prefix + expression_name.value};
            },
            [&](const RegexName& regex_name) -> VariantNode { return RegexName{prefix + regex_name.value}; }
    );
}

/*
 * Contains an expression tree as a map of string node names to nodes. Nodes contain leaves which may be references
 * to node names.
 *
 * ExpressionContext also contains the name of the root node as well as a map of node names to constant values.
 *
 * This class effectively contains as AST - there is likely a frontend implementation in a higher level language (for
 * example Python).
 */
struct ExpressionContext {
    ExpressionContext() = default;

    ARCTICDB_MOVE_COPY_DEFAULT(ExpressionContext)

    template<class T>
    class ConstantMap {
        std::unordered_map<std::string, std::shared_ptr<T>> map_;

      public:
        void set_value(const std::string& name, std::shared_ptr<T> val) { map_.try_emplace(name, val); }
        std::shared_ptr<T> get_value(const std::string& name) const { return map_.at(name); }
        bool contains(const std::string& name) const { return map_.contains(name); }

        template<class Func>
        void for_each(Func&& func) const {
            for (const auto& [name, val] : map_) {
                func(name, val);
            }
        }
    };

    void add_expression_node(const std::string& name, std::shared_ptr<ExpressionNode> expression_node) {
        expression_nodes_.set_value(name, std::move(expression_node));
    }

    void add_value(const std::string& name, std::shared_ptr<Value> value) { values_.set_value(name, std::move(value)); }

    void add_value_set(const std::string& name, std::shared_ptr<ValueSet> value_set) {
        value_sets_.set_value(name, std::move(value_set));
    }

    void add_regex(const std::string& name, std::shared_ptr<util::RegexGeneric> regex) {
        regex_matches_.set_value(name, std::move(regex));
    }

    // Merge another context's entries into this one, prefixing every name so that entries from
    // separate contexts cannot collide.
    void merge_from(const ExpressionContext& other, const std::string& prefix) {
        other.values_.for_each([&](const std::string& name, const auto& val) { values_.set_value(prefix + name, val); }
        );
        other.value_sets_.for_each([&](const std::string& name, const auto& val) {
            value_sets_.set_value(prefix + name, val);
        });
        other.regex_matches_.for_each([&](const std::string& name, const auto& val) {
            regex_matches_.set_value(prefix + name, val);
        });
        other.expression_nodes_.for_each([&](const std::string& name, const auto& node) {
            auto prefixed = std::make_shared<ExpressionNode>(*node);
            prefixed->condition_ = prefix_internal_names(prefixed->condition_, prefix);
            prefixed->left_ = prefix_internal_names(prefixed->left_, prefix);
            prefixed->right_ = prefix_internal_names(prefixed->right_, prefix);
            util::check(
                    !expression_nodes_.contains(prefix + name),
                    "Prefixed expression node {} already present",
                    prefix + name
            );
            add_expression_node(prefix + name, std::move(prefixed));
        });
    }

    ConstantMap<ExpressionNode> expression_nodes_;
    ConstantMap<Value> values_;
    ConstantMap<ValueSet> value_sets_;
    ConstantMap<util::RegexGeneric> regex_matches_;
    VariantNode root_node_name_;
    bool dynamic_schema_{false};
};

} // namespace arcticdb
