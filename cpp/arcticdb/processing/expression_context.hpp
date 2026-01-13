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

namespace arcticdb {

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
        void set_value(std::string name, std::shared_ptr<T> val) { map_.try_emplace(name, val); }
        std::shared_ptr<T> get_value(std::string name) const { return map_.at(name); }
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

    ConstantMap<ExpressionNode> expression_nodes_;
    ConstantMap<Value> values_;
    ConstantMap<ValueSet> value_sets_;
    ConstantMap<util::RegexGeneric> regex_matches_;
    VariantNode root_node_name_;
    bool dynamic_schema_{false};
};

} // namespace arcticdb
