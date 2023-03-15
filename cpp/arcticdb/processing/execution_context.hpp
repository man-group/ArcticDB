/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/pipeline/value_set.hpp>

namespace arcticdb {

/*
 * Contains an expression tree as a map of string node names to nodes. Nodes contain leaves which may be references
 * to node names.
 *
 * ExecutionContext also contains the name of the root node as well as a map of node names to constant values.
 *
 * This class effectively contains as AST - there is likely a frontend implementation in a higher level language (for
 * example Python).
 */
struct ExecutionContext {
    ExecutionContext() = default;
    using NormMetaDescriptor = std::shared_ptr<arcticdb::proto::descriptors::NormalizationMetadata>;

    ARCTICDB_MOVE_ONLY_DEFAULT(ExecutionContext)

    template <class T>
    class ConstantMap {
        std::unordered_map<std::string, std::shared_ptr<T>> map_;
    public:
        void set_value(std::string name, std::shared_ptr<T> val) {
            map_.try_emplace(name, val);
        }
        std::shared_ptr<T> get_value(std::string name) {
            return map_.at(name);
        }
    };
    enum class Optimisation : uint8_t {
        SPEED,
        MEMORY
    };
    void add_column(const std::string& column_name) {
        columns_.emplace(column_name);
    }

    void add_expression_node(const std::string& name, std::shared_ptr<ExpressionNode> expression_node) {
        expression_nodes_.set_value(name, std::move(expression_node));
    }

    void set_descriptor(const StreamDescriptor& desc) {
        output_descriptor_ = desc;
    }

    void set_norm_meta_descriptor(NormMetaDescriptor& desc) {
        norm_meta_ = desc;
    }

    NormMetaDescriptor get_norm_meta_descriptor() {
        return norm_meta_;
    }

    void add_value(const std::string& name, std::shared_ptr<Value> value) {
        values_.set_value(name, std::move(value));
    }

    void add_value_set(const std::string& name, std::shared_ptr<ValueSet> value_set) {
        value_sets_.set_value(name, std::move(value_set));
    }

    void check_output_column(const std::string& name, DataType type) {
        std::scoped_lock lock{*column_mutex_};
        auto it = output_columns_.find(name);
        if(it == std::end(output_columns_)) {
            output_columns_.try_emplace(name, type);
            output_descriptor_->add_field(scalar_field_proto(type, name));
            //TODO check type compatibility in dynamic schema
        }
    }

    bool dynamic_schema() const {
        return dynamic_schema_;
    }

    void set_dynamic_schema(bool dynamic_schema) {
        dynamic_schema_ = dynamic_schema;
    }

    std::unordered_set<std::string> columns_;
    ConstantMap<ExpressionNode> expression_nodes_;
    ConstantMap<Value> values_;
    ConstantMap<ValueSet> value_sets_;
    ExpressionName root_node_name_;
    Optimisation optimisation_ = Optimisation::SPEED;
    // TODO (AN-469): This class should be immutable (and possibly just incorporated into Clause, as there is always
    //  exactly one execution context per clause). Any mutable state that needs to be maintained as a ProcessingSegment
    //  moves through the processing pipeline should be stored somewhere else (possibly on the ProcessingSegment itself)
    std::optional<StreamDescriptor> output_descriptor_;
    std::optional<StreamDescriptor> orig_output_descriptor_;
    NormMetaDescriptor norm_meta_;
    std::unordered_map<std::string, DataType> output_columns_;
    std::unique_ptr<std::mutex> column_mutex_ = std::make_unique<std::mutex>();
    std::unique_ptr<std::mutex> name_index_mutex_ = std::make_unique<std::mutex>();
    bool dynamic_schema_ = false;
};

}//namespace arcticdb
