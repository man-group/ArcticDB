/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/column.hpp>

namespace arcticdb {

void initialise_output_column(const util::BitSet& condition,
                              const Column& left_input_column,
                              const Column& right_input_column,
                              Column& output_column) {
    util::check(&left_input_column != &output_column && &right_input_column != &output_column,
                "Cannot overwrite input column in ternary operator");
    util::check(left_input_column.last_row() == right_input_column.last_row(), "Mismatching column lengths in ternary operator");
    size_t output_logical_rows = condition.size();
    util::BitSet output_sparse_map;
    if (left_input_column.is_sparse() && right_input_column.is_sparse()) {
        output_sparse_map = (condition & left_input_column.sparse_map()) | ((~condition) & right_input_column.sparse_map());
    } else if (left_input_column.is_sparse()) {
        // right_input_column is dense
        output_sparse_map = (condition & left_input_column.sparse_map()) | ~condition;
    } else if (right_input_column.is_sparse()) {
        // left_input_column is dense
        output_sparse_map = (~condition & right_input_column.sparse_map()) | condition;
    } else {
        // Both input columns are dense
        // Bit vectors default initialise all bits to zero
        output_sparse_map.flip();
    }
    output_sparse_map.resize(output_logical_rows);
    auto output_physical_rows = output_sparse_map.count();
    // Input columns may have been sparse, but output column is dense
    if (output_physical_rows != output_logical_rows) {
        output_column.set_sparse_map(std::move(output_sparse_map));
    }
    if (output_physical_rows > 0) {
        output_column.allocate_data(output_physical_rows * get_type_size(output_column.type().data_type()));
    }
    output_column.set_row_data(output_logical_rows - 1);
}

template<typename FullOrEmpty, bool arguments_reversed>
requires std::is_same_v<FullOrEmpty, FullResult> || std::is_same_v<FullOrEmpty, EmptyResult>
void initialise_output_column(const util::BitSet& condition,
                              const Column& input_column,
                              Column& output_column) {
    util::check(&input_column != &output_column, "Cannot overwrite input column in ternary operator");
    size_t output_physical_rows;
    size_t output_logical_rows = condition.size();
    util::BitSet output_sparse_map;
    if (input_column.is_sparse()) {
        if constexpr (std::is_same_v<FullOrEmpty, FullResult>) {
            if constexpr (arguments_reversed) {
                output_sparse_map = condition | input_column.sparse_map();
            } else {
                output_sparse_map = ~condition | input_column.sparse_map();
            }
        } else {
            // EmptyResult
            if constexpr (arguments_reversed) {
                output_sparse_map = ~condition & input_column.sparse_map();
            } else {
                output_sparse_map = condition & input_column.sparse_map();
            }
        }
        output_sparse_map.resize(output_logical_rows);
        output_physical_rows = output_sparse_map.count();
        // Input column is sparse, but output column is dense
        if (output_physical_rows != output_logical_rows) {
            output_column.set_sparse_map(std::move(output_sparse_map));
        }
    } else {
        if constexpr (std::is_same_v<FullOrEmpty, FullResult>) {
            output_physical_rows = input_column.row_count();
        } else {
            // EmptyResult
            if constexpr (arguments_reversed) {
                output_physical_rows = output_logical_rows - condition.count();
            } else {
                output_physical_rows = condition.count();
            }
            if (output_physical_rows != output_logical_rows) {
                if constexpr (arguments_reversed) {
                    output_sparse_map = ~condition;
                } else {
                    output_sparse_map = condition;
                }
                output_sparse_map.resize(output_logical_rows);
                output_column.set_sparse_map(std::move(output_sparse_map));
            }
        }
    }
    if (output_physical_rows > 0) {
        output_column.allocate_data(output_physical_rows * get_type_size(output_column.type().data_type()));
    }
    output_column.set_row_data(output_logical_rows - 1);
}

template<bool arguments_reversed>
void initialise_output_column(const util::BitSet& condition, Column& output_column) {
    size_t output_physical_rows;
    size_t output_logical_rows = condition.size();
    auto output_sparse_map = condition;
    if constexpr (arguments_reversed) {
        output_sparse_map.flip();
        output_sparse_map.resize(condition.size());
    }
    output_physical_rows = output_sparse_map.count();
    if (output_physical_rows != output_logical_rows) {
        output_column.set_sparse_map(std::move(output_sparse_map));
    }
    if (output_physical_rows > 0) {
        output_column.allocate_data(output_physical_rows * get_type_size(output_column.type().data_type()));
    }
    output_column.set_row_data(output_logical_rows - 1);
}

template <typename left_input_tdt, typename right_input_tdt, typename output_tdt, typename functor>
requires std::is_invocable_r_v<
        typename output_tdt::DataTypeTag::raw_type,
        functor,
        bool,
        typename output_tdt::DataTypeTag::raw_type,
        typename output_tdt::DataTypeTag::raw_type>
void ternary_transform(const util::BitSet& condition,
                       const Column& left_input_column,
                       const Column& right_input_column,
                       Column& output_column,
                       functor&& f) {

    auto left_percentage = static_cast<double>(condition.count()) / static_cast<double>(condition.size());
    if (!left_input_column.is_sparse() && !right_input_column.is_sparse() &&
        ((left_percentage >= 0.5 && left_input_column.type() == output_column.type()) ||
        (left_percentage < 0.5 && right_input_column.type() == output_column.type()))) {
        output_column = left_percentage >= 0.5 ? left_input_column.clone() : right_input_column.clone();
        auto output_data = output_column.data();
        auto output_accessor = random_accessor<output_tdt>(&output_data);
        if (left_percentage >= 0.5) {
            auto final_condition = ~condition;
            final_condition.resize(condition.size());
            final_condition.optimize();
            auto right_data = right_input_column.data();
            auto right_accessor = random_accessor<right_input_tdt>(&right_data);
            auto end_bit = final_condition.end();
            for (auto set_bit = final_condition.first(); set_bit < end_bit; ++set_bit) {
                output_accessor.set(*set_bit, right_accessor.at(*set_bit));
            }
        } else {
            auto final_condition = condition;
            final_condition.optimize();
            auto left_data = left_input_column.data();
            auto left_accessor = random_accessor<left_input_tdt>(&left_data);
            auto end_bit = condition.end();
            for (auto set_bit = condition.first(); set_bit < end_bit; ++set_bit) {
                output_accessor.set(*set_bit, left_accessor.at(*set_bit));
            }
        }
        // Both inputs dense, implying output is also dense
//        auto left_data = left_input_column.data();
//        auto left_it = left_data.cbegin<left_input_tdt>();
//        const auto left_end_it = left_data.cend<left_input_tdt>();
//        auto right_data = right_input_column.data();
//        auto right_it = right_data.cbegin<right_input_tdt>();
//        const auto right_end_it = right_data.cend<right_input_tdt>();
//        auto output_it = output_data.begin<output_tdt>();
//        const auto output_end_it = output_data.end<output_tdt>();
//        for (size_t idx = 0;
//             left_it != left_end_it && right_it != right_end_it && output_it != output_end_it;
//             ++idx, ++left_it, ++right_it, ++output_it) {
//            *output_it = f(condition.get_bit(idx), *left_it, *right_it);
//        }
    } else {
        initialise_output_column(condition, left_input_column, right_input_column, output_column);
        auto output_data = output_column.data();
        // TODO: Consider optimisations
        // Use std::transform when input_column is dense
        // e.g. If the result is mostly value, then fully initialise output column to value, and then just iterate
        // true/false bits of condition as appropriate
        if (output_column.is_sparse()) {
            auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
            for (auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
                 output_it != output_end_it; ++output_it) {
                auto idx = output_it->idx();
                output_it->value() = f(condition.get_bit(idx),
                                       *left_input_column.scalar_at<typename left_input_tdt::DataTypeTag::raw_type>(
                                               idx),
                                       *right_input_column.scalar_at<typename right_input_tdt::DataTypeTag::raw_type>(
                                               idx));
            }
        } else {
            auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED>();
            for (auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED>();
                 output_it != output_end_it; ++output_it) {
                auto idx = output_it->idx();
                output_it->value() = f(condition.get_bit(idx),
                                       *left_input_column.scalar_at<typename left_input_tdt::DataTypeTag::raw_type>(
                                               idx),
                                       *right_input_column.scalar_at<typename right_input_tdt::DataTypeTag::raw_type>(
                                               idx));
            }
        }
    }
}

template <typename input_tdt, typename output_tdt, typename value_type, bool arguments_reversed, typename functor>
requires std::is_invocable_r_v<
        typename output_tdt::DataTypeTag::raw_type,
        functor,
        bool,
        typename output_tdt::DataTypeTag::raw_type,
        value_type>
void ternary_transform(const util::BitSet& condition,
                       const Column& input_column,
                       value_type value,
                       Column& output_column,
                       functor&& f) {
    initialise_output_column<FullResult, arguments_reversed>(condition, input_column, output_column);
    // TODO: Consider optimisations
    // Use std::transform when input_column is dense
    // e.g. If the result is mostly value, then fully initialise output column to value, and then just iterate
    // true/false bits of condition as appropriate
    auto output_data = output_column.data();
    if (output_column.is_sparse()) {
        auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        for (auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>(); output_it != output_end_it; ++output_it) {
            auto idx = output_it->idx();
            output_it->value() = f(condition.get_bit(idx), *input_column.scalar_at<typename input_tdt::DataTypeTag::raw_type>(idx), value);
        }
    } else {
        auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED>();
        for (auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED>(); output_it != output_end_it; ++output_it) {
            auto idx = output_it->idx();
            output_it->value() = f(condition.get_bit(idx), *input_column.scalar_at<typename input_tdt::DataTypeTag::raw_type>(idx), value);
        }
    }
}

template <typename input_tdt, bool arguments_reversed, typename functor>
requires std::is_invocable_r_v<
        typename input_tdt::DataTypeTag::raw_type,
        functor,
        typename input_tdt::DataTypeTag::raw_type>
void ternary_transform(const util::BitSet& condition,
                       const Column& input_column,
                       ARCTICDB_UNUSED EmptyResult empty_result,
                       Column& output_column,
                       functor&& f) {
    initialise_output_column<EmptyResult, arguments_reversed>(condition, input_column, output_column);
    auto output_data = output_column.data();
    if (output_column.is_sparse()) {
        auto output_end_it = output_data.end<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        for (auto output_it = output_data.begin<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>(); output_it != output_end_it; ++output_it) {
            auto idx = output_it->idx();
            output_it->value() = f(*input_column.scalar_at<typename input_tdt::DataTypeTag::raw_type>(idx));
        }
    } else {
        auto output_end_it = output_data.end<input_tdt, IteratorType::ENUMERATED>();
        for (auto output_it = output_data.begin<input_tdt, IteratorType::ENUMERATED>(); output_it != output_end_it; ++output_it) {
            auto idx = output_it->idx();
            output_it->value() = f(*input_column.scalar_at<typename input_tdt::DataTypeTag::raw_type>(idx));
        }
    }
}

template<typename output_tdt>
void ternary_transform(const util::BitSet& condition,
                       typename output_tdt::DataTypeTag::raw_type left_val,
                       typename output_tdt::DataTypeTag::raw_type right_val,
                       Column& output_column) {
    auto output_rows = condition.size();
    if (output_rows > 0) {
        output_column.allocate_data(output_rows * get_type_size(output_column.type().data_type()));
    }
    output_column.set_row_data(output_rows - 1);
    auto output_data = output_column.data();
    auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED>();
    for (auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED>(); output_it != output_end_it; ++output_it) {
        auto idx = output_it->idx();
        output_it->value() = condition[idx] ? left_val : right_val;
    }
}

template <typename value_tdt, bool arguments_reversed>
void ternary_transform(const util::BitSet& condition,
                       typename value_tdt::DataTypeTag::raw_type value,
                       ARCTICDB_UNUSED EmptyResult empty_result,
                       Column& output_column) {
    initialise_output_column<arguments_reversed>(condition, output_column);
    auto output_data = output_column.data();
    auto output_end_it = output_data.end<value_tdt>();
    for (auto output_it = output_data.begin<value_tdt>(); output_it != output_end_it; ++output_it) {
        *output_it = value;
    }
}

}