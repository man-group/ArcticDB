/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/column.hpp>

namespace arcticdb {

template<bool arguments_reversed>
void initialise_output_column(const util::BitSet& condition, const Column& input_column, Column& output_column) {
    util::check(&input_column != &output_column, "Cannot overwrite input column in ternary operator");
    size_t output_physical_rows;
    size_t output_logical_rows = condition.size();
    auto output_sparse_map = condition;
    if (input_column.is_sparse()) {
        if constexpr (arguments_reversed) {
            output_sparse_map |= input_column.sparse_map();
        } else {
            output_sparse_map = ~output_sparse_map | input_column.sparse_map();
        }
        output_sparse_map.resize(output_logical_rows);
        output_physical_rows = output_sparse_map.count();
        // Input column is sparse, but output column is dense
        if (output_physical_rows != output_logical_rows) {
            output_column.set_sparse_map(std::move(output_sparse_map));
        }
    } else {
        output_physical_rows = input_column.row_count();
    }
    if (output_physical_rows > 0) {
        output_column.allocate_data(output_physical_rows * get_type_size(output_column.type().data_type()));
    }
    output_column.set_row_data(output_logical_rows - 1);
}

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
void ternary_transform(const util::BitSet& condition,
                       const Column& left_input_column,
                       const Column& right_input_column,
                       Column& output_column,
                       functor&& f) {
    initialise_output_column(condition, left_input_column, right_input_column, output_column);
    // TODO: Consider optimisations
    // Use std::transform when input_column is dense
    // e.g. If the result is mostly value, then fully initialise output column to value, and then just iterate
    // true/false bits of condition as appropriate
    auto output_data = output_column.data();
    if (output_column.is_sparse()) {
        auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        for (auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>(); output_it != output_end_it; ++output_it) {
            auto idx = output_it->idx();
            output_it->value() = f(condition.get_bit(idx),
                                   static_cast<typename output_tdt::DataTypeTag::raw_type>(*left_input_column.scalar_at<typename left_input_tdt::DataTypeTag::raw_type>(idx)),
                                   static_cast<typename output_tdt::DataTypeTag::raw_type>(*right_input_column.scalar_at<typename right_input_tdt::DataTypeTag::raw_type>(idx)));
        }
    } else {
        auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED>();
        for (auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED>(); output_it != output_end_it; ++output_it) {
            auto idx = output_it->idx();
            output_it->value() = f(condition.get_bit(idx),
                                   static_cast<typename output_tdt::DataTypeTag::raw_type>(*left_input_column.scalar_at<typename left_input_tdt::DataTypeTag::raw_type>(idx)),
                                   static_cast<typename output_tdt::DataTypeTag::raw_type>(*right_input_column.scalar_at<typename right_input_tdt::DataTypeTag::raw_type>(idx)));
        }
    }
}

template <typename input_tdt, typename output_tdt, typename value_type, bool arguments_reversed>
static void ternary(const util::BitSet& condition, const Column& input_column, value_type value, Column& output_column) {
    initialise_output_column<arguments_reversed>(condition, input_column, output_column);
    // TODO: Consider optimisations
    // Use std::transform when input_column is dense
    // e.g. If the result is mostly value, then fully initialise output column to value, and then just iterate
    // true/false bits of condition as appropriate
    auto output_data = output_column.data();
    if (output_column.is_sparse()) {
        auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        for (auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>(); output_it != output_end_it; ++output_it) {
            auto idx = output_it->idx();
            if constexpr (arguments_reversed) {
                output_it->value() = condition.get_bit(idx) ?
                                     value : static_cast<typename output_tdt::DataTypeTag::raw_type>(*input_column.scalar_at<typename input_tdt::DataTypeTag::raw_type>(idx));
            } else {
                output_it->value() = condition.get_bit(idx) ?
                                     static_cast<typename output_tdt::DataTypeTag::raw_type>(*input_column.scalar_at<typename input_tdt::DataTypeTag::raw_type>(idx)) : value;
            }
        }
    } else {
        auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED>();
        for (auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED>(); output_it != output_end_it; ++output_it) {
            auto idx = output_it->idx();
            if constexpr (arguments_reversed) {
                output_it->value() = condition.get_bit(idx) ?
                                     value : static_cast<typename output_tdt::DataTypeTag::raw_type>(*input_column.scalar_at<typename input_tdt::DataTypeTag::raw_type>(idx));
            } else {
                output_it->value() = condition.get_bit(idx) ?
                                     static_cast<typename output_tdt::DataTypeTag::raw_type>(*input_column.scalar_at<typename input_tdt::DataTypeTag::raw_type>(idx)) : value;
            }
        }
    }
}

template <typename input_tdt, bool arguments_reversed>
static void ternary(const util::BitSet& condition, const Column& input_column, Column& output_column) {
    util::check(&input_column != &output_column, "Cannot overwrite input column in ternary operator");
    size_t output_physical_rows;
    size_t output_logical_rows = condition.size();
    util::BitSet output_sparse_map;
    if (input_column.is_sparse()) {
        if constexpr (arguments_reversed) {
            output_sparse_map = ~condition & input_column.sparse_map();
        } else {
            output_sparse_map = condition & input_column.sparse_map();
        }
        output_sparse_map.resize(output_logical_rows);
        output_physical_rows = output_sparse_map.count();
        // Input column is sparse, but output column is dense
        if (output_physical_rows != output_logical_rows) {
            output_column.set_sparse_map(std::move(output_sparse_map));
        }
    } else {
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
    if (output_physical_rows > 0) {
        output_column.allocate_data(output_physical_rows * get_type_size(output_column.type().data_type()));
    }
    output_column.set_row_data(output_logical_rows - 1);
    auto output_data = output_column.data();
    if (output_column.is_sparse()) {
        auto output_end_it = output_data.end<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        for (auto output_it = output_data.begin<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>(); output_it != output_end_it; ++output_it) {
            auto idx = output_it->idx();
            output_it->value() = static_cast<typename input_tdt::DataTypeTag::raw_type>(*input_column.scalar_at<typename input_tdt::DataTypeTag::raw_type>(idx));
        }
    } else {
        auto output_end_it = output_data.end<input_tdt, IteratorType::ENUMERATED>();
        for (auto output_it = output_data.begin<input_tdt, IteratorType::ENUMERATED>(); output_it != output_end_it; ++output_it) {
            auto idx = output_it->idx();
            output_it->value() = static_cast<typename input_tdt::DataTypeTag::raw_type>(*input_column.scalar_at<typename input_tdt::DataTypeTag::raw_type>(idx));
        }
    }
}

template <typename value_tdt, bool arguments_reversed>
static void ternary(const util::BitSet& condition, typename value_tdt::DataTypeTag::raw_type value, Column& output_column) {
    initialise_output_column<arguments_reversed>(condition, output_column);
    auto output_data = output_column.data();
    auto output_end_it = output_data.end<value_tdt>();
    for (auto output_it = output_data.begin<value_tdt>(); output_it != output_end_it; ++output_it) {
        *output_it = value;
    }
}

}