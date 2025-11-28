/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <arcticdb/column_store/column.hpp>

namespace arcticdb {

// Calculates the number of physical rows in the output column and allocates memory for this
// If this is not equal to the number of logical rows in the output column, also set the sparse map based on the input
// condition, and the sparse maps of the input columns
void initialise_output_column(
        const util::BitSet& condition, const Column& left_input_column, const Column& right_input_column,
        Column& output_column
) {
    util::check(
            &left_input_column != &output_column && &right_input_column != &output_column,
            "Cannot overwrite input column in ternary operator"
    );
    util::check(
            left_input_column.last_row() == right_input_column.last_row(),
            "Mismatching column lengths in ternary operator"
    );
    size_t output_logical_rows = condition.size();
    util::BitSet output_sparse_map;
    if (left_input_column.is_sparse() && right_input_column.is_sparse()) {
        output_sparse_map =
                (condition & left_input_column.sparse_map()) | ((~condition) & right_input_column.sparse_map());
    } else if (left_input_column.is_sparse()) {
        // right_input_column is dense
        output_sparse_map = (condition & left_input_column.sparse_map()) | ~condition;
    } else if (right_input_column.is_sparse()) {
        // left_input_column is dense
        output_sparse_map = (~condition & right_input_column.sparse_map()) | condition;
    } else {
        // Both input columns are dense
        // Bit vectors default initialise all bits to zero
        output_sparse_map.set();
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

// As above, but with a single input column
// The FullOrEmpty template parameter controls whether rows where the condition is false should be populated or not
// e.g. when choosing between a column and a value, this will be FullResult, whereas when choosing between a column
// and another column that is missing from this row-slice with dynamic schema, this will be EmptyResult
template<typename FullOrEmpty>
requires std::same_as<FullOrEmpty, FullResult> || std::same_as<FullOrEmpty, EmptyResult>
void initialise_output_column(const util::BitSet& condition, const Column& input_column, Column& output_column) {
    util::check(&input_column != &output_column, "Cannot overwrite input column in ternary operator");
    size_t output_physical_rows;
    size_t output_logical_rows = condition.size();
    util::BitSet output_sparse_map;
    if (input_column.is_sparse()) {
        if constexpr (std::is_same_v<FullOrEmpty, FullResult>) {
            output_sparse_map = ~condition | input_column.sparse_map();
        } else {
            // EmptyResult
            output_sparse_map = condition & input_column.sparse_map();
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
            output_physical_rows = condition.count();
            if (output_physical_rows != output_logical_rows) {
                output_sparse_map = condition;
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

// As above, but for the special case where the choosing is between a value, and a column that is missing from this
// row-slice with dynamic schema
void initialise_output_column(const util::BitSet& condition, Column& output_column) {
    size_t output_physical_rows;
    size_t output_logical_rows = condition.size();
    auto output_sparse_map = condition;
    output_physical_rows = output_sparse_map.count();
    if (output_physical_rows != output_logical_rows) {
        output_column.set_sparse_map(std::move(output_sparse_map));
    }
    if (output_physical_rows > 0) {
        output_column.allocate_data(output_physical_rows * get_type_size(output_column.type().data_type()));
    }
    output_column.set_row_data(output_logical_rows - 1);
}

template<typename left_input_tdt, typename right_input_tdt, typename output_tdt, typename functor>
requires std::is_invocable_r_v<
        typename output_tdt::DataTypeTag::raw_type, functor, bool, typename output_tdt::DataTypeTag::raw_type,
        typename output_tdt::DataTypeTag::raw_type>
void ternary_transform(
        const util::BitSet& condition, const Column& left_input_column, const Column& right_input_column,
        Column& output_column, functor&& f
) {
    initialise_output_column(condition, left_input_column, right_input_column, output_column);
    auto left_data = left_input_column.data();
    auto right_data = right_input_column.data();
    auto output_data = output_column.data();
    // By construction, where the output column has a value (in the sparse sense), if the condition at this index is
    // true, then the left input column must have a value at this index as well. Similarly, if the condition is false,
    // then the right input column must have a value at this index. All of the loops below take advantage of this fact,
    // by using the efficient forward iterators over the input columns, and running them forwards until the required
    // index is found.
    // ColumnDataRandomAccessor was experimented with, but found to be x5 slower in the "middle" case of 50% on bits in
    // all of condition, left_input_column.sparse_map(), and right_input_column.sparse_map()
    // A possible future optimisation would be to check the counts in these bitsets, as well as in the output column's
    // sparse map (if present), and to switch to more efficient implementations depending on the situation.
    // loop is not used in cases where there are more efficient options
    auto loop = [&condition, f = std::forward<functor>(f)]<typename L, typename R, typename O>(
                        L left_it, R right_it, O output_it, const O output_end_it
                ) {
        for (; output_it != output_end_it; ++output_it) {
            const auto idx = output_it->idx();
            if (condition.get_bit(idx)) {
                while (left_it->idx() != idx) {
                    ++left_it;
                }
                output_it->value() = f(true, left_it->value(), {});
            } else {
                while (right_it->idx() != idx) {
                    ++right_it;
                }
                output_it->value() = f(false, {}, right_it->value());
            }
        }
    };
    if (!left_input_column.is_sparse() && !right_input_column.is_sparse()) {
        // Both inputs dense, implying output is also dense
        auto left_it = left_data.cbegin<left_input_tdt>();
        auto right_it = right_data.cbegin<right_input_tdt>();
        auto output_it = output_data.begin<output_tdt>();
        for (size_t idx = 0; idx < condition.size(); ++idx, ++left_it, ++right_it, ++output_it) {
            *output_it = f(condition.get_bit(idx), *left_it, *right_it);
        }
    } else if (left_input_column.is_sparse() && right_input_column.is_sparse()) {
        // Both inputs sparse
        auto left_it = left_data.cbegin<left_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        auto right_it = right_data.cbegin<right_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        if (output_column.is_sparse()) {
            auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
            const auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
            loop(left_it, right_it, output_it, output_end_it);
        } else {
            // It is possible that both input columns are sparse, but the output column is dense due to condition
            // selecting only on bits from each column
            auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED>();
            const auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED>();
            loop(left_it, right_it, output_it, output_end_it);
        }
    } else if (left_input_column.is_sparse()) {
        // Left is sparse, right is dense
        auto left_it = left_data.cbegin<left_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        if (output_column.is_sparse()) {
            auto right_it = right_data.cbegin<right_input_tdt, IteratorType::ENUMERATED>();
            auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
            const auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
            loop(left_it, right_it, output_it, output_end_it);
        } else {
            // It is possible that an input columns is sparse, but the output column is dense due to condition
            // selecting only on bits from the sparse column
            auto right_it = right_data.cbegin<right_input_tdt>();
            const auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED>();
            for (auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED>(); output_it != output_end_it;
                 ++output_it, ++right_it) {
                const auto idx = output_it->idx();
                if (condition.get_bit(idx)) {
                    while (left_it->idx() != idx) {
                        ++left_it;
                    }
                    output_it->value() = f(true, left_it->value(), {});
                } else {
                    // Unlike in the loop lambda, we do not need a while loop here, as both right_it and output_it are
                    // dense, and are being incremented in the top level loop
                    output_it->value() = f(false, {}, *right_it);
                }
            }
        }
    } else if (right_input_column.is_sparse()) {
        // Left is dense, right is sparse
        auto right_it = right_data.cbegin<right_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        if (output_column.is_sparse()) {
            auto left_it = left_data.cbegin<left_input_tdt, IteratorType::ENUMERATED>();
            auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
            const auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
            loop(left_it, right_it, output_it, output_end_it);
        } else {
            // It is possible that an input columns is sparse, but the output column is dense due to condition
            // selecting only on bits from the sparse column
            auto left_it = left_data.cbegin<left_input_tdt>();
            const auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED>();
            for (auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED>(); output_it != output_end_it;
                 ++output_it, ++left_it) {
                const auto idx = output_it->idx();
                if (condition.get_bit(idx)) {
                    // Unlike in the loop lambda, we do not need a while loop here, as both left_it and output_it are
                    // dense, and are being incremented in the top level loop
                    output_it->value() = f(true, *left_it, {});
                } else {
                    while (right_it->idx() != idx) {
                        ++right_it;
                    }
                    output_it->value() = f(false, {}, right_it->value());
                }
            }
        }
    }
}

template<typename input_tdt, typename output_tdt, bool arguments_reversed, typename functor>
requires std::is_invocable_r_v<
        typename output_tdt::DataTypeTag::raw_type, functor, bool, typename output_tdt::DataTypeTag::raw_type,
        typename output_tdt::DataTypeTag::raw_type>
void ternary_transform(
        const util::BitSet& condition, const Column& input_column, typename output_tdt::DataTypeTag::raw_type value,
        Column& output_column, functor&& f
) {
    util::BitSet transformed_condition;
    if constexpr (arguments_reversed) {
        transformed_condition = ~condition;
        transformed_condition.resize(condition.size());
    } else {
        transformed_condition = condition;
    }
    initialise_output_column<FullResult>(transformed_condition, input_column, output_column);
    auto input_data = input_column.data();
    auto output_data = output_column.data();
    // See comments in similar method above that takes 2 input columns for why this works
    // Compute the RHS result f(false, {}, value) just once
    auto loop = [&transformed_condition,
                 value_res = f(false, {}, value),
                 f = std::move(f)]<typename I, typename O>(I input_it, O output_it, const O output_end_it) {
        for (; output_it != output_end_it; ++output_it) {
            const auto idx = output_it->idx();
            if (transformed_condition.get_bit(idx)) {
                while (input_it->idx() != idx) {
                    ++input_it;
                }
                output_it->value() = f(true, input_it->value(), {});
            } else {
                output_it->value() = value_res;
            }
        }
    };
    if (input_column.is_sparse()) {
        auto input_it = input_data.cbegin<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        if (output_column.is_sparse()) {
            auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
            const auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
            loop(input_it, output_it, output_end_it);
        } else {
            // It is possible that the input column is sparse, but the output column is dense due to condition
            // selecting only on bits from the input column
            auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED>();
            const auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED>();
            loop(input_it, output_it, output_end_it);
        }
    } else {
        // Input column is dense implies output column is dense
        auto input_it = input_data.cbegin<input_tdt>();
        const auto output_end_it = output_data.end<output_tdt>();
        size_t idx{0};
        for (auto output_it = output_data.begin<output_tdt>(); output_it != output_end_it;
             ++output_it, ++input_it, ++idx) {
            *output_it = f(transformed_condition.get_bit(idx), *input_it, value);
        }
    }
}

template<typename input_tdt, bool arguments_reversed, typename functor>
requires std::is_invocable_r_v<
        typename input_tdt::DataTypeTag::raw_type, functor, typename input_tdt::DataTypeTag::raw_type>
void ternary_transform(
        const util::BitSet& condition, const Column& input_column, ARCTICDB_UNUSED EmptyResult empty_result,
        Column& output_column, functor&& f
) {
    util::BitSet transformed_condition;
    if constexpr (arguments_reversed) {
        transformed_condition = ~condition;
        transformed_condition.resize(condition.size());
    } else {
        transformed_condition = condition;
    }
    initialise_output_column<EmptyResult>(transformed_condition, input_column, output_column);
    auto input_data = input_column.data();
    auto output_data = output_column.data();
    // See comments in similar method above that takes 2 input columns for why this works
    auto loop = [&transformed_condition,
                 f = std::forward<functor>(f)]<typename I, typename O>(I input_it, O output_it, const O output_end_it) {
        for (; output_it != output_end_it; ++output_it) {
            const auto idx = output_it->idx();
            if (transformed_condition.get_bit(idx)) {
                while (input_it->idx() != idx) {
                    ++input_it;
                }
                output_it->value() = f(input_it->value());
            }
        }
    };
    if (input_column.is_sparse()) {
        // Input column is sparse implies output column is sparse
        auto input_it = input_data.cbegin<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        auto output_it = output_data.begin<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        const auto output_end_it = output_data.end<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        loop(input_it, output_it, output_end_it);
    } else {
        // Input column dense DOES NOT imply output column is dense, as wherever condition is false we will have a
        // missing value in the output column
        if (output_column.is_sparse()) {
            auto input_it = input_data.cbegin<input_tdt, IteratorType::ENUMERATED>();
            auto output_it = output_data.begin<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
            const auto output_end_it = output_data.end<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
            loop(input_it, output_it, output_end_it);
        } else {
            // Special case where transformed_condition is all true
            auto input_it = input_data.cbegin<input_tdt>();
            const auto output_end_it = output_data.end<input_tdt>();
            for (auto output_it = output_data.begin<input_tdt>(); output_it != output_end_it; ++output_it, ++input_it) {
                *output_it = f(*input_it);
            }
        }
    }
}

template<typename output_tdt>
void ternary_transform(
        const util::BitSet& condition, typename output_tdt::DataTypeTag::raw_type left_val,
        typename output_tdt::DataTypeTag::raw_type right_val, Column& output_column
) {
    auto output_rows = condition.size();
    auto output_bytes = output_rows * get_type_size(output_column.type().data_type());
    if (output_bytes > 0) {
        output_column.allocate_data(output_bytes);
    }
    output_column.set_row_data(output_rows - 1);
    if (output_rows > 0) {
        auto data = output_column.ptr_cast<typename output_tdt::DataTypeTag::raw_type>(0, output_bytes);
        auto mostly_left = (static_cast<double>(condition.count()) / static_cast<double>(output_rows)) > 0.5;
        if (mostly_left) {
            std::fill_n(data, output_rows, left_val);
            auto transformed_condition = ~condition;
            transformed_condition.resize(output_rows);
            auto end_bit = transformed_condition.end();
            for (auto set_bit = transformed_condition.first(); set_bit < end_bit; ++set_bit) {
                *(data + *set_bit) = right_val;
            }
        } else {
            // Mostly right
            std::fill_n(data, output_rows, right_val);
            auto end_bit = condition.end();
            for (auto set_bit = condition.first(); set_bit < end_bit; ++set_bit) {
                *(data + *set_bit) = left_val;
            }
        }
    }
    // The below code does the same as above in a style more similar to the other functions in this file
    // The above implementation was found to be approximately the same speed as the below when condition is 50/50
    // true/false, but the above is faster the more unbalanced condition is (x8 faster for a 99/1 split)
    //    auto output_data = output_column.data();
    //    const auto output_end_it = output_data.end<output_tdt, IteratorType::ENUMERATED>();
    //    for (auto output_it = output_data.begin<output_tdt, IteratorType::ENUMERATED>(); output_it != output_end_it;
    //    ++output_it) {
    //        auto idx = output_it->idx();
    //        output_it->value() = condition.get_bit(idx) ? left_val : right_val;
    //    }
}

template<typename value_tdt, bool arguments_reversed>
void ternary_transform(
        const util::BitSet& condition, typename value_tdt::DataTypeTag::raw_type value,
        ARCTICDB_UNUSED EmptyResult empty_result, Column& output_column
) {
    util::BitSet transformed_condition;
    if constexpr (arguments_reversed) {
        transformed_condition = ~condition;
        transformed_condition.resize(condition.size());
    } else {
        transformed_condition = condition;
    }
    initialise_output_column(transformed_condition, output_column);
    auto output_data = output_column.data();
    const auto output_end_it = output_data.end<value_tdt>();
    for (auto output_it = output_data.begin<value_tdt>(); output_it != output_end_it; ++output_it) {
        *output_it = value;
    }
    // The below code does the same as the above, but was not found to be any faster
    //    auto output_rows = output_column.row_count();
    //    if (output_rows > 0) {
    //        auto data = output_column.ptr_cast<typename value_tdt::DataTypeTag::raw_type>(
    //                0,
    //                output_rows * get_type_size(output_column.type().data_type())
    //        );
    //        std::fill_n(data, output_rows, value);
    //    }
}

} // namespace arcticdb