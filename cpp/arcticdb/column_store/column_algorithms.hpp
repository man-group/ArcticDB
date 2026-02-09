/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <concepts>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/util/lambda_inlining.hpp>

namespace arcticdb {

template<typename input_tdt, typename functor>
requires util::instantiation_of<input_tdt, TypeDescriptorTag> &&
         std::is_invocable_r_v<void, functor, typename input_tdt::DataTypeTag::raw_type>
static void for_each(const Column& input_column, functor&& f) {
    auto input_data = input_column.data();
    std::for_each(input_data.cbegin<input_tdt>(), input_data.cend<input_tdt>(), std::forward<functor>(f));
}

// Thin wrapper around std::for_each that is marked flatten to force-inline callees.
// Useful when running hot lambdas over large data.
template<typename Iterator, typename functor>
ARCTICDB_FLATTEN static void for_each_flattened(Iterator begin, Iterator end, functor&& f) {
    std::for_each(begin, end, std::forward<functor>(f));
}

template<typename input_tdt, typename functor>
requires util::instantiation_of<input_tdt, TypeDescriptorTag> &&
         std::is_invocable_r_v<void, functor, ColumnData::Enumeration<typename input_tdt::DataTypeTag::raw_type>>
static void for_each_enumerated(
        const Column& input_column, functor&& f, std::optional<size_t> start_idx = std::nullopt,
        std::optional<size_t> end_idx = std::nullopt
) {
    auto input_data = input_column.data();
    // When `start_idx` or `end_idx` are set we use `std::advance` to get the `begin` and `end` iterators to the correct
    // locations. This is inefficient because `ColumnDataIterator` is not random access
    // TODO: Prove a random access `ColumnData::iterator_at(position)`.
    // Alternatively we could make `ColumnDataIterator` random access but this can be tricky because random access in a
    // `ChunkedBuffer` is `O(log(n))`, but according to standard an iterator `+=` should be `O(1)` to be marked as
    // random access. Otherwise something like `std::for_each` might turn out `O(n*log(n))` if implemented with `it+=1`
    // instead of `it++`.
    if (input_column.is_sparse()) {
        auto begin = input_data.cbegin<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        if (start_idx.has_value()) {
            // We need to get the `physical_offset` to know how many physical values to advance when column is sparse.
            auto physical_offset = input_column.get_physical_offset(*start_idx);
            std::advance(begin, physical_offset);
        }
        auto end = input_data.cend<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        if (end_idx.has_value()) {
            end = input_data.cbegin<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
            auto physical_offset = input_column.get_physical_offset(*end_idx);
            std::advance(end, physical_offset);
        }
        for_each_flattened(begin, end, std::forward<functor>(f));
    } else {
        auto begin = input_data.cbegin<input_tdt, IteratorType::ENUMERATED, IteratorDensity::DENSE>();
        if (start_idx.has_value()) {
            std::advance(begin, *start_idx);
        }
        auto end = input_data.cend<input_tdt, IteratorType::ENUMERATED, IteratorDensity::DENSE>();
        if (end_idx.has_value()) {
            end = input_data.cbegin<input_tdt, IteratorType::ENUMERATED, IteratorDensity::DENSE>();
            std::advance(end, *end_idx);
        }
        for_each_flattened(begin, end, std::forward<functor>(f));
    }
}

template<typename input_tdt, typename output_tdt, typename functor>
requires util::instantiation_of<input_tdt, TypeDescriptorTag> &&
         std::is_invocable_r_v<
                 typename output_tdt::DataTypeTag::raw_type, functor, typename input_tdt::DataTypeTag::raw_type>
static void transform(const Column& input_column, Column& output_column, functor&& f) {
    auto input_data = input_column.data();
    initialise_output_column(input_column, output_column);
    auto output_data = output_column.data();
    std::transform(
            input_data.cbegin<input_tdt>(),
            input_data.cend<input_tdt>(),
            output_data.begin<output_tdt>(),
            std::forward<functor>(f)
    );
}

template<typename left_input_tdt, typename right_input_tdt, typename output_tdt, typename functor>
requires util::instantiation_of<left_input_tdt, TypeDescriptorTag> &&
         util::instantiation_of<right_input_tdt, TypeDescriptorTag> &&
         std::is_invocable_r_v<
                 typename output_tdt::DataTypeTag::raw_type, functor, typename left_input_tdt::DataTypeTag::raw_type,
                 typename right_input_tdt::DataTypeTag::raw_type>
static void transform(
        const Column& left_input_column, const Column& right_input_column, Column& output_column, functor&& f
) {
    auto left_input_data = left_input_column.data();
    auto right_input_data = right_input_column.data();
    initialise_output_column(left_input_column, right_input_column, output_column);
    auto output_data = output_column.data();
    auto output_it = output_data.begin<output_tdt>();

    if (!left_input_column.is_sparse() && !right_input_column.is_sparse()) {
        // Both dense, use std::transform over the shorter column to avoid going out-of-bounds
        if (left_input_column.row_count() <= right_input_column.row_count()) {
            std::transform(
                    left_input_data.cbegin<left_input_tdt>(),
                    left_input_data.cend<left_input_tdt>(),
                    right_input_data.cbegin<right_input_tdt>(),
                    output_it,
                    std::forward<functor>(f)
            );
        } else {
            std::transform(
                    right_input_data.cbegin<left_input_tdt>(),
                    right_input_data.cend<left_input_tdt>(),
                    left_input_data.cbegin<right_input_tdt>(),
                    output_it,
                    std::forward<functor>(f)
            );
        }
    } else if (left_input_column.is_sparse() && right_input_column.is_sparse()) {
        auto left_it = left_input_data.cbegin<left_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        auto right_it = right_input_data.cbegin<right_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        auto end_bit = output_column.sparse_map().end();
        for (auto set_bit = output_column.sparse_map().first(); set_bit < end_bit; ++set_bit) {
            const auto idx = *set_bit;
            while (left_it->idx() != idx) {
                ++left_it;
            }
            while (right_it->idx() != idx) {
                ++right_it;
            }
            *output_it++ = f(left_it->value(), right_it->value());
        }
    } else if (left_input_column.is_sparse() && !right_input_column.is_sparse()) {
        // One sparse, one dense. Use the enumerating forward iterator over the sparse column as it is more
        // efficient than random access
        auto right_accessor = random_accessor<right_input_tdt>(&right_input_data);
        const auto right_column_row_count = right_input_column.row_count();
        const auto left_input_data_cend =
                left_input_data.cend<left_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        for (auto left_it = left_input_data.cbegin<left_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
             left_it != left_input_data_cend && left_it->idx() < right_column_row_count;
             ++left_it) {
            *output_it++ = f(left_it->value(), right_accessor.at(left_it->idx()));
        }
    } else if (!left_input_column.is_sparse() && right_input_column.is_sparse()) {
        // One sparse, one dense. Use the enumerating forward iterator over the sparse column as it is more
        // efficient than random access
        auto left_accessor = random_accessor<left_input_tdt>(&left_input_data);
        const auto left_column_row_count = left_input_column.row_count();
        const auto right_input_data_cend =
                right_input_data.cend<right_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        for (auto right_it =
                     right_input_data.cbegin<right_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
             right_it != right_input_data_cend && right_it->idx() < left_column_row_count;
             ++right_it) {
            *output_it++ = f(left_accessor.at(right_it->idx()), right_it->value());
        }
    }
}

template<typename input_tdt, std::predicate<typename input_tdt::DataTypeTag::raw_type> functor>
requires util::instantiation_of<input_tdt, TypeDescriptorTag>
static void transform(
        const Column& input_column, util::BitSet& output_bitset, bool sparse_missing_value_output, functor&& f
) {
    if (input_column.is_sparse()) {
        initialise_output_bitset(input_column, sparse_missing_value_output, output_bitset);
    } else {
        // This allows for empty/full result optimisations, technically bitsets are always dynamically sized
        output_bitset.resize(input_column.row_count());
    }
    util::BitSet::bulk_insert_iterator inserter(output_bitset);
    arcticdb::for_each_enumerated<input_tdt>(
            input_column,
            [&inserter, f = std::forward<functor>(f)] ARCTICDB_LAMBDA_INLINE(auto enumerated_it) {
                if (f(enumerated_it.value())) {
                    inserter = enumerated_it.idx();
                }
            }
    );
    inserter.flush();
}

template<
        typename left_input_tdt, typename right_input_tdt,
        std::relation<typename left_input_tdt::DataTypeTag::raw_type, typename right_input_tdt::DataTypeTag::raw_type>
                functor>
requires util::instantiation_of<left_input_tdt, TypeDescriptorTag> &&
         util::instantiation_of<right_input_tdt, TypeDescriptorTag>
static void transform(
        const Column& left_input_column, const Column& right_input_column, util::BitSet& output_bitset,
        bool sparse_missing_value_output, functor&& f
) {
    auto left_input_data = left_input_column.data();
    auto right_input_data = right_input_column.data();
    util::check(
            left_input_column.last_row() == right_input_column.last_row(),
            "Mismatching logical column lengths in arcticdb::transform"
    );
    util::BitSet::bulk_insert_iterator inserter(output_bitset);

    if (!left_input_column.is_sparse() && !right_input_column.is_sparse()) {
        // Both dense, use std::for_each over the shorter column to avoid going out-of-bounds
        auto rows = std::max(left_input_column.last_row(), right_input_column.last_row()) + 1;
        output_bitset.resize(rows);
        if (sparse_missing_value_output && left_input_column.row_count() != right_input_column.row_count()) {
            // Dense columns of different lengths, and missing values should be on in the output bitset
            output_bitset.set_range(
                    std::min(left_input_column.last_row(), right_input_column.last_row()) + 1, rows - 1
            );
        }
        auto pos = 0u;
        if (left_input_column.row_count() <= right_input_column.row_count()) {
            auto right_it = right_input_data.cbegin<right_input_tdt>();
            std::for_each(
                    left_input_data.cbegin<left_input_tdt>(),
                    left_input_data.cend<left_input_tdt>(),
                    [&right_it, &inserter, &pos, f = std::forward<functor>(f)](auto left_value) {
                        if (f(left_value, *right_it++)) {
                            inserter = pos;
                        }
                        ++pos;
                    }
            );
        } else {
            auto left_it = left_input_data.cbegin<left_input_tdt>();
            std::for_each(
                    right_input_data.cbegin<right_input_tdt>(),
                    right_input_data.cend<right_input_tdt>(),
                    [&left_it, &inserter, &pos, f = std::forward<functor>(f)](auto right_value) {
                        if (f(*left_it++, right_value)) {
                            inserter = pos;
                        }
                        ++pos;
                    }
            );
        }
    } else if (left_input_column.is_sparse() && right_input_column.is_sparse()) {
        // Both sparse, only check the intersection of on-bits from both sparse maps
        auto bits_to_check = left_input_column.sparse_map() & right_input_column.sparse_map();
        if (sparse_missing_value_output) {
            output_bitset = bits_to_check;
            output_bitset.flip();
        }
        // Both columns should have the same number of logical rows, so just use one of them
        output_bitset.resize(left_input_column.last_row() + 1);
        auto left_accessor = random_accessor<left_input_tdt>(&left_input_data);
        auto right_accessor = random_accessor<right_input_tdt>(&right_input_data);
        // TODO: experiment with more efficient bitset traversal methods
        // https://github.com/tlk00/BitMagic/tree/master/samples/bvsample25
        auto end_bit = bits_to_check.end();
        for (auto set_bit = bits_to_check.first(); set_bit < end_bit; ++set_bit) {
            if (f(left_accessor.at(*set_bit), right_accessor.at(*set_bit))) {
                inserter = *set_bit;
            }
        }
    } else if (left_input_column.is_sparse() && !right_input_column.is_sparse()) {
        // One sparse, one dense. Use the enumerating forward iterator over the sparse column as it is more
        // efficient than random access
        initialise_output_bitset(left_input_column, sparse_missing_value_output, output_bitset);
        auto right_accessor = random_accessor<right_input_tdt>(&right_input_data);
        const auto right_column_row_count = right_input_column.row_count();
        const auto left_input_data_cend =
                left_input_data.cend<left_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        for (auto left_it = left_input_data.cbegin<left_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
             left_it != left_input_data_cend && left_it->idx() < right_column_row_count;
             ++left_it) {
            if (f(left_it->value(), right_accessor.at(left_it->idx()))) {
                inserter = left_it->idx();
            }
        }
    } else if (!left_input_column.is_sparse() && right_input_column.is_sparse()) {
        // One sparse, one dense. Use the enumerating forward iterator over the sparse column as it is more
        // efficient than random access
        initialise_output_bitset(right_input_column, sparse_missing_value_output, output_bitset);
        auto left_accessor = random_accessor<left_input_tdt>(&left_input_data);
        const auto left_column_row_count = left_input_column.row_count();
        const auto right_input_data_cend =
                right_input_data.cend<right_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        for (auto right_it =
                     right_input_data.cbegin<right_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
             right_it != right_input_data_cend && right_it->idx() < left_column_row_count;
             ++right_it) {
            if (f(left_accessor.at(right_it->idx()), right_it->value())) {
                inserter = right_it->idx();
            }
        }
    }
    inserter.flush();
}

} // namespace arcticdb
