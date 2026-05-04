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

template<typename input_tdt, typename functor>
requires util::instantiation_of<input_tdt, TypeDescriptorTag> &&
         std::is_invocable_r_v<void, functor, typename input_tdt::DataTypeTag::raw_type&>
static void for_each(Column& input_column, functor&& f) {
    auto input_data = input_column.data();
    std::for_each(input_data.begin<input_tdt>(), input_data.end<input_tdt>(), std::forward<functor>(f));
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
static void for_each_enumerated(const Column& input_column, functor&& f) {
    auto input_data = input_column.data();
    if (input_column.is_sparse()) {
        auto begin = input_data.cbegin<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        auto end = input_data.cend<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        std::for_each(begin, end, std::forward<functor>(f));
    } else {
        auto begin = input_data.cbegin<input_tdt, IteratorType::ENUMERATED, IteratorDensity::DENSE>();
        auto end = input_data.cend<input_tdt, IteratorType::ENUMERATED, IteratorDensity::DENSE>();
        std::for_each(begin, end, std::forward<functor>(f));
    }
}

// Variant of for_each_enumerated that uses for_each_flattened to force-inline all callees in the loop body.
// This increases compile-time memory usage, so only use in performance-critical hot paths.
template<typename input_tdt, typename functor>
requires util::instantiation_of<input_tdt, TypeDescriptorTag> &&
         std::is_invocable_r_v<void, functor, ColumnData::Enumeration<typename input_tdt::DataTypeTag::raw_type>>
static void for_each_enumerated_flattened(
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
        auto end = input_data.cend<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        if (start_idx.has_value()) {
            // We need to advance the iterator to the first physical position where `begin->idx() >= *start_idx`.
            // Guard against `begin == end` to avoid advancing past the last set bit: BitMagic's get_next() returns 0
            // (not a sentinel) when exhausted, which would cause idx_ to wrap and the loop to never terminate.
            while (begin != end && begin->idx() < static_cast<ssize_t>(*start_idx)) {
                ++begin;
            }
        }
        if (end_idx.has_value()) {
            auto truncated_end = begin;
            // Same guard: stop at natural cend if end_idx exceeds all set bit positions.
            while (truncated_end != end && truncated_end->idx() < static_cast<ssize_t>(*end_idx)) {
                ++truncated_end;
            }
            end = truncated_end;
        }
        for_each_flattened(begin, end, std::forward<functor>(f));
    } else {
        auto begin = input_data.cbegin<input_tdt, IteratorType::ENUMERATED, IteratorDensity::DENSE>();
        if (start_idx.has_value()) {
            std::advance(begin, *start_idx);
        }
        auto end = input_data.cend<input_tdt, IteratorType::ENUMERATED, IteratorDensity::DENSE>();
        if (end_idx.has_value()) {
            end = begin;
            std::advance(end, *end_idx - start_idx.value_or(0));
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

// ─── Sorted-column search ────────────────────────────────────────────────────────────────────────
//
// All four search functions take a [begin, end) iterator pair, mirroring std::lower_bound /
// std::upper_bound.

// Constraints shared by the four sorted-search functions: scalar type, dense iterator, and a
// numeric raw type (integers, floats, or timestamps — matches `is_numeric_type` in entity/types.hpp).
template<typename TDT, IteratorDensity ID>
concept SortedSearchInputs = util::instantiation_of<TDT, TypeDescriptorTag> && (TDT::dimension() == Dimension::Dim0) &&
                             (ID == IteratorDensity::DENSE) && is_numeric_type(TDT::DataTypeTag::data_type);

namespace search_detail {

// Iterator-pair binary search.
// `is_before_answer(probe, value)` returns true while a probe is strictly before the answer (so we move begin past it).
// For lower_bound that is `probe < value`; for upper_bound it is `probe <= value`.
// `within_block_bisect` is std::lower_bound or std::upper_bound run on the contiguous block memory.
template<typename TDT, IteratorType IT, IteratorDensity ID, typename IsBeforeAnswer, typename WithinBlockBisect>
ColumnData::ColumnDataIterator<TDT, IT, ID, true> bound_search(
        ColumnData::ColumnDataIterator<TDT, IT, ID, true> begin, ColumnData::ColumnDataIterator<TDT, IT, ID, true> end,
        typename TDT::DataTypeTag::raw_type value, IsBeforeAnswer is_before, WithinBlockBisect bisect
) {
    using RawType = typename TDT::DataTypeTag::raw_type;
    static_assert(ID == IteratorDensity::DENSE, "Sorted search currently supports DENSE only");
    static_assert(TDT::dimension() == Dimension::Dim0, "Sorted search supports Dim0 only");
    util::check(begin.parent() == end.parent(), "bound_search: begin and end have different parents");

    const ColumnData* data = begin.parent();

    // Narrow the block range until all possible values sit in the same block.
    // Use inclusive first_block and inclusive last_block for easier to read binary search condition.
    auto first_block = begin.current_block_index();
    auto last_block = end.current_block_index() - (end.current_in_block_offset() == 0);
    while (first_block < last_block) {
        const size_t mid_block_idx = (first_block + last_block) / 2;
        auto mid_block = data->template typed_block_at_position<TDT>(mid_block_idx);
        ColumnData::ColumnDataIterator<TDT, IT, ID, true> last_in_mid(data, mid_block_idx, mid_block->row_count() - 1);
        auto first_in_next = last_in_mid;
        ++first_in_next;
        if (is_before(*last_in_mid.current_ptr(), value)) {
            begin = first_in_next;
            first_block = mid_block_idx + 1;
        } else {
            end = first_in_next;
            last_block = mid_block_idx;
        }
    }

    if (begin == end) {
        return begin;
    }

    const size_t block_pos = begin.current_block_index();
    auto block = data->template typed_block_at_position<TDT>(block_pos);
    const size_t first = begin.current_in_block_offset();
    const size_t last = (end.current_block_index() == block_pos) ? end.current_in_block_offset() : block->row_count();
    const RawType* found = bisect(block->data() + first, block->data() + last, value);
    if (found == block->data() + last) {
        return end;
    }
    return ColumnData::ColumnDataIterator<TDT, IT, ID, true>(
            data, block_pos, static_cast<size_t>(found - block->data())
    );
}

// Gallop forward from `begin` in steps of 2**n until an element after value is reached.
// Returns the exponential range known to contain the first element for which `!is_before`.
template<typename TDT, IteratorType IT, IteratorDensity ID, typename IsBeforeAnswer>
std::pair<ColumnData::ColumnDataIterator<TDT, IT, ID, true>, ColumnData::ColumnDataIterator<TDT, IT, ID, true>>
gallop_bracket(
        const ColumnData::ColumnDataIterator<TDT, IT, ID, true>& begin,
        const ColumnData::ColumnDataIterator<TDT, IT, ID, true>& end, typename TDT::DataTypeTag::raw_type value,
        IsBeforeAnswer is_before
) {
    const ColumnData* data = begin.parent();

    if (begin == end) {
        return {begin, end};
    }

    const auto first_block = *begin.current_block();
    const size_t first_block_idx = begin.current_block_index();
    const size_t end_block_idx = end.current_block_index();

    auto prev = begin;
    auto current = begin;

    // Probes at (block_idx, in_block_offset) and returns whether it's before value.
    // If yes, the element we search for is in range [current, end) and we need to continue galloping.
    // If not, the element we search for is in range [prev, current) and we can stop galloping.
    auto probe_and_update_range = [&](size_t block_idx, size_t in_block_offset) {
        prev = current;
        current = ColumnData::ColumnDataIterator<TDT, IT, ID, true>(data, block_idx, in_block_offset);
        auto value_at_current = *current.current_ptr();
        ++current;
        return is_before(value_at_current, value);
    };

    // Probe within first block at first_offset + 2**n
    const size_t first_offset = begin.current_in_block_offset();
    const size_t up_to =
            end_block_idx > first_block_idx ? first_block.row_count() : end.current_in_block_offset();
    size_t step = 1;
    for (; first_offset + step < up_to; step *= 2) {
        if (!probe_and_update_range(first_block_idx, first_offset + step)) {
            return {prev, current};
        }
    }

    if (end_block_idx == first_block_idx) {
        // If end is in the first block, the resulting range is [current, end)
        return {current, end};
    }

    // Probe at the last element of first block
    if (!probe_and_update_range(first_block_idx, first_block.row_count() - 1)) {
        return {prev, current};
    }

    // Answer is beyond the starting block — probe the last elements of blocks at first_idx + 2**n
    step = 1;
    for (; first_block_idx + step < end_block_idx; step *= 2) {
        const size_t block_idx = first_block_idx + step;
        auto block = data->template typed_block_at_position<TDT>(block_idx);
        if (!probe_and_update_range(block_idx, block->row_count() - 1)) {
            return {prev, current};
        }
    }
    return {current, end};
}

} // namespace search_detail

// Returns an iterator to the first element in [begin, end) that is not less than `value`.
// Complexity is `O(log(std::distance(begin, end)))`.
// Mirrors std::lower_bound semantics.
template<typename TDT, IteratorType IT, IteratorDensity ID>
requires SortedSearchInputs<TDT, ID>
ColumnData::ColumnDataIterator<TDT, IT, ID, true> lower_bound(
        const ColumnData::ColumnDataIterator<TDT, IT, ID, true>& begin,
        const ColumnData::ColumnDataIterator<TDT, IT, ID, true>& end, typename TDT::DataTypeTag::raw_type value
) {
    using RawType = typename TDT::DataTypeTag::raw_type;
    return search_detail::bound_search<TDT, IT, ID>(
            begin,
            end,
            value,
            [](RawType probe, RawType v) { return probe < v; },
            [](const RawType* lo, const RawType* hi, RawType v) { return std::lower_bound(lo, hi, v); }
    );
}

// Returns an iterator to the first element in [begin, end) that is greater than `value`.
// Mirrors std::upper_bound semantics.
template<typename TDT, IteratorType IT, IteratorDensity ID>
requires SortedSearchInputs<TDT, ID>
ColumnData::ColumnDataIterator<TDT, IT, ID, true> upper_bound(
        const ColumnData::ColumnDataIterator<TDT, IT, ID, true>& begin,
        const ColumnData::ColumnDataIterator<TDT, IT, ID, true>& end, typename TDT::DataTypeTag::raw_type value
) {
    using RawType = typename TDT::DataTypeTag::raw_type;
    return search_detail::bound_search<TDT, IT, ID>(
            begin,
            end,
            value,
            [](RawType probe, RawType v) { return !(v < probe); },
            [](const RawType* lo, const RawType* hi, RawType v) { return std::upper_bound(lo, hi, v); }
    );
}

// Exponential (galloping) lower_bound finds the same answer as lower_bound but first does an exponential scan
// to find the answer more quickly if near begin.
// Complexity is `O(log(std::distance(begin, answer)))`.
// Note that this is faster than regular lower bound when answer is near begin but has a higher constant.
template<typename TDT, IteratorType IT, IteratorDensity ID>
requires SortedSearchInputs<TDT, ID>
ColumnData::ColumnDataIterator<TDT, IT, ID, true> exponential_lower_bound(
        const ColumnData::ColumnDataIterator<TDT, IT, ID, true>& begin,
        const ColumnData::ColumnDataIterator<TDT, IT, ID, true>& end, typename TDT::DataTypeTag::raw_type value
) {
    using RawType = typename TDT::DataTypeTag::raw_type;
    auto [bracket_start, bracket_end] =
            search_detail::gallop_bracket<TDT, IT, ID>(begin, end, value, [](RawType probe, RawType v) {
                return probe < v;
            });
    return lower_bound<TDT, IT, ID>(bracket_start, bracket_end, value);
}

// Exponential (galloping) upper_bound.
template<typename TDT, IteratorType IT, IteratorDensity ID>
requires SortedSearchInputs<TDT, ID>
ColumnData::ColumnDataIterator<TDT, IT, ID, true> exponential_upper_bound(
        const ColumnData::ColumnDataIterator<TDT, IT, ID, true>& begin,
        const ColumnData::ColumnDataIterator<TDT, IT, ID, true>& end, typename TDT::DataTypeTag::raw_type value
) {
    using RawType = typename TDT::DataTypeTag::raw_type;
    auto [bracket_start, bracket_end] =
            search_detail::gallop_bracket<TDT, IT, ID>(begin, end, value, [](RawType probe, RawType v) {
                return !(v < probe);
            });
    return upper_bound<TDT, IT, ID>(bracket_start, bracket_end, value);
}

} // namespace arcticdb
