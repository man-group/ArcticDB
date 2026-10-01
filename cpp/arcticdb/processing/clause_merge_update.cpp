/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/write_frame.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>
#include <arcticdb/version/schema_checks.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/column_store/column_reslicer.hpp>
#include <arcticdb/util/collection_utils.hpp>
#include <ankerl/unordered_dense.h>
#include <boost/regex.hpp>

#include <optional>
#include <ranges>
#include <type_traits>

namespace {
using namespace arcticdb;

using IndexType = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;

template<typename T>
position_t get_string_na_placeholder(const T& value) {
    if constexpr (std::same_as<std::remove_cvref_t<T>, PyObject*>) {
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                is_py_nan(value) || is_py_none(value),
                "This function expects that checks are made earlier to ensure a \"na\" python object is passed"
        );
        return is_py_nan(value) ? string_nan : string_none;
    } else {
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                value == string_nan || value == string_none,
                "This function expects that checks are made earlier to ensure a \"na\" string pool offset is passed"
        );
        return value;
    }
}

struct TargetRange {
    size_t start_row_in_first_row_slice{};
    size_t end_row_in_last_row_slice{};
};

/// Keep only the row slice groups listed in row_slices_to_keep, then compact ranges_and_keys to the entities still
/// referenced and renumber the entries in offsets to their new positions.
/// @param row_slices_to_keep Indices into offsets of the groups to retain; must be sorted and unique.
/// @param offsets Row slice groups, each an ordered list of indices into ranges_and_keys. Filtered and reindexed in
/// place.
/// @param ranges_and_keys Flat list of entities indexed by offsets. Compacted in place to the retained entities.
void filter_selected_ranges_and_keys_and_reindex_entities(
        const std::span<const size_t> row_slices_to_keep, std::vector<std::vector<size_t>>& offsets,
        std::vector<RangesAndKey>& ranges_and_keys
) {
    ARCTICDB_DEBUG_CHECK(
            ErrorCode::E_ASSERTION_FAILURE,
            std::ranges::adjacent_find(row_slices_to_keep, std::ranges::greater_equal{}) == row_slices_to_keep.end(),
            "Elements of rows slices to keep must be sorted and unique"
    );
    std::vector<std::vector<size_t>> new_offsets;
    new_offsets.reserve(row_slices_to_keep.size());
    for (const size_t row_slice_to_keep : row_slices_to_keep) {
        new_offsets.emplace_back(std::move(offsets[row_slice_to_keep]));
    }
    offsets = std::move(new_offsets);
    size_t new_entity_id = 0;
    std::vector<RangesAndKey> new_ranges_and_keys;
    new_ranges_and_keys.reserve(ranges_and_keys.size());
    ankerl::unordered_dense::map<size_t, size_t> offset_to_entity;
    for (std::span<size_t> row_slice : offsets) {
        for (size_t& entity_id : row_slice) {
            auto [it, inserted] = offset_to_entity.emplace(entity_id, new_entity_id);
            if (inserted) {
                new_ranges_and_keys.emplace_back(std::move(ranges_and_keys[entity_id]));
                ++new_entity_id;
            }

            entity_id = it->second;
        }
    }
    ranges_and_keys = std::move(new_ranges_and_keys);
}

template<util::type_descriptor_tag TDT>
void rebuild_sequence_column_in_new_pool(
        Column& target_column, const StringPool& old_string_pool, StringPool& new_string_pool,
        const util::BitSet* target_rows_to_add_in_new_pool = nullptr
) {
    if (target_rows_to_add_in_new_pool) {
        ColumnData target_column_data = target_column.data();
        auto accessor = random_accessor<TDT>(&target_column_data);
        for (auto row = target_rows_to_add_in_new_pool->first(); row != target_rows_to_add_in_new_pool->end(); ++row) {
            if (is_a_string(accessor[*row])) {
                const std::string_view string_value = old_string_pool.get_const_view(accessor[*row]);
                const OffsetString& new_offset = new_string_pool.get(string_value);
                accessor[*row] = new_offset.offset();
            }
        }
    } else {
        arcticdb::for_each<TDT>(target_column, [&](auto& value) {
            if (is_a_string(value)) {
                const std::string_view string_value = old_string_pool.get_const_view(value);
                const OffsetString& new_offset = new_string_pool.get(string_value);
                value = new_offset.offset();
            }
        });
    }
}

template<util::type_descriptor_tag TDT>
struct NaAwareHasher : ankerl::unordered_dense::hash<MatchKeyType<TDT>> {
    using Base = ankerl::unordered_dense::hash<MatchKeyType<TDT>>;
    uint64_t operator()(MatchKeyType<TDT> value) const {
        if constexpr (is_floating_point_type(TDT::data_type())) {
            if (is_na<TDT>(value)) {
                // IEEE allows multiple different bit representations of NaN. std::isnan is required to return true
                // for all different bit representations of NaN, std::quiet_NaN is an implementation defined
                // constant, so hashing it canonicalises every missing value to the same bucket.
                return Base::operator()(std::numeric_limits<MatchKeyType<TDT>>::quiet_NaN());
            }
        }
        return Base::operator()(value);
    }
};

template<util::type_descriptor_tag TDT>
auto map_column_values_to_rows(const ColumnWithStrings& column, bool match_na) {
    ankerl::unordered_dense::map<MatchKeyType<TDT>, std::vector<size_t>, NaAwareHasher<TDT>, NaAwareComparator<TDT>>
            target_values(0, NaAwareHasher<TDT>{}, NaAwareComparator<TDT>{match_na});
    arcticdb::for_each_enumerated<TDT>(*column.column_, [&](auto row) {
        if (match_na || !is_na<TDT>(row.value())) {
            if constexpr (is_sequence_type(TDT::data_type())) {
                target_values[column.string_at_offset(row.value())].emplace_back(row.idx());
            } else {
                target_values[row.value()].emplace_back(row.idx());
            }
        }
    });
    return target_values;
}

template<typename Source>
struct InsertSourceData {
    std::span<const timestamp> index;
    Source data;
    std::pair<size_t, size_t> global_row_range;
};

struct InsertTargetData {
    std::span<ColumnWithStrings> indexes;
    std::span<ColumnWithStrings> columns;
    TypeDescriptor type;
    TargetRange range;
    std::span<StringPool> new_string_pools;
};

std::vector<size_t> compute_target_slice_offset(const InsertTargetData& target) {
    std::vector<size_t> result;
    std::transform_exclusive_scan(
            target.indexes.begin(),
            target.indexes.end(),
            std::back_inserter(result),
            size_t{0},
            std::plus{},
            [](const ColumnWithStrings& col) -> size_t { return col.column_->row_count(); }
    );
    return result;
}

/// Performs merging of sorted lists with update on matching rows. The index column dictates the ordering of the
/// rows. The target is composed of multiple row slices. The following holds: is_sorted(index in row slice i) &&
/// all(index values in row slice i) <= all(index values in row slice j) for i < j. The source is a single row
/// slice.  Insertion is stable and all new values are inserted after all existing values with the same index value.
/// The source and target must have the same index type.
template<util::type_descriptor_tag TargetTDT, util::type_descriptor_tag SourceTDT, typename Source>
requires(
        TargetTDT::dimension() == Dimension::Dim0 && SourceTDT::dimension() == Dimension::Dim0 &&
        TargetTDT::data_type() == SourceTDT::data_type()
)
std::vector<std::shared_ptr<Column>> merge(
        const InsertSourceData<Source>& source, const InsertTargetData& target,
        const MergeUpdateClause::MatchRecord& match_record, const MergeStrategy& strategy,
        const ReslicingInfo& reslicing_info
) {
    using ColumnRandomAccessorType = ColumnDataRandomAccessor<TargetTDT>;
    const std::vector<size_t> target_slice_offset = compute_target_slice_offset(target);

    auto new_columns = util::reserve_vector<std::shared_ptr<Column>>(reslicing_info.num_segments());
    auto new_column_datas = util::reserve_vector<ColumnData>(reslicing_info.num_segments());
    auto new_column_accessors = util::reserve_vector<ColumnRandomAccessorType>(reslicing_info.num_segments());
    for (size_t i = 0; i < reslicing_info.num_segments(); ++i) {
        new_columns.push_back(std::make_shared<Column>(
                target.type, reslicing_info.rows_in_slice(i), AllocationType::PRESIZED, Sparsity::NOT_PERMITTED
        ));
        new_column_datas.emplace_back(new_columns.back()->data());
        new_column_accessors.emplace_back(random_accessor<TargetTDT>(&new_column_datas.back()));
    }
    size_t new_column_row_slice_index{};
    // Offset within the current output column
    size_t new_column_row_idx{};
    // Position in the combined [0, total_rows()) output
    size_t output_row_idx{};
    size_t rows_in_current_slice = reslicing_info.rows_in_slice(0);
    auto new_column_it = new_column_datas.front().begin<TargetTDT>();

    size_t target_row_slice = 0;
    ColumnData target_index_data = target.indexes[target_row_slice].column_->data();
    auto target_index_it = target_index_data.begin<IndexType>();
    std::advance(target_index_it, target.range.start_row_in_first_row_slice);
    auto target_index_end = target_index_data.end<IndexType>();

    ColumnData target_column_data = target.columns[target_row_slice].column_->data();
    auto target_data = random_accessor<TargetTDT>(&target_column_data);
    size_t target_row_idx = target.range.start_row_in_first_row_slice;

    const auto target_index_is_exhausted = [&] {
        return target_row_slice == target.columns.size() - 1 && target_index_it == target_index_end;
    };

    const auto advance_target = [&] {
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                !target_index_is_exhausted(),
                "Cannot advance target index iterator past the end"
        );
        ++target_index_it;
        ++target_row_idx;
        if (target_index_it == target_index_end && target_row_slice < target.columns.size() - 1) [[unlikely]] {
            ++target_row_slice;
            target_row_idx = 0;
            target_column_data = target.columns[target_row_slice].column_->data();
            target_index_data = target.indexes[target_row_slice].column_->data();
            target_index_it = target_index_data.begin<IndexType>();
            target_index_end = target_index_data.end<IndexType>();
            target_data = random_accessor<TargetTDT>(&target_column_data);
            if (target_row_slice == target.columns.size() - 1 &&
                target.range.end_row_in_last_row_slice !=
                        static_cast<size_t>(target.columns.back().column_->row_count())) [[unlikely]] {
                target_index_end = std::next(target_index_it, target.range.end_row_in_last_row_slice);
            }
        }
    };

    const auto advance_output = [&](size_t step_size = 1) {
        util::check(
                output_row_idx + step_size <= reslicing_info.total_rows(),
                "Cannot advance the output by {} rows, only {} are left in the output",
                step_size,
                reslicing_info.total_rows() - output_row_idx
        );
        output_row_idx += step_size;
        while (new_column_row_idx + step_size >= rows_in_current_slice &&
               new_column_row_slice_index + 1 < reslicing_info.num_segments()) {
            step_size -= rows_in_current_slice - new_column_row_idx;
            ++new_column_row_slice_index;
            rows_in_current_slice = reslicing_info.rows_in_slice(new_column_row_slice_index);
            new_column_row_idx = 0;
            new_column_it = new_column_datas[new_column_row_slice_index].begin<TargetTDT>();
        }
        new_column_row_idx += step_size;
        std::advance(new_column_it, step_size);
    };

    // GIL will be acquired if there is a string that is not pure ASCII/UTF-8
    // In this case a PyObject will be allocated by convert::py_unicode_to_buffer
    // If such a string is encountered in a column, then the GIL will be held until that whole column has
    // been processed, on the assumption that if a column has one such string it will probably have many.
    std::optional<ScopedGILLock> scoped_gil_lock;

    const auto set_string_from_source = [&](size_t row) {
        if constexpr (is_sequence_type(SourceTDT::data_type())) {
            const auto source_value = get_source_value<SourceTDT>(source.data, row);
            if (is_na<SourceTDT>(source_value)) {
                *new_column_it = get_string_na_placeholder(source_value);
            } else {
                const auto [source_string, _] = get_source_string<SourceTDT>(
                        source.data,
                        row,
                        target.columns.front().column_name_,
                        source.global_row_range.first,
                        &scoped_gil_lock
                );
                *new_column_it = target.new_string_pools[new_column_row_slice_index].get(*source_string).offset();
            }
        }
    };

    const auto set_string_from_source_at = [&](size_t source_row, size_t output_row) {
        if constexpr (is_sequence_type(SourceTDT::data_type())) {
            const auto source_value = get_source_value<SourceTDT>(source.data, source_row);
            const auto [slice_index, offset_in_slice] = reslicing_info.slice_and_offset_for_row(output_row);
            if (is_na<SourceTDT>(source_value)) {
                new_column_accessors[slice_index][offset_in_slice] = get_string_na_placeholder(source_value);
            } else {
                const auto [source_string, _] = get_source_string<SourceTDT>(
                        source.data,
                        source_row,
                        target.columns.front().column_name_,
                        source.global_row_range.first,
                        &scoped_gil_lock
                );
                new_column_accessors[slice_index][offset_in_slice] =
                        target.new_string_pools[slice_index].get(*source_string).offset();
            }
        }
    };

    const auto set_string_from_target = [&](size_t target_row) {
        if constexpr (is_sequence_type(TargetTDT::data_type())) {
            const position_t offset = target_data[target_row];
            if (is_a_string(offset)) {
                const std::string_view string_data = *target.columns[target_row_slice].string_at_offset(offset);
                *new_column_it = target.new_string_pools[new_column_row_slice_index].get(string_data).offset();
            } else {
                *new_column_it = offset;
            }
        }
    };

    util::BitSet updated(reslicing_info.total_rows());
    std::vector<size_t> source_rows_to_insert;

    size_t source_row_idx = 0;
    size_t total_inserted_rows = 0;

    // Stable merge target and source into the new column. If there is a sequence of repeated index values and new
    // rows must be inserted, the new rows will appear after the rows from the target
    while (!target_index_is_exhausted() && source_row_idx < source.index.size()) {
        // Copy all target values smaller than the current source index to the output buffer
        while (!target_index_is_exhausted() && *target_index_it < source.index[source_row_idx]) {
            if constexpr (is_sequence_type(TargetTDT::data_type())) {
                set_string_from_target(target_row_idx);
            } else {
                *new_column_it = target_data[target_row_idx];
            }
            advance_output();
            advance_target();
        }

        if (target_index_is_exhausted()) {
            break;
        }

        // Target index values are equal to the source index values. Since the index is matching, it's possible to
        // perform both an update and an insert.
        // 1. Perform the update using the random accessor to the target
        // 2. Iterate over all target rows with the same index value and place them in the output, skipping the ones
        //    updated in the previous step
        // 3. Append the source rows which were not matched

        source_rows_to_insert.clear();
        const timestamp current_index_value = *target_index_it;
        // If the source has no row at the current target index value, all remaining source rows are smaller than it
        // and must be inserted before it. The target rows are left to the next iteration.
        const bool source_has_index_value = source.index[source_row_idx] == current_index_value;
        // Apply updates
        while (source_row_idx < source.index.size() && source.index[source_row_idx] == current_index_value) {
            if (strategy.update()) {
                for (size_t i = 0; i < target.columns.size(); ++i) {
                    const std::vector<size_t>& matched = match_record.matched_rows(i)[source_row_idx];
                    for (size_t target_row : matched) {
                        const size_t index_in_output = target_slice_offset[i] + total_inserted_rows + target_row -
                                                       target.range.start_row_in_first_row_slice;
                        updated.set(index_in_output);
                        if constexpr (is_sequence_type(SourceTDT::data_type())) {
                            set_string_from_source_at(source_row_idx, index_in_output);
                        } else {
                            const auto [column, offset] = reslicing_info.slice_and_offset_for_row(index_in_output);
                            new_column_accessors[column][offset] = source.data[source_row_idx];
                        }
                    }
                }
            }
            if (!match_record.is_source_row_matched(source_row_idx)) {
                source_rows_to_insert.emplace_back(source_row_idx);
            }
            ++source_row_idx;
        }
        // Place target values on non-updated output positions
        while (source_has_index_value && !target_index_is_exhausted() && *target_index_it == current_index_value) {
            if (!updated.test(output_row_idx)) {
                if constexpr (is_sequence_type(TargetTDT::data_type())) {
                    set_string_from_target(target_row_idx);
                } else {
                    *new_column_it = target_data[target_row_idx];
                }
            }
            advance_target();
            advance_output();
        }
        // Append unmatched source rows
        total_inserted_rows += source_rows_to_insert.size();
        for (size_t source_row_to_insert : source_rows_to_insert) {
            if constexpr (is_sequence_type(SourceTDT::data_type())) {
                set_string_from_source(source_row_to_insert);
            } else {
                *new_column_it = source.data[source_row_to_insert];
            }
            advance_output();
        }

        if (target_index_it == target_index_end) {
            break;
        }

        // Target index values are larger than the source index value. Append the source data to the output buffer.
        while (source_row_idx < source.index.size() && *target_index_it > source.index[source_row_idx]) {
            if constexpr (is_sequence_type(SourceTDT::data_type())) {
                set_string_from_source(source_row_idx);
            } else {
                *new_column_it = source.data[source_row_idx];
            }
            ++source_row_idx;
            advance_output();
            ++total_inserted_rows;
        }
    }
    // Not all target rows were processed, copy the rest
    while (!target_index_is_exhausted()) {
        if constexpr (is_sequence_type(TargetTDT::data_type())) {
            set_string_from_target(target_row_idx);
        } else {
            *new_column_it = target_data[target_row_idx];
        }
        advance_target();
        advance_output();
    }

    // Not all source rows were processed, copy the rest
    if constexpr (is_sequence_type(SourceTDT::data_type())) {
        for (; source_row_idx < source.index.size(); ++source_row_idx) {
            set_string_from_source(source_row_idx);
            advance_output();
        }
    } else {
        while (source_row_idx < source.index.size()) {
            const size_t free_elements_in_column = rows_in_current_slice - new_column_row_idx;
            util::check(
                    free_elements_in_column > 0,
                    "The output row slices are full but {} source rows are left to copy",
                    source.index.size() - source_row_idx
            );
            std::copy_n(source.data.begin() + source_row_idx, free_elements_in_column, new_column_it);
            source_row_idx += free_elements_in_column;
            advance_output(free_elements_in_column);
        }
    }

    return new_columns;
}

template<util::type_descriptor_tag TargetTDT, util::type_descriptor_tag SourceTDT, typename Source>
void update_column(
        const MergeStrategy& strategy, const StringPool& old_string_pool, const Source& variant_source,
        const std::span<const std::vector<size_t>> rows_to_update, [[maybe_unused]] const size_t source_row_offset,
        const bool is_matching_column, [[maybe_unused]] std::string_view column_name, Column& target_column,
        StringPool& new_string_pool
) {
    // The target type is checked before the source type because a pandas source never has fixed width strings.
    if constexpr (is_sequence_type(TargetTDT::data_type())) {
        if (is_matching_column && strategy.update_only()) {
            // Only move the data to the new string pool.
            rebuild_sequence_column_in_new_pool<TargetTDT>(target_column, old_string_pool, new_string_pool);
            return;
        }
        if constexpr (is_fixed_string_type(TargetTDT::data_type())) {
            user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    "Fixed string sequences are not supported for merge update"
            );
        }
    } else if (is_matching_column) {
        return;
    }
    if constexpr (std::same_as<TargetTDT, SourceTDT>) {
        [[maybe_unused]] util::BitSet target_rows_not_matched_by_source = [&]() {
            if constexpr (is_sequence_type(TargetTDT::data_type())) {
                util::BitSet result(target_column.row_count());
                result.flip();
                return result;
            }
            return util::BitSet{};
        }();
        [[maybe_unused]] std::optional<ScopedGILLock> gil;
        util::variant_match(variant_source, [&](const auto source) {
            ColumnData target_column_data = target_column.data();
            auto target = random_accessor<TargetTDT>(&target_column_data);
            for (size_t source_row_idx = 0; source_row_idx < rows_to_update.size(); ++source_row_idx) {
                for (const size_t target_row_idx : rows_to_update[source_row_idx]) {
                    const auto source_value = get_source_value<SourceTDT>(source, source_row_idx);
                    if constexpr (is_sequence_type(SourceTDT::data_type())) {
                        if (is_na<SourceTDT>(source_value)) {
                            target[target_row_idx] = get_string_na_placeholder(source_value);
                        } else {
                            auto [source_string, opt_owner] = get_source_string<SourceTDT>(
                                    source, source_row_idx, column_name, source_row_offset, &gil
                            );
                            target[target_row_idx] = new_string_pool.get(*source_string).offset();
                        }
                        target_rows_not_matched_by_source.set(target_row_idx, false);
                    } else {
                        target[target_row_idx] = source_value;
                    }
                }
            }
        });
        if constexpr (is_sequence_type(TargetTDT::data_type())) {
            rebuild_sequence_column_in_new_pool<TargetTDT>(
                    target_column, old_string_pool, new_string_pool, &target_rows_not_matched_by_source
            );
        }
    }
}

RowRange get_row_range(std::span<const ProcessingUnit> row_slice) {
    return {row_slice.front().row_ranges_->front()->first, row_slice.back().row_ranges_->back()->second};
}

std::optional<ssize_t> first_different_index_value_position(const ColumnData index) {
    using IndexType = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
    auto it = index.cbegin<IndexType, IteratorType::ENUMERATED>();
    const timestamp first_source_row = it->value();
    const auto end = index.cend<IndexType, IteratorType::ENUMERATED>();
    const auto first_different_position = exponential_upper_bound(++it, end, first_source_row);
    return end == first_different_position ? std::nullopt : std::optional{first_different_position->idx()};
};

TargetRange get_target_start_end(std::span<const ProcessingUnit> row_slices) {
    TargetRange result{
            .start_row_in_first_row_slice = 0,
            .end_row_in_last_row_slice = row_slices.back().segments_->back()->row_count()
    };
    if (row_slices.front().entity_fetch_count_) {
        if (row_slices.front().entity_fetch_count_->front() > 1) {
            const ColumnData index = row_slices.front().segments_->front()->column(0).data();
            const std::optional<ssize_t> first_different = first_different_index_value_position(index);
            util::check(
                    first_different.has_value(),
                    "A row slice shared between two processing units cannot consist of a single index value"
            );
            result.start_row_in_first_row_slice = *first_different;
        }
        if (row_slices.size() > 1 && row_slices.back().entity_fetch_count_->back() > 1) {
            const ColumnData index = row_slices.back().segments_->back()->column(0).data();
            result.end_row_in_last_row_slice = first_different_index_value_position(index).value_or(
                    row_slices.back().segments_->back()->row_count()
            );
        }
    }
    return result;
}

/// For each row of source that falls in the row slice in proc find all rows whose index matches the source index
/// value. The matching rows will be sorted in increasing order. Since both source and target are timestamp indexed
/// and ordered, only forward iteration on both source and target is needed and binary search can be used to check
/// if a source index value exists in the target index. At the end some vectors in the output can be empty which
/// means that that particular row in source did not match anything in the target. It is allowed for one row in
/// target to be matched by multiple rows in source only if MergeUpdateClause::on_ is not empty. If
/// MergeUpdateClause::on_ is not empty, there will be further filtering that might remove some matches. Otherwise,
/// one row will be updated multiple times, which is not allowed.
///
/// Complexity: $$O(m * log(n/m) + n)$$ where: m is the count of source rows in the bounds of the segment, n is the
/// number of target rows in the segment.
MergeUpdateClause::MatchRecord filter_index_match(
        std::span<const timestamp> source_index, std::span<ProcessingUnit> row_slices
) {
    using IndexType = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
    MergeUpdateClause::MatchRecord result(row_slices, source_index.size());
    if (source_index.empty()) {
        return result;
    }
    size_t source_row_start_for_next_slice{};
    size_t source_row{};
    for (size_t i = 0; i < row_slices.size(); ++i) {
        const ProcessingUnit& proc = row_slices[i];
        const Column& target_index = proc.segments_->front()->column(0);
        ColumnData target_index_column_data = target_index.data();
        auto last_target_row_it = exponential_upper_bound<IndexType, IteratorType::ENUMERATED>(
                target_index_column_data, source_index.back()
        );
        auto target_row_it = target_index_column_data.cbegin<IndexType, IteratorType::ENUMERATED>();
        if (target_row_it->value() == source_index[source_row_start_for_next_slice]) {
            source_row = source_row_start_for_next_slice;
        }

        // This loop can be inverted so that if source_row_end - source_row_start is > len(target) the complexity
        // becomes O(n * log_2(m/n) + m) where: m is the count of source rows in the bounds of the segment, n is the
        // number of target rows in the segment.
        while (target_row_it != last_target_row_it && source_row < source_index.size()) {
            const timestamp source_ts = source_index[source_row];
            auto target_match_it = exponential_lower_bound(target_row_it, last_target_row_it, source_ts);
            if (target_match_it == last_target_row_it) {
                break;
            }
            source_row_start_for_next_slice = source_row;
            target_row_it = target_match_it;
            while (target_row_it != last_target_row_it && target_row_it->value() == source_ts) {
                result.add_match(source_row, i, target_row_it->idx());
                ++target_row_it;
            }
            ++source_row;
            // Optimizes the case of repeated index values. All matched rows corresponding to a particular index
            // value must be the same, so just copy the matched rows in case index values are repeated.
            while (source_row < source_index.size() && source_index[source_row] == source_ts) {
                result.clone_source_match(source_row - 1, source_row, i);
                ++source_row;
            }
        }
    }

    return result;
}

std::span<const timestamp>::iterator source_range_start_for_group(
        std::span<const timestamp> source_index, timestamp segment_start, size_t row_slice_group,
        const MergeStrategy& strategy
) {
    if (strategy.insert() && row_slice_group == 0 && source_index.front() < segment_start) {
        // When inserting, all source data before the first row slice is prepended to it.
        return source_index.begin();
    }
    return std::ranges::lower_bound(source_index, segment_start);
}

std::span<const timestamp>::iterator source_range_end_for_group(
        std::span<const timestamp> source_index, std::span<const timestamp>::iterator source_range_start,
        timestamp segment_end, size_t row_slice_group, const std::vector<std::vector<size_t>>& offsets,
        const std::vector<RangesAndKey>& ranges_and_keys, const MergeStrategy& strategy
) {
    // When inserting, unmatched source values in the gap before the next row slice are appended to this one, and
    // the last row slice group takes all remaining source values. std::max keeps at least the segment's own end in
    // case the next row slice starts before this one ends (overlapping time slices).
    if (strategy.insert() && row_slice_group == offsets.size() - 1) {
        return source_index.end();
    }
    const timestamp effective_segment_end =
            strategy.insert()
                    ? std::max(
                              segment_end, ranges_and_keys[offsets[row_slice_group + 1].front()].key_.time_range().first
                      )
                    : segment_end;
    return std::ranges::upper_bound(source_range_start, source_index.end(), effective_segment_end - 1);
}

size_t compute_total_upsert_row_count(
        std::span<const ColumnWithStrings> target_index_datas, const TargetRange& target_range,
        const size_t unmatched_source_rows
) {
    // One index value can appear in more than one row slice. In that case it can be shared by two processing units,
    // each working on part of the target data.
    const size_t num_rows_out_of_target_range =
            target_range.start_row_in_first_row_slice +
            (target_index_datas.back().column_->row_count() - target_range.end_row_in_last_row_slice);
    return std::accumulate(
                   target_index_datas.begin(),
                   target_index_datas.end(),
                   unmatched_source_rows,
                   [](size_t acc, const ColumnWithStrings& col) { return acc + col.column_->row_count(); }
           ) -
           num_rows_out_of_target_range;
}

void initialize_col_slices(
        const StreamDescriptor& descriptor, const std::span<const std::shared_ptr<Column>> new_indexes,
        std::span<ProcessingUnit> dest
) {
    for (auto&& [row_slice_idx, row_slice] : folly::enumerate(dest)) {
        const std::shared_ptr<Column>& index_col = new_indexes[row_slice_idx];
        row_slice.segments_->emplace_back(std::make_shared<SegmentInMemory>(descriptor, index_col->row_count()));
        row_slice.segments_->back()->columns()[0] = index_col;
    }
}

void finalize_col_slices(
        const std::span<const std::shared_ptr<Column>> new_indexes, std::vector<StringPool>* new_string_pools,
        std::span<ProcessingUnit> dest
) {
    for (auto&& [row_slice_idx, row_slice] : folly::enumerate(dest)) {
        row_slice.segments_->back()->set_row_data(new_indexes[row_slice_idx]->row_count() - 1);
        if (new_string_pools) {
            row_slice.segments_->back()->string_pool() = std::move((*new_string_pools)[row_slice_idx]);
            (*new_string_pools)[row_slice_idx].clear();
        }
    }
}

void set_upsert_ranges(
        const TargetRange& target_range, const std::span<const ProcessingUnit> input_row_slices,
        std::span<ProcessingUnit> dest
) {
    const size_t num_col_slices = input_row_slices.begin()->col_ranges_->size();
    // Index key is merged in version_core.cpp::merge_update_impl. Since there are multiple parallel writes and the
    // different processing units are not aware of how many rows were added before we cannot emit "final row
    // ranges". The row ranges this clause emits are in the "coordinate system" of the unmodified target (meaning
    // they don't account for insertion). Setting all resulting row ranges to the same values means: "The data that
    // was originally in range row_range must be replaced by the concatenation of all new row ranges that have
    // row_range set"
    const auto row_range = std::make_shared<RowRange>(
            input_row_slices.front().row_ranges_->front()->first + target_range.start_row_in_first_row_slice,
            input_row_slices.back().row_ranges_->back()->first + target_range.end_row_in_last_row_slice
    );
    for (ProcessingUnit& row_slice : dest) {
        row_slice.row_ranges_ = std::vector(num_col_slices, row_range);
        row_slice.col_ranges_ = input_row_slices.front().col_ranges_;
    }
}

} // namespace

namespace arcticdb {

namespace ranges = std::ranges;
using namespace pipelines;

MergeUpdateClause::MergeUpdateClause(
        std::vector<std::string>&& on, MergeStrategy strategy, std::shared_ptr<InputFrame> source,
        size_t rows_per_segment
) :

    on_(std::move(on)),
    strategy_(strategy),
    source_(std::move(source)),
    rows_per_segment_(rows_per_segment) {
    std::erase_if(on_, [&](const std::string& column) { return !on_set_.insert(column).second; });
    const bool is_source_arrow = !source_->has_only_tensors();
    if (is_source_arrow) {
        // WriteToSegmentTask adds the index column itself, so the column range covers only the data columns.
        FrameSlice full_slice{
                std::make_shared<StreamDescriptor>(source_->desc().clone()),
                ColRange{source_->desc().index().field_count(), source_->desc().field_count()},
                RowRange{0, source_->num_rows}
        };
        source_as_segment_ = std::get<1>(WriteToSegmentTask(source_, std::move(full_slice))());
        for (size_t col = 0; col < source_as_segment_.num_columns(); ++col) {
            schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                    !source_as_segment_.column(col).is_sparse(),
                    "Merge update does not support Arrow source data with null values yet. Column \"{}\" contains "
                    "nulls.",
                    source_as_segment_.field(col).name()
            );
        }
    }
}

std::vector<std::vector<size_t>> MergeUpdateClause::structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys
) {
    if (ranges_and_keys.empty()) {
        return {};
    }
    if (!source_->has_index()) {
        return structure_by_row_slice(ranges_and_keys);
    }
    return structure_for_processing_log(ranges_and_keys);
}

std::vector<std::vector<size_t>> MergeUpdateClause::structure_for_processing_log(
        std::vector<RangesAndKey>& ranges_and_keys
) {

    std::vector<std::vector<size_t>> offsets = must_structure_by_time_slice() ? structure_by_time_slice(ranges_and_keys)
                                                                              : structure_by_row_slice(ranges_and_keys);
    std::vector<size_t> row_slice_groups_to_keep;
    std::span<const timestamp> source_index = get_source_column<IndexType>(0);
    const bool source_ends_before_first_row_slice =
            source_index.back() < ranges_and_keys.begin()->key_.time_range().first;
    const bool source_starts_after_the_last_row_slice =
            source_index.front() >= ranges_and_keys.back().key_.time_range().second;
    if (strategy_.update_only() && (source_ends_before_first_row_slice || source_starts_after_the_last_row_slice)) {
        ranges_and_keys.clear();
        return {};
    }

    for (size_t row_slice_group = 0; row_slice_group < offsets.size(); ++row_slice_group) {
        const TimestampRange time_range{
                ranges_and_keys.at(offsets.at(row_slice_group).front()).key_.time_range().first,
                ranges_and_keys.at(offsets.at(row_slice_group).back()).key_.time_range().second
        };

        const auto source_range_start =
                source_range_start_for_group(source_index, time_range.first, row_slice_group, strategy_);
        if (source_range_start == source_index.end()) {
            // All remaining row slice groups start after the last source index value, so no match is possible.
            break;
        }

        const auto source_range_end = source_range_end_for_group(
                source_index,
                source_range_start,
                time_range.second,
                row_slice_group,
                offsets,
                ranges_and_keys,
                strategy_
        );
        if (source_range_start == source_range_end) {
            // This row slice group owns no source rows.
            continue;
        }
        const std::pair<size_t, size_t> source_row_range = {
                source_range_start - source_index.begin(), source_range_end - source_index.begin()
        };
        const RowRange row_range{
                ranges_and_keys[offsets[row_slice_group].front()].row_range().first,
                ranges_and_keys[offsets[row_slice_group].back()].row_range().second
        };
        source_start_end_for_row_range_.insert({row_range, source_row_range});
        row_slice_groups_to_keep.push_back(row_slice_group);
    }
    filter_selected_ranges_and_keys_and_reindex_entities(row_slice_groups_to_keep, offsets, ranges_and_keys);
    return offsets;
}

std::vector<std::vector<EntityId>> MergeUpdateClause::structure_for_processing(std::vector<std::vector<EntityId>>&&) {
    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("MergeUpdate clause should be the first clause in the pipeline");
}

/// Decide which target rows are updated and which source rows are inserted.
/// 1. If there is a timestamp index, MergeUpdateClause::match uses filter_index_match to build a MatchRecord: for each
/// source row that falls in the processed slice it records the target rows whose index value matches.
/// 2. For each column in MergeUpdateClause::on_, filter_on_additional_columns_match prunes the MatchRecord, dropping
/// any recorded target row whose value in that column differs from the source row's.
/// The ordering of the columns in MergeUpdateClause::on_ therefore matters: starting with the columns least likely to
/// match prunes candidates earlier and is more efficient.
std::vector<EntityId> MergeUpdateClause::process(std::vector<EntityId>&& entity_ids) const {
    if (entity_ids.empty()) {
        return {};
    }
    auto proc = gather_entities<
            std::shared_ptr<SegmentInMemory>,
            std::shared_ptr<RowRange>,
            std::shared_ptr<ColRange>,
            std::shared_ptr<AtomKey>,
            EntityFetchCount>(*component_manager_, std::move(entity_ids));
    for (const std::shared_ptr<SegmentInMemory>& segment : *proc.segments_) {
        for (size_t col = 0; col < segment->num_columns(); ++col) {
            schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                    !segment->column(col).is_sparse(),
                    "Merge update does not support sparse target data yet. Column \"{}\" is sparse.",
                    segment->field(col).name()
            );
        }
    }
    std::vector<ProcessingUnit> row_slices =
            must_structure_by_time_slice() ? split_by_row_slice(std::move(proc)) : std::vector{std::move(proc)};
    const std::pair<size_t, size_t> source_start_end = get_source_start_end(row_slices);
    MatchRecord matched = match(row_slices, source_start_end);
    matched.validate_rows_to_update(strategy_);
    if (strategy_.update_only()) {
        if (!matched.has_matched_target_rows()) {
            // No target row in this row slice changes, so emit nothing. merge_slices_and_keys keeps the
            // existing data keys for row slices that are not re-emitted.
            return {};
        }
        auto new_row_slices = update(matched, std::move(row_slices), source_start_end);
        std::vector<EntityId> res;
        for (ProcessingUnit& row_slice : new_row_slices) {
            const size_t entity_count = row_slice.segments_->size();
            const MergeUpdateRowSlicingInfoComponent row_slice_info(1, 0, row_slice.segments_->front()->row_count());
            std::vector<EntityId> entts = component_manager_->add_entities(
                    std::move(*row_slice.segments_),
                    std::move(*row_slice.row_ranges_),
                    std::move(*row_slice.col_ranges_),
                    std::vector<EntityFetchCount>(entity_count, 1),
                    std::vector(entity_count, row_slice_info)
            );
            res.insert(res.end(), std::make_move_iterator(entts.begin()), std::make_move_iterator(entts.end()));
        }
        return res;
    }
    const StreamDescriptor& target_descriptor = get_source_descriptor(); // TODO: Not true for dynamic schema

    if (target_descriptor.index().type() == IndexDescriptor::Type::ROWCOUNT) {
        MergeUpdateNotMatchedSourceRowsComponent unmatched_source_rows_component(
                std::make_shared<util::BitSet>(matched.unmatched_source_rows())
        );
        if (strategy_.update() && matched.has_matched_target_rows()) {
            // Update is requested and there are matched rows.
            std::vector<ProcessingUnit> new_row_slices = update(matched, std::move(row_slices), source_start_end);
            util::check(
                    new_row_slices.size() == 1,
                    "Row range indexed data must produce exactly one row slice as a result of Merge Update"
            );
            std::vector<EntityId> res;
            for (ProcessingUnit& row_slice : new_row_slices) {
                const size_t entity_count = row_slice.segments_->size();
                const MergeUpdateRowSlicingInfoComponent row_slice_info(
                        1, 0, row_slice.segments_->front()->row_count()
                );
                std::vector<EntityId> entts = component_manager_->add_entities(
                        std::move(*row_slice.segments_),
                        std::move(*row_slice.row_ranges_),
                        std::move(*row_slice.col_ranges_),
                        std::vector<EntityFetchCount>(entity_count, 1),
                        std::vector(entity_count, row_slice_info),
                        std::vector(entity_count, unmatched_source_rows_component)
                );
                res.insert(res.end(), std::make_move_iterator(entts.begin()), std::make_move_iterator(entts.end()));
            }
            return res;
        }
        // Either there are no matched rows or this is insert only scenario. Either way we must not produce any
        // output for the next clause (Write Clause) because this did nothing. However we must store the matched
        // rows set so that after the pipeline ends insertion can be performed. Create a dummy entity holding
        // only the set
        component_manager_->add_entities(std::vector{std::move(unmatched_source_rows_component)});
        return {};
    }

    auto new_row_slices = update_and_insert(matched, target_descriptor, std::move(row_slices), source_start_end);

    std::vector<EntityId> res;
    for (auto&& [row_slice_idx, row_slice] : folly::enumerate(new_row_slices)) {
        const MergeUpdateRowSlicingInfoComponent row_slice_info(
                static_cast<int>(new_row_slices.size()),
                static_cast<int>(row_slice_idx),
                row_slice.segments_->front()->row_count()
        );
        const size_t entity_count = row_slice.segments_->size();
        std::vector<EntityId> entts = component_manager_->add_entities(
                std::move(*row_slice.segments_),
                std::move(*row_slice.row_ranges_),
                std::move(*row_slice.col_ranges_),
                std::vector<EntityFetchCount>(entity_count, 1),
                std::vector(entity_count, row_slice_info)
        );
        res.insert(res.end(), std::make_move_iterator(entts.begin()), std::make_move_iterator(entts.end()));
    }
    return res;
}

MergeUpdateClause::MatchRecord MergeUpdateClause::initialize_rows_to_update_for_row_range_indexed_data(
        std::span<ProcessingUnit> row_slices, const StreamDescriptor& source_descriptor,
        std::pair<size_t, size_t> source_start_end
) const {
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            !on_.empty(),
            "MergeUpdate requires at least one column in the \"on\" parameter when the DataFrame is not a "
            "timeseries"
    );
    const size_t source_row_count = source_start_end.second - source_start_end.first;
    MatchRecord result(row_slices, source_row_count);
    // GIL will be acquired if there is a string that is not pure ASCII/UTF-8
    // In this case a PyObject will be allocated by convert::py_unicode_to_buffer
    // If such a string is encountered in a column, then the GIL will be held until that whole column has
    // been processed, on the assumption that if a column has one such string it will probably have many.
    std::optional<ScopedGILLock> scoped_gil_lock;
    for (size_t row_slice_idx = 0; row_slice_idx < row_slices.size(); ++row_slice_idx) {
        ProcessingUnit& proc = row_slices[row_slice_idx];
        const std::string_view column_name = on_.front();
        const size_t source_field_position = field_index_for_matching_on_column(column_name, source_descriptor);
        const Field& source_field = source_descriptor.field(source_field_position);
        details::visit_type(source_field.type().data_type(), [&](auto source_field_dt) {
            using SourceTDT = ScalarTagType<std::decay_t<decltype(source_field_dt)>>;
            const ColumnWithStrings target_column = std::get<ColumnWithStrings>(proc.get(ColumnName{column_name}));
            details::visit_type(target_column.column_->type().data_type(), [&](auto target_field_dt) {
                using TargetTDT = ScalarTagType<decltype(target_field_dt)>;

                if constexpr (!std::same_as<std::decay_t<SourceTDT>, std::decay_t<TargetTDT>>) {
                    schema::raise<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                            "Source type {} does not match target type {}. Dynamic schema is not implemented yet",
                            SourceTDT::data_type(),
                            TargetTDT::data_type()
                    );
                } else {
                    auto target_values_to_rows =
                            map_column_values_to_rows<TargetTDT>(target_column, strategy_.match_na);
                    util::variant_match(get_source_column<SourceTDT>(source_field_position), [&](auto source_data) {
                        for (size_t source_row_idx = 0; source_row_idx < source_row_count; ++source_row_idx) {
                            auto source_value = get_source_value<SourceTDT>(source_data, source_row_idx);
                            if (is_na<SourceTDT>(source_value) && !strategy_.match_na) {
                                continue;
                            }
                            if constexpr (is_sequence_type(SourceTDT::data_type())) {
                                auto [source_string, opt_owner] = get_source_string<SourceTDT>(
                                        source_data, source_row_idx, column_name, 0, &scoped_gil_lock
                                );
                                result.add_match(source_row_idx, row_slice_idx, target_values_to_rows[source_string]);
                            } else {
                                result.add_match(source_row_idx, row_slice_idx, target_values_to_rows[source_value]);
                            }
                        }
                    });
                }
            });
        });
    }

    return result;
}

std::pair<size_t, size_t> MergeUpdateClause::get_source_start_end(std::span<const ProcessingUnit> row_slices) const {
    if (source_->has_index()) {
        const pipelines::RowRange row_range = get_row_range(row_slices);
        const auto source_row_range_it = source_start_end_for_row_range_.find(row_range);
        util::check(
                source_row_range_it != source_start_end_for_row_range_.end(),
                "Source row range for row range [{}, {}] not found",
                row_range.first,
                row_range.second
        );
        const std::pair<size_t, size_t> source_range = source_row_range_it->second;
        std::pair<size_t, size_t> result = source_range;
        std::span<const timestamp> source_index = get_source_column<IndexType>(0, source_range);
        // A shared boundary row slice (entity_fetch_count > 1) is co-owned with a neighbouring group; trim the source
        // range to the portion this processing unit owns. If neither boundary is shared both branches are skipped and
        // the full source range is returned.
        if (row_slices.front().entity_fetch_count_->front() > 1) {
            // The index value spanning multiple row slices appears at the beginning of the row slices, which means that
            // the other processing unit will handle all rows of the segment containing the index value; this processing
            // unit must handle only the rows that do not contain index value. Example:
            //     0         1         2         3
            // [a, b, c] [c, c, c] [c, d, e] [e, f, g]
            // When the source contains value "c," structure_by_time_slice produces [0, 1, 2] [2, 3] [3]; the last group
            // is dropped because it does not contain "c," leaving [0, 1, 2] [2, 3] to process. This case handles the
            // group [2, 3].
            auto next_index_value =
                    std::ranges::upper_bound(source_index, row_slices.front().atom_keys_->front()->time_range().first);
            result.first += next_index_value - source_index.begin();
        }
        if (row_slices.size() > 1 && row_slices.back().entity_fetch_count_->back() > 1) {
            // The index value spanning multiple row slices appears at the end of the row slices, which means that the
            // other processing unit will handle all rows of the segment not containing the index value; this processing
            // unit must handle only the rows that contain index value. Example:
            //     0         1         2         3
            // [a, b, c] [c, c, c] [c, d, e] [e, f, g]
            // When the source contains value "c," structure_by_time_slice produces [0, 1, 2] [2, 3] [3]; the last group
            // is dropped because it does not contain "c," leaving [0, 1, 2] [2, 3] to process. This case handles the
            // group [0, 1, 2].
            const auto last_index_value =
                    std::ranges::upper_bound(source_index, row_slices.back().atom_keys_->back()->time_range().first);
            result.second = source_range.first + (last_index_value - source_index.begin());
        }
        return result;
    } else {
        return std::pair{0, source_->num_rows};
    }
}

std::vector<ProcessingUnit> MergeUpdateClause::update_and_insert(
        const MatchRecord& match_record, const StreamDescriptor& target_descriptor,
        std::vector<ProcessingUnit>&& row_slices, std::pair<size_t, size_t> source_start_end
) const {
    ARCTICDB_DEBUG_CHECK(
            ErrorCode::E_ASSERTION_FAILURE,
            strategy_.insert(),
            "MergeUpdateClause::update_and_insert should only be called for strategies that allow insertion"
    );
    // A group with nothing to insert can be left untouched only if it is self-contained. If it
    // co-owns a boundary slice (entity_fetch_count > 1) with a neighbouring group that does insert,
    // that neighbour rewrites part of the shared slice; this group must therefore also re-emit its
    // owned portion, otherwise the stale old slice overlaps the neighbour's new slice during index
    // reconstruction (merge_slices_and_keys) and produces a non-contiguous, unreadable index.
    // Only the first and last row slices of a group can be shared with a neighbour (a group is a
    // contiguous window of row slices), matching how get_source_start_end/get_target_start_end trim.
    const bool co_owns_shared_slice =
            row_slices.front().entity_fetch_count_->front() > 1 || row_slices.back().entity_fetch_count_->back() > 1;
    if (strategy_.insert_only() && match_record.total_unmatched_source_rows() == 0 && !co_owns_shared_slice) {
        // Nothing to insert and no match to update: leave the group untouched and emit nothing.
        // merge_slices_and_keys keeps the existing data keys for row slices that are not re-emitted.
        return {};
    }
    const std::span<const timestamp> source_index = get_source_column<IndexType>(0, source_start_end);
    const StreamDescriptor source_descriptor = get_source_descriptor();
    const size_t num_col_slices = row_slices.begin()->col_ranges_->size();
    ARCTICDB_DEBUG_CHECK(
            ErrorCode::E_ASSERTION_FAILURE,
            std::ranges::all_of(
                    row_slices, [&](const auto& proc) { return proc.col_ranges_->size() == num_col_slices; }
            ),
            "All row slices should have the same number of column ranges"
    );

    std::vector<ColumnWithStrings> target_datas;
    std::vector<ColumnWithStrings> target_index_datas;
    target_index_datas.reserve(row_slices.size());
    std::ranges::transform(row_slices, std::back_inserter(target_index_datas), [&](const auto& proc) {
        return ColumnWithStrings(proc.segments_->front()->column_ptr(0), nullptr, target_descriptor.field(0).name());
    });
    bool has_string_column_in_column_slice = false;
    const TargetRange target_range = get_target_start_end(row_slices);
    using IndexType = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
    const size_t total_upsert_row_count = compute_total_upsert_row_count(
            target_index_datas, target_range, match_record.total_unmatched_source_rows()
    );
    util::check(total_upsert_row_count > 0, "Merge update produced a row slice group containing no rows");
    const ReslicingInfo reslicing_info{total_upsert_row_count, max_rows_per_segment(rows_per_segment_)};
    std::vector<ProcessingUnit> result(reslicing_info.num_segments());
    for (ProcessingUnit& proc : result) {
        proc.segments_.emplace(util::reserve_vector<std::shared_ptr<SegmentInMemory>>(num_col_slices));
    }
    std::vector<StringPool> new_string_pools(reslicing_info.num_segments());
    std::vector<std::shared_ptr<Column>> new_indexes = merge<IndexType, IndexType>(
            InsertSourceData{.index = source_index, .data = source_index, .global_row_range = source_start_end},
            InsertTargetData{
                    .indexes = target_index_datas,
                    .columns = target_index_datas,
                    .type = IndexType::type_descriptor(),
                    .range = target_range,
                    .new_string_pools = new_string_pools
            },
            match_record,
            MergeStrategy{.not_matched_by_target = MergeAction::INSERT},
            reslicing_info
    );
    if (target_descriptor.field_count() == target_descriptor.index().field_count()) {
        // Handle degenerate case of index-only dataframe
        initialize_col_slices((*row_slices.front().segments_)[0]->descriptor(), new_indexes, result);
        finalize_col_slices(new_indexes, nullptr, result);
    } else {
        size_t col_slice_idx = 0;
        for (size_t field_idx = target_descriptor.index().field_count(); field_idx < target_descriptor.field_count();
             ++field_idx) {
            const Field& target_field = target_descriptor.field(field_idx);
            const std::string_view column_name = target_field.name();
            target_datas.clear();
            std::ranges::transform(row_slices, std::back_inserter(target_datas), [&](ProcessingUnit& row_slice) {
                return std::get<ColumnWithStrings>(row_slice.get(ColumnName{column_name}));
            });
            std::vector<std::shared_ptr<Column>> new_column_slices =
                    details::visit_type(target_field.type().data_type(), [&]<typename TypeTag>(TypeTag) {
                        using TargetDataTDT = ScalarTagType<TypeTag>;
                        has_string_column_in_column_slice |= is_sequence_type(TargetDataTDT::data_type());
                        const Field& source_field = get_source_descriptor().field(field_idx);
                        return details::visit_type(
                                source_field.type().data_type(),
                                [&]<typename SourceTypeTag>(SourceTypeTag) -> std::vector<std::shared_ptr<Column>> {
                                    using SourceDataTDT = ScalarTagType<SourceTypeTag>;
                                    if constexpr (std::same_as<SourceDataTDT, TargetDataTDT>) {
                                        // By construction, we cannot update the columns used to perform the match; only
                                        // inserts are allowed
                                        const auto strategy =
                                                on_set_.contains(column_name)
                                                        ? MergeStrategy{.not_matched_by_target = MergeAction::INSERT}
                                                        : strategy_;
                                        const InsertTargetData target_data{
                                                .indexes = target_index_datas,
                                                .columns = target_datas,
                                                .type = target_field.type(),
                                                .range = target_range,
                                                .new_string_pools = new_string_pools
                                        };
                                        return util::variant_match(
                                                get_source_column<SourceDataTDT>(field_idx, source_start_end),
                                                [&](const auto source) {
                                                    const auto source_data = InsertSourceData{
                                                            .index = source_index,
                                                            .data = source,
                                                            .global_row_range = source_start_end
                                                    };
                                                    return merge<TargetDataTDT, SourceDataTDT>(
                                                            source_data,
                                                            target_data,
                                                            match_record,
                                                            strategy,
                                                            reslicing_info
                                                    );
                                                }
                                        );
                                    } else {
                                        // This can't be reached. But if it's missing the warning for not returning
                                        // from non-void function will be triggered
                                        internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Incompatible types");
                                    }
                                }
                        );
                    });
            // Start working on new column slice.
            if (field_idx == (*row_slices.front().col_ranges_)[col_slice_idx]->first) {
                initialize_col_slices(
                        (*row_slices.front().segments_)[col_slice_idx]->descriptor(), new_indexes, result
                );
            }
            // For each row slice set the corresponding column.
            const size_t col_in_slice = field_idx - (*row_slices.front().col_ranges_)[col_slice_idx]->first + 1;
            for (auto&& [row_slice_idx, row_slice] : folly::enumerate(result)) {
                row_slice.segments_->back()->columns()[col_in_slice] = std::move(new_column_slices[row_slice_idx]);
            }

            // The last column in the column slice is processed. Finish working on th segment by setting the string pool
            // and the row data.
            if (field_idx == (*row_slices.front().col_ranges_)[col_slice_idx]->second - 1) {
                finalize_col_slices(
                        new_indexes, has_string_column_in_column_slice ? &new_string_pools : nullptr, result
                );
                ++col_slice_idx;
                has_string_column_in_column_slice = false;
            }
        }
    }

    set_upsert_ranges(target_range, row_slices, result);
    return std::vector{std::move(result)};
}

std::vector<ProcessingUnit> MergeUpdateClause::update(
        const MatchRecord& match_record, std::vector<ProcessingUnit>&& row_slices,
        std::pair<size_t, size_t> source_start_end
) const {
    std::vector<ProcessingUnit> result;
    const StreamDescriptor& source_descriptor = get_source_descriptor();
    for (size_t i = 0; i < row_slices.size(); ++i) {
        const ProcessingUnit& proc = row_slices[i];
        const std::span<const std::shared_ptr<SegmentInMemory>> target_segments = *proc.segments_;
        // Update one column at a time to increase cache coherency and to avoid calling visit_field for each row
        // being updated
        size_t source_field_pos = source_descriptor.index().field_count();
        for (size_t segment_idx = 0; segment_idx < target_segments.size(); ++segment_idx) {
            StringPool new_string_pool;
            bool segment_contains_string_column = false;
            SegmentInMemory& target_segment = *target_segments[segment_idx];
            const size_t slice_size = target_segment.num_columns();
            const size_t index_fields = target_segment.descriptor().index().field_count();
            for (size_t column_index_in_slice = index_fields; column_index_in_slice < slice_size;
                 ++column_index_in_slice, ++source_field_pos) {
                const Field& target_field = target_segment.descriptor().field(column_index_in_slice);
                const Field& source_field = source_descriptor.field(source_field_pos);
                details::visit_type(
                        target_field.type().data_type(),
                        [&]<typename TargetDataTypeTag>(TargetDataTypeTag) {
                            details::visit_type(
                                    source_field.type().data_type(),
                                    [&]<typename SourceDataTypeTag>(SourceDataTypeTag) {
                                        using TargetTDT = ScalarTagType<TargetDataTypeTag>;
                                        using SourceTDT = ScalarTagType<SourceDataTypeTag>;
                                        Column& target_column = target_segment.column(column_index_in_slice);
                                        const auto source =
                                                get_source_column<SourceTDT>(source_field_pos, source_start_end);
                                        std::span<const std::vector<size_t>> rows_to_update =
                                                match_record.matched_rows(i);
                                        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                                                !rows_to_update.empty(),
                                                "There must be at least one source row inside the target row slice."
                                        );
                                        // String columns are always recreated from scratch regardless if an update or
                                        // insert is happening. This is because the string pool must be updated. With
                                        // data read from disk, the map_ member of the pool is not populated. Which
                                        // means that the mapping between a string and offset in the pool is missing. To
                                        // get it, we need to rebuild the pool anyway.
                                        segment_contains_string_column |=
                                                is_sequence_type(TargetDataTypeTag::data_type);
                                        update_column<TargetTDT, SourceTDT>(
                                                strategy_,
                                                target_segment.string_pool(),
                                                source,
                                                rows_to_update,
                                                source_start_end.first,
                                                on_set_.contains(target_field.name()),
                                                target_field.name(),
                                                target_column,
                                                new_string_pool
                                        );
                                    }
                            );
                        }
                );
            }
            if (segment_contains_string_column) {
                target_segment.set_string_pool(std::make_shared<StringPool>(std::move(new_string_pool)));
            }
        }
        result.emplace_back(std::move(row_slices[i]));
    }

    return result;
}

MergeUpdateClause::MatchRecord MergeUpdateClause::match(
        std::span<ProcessingUnit> row_slices, std::pair<size_t, size_t> source_start_end
) const {
    std::optional<MatchRecord> maybe_index_match;
    if (source_->has_index()) {
        maybe_index_match.emplace(filter_index_match(get_source_column<IndexType>(0, source_start_end), row_slices));
    }
    return filter_on_additional_columns_match(
            get_source_descriptor(), get_source_descriptor(), row_slices, std::move(maybe_index_match), source_start_end
    );
}

bool MergeUpdateClause::is_source_arrow() const { return !source_->has_only_tensors(); }

// The converted Arrow segment can store a column with a different type than the Arrow input, e.g. strings become
// UTF_DYNAMIC64, so for Arrow sources the segment's descriptor describes the data the clause actually reads.
const StreamDescriptor& MergeUpdateClause::get_source_descriptor() const {
    return is_source_arrow() ? source_as_segment_.descriptor() : source_->desc();
}

/// Complexity: $$O(c * n * m)$$ m is the count of source rows in the bounds of the segment, n is the  number of target
/// rows in the segment. c is the number of columns in MergeUpdateClause::on_. It can be reached if the data in source
/// and target is the same up to the very last column in MergeUpdateClause::on_.
MergeUpdateClause::MatchRecord MergeUpdateClause::filter_on_additional_columns_match(
        const StreamDescriptor& source_descriptor, const StreamDescriptor& target_descriptor,
        std::span<ProcessingUnit> row_slices, std::optional<MatchRecord>&& index_match,
        std::pair<size_t, size_t> source_start_end
) const {
    ranges::subrange on = on_;
    MatchRecord matched_rows = [&] {
        const IndexDescriptor::Type source_index_type = source_descriptor.index().type();
        const IndexDescriptor::Type target_index_type = target_descriptor.index().type();
        if (index_match) {
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    (source_index_type == target_index_type) && (source_index_type == IndexDescriptor::Type::TIMESTAMP),
                    "Source and target index types must both be TIMESTAMP. Source: {}, target: {}",
                    source_index_type,
                    target_index_type
            );
            return std::move(*index_match);
        }
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                (source_index_type == target_index_type) && (source_index_type == IndexDescriptor::Type::ROWCOUNT),
                "Source and target index types to be both ROWCOUNT. Source: {}, target: {}",
                source_index_type,
                target_index_type
        );
        on = on.next();
        return initialize_rows_to_update_for_row_range_indexed_data(row_slices, source_descriptor, source_start_end);
    }();

    if (on.empty()) {
        return matched_rows;
    }
    const std::pair<size_t, size_t> source_range = source_start_end;
    for (const std::string_view column_name : on) {
        const size_t source_field_position = field_index_for_matching_on_column(column_name, source_descriptor);
        const Field& source_field = source_descriptor.field(source_field_position);
        // TODO: For dynamic schema the two fields might have different indexes
        const size_t target_field_position = source_field_position;
        const Field& target_field = target_descriptor.field(target_field_position);
        details::visit_type(target_field.type().data_type(), [&]<typename TargetDataTypeTag>(TargetDataTypeTag) {
            details::visit_type(source_field.type().data_type(), [&]<typename SourceDataTypeTag>(SourceDataTypeTag) {
                if constexpr (std::same_as<TargetDataTypeTag, SourceDataTypeTag>) {
                    using TargetTDT = ScalarTagType<TargetDataTypeTag>;
                    using SourceTDT = ScalarTagType<SourceDataTypeTag>;
                    auto source = get_source_column<SourceTDT>(source_field_position, source_range);
                    matched_rows.filter_matching_rows<TargetTDT, SourceTDT>(
                            target_field.name(), source_range.first, source, strategy_.match_na
                    );
                }
            });
        });
    }
    return matched_rows;
}

const ClauseInfo& MergeUpdateClause::clause_info() const { return clause_info_; }

void MergeUpdateClause::set_processing_config(const ProcessingConfig&) {}

void MergeUpdateClause::set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
    component_manager_ = std::move(component_manager);
}

OutputSchema MergeUpdateClause::modify_schema(OutputSchema&& output_schema) const {
    schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
            columns_match(output_schema.stream_descriptor(), get_source_descriptor()),
            "Cannot perform merge update when the source and target schema are not the same.\nSource schema: "
            "{}\nTarget schema: {}",
            get_source_descriptor(),
            output_schema.stream_descriptor()
    );
    return output_schema;
}

OutputSchema MergeUpdateClause::join_schemas(std::vector<OutputSchema>&&) const {
    util::raise_rte("MergeUpdateClause::join_schemas should never be called");
}

std::string MergeUpdateClause::to_string() const { return "MERGE_UPDATE"; }

const SegmentInMemory& MergeUpdateClause::source_as_segment() const { return source_as_segment_; }

size_t MergeUpdateClause::field_index_for_matching_on_column(std::string_view name, const StreamDescriptor& descriptor)
        const {
    // In case of unnamed index columns we set the fake_name property of the metadata to true and assign the name
    // "index" to the column. In this specific case the user must be able to match on a column named "index".
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            descriptor.index().type() != IndexDescriptor::Type::TIMESTAMP ||
                    (descriptor.field(0).name() != name || (fake_index_name_ && descriptor.field(0).name() == name)),
            "The \"on\" parameter must not contain the datetime index column \"{}\". Note that for date time indexed "
            "column the index column is always used for matching. Descriptor: {}",
            name,
            descriptor
    );
    const boost::regex repetition_regex{fmt::format("^__col_{}__\\d+$", name)};
    const std::string multiindex_mangled_name = stream::mangled_name(name);
    std::ranges::subrange data_fields{
            std::next(descriptor.fields().begin(), descriptor.index().field_count()), descriptor.fields().end()
    };
    const auto is_name_or_mangled_name = [&](const Field& field) {
        return field.name() == name || field.name() == multiindex_mangled_name ||
               boost::regex_match(field.name().begin(), field.name().end(), repetition_regex);
    };
    const auto field_it = std::ranges::find_if(data_fields, is_name_or_mangled_name);
    user_input::check<ErrorCode::E_COLUMN_NOT_FOUND>(
            field_it != data_fields.end(),
            "Column \"{}\" specified in the 'on' parameter does not exist. Descriptor: {}",
            name,
            descriptor
    );
    const auto repetition_it = std::ranges::find_if(
            data_fields.next(std::distance(data_fields.begin(), field_it) + 1), is_name_or_mangled_name
    );
    user_input::check<ErrorCode::E_DUPLICATE_COLUMN>(
            repetition_it == data_fields.end(),
            "Column \"{}\" specified in the 'on' appears more than once in the dataframe. Descriptor: {}",
            name,
            descriptor
    );
    return std::distance(descriptor.fields().begin(), field_it);
}

bool MergeUpdateClause::must_structure_by_time_slice() const {
    return source_->has_index() && !on_.empty() && strategy_.insert();
}

MergeUpdateClause::MatchRecord::MatchRecord(std::span<ProcessingUnit> row_slices, const size_t num_source_rows) :
    matched_target_rows_(row_slices.size(), std::vector<std::vector<size_t>>(num_source_rows)),
    row_slices_(row_slices),
    source_row_matched_count_(num_source_rows) {}

void MergeUpdateClause::MatchRecord::add_match(size_t source_row, size_t target_row_slice, size_t target_row) {
    ++source_row_matched_count_[source_row];
    matched_target_rows_[target_row_slice][source_row].push_back(target_row);
    ++total_matched_target_rows_count_;
}
void MergeUpdateClause::MatchRecord::add_match(
        size_t source_row, size_t target_row_slice, std::span<size_t> target_rows
) {
    source_row_matched_count_[source_row] += target_rows.size();
    const auto end = matched_target_rows_[target_row_slice][source_row].end();
    matched_target_rows_[target_row_slice][source_row].insert(end, target_rows.begin(), target_rows.end());
    total_matched_target_rows_count_ += target_rows.size();
}

void MergeUpdateClause::MatchRecord::clone_source_match(
        size_t source_row_src, size_t source_row_dst, size_t row_slice
) {
    util::check(
            matched_target_rows_[row_slice][source_row_dst].empty(), "Destination source row must not have matches"
    );
    source_row_matched_count_[source_row_dst] = source_row_matched_count_[source_row_src];
    matched_target_rows_[row_slice][source_row_dst] = matched_target_rows_[row_slice][source_row_src];
    total_matched_target_rows_count_ += source_row_matched_count_[source_row_dst];
}

size_t MergeUpdateClause::MatchRecord::total_unmatched_source_rows() const {
    return std::ranges::count_if(source_row_matched_count_, [](const size_t count) { return count == 0; });
}

void MergeUpdateClause::MatchRecord::validate_rows_to_update(const MergeStrategy& strategy) const {
    // TODO: This can be inlined in the loop iterating over all columns to avoid iterating the source one more
    // time. The loop structure makes it not intuitive. The performance cost must be evaluated. Monday:
    // 10655963947
    if (!strategy.update() || total_matched_target_rows_count_ == 0) {
        return;
    }
    util::BitSet matched_rows;
    for (size_t row_slice_idx = 0; row_slice_idx < matched_target_rows_.size(); ++row_slice_idx) {
        const RowRange row_range = *row_slices_[row_slice_idx].row_ranges_->front();
        matched_rows.resize(row_range.diff());
        for (size_t source_row_idx = 0; source_row_idx < matched_target_rows_[row_slice_idx].size(); ++source_row_idx) {
            for (const size_t target_row : matched_target_rows_[row_slice_idx][source_row_idx]) {
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                        matched_rows[target_row] == false,
                        "Multiple source rows match the same target row: {}. Second source index to match is {} Final "
                        "value is ambiguous.",
                        row_range.start() + target_row,
                        source_row_idx
                );
                matched_rows[target_row] = true;
            }
        }
        matched_rows.clear();
    }
}

const std::vector<std::vector<size_t>>& MergeUpdateClause::MatchRecord::matched_rows(size_t target_row_slice) const {
    return matched_target_rows_[target_row_slice];
}

bool MergeUpdateClause::MatchRecord::is_source_row_matched(size_t source_row) const {
    return source_row_matched_count_[source_row] > 0;
}

bool MergeUpdateClause::MatchRecord::has_matched_target_rows() const { return total_matched_target_rows_count_ > 0; }

[[nodiscard]] util::BitSet MergeUpdateClause::MatchRecord::unmatched_source_rows() const {
    util::BitSet result(source_row_matched_count_.size());
    for (size_t i = 0; i < source_row_matched_count_.size(); ++i) {
        if (source_row_matched_count_[i] == 0) {
            result.set(i);
        }
    }
    return result;
}

} // namespace arcticdb
