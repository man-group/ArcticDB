/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/clause.hpp>
#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>
#include <arcticdb/version/schema_checks.hpp>
#include <arcticdb/pipeline/slicing.hpp>
#include <arcticdb/stream/index.hpp>
#include <ankerl/unordered_dense.h>
#include <boost/regex.hpp>

#include <ranges>

namespace {
using namespace arcticdb;

template<util::type_descriptor_tag TDT>
using SourceRawType =
        std::conditional_t<is_sequence_type(TDT::data_type()), PyObject* const, typename TDT::DataTypeTag::raw_type>;

struct TargetRange {
    size_t start_row_in_first_row_slice{};
    size_t end_row_in_last_row_slice{};
};

/// Remove all row slice entities and ranges and keys whose indexes do not appear in row_slices_to_keep
/// @param offsets Must be structured by row slice with ranges and keys
/// @param ranges_and_keys Must be structured by row slice with offsets
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
position_t write_py_string_to_pool_or_throw(
        PyObject* const py_string_object, const size_t row_in_segment, const RowRange& row_range,
        std::optional<ScopedGILLock>& gil_lock, StringPool& new_string_pool, const std::string_view column_name
) {
    return util::variant_match(
            add_py_string_to_pool<TDT::data_type()>(py_string_object, gil_lock, new_string_pool),
            [](position_t offset) { return offset; },
            [&](convert::StringEncodingError&& err) -> position_t {
                err.row_index_in_slice_ = row_in_segment;
                err.raise(column_name, row_range.first);
            }
    );
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
bool merge_update_string_column(
        Column& target_column, const std::span<const std::vector<size_t>> rows_to_update, std::string_view column_name,
        const RowRange& row_range, const StringPool& target_string_pool,
        const std::span<PyObject* const> source_column_view, StringPool& new_string_pool
) {
    bool column_is_changed = false;
    if constexpr (is_fixed_string_type(TDT::data_type())) {
        user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                "Fixed string sequences are not supported for merge update"
        );
    } else if constexpr (is_dynamic_string_type(TDT::data_type())) {
        util::BitSet target_rows_not_matched_by_source(target_column.row_count());
        // GIL will be acquired if there is a string that is not pure ASCII/UTF-8
        // In this case a PyObject will be allocated by convert::py_unicode_to_buffer
        // If such a string is encountered in a column, then the GIL will be held until that whole column has
        // been processed, on the assumption that if a column has one such string it will probably have many.
        std::optional<ScopedGILLock> gil_lock;
        ColumnData target_column_data = target_column.data();
        auto target_column_accessor = random_accessor<TDT>(&target_column_data);
        for (size_t source_row = 0; source_row < rows_to_update.size(); ++source_row) {
            for (size_t target_row : rows_to_update[source_row]) {
                const auto py_string_object = source_column_view[source_row];
                target_column_accessor[target_row] = write_py_string_to_pool_or_throw<TDT>(
                        py_string_object, target_row, row_range, gil_lock, new_string_pool, column_name
                );
                column_is_changed = true;
                target_rows_not_matched_by_source.set(target_row);
            }
        }
        target_rows_not_matched_by_source.flip();
        rebuild_sequence_column_in_new_pool<TDT>(
                target_column, target_string_pool, new_string_pool, &target_rows_not_matched_by_source
        );
    }
    return column_is_changed;
}

struct MergeUpdateStringColumnFlags {
    /// Only move the data to a new string pool.
    bool data_not_changed = false;
};

template<util::type_descriptor_tag TDT>
bool merge_update_string_column(
        Column& target_column, const MergeUpdateStringColumnFlags& flags,
        const std::span<const std::vector<size_t>> rows_to_update, std::string_view column_name,
        const RowRange& row_range, const StringPool& target_string_pool,
        const std::span<PyObject* const> source_column_view, StringPool& new_string_pool
) {
    if (flags.data_not_changed) {
        rebuild_sequence_column_in_new_pool<TDT>(target_column, target_string_pool, new_string_pool);
        return false;
    }

    return merge_update_string_column<TDT>(
            target_column,
            rows_to_update,
            column_name,
            row_range,
            target_string_pool,
            source_column_view,
            new_string_pool
    );
}

struct NaNAwareFloatComparator {
    template<std::floating_point T>
    bool operator()(const T a, const T b) const {
        return a == b || (std::isnan(a) && std::isnan(b));
    }
};

struct NaNAwareFloatHasher {
    using is_avalanching = void;
    template<std::floating_point T>
    uint64_t operator()(const T a) const {
        if (std::isnan(a)) {
            // IEEE allows multiple different bit representations of NaN. std::isnan is required to return true for all
            // different bit bit representations of NaN, std::quient_NaN is an implementation defined constant.
            return ankerl::unordered_dense::hash<T>()(std::numeric_limits<T>::quiet_NaN());
        } else {
            return std::hash<T>()(a);
        }
    }
};

template<
        util::type_descriptor_tag SourceTDT, util::type_descriptor_tag TargetTDT,
        typename SourceValueRawType = TargetTDT::DataTypeTag::raw_type,
        typename TargetValueRawType = TargetTDT::DataTypeTag::raw_type>
requires std::same_as<std::decay_t<SourceTDT>, std::decay_t<TargetTDT>>
std::variant<bool, convert::StringEncodingError> are_merge_values_matching(
        const SourceValueRawType& source_value, const TargetValueRawType& target_value,
        const StringPool& target_string_pool, std::optional<ScopedGILLock>& scoped_gil_lock
) {
    if constexpr (is_sequence_type(SourceTDT::data_type())) {
        const bool is_source_null = is_py_none(source_value) || is_py_nan(source_value);
        const bool is_target_null = !is_a_string(target_value);
        if (is_source_null ^ is_target_null) {
            return false;
        } else if (is_source_null && is_target_null) {
            return true;
        } else {
            return util::variant_match(
                    create_py_object_wrapper_or_error<TargetTDT::data_type()>(source_value, scoped_gil_lock),
                    [](convert::StringEncodingError&& err) -> std::variant<bool, convert::StringEncodingError> {
                        return err;
                    },
                    [&](convert::PyStringWrapper&& wrapper) -> std::variant<bool, convert::StringEncodingError> {
                        return target_string_pool.get_const_view(target_value) ==
                               std::string_view(wrapper.buffer_, wrapper.length_);
                    }
            );
        }
    } else if constexpr (is_floating_point_type(SourceTDT::data_type())) {
        constexpr static NaNAwareFloatComparator comparator;
        return comparator(source_value, target_value);
    } else {
        return source_value == target_value;
    }
}

template<util::type_descriptor_tag TDT>
auto map_column_values_to_rows(const ColumnWithStrings& column) {
    constexpr static bool is_target_sequence_type = is_sequence_type(TDT::data_type());
    constexpr static bool is_target_floating_point_type = is_floating_point_type(TDT::data_type());
    using TargetRawType = typename TDT::DataTypeTag::raw_type;
    using TargetValueType = std::conditional_t<is_target_sequence_type, std::optional<std::string_view>, TargetRawType>;
    using Hasher = std::conditional_t<
            is_target_floating_point_type,
            NaNAwareFloatHasher,
            ankerl::unordered_dense::hash<TargetValueType>>;
    using Comparator =
            std::conditional_t<is_target_floating_point_type, NaNAwareFloatComparator, std::equal_to<TargetValueType>>;
    ankerl::unordered_dense::map<TargetValueType, std::vector<size_t>, Hasher, Comparator> target_values;
    arcticdb::for_each_enumerated<TDT>(*column.column_, [&](auto row) {
        if constexpr (is_target_sequence_type) {
            if (is_a_string(row.value())) {
                target_values[column.string_at_offset(row.value())].emplace_back(row.idx());
            } else {
                target_values[std::nullopt].emplace_back(row.idx());
            }
        } else {
            target_values[row.value()].emplace_back(row.idx());
        }
    });
    return target_values;
}

template<typename SourceRawType>
struct InsertSourceData {
    std::span<const timestamp> index;
    std::span<const SourceRawType> data;
    std::pair<size_t, size_t> global_row_range;
};

struct InsertTargetData {
    std::span<ColumnWithStrings> indexes;
    std::span<ColumnWithStrings> columns;
    TypeDescriptor type;
    TargetRange range;
    StringPool& new_string_pool;
};

/// Performs merging of sorted lists with update on matching rows. The index column dictates the ordering of the rows.
/// The target is composed of multiple row slices the following hods:
/// is_sorted(index in row slice i) && all(index values in row slice i) <= all(index values in row slice j) for i < j.
/// The source is a single row slice.  Insertion is stable and all new values are inserted after all existing values
/// with the same index value. The source and target must have the same index type.
template<util::type_descriptor_tag TargetColumnTypeDescriptorTag, typename SourceRawType>
requires(TargetColumnTypeDescriptorTag::dimension() == Dimension::Dim0)
Column merge(
        const InsertSourceData<SourceRawType>& source, const InsertTargetData& target,
        const MergeUpdateClause::MatchRecord& match_record, const MergeStrategy& strategy
) {
    using IndexType = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
    // One index value can appear in more than one row slice. In that case it can be shared by two processing units
    // in that case. Each processing unit works on part of the target data.
    const size_t num_rows_out_of_target_range =
            target.range.start_row_in_first_row_slice +
            (target.columns.back().column_->row_count() - target.range.end_row_in_last_row_slice);
    const size_t combined_row_count =
            std::accumulate(
                    target.columns.begin(),
                    target.columns.end(),
                    match_record.total_unmatched_source_rows(),
                    [](size_t acc, const ColumnWithStrings& col) { return acc + col.column_->row_count(); }
            ) -
            num_rows_out_of_target_range;
    Column new_column(target.type, combined_row_count, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED);
    ColumnData new_column_data = new_column.data();
    auto new_column_it = new_column_data.begin<TargetColumnTypeDescriptorTag>();
    auto new_data = random_accessor<TargetColumnTypeDescriptorTag>(&new_column_data);

    size_t target_row_slice = 0;
    ColumnData target_index_data = target.indexes[target_row_slice].column_->data();
    auto target_index_it = target_index_data.begin<IndexType>();
    std::advance(target_index_it, target.range.start_row_in_first_row_slice);
    auto target_index_end = target_index_data.end<IndexType>();

    ColumnData target_column_data = target.columns[target_row_slice].column_->data();
    auto target_data = random_accessor<TargetColumnTypeDescriptorTag>(&target_column_data);
    size_t target_row_idx = target.range.start_row_in_first_row_slice;
    size_t new_column_row_idx{};

    std::vector<size_t> row_slice_start_in_output(target.columns.size());
    std::vector<size_t> inserted_rows(target.columns.size(), 0);

    StringPool intermediate_pool;

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
            row_slice_start_in_output[target_row_slice] = new_column_row_idx;
            target_row_idx = 0;
            target_column_data = target.columns[target_row_slice].column_->data();
            target_index_data = target.indexes[target_row_slice].column_->data();
            target_index_it = target_index_data.begin<IndexType>();
            target_index_end = target_index_data.end<IndexType>();
            target_data = random_accessor<TargetColumnTypeDescriptorTag>(&target_column_data);
            if (target_row_slice == target.columns.size() - 1 &&
                target.range.end_row_in_last_row_slice !=
                        static_cast<size_t>(target.columns.back().column_->row_count())) [[unlikely]] {
                target_index_end = std::next(target_index_it, target.range.end_row_in_last_row_slice);
            }
        }
    };

    const auto advance_output = [&] {
        ++new_column_row_idx;
        ++new_column_it;
    };

    // GIL will be acquired if there is a string that is not pure ASCII/UTF-8
    // In this case a PyObject will be allocated by convert::py_unicode_to_buffer
    // If such a string is encountered in a column, then the GIL will be held until that whole column has
    // been processed, on the assumption that if a column has one such string it will probably have many.
    std::optional<ScopedGILLock> scoped_gil_lock;

    const auto set_string_from_source = [&](size_t row) {
        if constexpr (is_sequence_type(TargetColumnTypeDescriptorTag::data_type())) {
            *new_column_it = write_py_string_to_pool_or_throw<TargetColumnTypeDescriptorTag>(
                    source.data[row],
                    row,
                    RowRange{source.global_row_range.first, source.global_row_range.second},
                    scoped_gil_lock,
                    intermediate_pool,
                    target.columns.front().column_name_
            );
        }
    };

    const auto set_string_from_source_at = [&](size_t source_row, size_t output_row) {
        if constexpr (is_sequence_type(TargetColumnTypeDescriptorTag::data_type())) {
            new_data[output_row] = write_py_string_to_pool_or_throw<TargetColumnTypeDescriptorTag>(
                    source.data[source_row],
                    source_row,
                    RowRange{source.global_row_range.first, source.global_row_range.second},
                    scoped_gil_lock,
                    intermediate_pool,
                    target.columns.front().column_name_
            );
        }
    };

    const auto set_string_from_target = [&](size_t target_row) {
        if constexpr (is_sequence_type(TargetColumnTypeDescriptorTag::data_type())) {
            const position_t offset = target_data[target_row];
            if (is_a_string(offset)) {
                const StringPool& pool = *target.columns[target_row_slice].string_pool_;
                const std::string_view string_data = pool.get_const_view(offset);
                *new_column_it = intermediate_pool.get(string_data).offset();
            } else {
                *new_column_it = offset;
            }
        }
    };

    size_t source_row_idx = 0;

    // Stable merge target and source into the new column. If there is a sequence of repeated index values and new
    // rows must be inserted, the new rows will appear after the rows from the target
    while (!target_index_is_exhausted() && source_row_idx < source.index.size()) {
        // Copy all target values smaller than the current source index to the output buffer
        while (!target_index_is_exhausted() && *target_index_it < source.index[source_row_idx]) {
            if constexpr (is_sequence_type(TargetColumnTypeDescriptorTag::data_type())) {
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
        // perform both an update and an insert. First, copy all target values to the output buffer and then append
        // the new values. In case rows_to_update[source_row_idx] is not empty, then an update must happen. The
        // update is performed in place in the new output buffer. It is crucial to take into account that the
        // indexes in rows_to_update[source_row_idx] are pointing to rows in the original column (before any
        // insertions happened)

        // Source and target index values are equal. Copy the target data first.
        const size_t pre_equal_run_row_slice = target_row_slice;
        const timestamp equal_run_index_value = *target_index_it;
        // Since the index is ordered, no index in source_index[source_row_idx] can be less than target_row_idx.
        while (!target_index_is_exhausted() && *target_index_it == equal_run_index_value &&
               source.index[source_row_idx] == equal_run_index_value) {
            if constexpr (is_sequence_type(TargetColumnTypeDescriptorTag::data_type())) {
                set_string_from_target(target_row_idx);
            } else {
                *new_column_it = target_data[target_row_idx];
            }
            advance_output();
            advance_target();
        }

        // For each source row equal to the target row, there are two options.
        // * the output buffer must be updated.
        // * insertion must happen by appending to the end of the output buffer.
        size_t inserted_rows_with_matching_index = 0;
        while (source_row_idx < source.index.size() && source.index[source_row_idx] == equal_run_index_value) {
            if (!match_record.is_source_row_matched(source_row_idx)) {
                if constexpr (is_sequence_type(TargetColumnTypeDescriptorTag::data_type())) {
                    set_string_from_source(source_row_idx);
                } else {
                    *new_column_it = source.data[source_row_idx];
                }
                advance_output();
                ++inserted_rows_with_matching_index;
            } else if (strategy.update()) {
                for (size_t i = pre_equal_run_row_slice; i <= target_row_slice; ++i) {
                    // TODO: this is suboptimal for string columns. This approach first places all target value and
                    // then updates the output buffer with the source values. The rows to update are not ordered. So
                    // unless we build a map or iterate each time the rows to updated to check if a row must be updated
                    // this approach is faster for non-string columns. However for string columns this can introduce
                    // ghost string data into the string pool, thus the string pool is rebuilt at the end of the
                    // function.
                    std::span<const std::vector<size_t>> matched_rows = match_record.matched_rows(i);
                    for (const size_t target_row_to_update_in_slice : matched_rows[source_row_idx]) {
                        const size_t target_offset = i == 0 ? target.range.start_row_in_first_row_slice : 0;
                        // matched_rows holds indexes into the target segment. But the output segment can start at
                        // an offest in the target in case the work is shared between two processing units. Only
                        // the first and last row slices of a time slice can be requested by more than one
                        // processing unit
                        const size_t output_row_to_update = target_row_to_update_in_slice +
                                                            row_slice_start_in_output[i] + inserted_rows[i] -
                                                            target_offset;
                        if constexpr (is_sequence_type(TargetColumnTypeDescriptorTag::data_type())) {
                            set_string_from_source_at(source_row_idx, output_row_to_update);
                        } else {
                            new_data[output_row_to_update] = source.data[source_row_idx];
                        }
                    }
                }
            }
            ++source_row_idx;
        }
        inserted_rows[target_row_slice] += inserted_rows_with_matching_index;

        if (target_index_it == target_index_end) {
            break;
        }

        // Target index values are larger than the source index value. Append the source data to the output buffer.
        while (source_row_idx < source.index.size() && *target_index_it > source.index[source_row_idx]) {
            if constexpr (is_sequence_type(TargetColumnTypeDescriptorTag::data_type())) {
                set_string_from_source(source_row_idx);
            } else {
                *new_column_it = source.data[source_row_idx];
            }
            ++source_row_idx;
            advance_output();
            ++inserted_rows[target_row_slice];
        }
    }
    // Not all target rows were processed, copy the rest
    while (!target_index_is_exhausted()) {
        if constexpr (is_sequence_type(TargetColumnTypeDescriptorTag::data_type())) {
            set_string_from_target(target_row_idx);
        } else {
            *new_column_it = target_data[target_row_idx];
        }
        advance_target();
        advance_output();
    }

    // Not all source rows were processed, copy the rest
    if constexpr (is_sequence_type(TargetColumnTypeDescriptorTag::data_type())) {
        for (; source_row_idx < source.index.size(); ++source_row_idx) {
            set_string_from_source(source_row_idx);
            advance_output();
        }
    } else {
        std::copy(source.data.begin() + source_row_idx, source.data.end(), new_column_it);
    }

    if constexpr (is_sequence_type(TargetColumnTypeDescriptorTag::data_type())) {
        rebuild_sequence_column_in_new_pool<TargetColumnTypeDescriptorTag>(
                new_column, intermediate_pool, target.new_string_pool
        );
    }
    return new_column;
}

template<util::type_descriptor_tag ScalarType>
bool update_column(
        const MergeStrategy& strategy, const StringPool& old_string_pool,
        const std::span<const SourceRawType<ScalarType>> source_data,
        const std::span<const std::vector<size_t>> rows_to_update, const RowRange& row_range,
        const bool is_matching_column, std::string_view column_name, Column& target_column, StringPool& new_string_pool
) {
    bool row_slice_changed = false;
    if constexpr (is_sequence_type(ScalarType::data_type())) {
        const MergeUpdateStringColumnFlags column_update_flags{
                .data_not_changed = is_matching_column && strategy.update_only()
        };
        row_slice_changed = merge_update_string_column<ScalarType>(
                target_column,
                column_update_flags,
                rows_to_update,
                column_name,
                row_range,
                old_string_pool,
                source_data,
                new_string_pool
        );
    } else {
        if (is_matching_column) {
            return false;
        }
        ColumnData target_column_data = target_column.data();
        auto target = random_accessor<ScalarType>(&target_column_data);
        for (size_t source_row_idx = 0; source_row_idx < source_data.size(); ++source_row_idx) {
            row_slice_changed |= !rows_to_update[source_row_idx].empty();
            for (const size_t target_row_idx : rows_to_update[source_row_idx]) {
                target[target_row_idx] = source_data[source_row_idx];
            }
        }
    }
    return row_slice_changed;
}

std::vector<std::vector<size_t>> split_by_row_slice(
        const std::span<const RangesAndKey> ranges, std::span<const size_t> indexes
) {
    std::vector<std::vector<size_t>> res = {{indexes.front()}};
    RowRange prev_row_range = ranges[indexes.front()].row_range();
    for (size_t idx : indexes.subspan(1)) {
        const RowRange current_row_range = ranges[idx].row_range();
        if (current_row_range != prev_row_range) {
            res.emplace_back();
        }
        res.back().emplace_back(idx);
        prev_row_range = current_row_range;
    }
    return res;
}

pipelines::RowRange get_row_range(std::span<const ProcessingUnit> row_slice) {
    return {row_slice.front().row_ranges_->front()->first, row_slice.back().row_ranges_->back()->second};
}

ssize_t first_different_index_value_position(const ColumnData index) {
    using IndexType = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
    auto it = index.cbegin<IndexType, IteratorType::ENUMERATED>();
    const timestamp first_source_row = it->value();
    return exponential_upper_bound(++it, index.cend<IndexType, IteratorType::ENUMERATED>(), first_source_row)->idx();
};

TargetRange get_target_start_end(std::span<const ProcessingUnit> row_slices) {
    TargetRange result{
            .start_row_in_first_row_slice = 0,
            .end_row_in_last_row_slice = row_slices.back().segments_->back()->row_count()
    };
    if (!row_slices.empty() && row_slices.front().entity_fetch_count_) {
        if (row_slices.front().entity_fetch_count_->front() > 1) {
            const ColumnData index = row_slices.front().segments_->front()->column(0).data();
            result.start_row_in_first_row_slice = first_different_index_value_position(index);
        }
        if (row_slices.size() > 1 && row_slices.back().entity_fetch_count_->back() > 1) {
            const ColumnData index = row_slices.back().segments_->back()->column(0).data();
            result.end_row_in_last_row_slice = first_different_index_value_position(index);
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
/// Complexity: $$O(m * log_2(n) + n)$$ where: m is the count of source rows in the bounds of the segment, n is the
/// number of target rows in the segment.
MergeUpdateClause::MatchRecord filter_index_match(
        std::span<const timestamp> source_index, std::span<ProcessingUnit> row_slices
) {
    using IndexType = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
    MergeUpdateClause::MatchRecord result(row_slices, source_index.size());
    if (source_index.empty()) {
        return result;
    }
    for (size_t i = 0; i < row_slices.size(); ++i) {
        const ProcessingUnit& proc = row_slices[i];
        const Column& target_index = proc.segments_->front()->column(0);
        ColumnData target_index_column_data = target_index.data();
        auto last_target_row_it = exponential_upper_bound<IndexType, IteratorType::ENUMERATED>(
                target_index_column_data, source_index.back()
        );
        size_t source_row{};
        auto target_row_it = target_index_column_data.cbegin<IndexType, IteratorType::ENUMERATED>();
        // This loop can be inverted so that if source_row_end - source_row_start is > len(target) the complexity
        // becomes O(n * log_2(m)) where: m is the count of source rows in the bounds of the segment, n is the number of
        // target rows in the segment.
        while (target_row_it != last_target_row_it && source_row < source_index.size()) {
            const timestamp source_ts = source_index[source_row];
            auto target_match_it = exponential_lower_bound(target_row_it, last_target_row_it, source_ts);
            if (target_match_it == last_target_row_it) {
                break;
            }
            target_row_it = target_match_it;
            while (target_row_it != last_target_row_it && target_row_it->value() == source_ts) {
                result.add_match(source_row, i, target_row_it->idx());
                ++target_row_it;
            }
            // Optimizes the case of repeated index values. All matched rows corresponding to a particular index value
            // must be the same, so just copy the matched rows in case index values are repeated.
            while (++source_row < source_index.size() && source_index[source_row] == source_ts) {
                result.clone_source_match(source_row - 1, source_row, i);
            }
        }
    }

    return result;
}

} // namespace

namespace arcticdb {

namespace ranges = std::ranges;
using namespace pipelines;

MergeUpdateClause::MergeUpdateClause(
        std::vector<std::string>&& on, MergeStrategy strategy, std::shared_ptr<InputFrame> source
) :

    on_(std::move(on)),
    strategy_(strategy),
    source_(std::move(source)) {
    std::erase_if(on_, [&](const std::string& column) { return !on_set_.insert(column).second; });
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

    using IndexType = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
    std::vector<std::vector<size_t>> offsets = must_structure_by_time_slice() ? structure_by_time_slice(ranges_and_keys)
                                                                              : structure_by_row_slice(ranges_and_keys);
    std::vector<size_t> row_slices_to_keep;
    // TODO: Arrow is not supported yet.
    const TypedTensor<IndexType::DataTypeTag::raw_type> index_tensor(source_->get_tensor(0));
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            util::is_cstyle_array<IndexType::DataTypeTag::raw_type>(index_tensor),
            "Fortran-style arrays are not supported by merge update yet. The index column has data type {} of "
            "size {} bytes but the stride is {} bytes",
            IndexType::data_type(),
            sizeof(IndexType::DataTypeTag::raw_type),
            index_tensor.strides()[0]
    );
    const bool source_ends_before_first_row_slice =
            index_tensor.at(source_->num_rows - 1) < ranges_and_keys.begin()->key_.time_range().first;
    const bool source_starts_after_the_last_row_slice =
            index_tensor.at(0) >= ranges_and_keys.back().key_.time_range().second;
    if (strategy_.update_only() && (source_ends_before_first_row_slice || source_starts_after_the_last_row_slice)) {
        ranges_and_keys.clear();
        return {};
    }

    std::span source_index(index_tensor.data(), source_->num_rows);
    for (size_t row_slice_idx = 0; row_slice_idx < offsets.size(); ++row_slice_idx) {
        const std::vector<std::vector<size_t>>& row_slice_offsets =
                must_structure_by_time_slice() ? ::split_by_row_slice(ranges_and_keys, offsets[row_slice_idx])
                                               : std::vector<std::vector<size_t>>{offsets[row_slice_idx]};
        const TimestampRange time_range{
                ranges_and_keys[row_slice_offsets.front().front()].key_.time_range().first,
                ranges_and_keys[row_slice_offsets.back().back()].key_.time_range().second
        };

        const auto source_range_start = [&] {
            if (strategy_.insert() && row_slice_idx == 0 && source_index.front() < time_range.first) {
                // In case of inserting all data before the first segment gets prepended to it
                return source_index.begin();
            }
            return ranges::lower_bound(source_index, time_range.first);
        }();

        if (source_range_start == source_index.end()) {
            // All remaining row ranges start after the last index value of the source, thus not match is possible.
            break;
        }

        auto source_range_end = source_index.end();
        if (*source_range_start >= time_range.second) {
            // The current segment does not contain any source value because all source values larger than the
            // segment start are also larger than the segment end.
            if (!strategy_.insert()) {
                // If insertion is not allowed, continue checking next row slices until segment's start is larger
                // than the source index end.
                continue;
            }
            if (row_slice_idx != offsets.size() - 1) {
                // In case insertion is enabled, source values in between two row slices will be appended to the
                // former. This is done by extending the time range of the current row slice so that it's 1 ns less than
                // the start of the next row slice.
                const timestamp next_row_slice_start =
                        ranges_and_keys[offsets[row_slice_idx + 1].front()].key_.time_range().first;
                if (*source_range_start >= next_row_slice_start) {
                    // The extended segment does not contain any source values. All source values are after the
                    // extended segment end. Continue checking next row slices.
                    continue;
                }
                const timestamp extended_segment_end = next_row_slice_start - 1;
                source_range_end = std::upper_bound(source_range_start, source_index.end(), extended_segment_end);
            }
        } else {
            if (strategy_.insert()) {
                if (row_slice_idx == offsets.size() - 1) {
                    // When strategy allows insertion and this is the last segment all remaining source values get
                    // attributed to the last segment
                    source_range_end = source_index.end();
                } else {
                    // The segment contains source values. In addition, unmatched source values that fall in the gap
                    // between this segment and the next one are appended to this segment (as in the branch above, where
                    // the segment had no source values of its own). std::max keeps at least the segment's own range in
                    // case the next segment starts before this one ends (overlapping time slices).
                    const timestamp next_row_slice_start =
                            ranges_and_keys[offsets[row_slice_idx + 1].front()].key_.time_range().first;
                    const timestamp extended_segment_end = std::max(time_range.second, next_row_slice_start) - 1;
                    source_range_end =
                            ranges::upper_bound(source_range_start, source_index.end(), extended_segment_end);
                }
            } else {
                source_range_end = ranges::upper_bound(source_range_start, source_index.end(), time_range.second - 1);
            }
        }
        const std::pair<size_t, size_t> source_row_range = {
                source_range_start - source_index.begin(), source_range_end - source_index.begin()
        };
        const pipelines::RowRange row_range{
                ranges_and_keys[row_slice_offsets.front().front()].row_range().first,
                ranges_and_keys[row_slice_offsets.back().back()].row_range().second
        };
        source_start_end_for_row_range_.insert({row_range, source_row_range});
        row_slices_to_keep.push_back(row_slice_idx);
    }
    filter_selected_ranges_and_keys_and_reindex_entities(row_slices_to_keep, offsets, ranges_and_keys);
    return offsets;
}

std::vector<std::vector<EntityId>> MergeUpdateClause::structure_for_processing(std::vector<std::vector<EntityId>>&&) {
    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("MergeUpdate clause should be the first clause in the pipeline");
}

/// Decide which rows of should be updated and which rows from source should be inserted.
/// 1. If there's a timestamp index  use MergeUpdateClause::filter_index_match this will produce a vector of size
/// equal to the number of rows from the source that fall into the processed slice. Each vector will contain a
/// vector of row-indexes in target that match the corresponding source index value.
/// 2. For each column in MergeUpdateClause::on_ iterate over the vector of vectors produced in the previous step.
/// Checking for match only the target rows that are in the inner vector. If there is no match for this particular
/// column remove the target row index.
/// This means that the ordering of the columns in MergeUpdateClause::on_ matters and it would be more efficient to
/// start with the columns that have a lesser chance of matching.
std::vector<EntityId> MergeUpdateClause::process(std::vector<EntityId>&& entity_ids) const {
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            source_->has_only_tensors(), "Arrow format is not supported as input for merge update"
    );
    if (entity_ids.empty()) {
        return {};
    }
    auto proc = gather_entities<
            std::shared_ptr<SegmentInMemory>,
            std::shared_ptr<RowRange>,
            std::shared_ptr<ColRange>,
            std::shared_ptr<AtomKey>,
            EntityFetchCount>(*component_manager_, std::move(entity_ids));
    std::vector<ProcessingUnit> row_slices =
            must_structure_by_time_slice() ? split_by_row_slice(std::move(proc)) : std::vector{std::move(proc)};
    const MatchRecord matched = match(row_slices);
    matched.validate_rows_to_update(strategy_);
    if (strategy_.update_only()) {
        if (auto [new_row_slices, time_slice_changed] = update(matched, std::move(row_slices)); time_slice_changed) {
            std::vector<EntityId> res;
            for (ProcessingUnit& row_slice : new_row_slices) {
                const size_t entity_count = row_slice.segments_->size();
                std::vector<EntityId> entts = component_manager_->add_entities(
                        std::move(*row_slice.segments_),
                        std::move(*row_slice.row_ranges_),
                        std::move(*row_slice.col_ranges_),
                        std::vector<EntityFetchCount>(entity_count, 1)
                );
                res.insert(res.end(), std::make_move_iterator(entts.begin()), std::make_move_iterator(entts.end()));
            }
            return res;
        }
    } else if (strategy_.insert()) {
        const StreamDescriptor& target_descriptor = source_->desc(); // TODO: Not true for dynamic schema
        auto [new_row_slices, time_slice_changed] =
                update_and_insert(matched, target_descriptor, std::move(row_slices));
        if (time_slice_changed) {
            std::vector<EntityId> res;
            for (auto& row_slice : new_row_slices) {
                const size_t entity_count = row_slice.segments_->size();
                std::vector<EntityId> entts = component_manager_->add_entities(
                        std::move(*row_slice.segments_),
                        std::move(*row_slice.row_ranges_),
                        std::move(*row_slice.col_ranges_),
                        std::vector<EntityFetchCount>(entity_count, 1),
                        std::vector(entity_count, MergeUpdateInsertedRowsEntity{matched.total_unmatched_source_rows()})
                );
                res.insert(res.end(), std::make_move_iterator(entts.begin()), std::make_move_iterator(entts.end()));
            }
            return res;
        }
    }
    return {};
}

MergeUpdateClause::MatchRecord MergeUpdateClause::initialize_rows_to_update_for_row_range_indexed_data(
        std::span<ProcessingUnit> row_slices, const StreamDescriptor& source_descriptor
) const {
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            !on_.empty(),
            "MergeUpdate requires at least one column in the \"on\" parameter when the DataFrame is not a "
            "timeseries"
    );
    const std::pair<size_t, size_t> source_range = get_source_start_end(row_slices);
    MatchRecord result(row_slices, source_range.second - source_range.first);
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
            using SourceType = SourceRawType<SourceTDT>;
            const ColumnWithStrings target_column = std::get<ColumnWithStrings>(proc.get(ColumnName{column_name}));
            details::visit_type(target_column.column_->type().data_type(), [&](auto target_field_dt) {
                using TargetTDT = ScalarTagType<decltype(target_field_dt)>;
                if constexpr (std::same_as<std::decay_t<SourceTDT>, std::decay_t<TargetTDT>>) {
                    auto target_values_to_rows = map_column_values_to_rows<TargetTDT>(target_column);
                    std::span source_data = source_->get_tensor(source_field_position).span<SourceType>();
                    for (size_t source_row_idx = 0; source_row_idx < source_data.size(); ++source_row_idx) {
                        auto source_value = source_data[source_row_idx];
                        if constexpr (is_sequence_type(SourceTDT::data_type())) {
                            if (is_py_none(source_value) || is_py_nan(source_value)) {
                                result.add_match(source_row_idx, row_slice_idx, target_values_to_rows[std::nullopt]);
                            } else {
                                util::variant_match(
                                        create_py_object_wrapper_or_error<SourceTDT::data_type()>(
                                                source_value, scoped_gil_lock
                                        ),
                                        [&](convert::StringEncodingError&& err) {
                                            err.row_index_in_slice_ = source_row_idx;
                                            err.raise(column_name, 0);
                                        },
                                        [&](convert::PyStringWrapper&& wrapper) {
                                            std::string_view wrapper_view{wrapper.buffer_, wrapper.length_};
                                            result.add_match(
                                                    source_row_idx,
                                                    row_slice_idx,
                                                    target_values_to_rows[std::optional{wrapper_view}]
                                            );
                                        }
                                );
                            }
                        } else {
                            result.add_match(source_row_idx, row_slice_idx, target_values_to_rows[source_value]);
                        }
                    }
                } else {
                    schema::raise<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                            "Source type {} does not match target type {}. Dynamic schema is not implemented yet",
                            SourceTDT::data_type(),
                            TargetTDT::data_type()
                    );
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
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                source_row_range_it != source_start_end_for_row_range_.end(),
                "Source row range for row range [{}, {}] not found",
                row_range.first,
                row_range.second
        );
        const std::pair<size_t, size_t> source_range = source_row_range_it->second;
        if (!row_slices.empty() && row_slices.front().entity_fetch_count_) {
            // An index value is spanning more than one row slices.
            std::pair<size_t, size_t> result = source_range;
            std::span<const timestamp> source_index =
                    source_->get_tensor(0).span<timestamp>(result.first, result.second - result.first);
            if (row_slices.front().entity_fetch_count_->front() > 1) {
                // This is the last row slice containing the index value that spans multiple row slices. It appears at
                // the beginning of the row slices, which means that the other processing unit will handle all rows of
                // the segment containing the index value; this processing unit must handle only the rows that do not
                // contain index value. Example:
                //     0         1         2         3
                // [a, b, c] [c, c, c] [c, d, e] [e, f, g]
                // When the source contains value "c," the structure for processing will be [0, 1, 2] [2, 3]. This case
                // handles processing [2, 3].
                auto next_index_value = std::ranges::upper_bound(
                        source_index, row_slices.front().atom_keys_->front()->time_range().first
                );
                result.first += next_index_value - source_index.begin();
            }
            if (row_slices.size() > 1 && row_slices.back().entity_fetch_count_->back() > 1) {
                // This is the last row slice containing the index value that spans multiple row slices. It appears at
                // the end of the row slices, which means that the other processing unit will handle all rows of
                // the segment not containing the index value; this processing unit must handle only the rows that
                // contain index value. Example:
                //     0         1         2         3
                // [a, b, c] [c, c, c] [c, d, e] [e, f, g]
                // When the source contains value "c," the structure for processing will be [0, 1, 2] [2, 3]. This case
                // handles processing [0, 1, 2].
                const auto last_index_value = std::ranges::upper_bound(
                        source_index, row_slices.back().atom_keys_->back()->time_range().first
                );
                result.second = source_range.first + (last_index_value - source_index.begin());
            }
            return result;
        } else {
            return source_range;
        }
    } else {
        return std::pair{0, source_->num_rows};
    }
}

std::pair<std::vector<ProcessingUnit>, bool> MergeUpdateClause::update_and_insert(
        const MatchRecord& match_record, const StreamDescriptor& target_descriptor,
        std::vector<ProcessingUnit>&& row_slices

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
    const bool co_owns_shared_slice = !row_slices.empty() && (row_slices.front().entity_fetch_count_->front() > 1 ||
                                                              row_slices.back().entity_fetch_count_->back() > 1);
    if (strategy_.insert_only() && match_record.total_unmatched_source_rows() == 0 && !co_owns_shared_slice) {
        return {std::move(row_slices), false};
    }
    internal::check<ErrorCode::E_NOT_IMPLEMENTED>(
            source_->desc().index().type() == IndexDescriptor::Type::TIMESTAMP,
            "Merge update with insertion is implemented only for timeseries"
    );
    const std::pair<size_t, size_t> source_start_end = get_source_start_end(row_slices);
    const std::span<const timestamp> source_index = get_source_index(row_slices);
    const StreamDescriptor source_descriptor = source_->desc();
    const size_t num_col_slices = row_slices.begin()->col_ranges_->size();
    ARCTICDB_DEBUG_CHECK(
            ErrorCode::E_ASSERTION_FAILURE,
            std::ranges::all_of(
                    row_slices, [&](const auto& proc) { return proc.col_ranges_->size() == num_col_slices; }
            ),
            "All row slices should have the same number of column ranges"
    );

    ProcessingUnit result{};
    result.segments_.emplace();
    result.segments_->reserve(num_col_slices);
    result.col_ranges_ = row_slices.front().col_ranges_;
    std::vector<ColumnWithStrings> target_datas, target_index_datas;
    target_index_datas.reserve(row_slices.size());
    std::ranges::transform(row_slices, std::back_inserter(target_index_datas), [&](const auto& proc) {
        return ColumnWithStrings(proc.segments_->front()->column_ptr(0), nullptr, target_descriptor.field(0).name());
    });
    StringPool new_string_pool;
    bool has_string_column_in_column_slice = false;
    const TargetRange target_range = get_target_start_end(row_slices);
    using IndexType = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
    auto new_index = std::make_shared<Column>(merge<IndexType>(
            InsertSourceData{.index = source_index, .data = source_index, .global_row_range = source_start_end},
            InsertTargetData{
                    .indexes = target_index_datas,
                    .columns = target_index_datas,
                    .type = TypeDescriptor{DataType::NANOSECONDS_UTC64, Dimension::Dim0},
                    .range = target_range,
                    .new_string_pool = new_string_pool
            },
            match_record,
            MergeStrategy{.not_matched_by_target = MergeAction::INSERT}
    ));
    size_t col_slice_idx = 0;
    const auto [source_start, source_end] = get_source_start_end(row_slices);
    for (size_t field_idx = target_descriptor.index().field_count(); field_idx < target_descriptor.field_count();
         ++field_idx) {
        const Field& target_field = target_descriptor.field(field_idx);
        const std::string_view column_name = target_field.name();
        target_datas.clear();
        std::ranges::transform(row_slices, std::back_inserter(target_datas), [&](ProcessingUnit& row_slice) {
            return std::get<ColumnWithStrings>(row_slice.get(ColumnName{column_name}));
        });
        Column new_column = details::visit_type(target_field.type().data_type(), [&]<typename TypeTag>(TypeTag) {
            using TargetDataTDT = ScalarTagType<TypeTag>;
            has_string_column_in_column_slice |= is_sequence_type(TargetDataTDT::data_type());
            return merge<TargetDataTDT>(
                    InsertSourceData{
                            .index = source_index,
                            .data = source_->get_tensor(field_idx).span<SourceRawType<TargetDataTDT>>(
                                    source_start, source_end - source_start
                            ),
                            .global_row_range = source_start_end
                    },
                    InsertTargetData{
                            .indexes = target_index_datas,
                            .columns = target_datas,
                            .type = target_field.type(),
                            .range = target_range,
                            .new_string_pool = new_string_pool
                    },
                    match_record,
                    // By construction, we cannot update the columns used to perform the match; only inserts are
                    // allowed
                    on_set_.contains(column_name) ? MergeStrategy{.not_matched_by_target = MergeAction::INSERT}
                                                  : strategy_
            );
        });
        if (field_idx == (*row_slices.front().col_ranges_)[col_slice_idx]->first) {
            const StreamDescriptor& desc = (*row_slices.front().segments_)[col_slice_idx]->descriptor();
            result.segments_->emplace_back(std::make_shared<SegmentInMemory>(desc, new_index->row_count()));
            result.segments_->back()->columns()[0] = new_index;
        }
        const size_t col_in_slice = field_idx - (*row_slices.front().col_ranges_)[col_slice_idx]->first + 1;
        result.segments_->back()->columns()[col_in_slice] = std::make_shared<Column>(std::move(new_column));
        if (field_idx == (*row_slices.front().col_ranges_)[col_slice_idx]->second - 1) {
            result.segments_->back()->set_row_data(new_index->row_count() - 1);
            ++col_slice_idx;
            if (has_string_column_in_column_slice) {
                result.segments_->back()->string_pool() = std::move(new_string_pool);
                new_string_pool.clear();
                has_string_column_in_column_slice = false;
            }
        }
    }
    const auto new_row_range = std::make_shared<RowRange>(
            row_slices.front().row_ranges_->front()->first + target_range.start_row_in_first_row_slice,
            row_slices.back().row_ranges_->back()->first + target_range.end_row_in_last_row_slice
    );
    result.row_ranges_ = std::vector(num_col_slices, new_row_range);
    return {std::vector{std::move(result)}, true};
}

std::pair<std::vector<ProcessingUnit>, bool> MergeUpdateClause::update(
        const MatchRecord& match_record, std::vector<ProcessingUnit>&& row_slices
) const {
    ARCTICDB_DEBUG_CHECK(
            ErrorCode::E_ASSERTION_FAILURE,
            strategy_.update_only(),
            "MergeUpdateClause::update should only be called for prue updates without insertion"
    );
    std::vector<ProcessingUnit> result;
    const StreamDescriptor& source_descriptor = source_->desc();
    bool time_slice_changed = false;
    const auto [source_start, source_end] = get_source_start_end(row_slices);
    for (size_t i = 0; i < row_slices.size(); ++i) {
        const ProcessingUnit& proc = row_slices[i];
        const std::span<const std::shared_ptr<SegmentInMemory>> target_segments = *proc.segments_;

        const std::span<const std::shared_ptr<ColRange>> col_ranges = *proc.col_ranges_;
        // Update one column at a time to increase cache coherency and to avoid calling visit_field for each row
        // being updated
        for (size_t segment_idx = 0; segment_idx < target_segments.size(); ++segment_idx) {
            StringPool new_string_pool;
            bool segment_contains_string_column = false;
            SegmentInMemory& target_segment = *target_segments[segment_idx];
            const size_t slice_size = target_segment.num_columns();
            const int index_fields = source_descriptor.index().field_count();
            for (size_t column_index_in_slice = index_fields; column_index_in_slice < slice_size;
                 ++column_index_in_slice) {
                const Field& target_field = target_segment.descriptor().field(column_index_in_slice);
                details::visit_type(target_field.type().data_type(), [&]<typename DataTypeTag>(DataTypeTag) {
                    using RawType = DataTypeTag::raw_type;
                    using ScalarType = ScalarTagType<DataTypeTag>;
                    // All column slices start with the index. Subtract the index field count so that we don't count the
                    // index twice.
                    const size_t column_position_in_ts_descriptor =
                            col_ranges[segment_idx]->start() + column_index_in_slice - index_fields;
                    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                            util::is_cstyle_array<RawType>(source_->get_tensor(column_position_in_ts_descriptor)),
                            "Fortran-style arrays are not supported by merge update yet. Column \"{}\" has data type "
                            "{} of size {} bytes but the stride is {} bytes",
                            target_field.name(),
                            target_field.type(),
                            sizeof(RawType),
                            source_->get_tensor(column_position_in_ts_descriptor).strides()[0]
                    );
                    Column& target_column = target_segment.column(column_index_in_slice);
                    const std::span source_data =
                            source_->get_tensor(column_position_in_ts_descriptor)
                                    .span<SourceRawType<ScalarType>>(source_start, source_end - source_start);
                    std::span<const std::vector<size_t>> rows_to_update = match_record.matched_rows(i);
                    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                            !rows_to_update.empty(),
                            "There must be at least one source row inside the target row slice."
                    );
                    // String columns are always recreated from scratch regardless if an update or insert is happening.
                    // This is because the string pool must be updated. With data read from disk, the map_ member of the
                    // pool is not populated. Which means that the mapping between a string and offset in the pool is
                    // missing. To get it, we need to rebuild the pool anyway.
                    segment_contains_string_column |= is_sequence_type(DataTypeTag::data_type);
                    time_slice_changed |= update_column<ScalarType>(
                            strategy_,
                            target_segment.string_pool(),
                            source_data,
                            rows_to_update,
                            *(*proc.row_ranges_)[segment_idx],
                            on_set_.contains(target_field.name()),
                            target_field.name(),
                            target_column,
                            new_string_pool
                    );
                });
            }
            if (segment_contains_string_column) {
                target_segment.set_string_pool(std::make_shared<StringPool>(std::move(new_string_pool)));
            }
        }
        result.emplace_back(std::move(row_slices[i]));
    }

    return {std::move(result), time_slice_changed};
}

MergeUpdateClause::MatchRecord MergeUpdateClause::match(std::span<ProcessingUnit> row_slices) const {
    std::optional<MatchRecord> maybe_index_match;
    if (source_->has_index()) {
        maybe_index_match.emplace(filter_index_match(get_source_index(row_slices), row_slices));
    }
    return filter_on_additional_columns_match(
            source_->desc(), source_->desc(), row_slices, std::move(maybe_index_match)
    );
}

std::span<const std::byte> MergeUpdateClause::get_source_data_bytes(size_t field_index, std::pair<size_t, size_t> range)
        const {
    auto [first_source_row, last_source_row] = range;
    const NativeTensor& tensor = source_->get_tensor(field_index);
    const size_t num_rows = last_source_row - first_source_row;
    const size_t first_source_byte = first_source_row * tensor.elsize();
    const void* source_data = tensor.data();
    return std::span{static_cast<const std::byte*>(source_data) + first_source_byte, num_rows * tensor.elsize()};
}

/// Complexity: $$O(c * n * m)$$ m is the count of source rows in the bounds of the segment, n is the  number of target
/// rows in the segment. c is the number of columns in MergeUpdateClause::on_. It can be reached if the data in source
/// and target is the same up to the very last column in MergeUpdateClause::on_.
MergeUpdateClause::MatchRecord MergeUpdateClause::filter_on_additional_columns_match(
        const StreamDescriptor& source_descriptor, const StreamDescriptor& target_descriptor,
        std::span<ProcessingUnit> row_slices, std::optional<MatchRecord>&& index_match
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
        return initialize_rows_to_update_for_row_range_indexed_data(row_slices, source_descriptor);
    }();

    if (on.empty()) {
        return matched_rows;
    }
    const std::pair<size_t, size_t> source_range = get_source_start_end(row_slices);
    for (const std::string_view column_name : on) {
        const size_t source_field_position = field_index_for_matching_on_column(column_name, source_descriptor);
        const Field& source_field = source_descriptor.field(source_field_position);
        // TODO: For dynamic schema the two fields might have different indexes
        const size_t target_field_position = source_field_position;
        const Field& target_field = target_descriptor.field(target_field_position);
        matched_rows.filter_matching_rows(
                target_field.name(),
                source_field.type().data_type(),
                target_field.type().data_type(),
                source_range.first,
                get_source_data_bytes(source_field_position, source_range)
        );
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
            columns_match(output_schema.stream_descriptor(), source_->desc()),
            "Cannot perform merge update when the source and target schema are not the same.\nSource schema: "
            "{}\nTarget schema: {}",
            source_->desc(),
            output_schema.stream_descriptor()
    );
    return output_schema;
}

OutputSchema MergeUpdateClause::join_schemas(std::vector<OutputSchema>&&) const {
    util::raise_rte("MergeUpdateClause::join_schemas should never be called");
}

std::string MergeUpdateClause::to_string() const { return "MERGE_UPDATE"; }

bool MergeUpdateClause::is_update_only() const {
    return strategy_ == MergeStrategy{MergeAction::UPDATE, MergeAction::DO_NOTHING};
}

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

std::span<const timestamp> MergeUpdateClause::get_source_index(std::span<const ProcessingUnit> row_slices) const {
    const auto [start, end] = get_source_start_end(row_slices);
    return source_->get_tensor(0).span<timestamp>(start, end - start);
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
    ++source_row_matched_count_[source_row];
    const auto end = matched_target_rows_[target_row_slice][source_row].end();
    matched_target_rows_[target_row_slice][source_row].insert(end, target_rows.begin(), target_rows.end());
    total_matched_target_rows_count_ += target_rows.size();
}

void MergeUpdateClause::MatchRecord::filter_matching_rows(
        std::string_view column_name, const DataType source_type, const DataType target_type,
        const size_t source_offset, const std::span<const std::byte> opaque_source_data
) {
    if (total_matched_target_rows_count_ == 0) {
        // This function can only remove matches in case of a mismatch. In case there are no matched
        // target rows, there's nothing left to remove.
        return;
    }
    // GIL will be acquired if there is a string that is not pure ASCII/UTF-8
    // In this case a PyObject will be allocated by convert::py_unicode_to_buffer
    // If such a string is encountered in a column, then the GIL will be held until that whole column has
    // been processed, on the assumption that if a column has one such string it will probably have many.
    std::optional<ScopedGILLock> scoped_gil_lock;
    details::visit_type(source_type, [&]<typename SourceDataTypeTag>(SourceDataTypeTag) {
        using SourceTDT = ScalarTagType<SourceDataTypeTag>;
        std::span<const SourceRawType<SourceTDT>> source_data{
                reinterpret_cast<const SourceRawType<SourceTDT>*>(opaque_source_data.data()),
                opaque_source_data.size() / sizeof(SourceRawType<SourceTDT>)
        };
        details::visit_type(target_type, [&]<typename TargetDataTypeTag>(TargetDataTypeTag) {
            using TargetTDT = ScalarTagType<TargetDataTypeTag>;
            using TargetRawType = TargetDataTypeTag::raw_type;
            // TODO: Relax for dynamic schema
            if constexpr (std::same_as<std::decay_t<SourceDataTypeTag>, std::decay_t<TargetDataTypeTag>>) {
                for (size_t row_slice_idx = 0; row_slice_idx < matched_target_rows_.size(); ++row_slice_idx) {
                    ProcessingUnit& row_slice = row_slices_[row_slice_idx];
                    const ColumnWithStrings target_column =
                            std::get<ColumnWithStrings>(row_slice.get(ColumnName{column_name}));
                    ColumnData col_data = target_column.column_->data();
                    auto target_column_accessor = random_accessor<TargetTDT>(&col_data);
                    std::vector<std::vector<size_t>>& matched_rows = matched_target_rows_[row_slice_idx];
                    for (size_t source_row_idx = 0; source_row_idx < matched_rows.size(); ++source_row_idx) {
                        const size_t discarded_matches =
                                std::erase_if(matched_rows[source_row_idx], [&](const size_t target_row) {
                                    const TargetRawType target_value = target_column_accessor[target_row];
                                    const auto& source_value = source_data[source_row_idx];
                                    auto are_values_equal = are_merge_values_matching<SourceTDT, TargetTDT>(
                                            source_value, target_value, *target_column.string_pool_, scoped_gil_lock
                                    );
                                    bool discard_match = false;
                                    if constexpr (is_sequence_type(TargetDataTypeTag::data_type)) {
                                        discard_match = util::variant_match(
                                                are_values_equal,
                                                [](const bool equal) { return !equal; },
                                                [&](convert::StringEncodingError& err) -> bool {
                                                    err.row_index_in_slice_ = source_row_idx;
                                                    err.raise(column_name, source_offset);
                                                }
                                        );
                                    } else {
                                        discard_match = !std::get<bool>(are_values_equal);
                                    }
                                    return discard_match;
                                });
                        source_row_matched_count_[source_row_idx] -= discarded_matches;
                        total_matched_target_rows_count_ -= discarded_matches;
                        if (total_matched_target_rows_count_ == 0) {
                            return;
                        }
                    }
                }
            } else {
                internal::raise<ErrorCode::E_NOT_IMPLEMENTED>(
                        "Target column \"{}\" type {} does not match source's type {}. Dynamic schema is not "
                        "implemented for merge updates yet.",
                        column_name,
                        TargetDataTypeTag::data_type,
                        SourceDataTypeTag::data_type
                );
            }
        });
    });
}

void MergeUpdateClause::MatchRecord::clone_source_match(
        size_t source_row_src, size_t source_row_dst, size_t row_slice
) {
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

std::span<const std::vector<size_t>> MergeUpdateClause::MatchRecord::matched_rows(size_t target_row_slice) const {
    return matched_target_rows_[target_row_slice];
}

bool MergeUpdateClause::MatchRecord::is_source_row_matched(size_t source_row) const {
    return source_row_matched_count_[source_row] > 0;
}

} // namespace arcticdb
