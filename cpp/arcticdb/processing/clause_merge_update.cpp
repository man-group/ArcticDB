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
    for (std::span<size_t> row_slice : offsets) {
        for (size_t& entity_id : row_slice) {
            new_ranges_and_keys.emplace_back(std::move(ranges_and_keys[entity_id]));
            entity_id = new_entity_id++;
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
        const util::BitSet* rows_to_add_in_new_pool = nullptr
) {
    if (rows_to_add_in_new_pool) {
        ColumnData target_column_data = target_column.data();
        auto accessor = random_accessor<TDT>(&target_column_data);
        for (auto row = rows_to_add_in_new_pool->first(); row != rows_to_add_in_new_pool->end(); ++row) {
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
    bool is_column_changed = false;
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
                is_column_changed = true;
                target_rows_not_matched_by_source.set(target_row);
            }
        }
        target_rows_not_matched_by_source.flip();
        rebuild_sequence_column_in_new_pool<TDT>(
                target_column, target_string_pool, new_string_pool, &target_rows_not_matched_by_source
        );
    }
    return is_column_changed;
}

struct MergeUpdateStringColumnFlags {
    /// The column is part of a timeseries dataframe
    bool is_timeseries = false;
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

template<util::type_descriptor_tag TargetColumnTypeDescriptorTag, typename SourceRawType>
Column merge(
        const Column& target_index, std::span<const timestamp> source_index, const Column& target,
        std::span<const SourceRawType> source_data, std::pair<size_t, size_t> source_range,
        const MergeUpdateClause::MatchRechord& match_rechord, const size_t row_slice, const MergeStrategy strategy
) {
    source_index = source_index.subspan(source_range.first, source_range.second - source_range.first);
    source_data = source_data.subspan(source_range.first, source_range.second - source_range.first);
    const std::span<const std::vector<size_t>> rows_to_update = match_rechord.matched_rows(row_slice);
    ARCTICDB_DEBUG_CHECK(
            ErrorCode::E_ASSERTION_FAILURE,
            source_index.size() == source_data.size(),
            "Source data size must match the index size"
    );
    ARCTICDB_DEBUG_CHECK(
            ErrorCode::E_ASSERTION_FAILURE,
            source_index.size() == rows_to_update.size(),
            "For each source data row in the segment there must be exactly one vector of target rows that must be "
            "updated by that source row."
    );
    using IndexType = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
    if constexpr (!is_sequence_type(TargetColumnTypeDescriptorTag::data_type())) {
        const size_t num_rows_to_insert = match_rechord.num_unmatched_rows(row_slice);
        Column new_column(
                target.type(),
                target.row_count() + num_rows_to_insert,
                AllocationType::PRESIZED,
                Sparsity::NOT_PERMITTED
        );
        ColumnData new_column_data = new_column.data();
        auto new_column_it = new_column_data.begin<TargetColumnTypeDescriptorTag>();
        auto new_data = random_accessor<TargetColumnTypeDescriptorTag>(&new_column_data);

        ColumnData target_index_data = target_index.data();
        auto target_index_it = target_index_data.begin<IndexType>();
        const auto target_index_end = target_index_data.end<IndexType>();

        ColumnData target_column_data = target.data();
        auto target_data = random_accessor<TargetColumnTypeDescriptorTag>(&target_column_data);
        size_t target_row_idx = 0;

        size_t source_row_idx = 0;

        size_t new_column_row_idx = 0;

        // Stable merge target and source into the new column. If there is a sequence of repeated index values and new
        // rows must be inserted, the new rows will appear after the rows from the target
        while (target_index_it != target_index_end && source_row_idx < source_index.size()) {
            // Target index values are smaller than the source index value. If the index is different, it's impossible
            // to perform an update. Just copy the target data to the output buffer
            while (target_index_it != target_index_end && *target_index_it < source_index[source_row_idx]) {
                *new_column_it++ = target_data[target_row_idx++];
                ++new_column_row_idx;
                ++target_index_it;
            }

            if (target_index_it == target_index_end) {
                break;
            }

            // Target index values are equal to the source index values. Since the index is matching, it's possible to
            // perform both an update and an insert. First, copy all target values to the output buffer and then append
            // the new values. In case rows_to_update[source_row_idx] is not empty, then an update must happen. The
            // update is performed in place in the new output buffer. It is crucial to take into account that the
            // indexes in rows_to_update[source_row_idx] are pointing to rows in the original column (before any
            // insertions happened)

            // Source and target index values are equal. Copy the target data first.
            const timestamp equal_run_index_value = *target_index_it;
            // Since the index is ordered, no index in source_index[source_row_idx] can be less than target_row_idx.
            // The number of inserted is used to correct for the fact that rows_to_update[source_row_idx] refers rows
            // in the original column.
            const size_t inserted_rows = new_column_row_idx - target_row_idx;
            while (target_index_it != target_index_end && *target_index_it == equal_run_index_value &&
                   source_index[source_row_idx] == equal_run_index_value) {
                *new_column_it++ = target_data[target_row_idx++];
                ++new_column_row_idx;
                ++target_index_it;
            }

            // For each source row equal to the target row, there are two options.
            // * source_index[source_row_idx] is non-empty -> the output buffer must be updated using inserted_rows as
            //   an offset
            // * source_index[source_row_idx] is empty -> insertion must happen by appending to the end of the output
            //   buffer.
            while (source_row_idx < source_index.size() && source_index[source_row_idx] == equal_run_index_value) {
                if (match_rechord.should_insert(
                            source_range.first + source_row_idx, row_slice, source_index[source_row_idx]
                    )) {
                    *new_column_it++ = source_data[source_row_idx];
                    ++new_column_row_idx;
                } else if (strategy.update()) {
                    for (const size_t target_row_to_update : rows_to_update[source_row_idx]) {
                        new_data[target_row_to_update + inserted_rows] = source_data[source_row_idx];
                    }
                }
                ++source_row_idx;
            }

            if (target_index_it == target_index_end) {
                break;
            }

            // Target index values are larger than the source index value. Append the source data to the output buffer.
            while (source_row_idx < source_index.size() && *target_index_it > source_index[source_row_idx]) {
                *new_column_it++ = source_data[source_row_idx++];
                ++new_column_row_idx;
            }
        }
        // Not all target rows were processed, copy the rest
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                static_cast<position_t>(target_row_idx) <= target.row_count(),
                "Cannot consume more target rows than there are in the target column"
        );
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                static_cast<position_t>(target_row_idx) >= target.row_count() ||
                        new_column_it != new_column_data.end<TargetColumnTypeDescriptorTag>(),
                "There are {} target rows to insert but the output buffer is exhausted",
                target.row_count() - static_cast<position_t>(target_row_idx)
        );
        while (static_cast<position_t>(target_row_idx) < target.row_count()) {
            *new_column_it++ = target_data[target_row_idx++];
        }
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                source_row_idx >= source_index.size() ||
                        new_column_it != new_column_data.end<TargetColumnTypeDescriptorTag>(),
                "There are {} source rows to insert but the output buffer is exhausted",
                source_index.size() - source_row_idx
        );
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                source_row_idx <= source_index.size(),
                "Cannot consume more source rows than there are in the source column"
        );
        // Not all source rows were processed, copy the rest
        std::copy(source_data.begin() + source_row_idx, source_data.end(), new_column_it);
        return new_column;
    } else {
        internal::raise<ErrorCode::E_NOT_IMPLEMENTED>("Insertion is not implemented for string columns yet");
    }
}

template<util::type_descriptor_tag ScalarType, typename SourceT>
bool update_column(
        const InputFrame& source, const MergeStrategy& strategy, const StringPool& old_string_pool,
        const std::span<const SourceT> source_data, const std::span<const std::vector<size_t>> rows_to_update,
        const RowRange& row_range, const bool is_matching_column, std::string_view column_name, Column& target_column,
        StringPool& new_string_pool
) {
    bool row_slice_changed = false;
    const bool is_timeseries = source.desc().index().type() == IndexDescriptor::Type::TIMESTAMP;
    if constexpr (is_sequence_type(ScalarType::data_type())) {
        const MergeUpdateStringColumnFlags column_update_flags{
                .is_timeseries = is_timeseries, .data_not_changed = is_matching_column && strategy.update_only()
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

/// Places all ranges and keys that could contain the same index value in a single logical unit to be processed by
/// MergeUpdateClause::process.
std::vector<std::vector<size_t>> structure_by_time_slice(std::span<RangesAndKey> ranges) {
    std::ranges::sort(ranges, [](const RangesAndKey& left, const RangesAndKey& right) {
        return std::tie(left.row_range().first, left.col_range().first) <
               std::tie(right.row_range().first, right.col_range().first);
    });
    std::vector<std::vector<size_t>> res;
    TimestampRange previous_time_range{std::numeric_limits<timestamp>::min(), std::numeric_limits<timestamp>::min()};
    for (const auto& [idx, ranges_and_key] : folly::enumerate(ranges)) {
        const TimestampRange current_time_range = ranges_and_key.key_.time_range();
        if (previous_time_range.second <= current_time_range.first) {
            res.emplace_back();
        }
        res.back().emplace_back(idx);
        previous_time_range = current_time_range;
    }
    return res;
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
    const TypedTensor<IndexType::DataTypeTag::raw_type> index_tensor(source_->opt_index_tensor().value());
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
        const std::vector<std::vector<size_t>> row_slice_offsets =
                must_structure_by_time_slice() ? ::split_by_row_slice(ranges_and_keys, offsets[row_slice_idx])
                                               : std::vector<std::vector<size_t>>{offsets[row_slice_idx]};
        bool keep_slice = false;
        for (const std::vector<size_t>& row_slice_offset : row_slice_offsets) {
            const TimestampRange& time_range = ranges_and_keys[row_slice_offset.front()].key_.time_range();
            if (source_start_end_for_row_range_.contains(time_range)) {
                row_slices_to_keep.push_back(row_slice_idx);
                keep_slice = true;
                continue;
            }

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
                    // former
                    const timestamp next_row_slice_start =
                            ranges_and_keys[offsets[row_slice_idx + 1].front()].key_.time_range().first;
                    if (*source_range_start >= next_row_slice_start) {
                        // The extended segment does not contain any source values. All source values are after the
                        // extended segment end. Continue checking next row slices.
                        continue;
                    }
                    source_range_end =
                            std::upper_bound(source_range_start, source_index.end(), next_row_slice_start - 1);
                }
            } else {
                source_range_end = ranges::upper_bound(source_range_start, source_index.end(), time_range.second - 1);
            }
            const std::pair<size_t, size_t> source_row_range = {
                    source_range_start - source_index.begin(), source_range_end - source_index.begin()
            };
            source_start_end_for_row_range_.insert({time_range, source_row_range});
            keep_slice = true;
        }
        if (keep_slice) {
            row_slices_to_keep.push_back(row_slice_idx);
        }
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
    if (entity_ids.empty()) {
        return {};
    }
    auto proc = gather_entities<
            std::shared_ptr<SegmentInMemory>,
            std::shared_ptr<RowRange>,
            std::shared_ptr<ColRange>,
            std::shared_ptr<AtomKey>>(*component_manager_, std::move(entity_ids));
    std::vector<ProcessingUnit> row_slices =
            must_structure_by_time_slice() ? split_by_row_slice(std::move(proc)) : std::vector{std::move(proc)};
    std::optional<MatchRechord> maybe_index_match = [&] {
        std::optional<MatchRechord> result;
        if (source_->has_index()) {
            using IndexType = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
            const TypedTensor<IndexType::DataTypeTag::raw_type> index_tensor(source_->opt_index_tensor().value());
            result.emplace(filter_index_match(std::span(index_tensor.data(), source_->num_rows), row_slices));
        }
        return result;
    }();
    // TODO: Source and target descriptor can differ for dynamic schema

    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            !source_->has_segment(), "Arrow format is not supported as input for merge update"
    );
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            source_->has_tensors(), "Input frame does not contain neither a segment nor tensors"
    );
    const MatchRechord matched = filter_on_additional_columns_match(
            source_->desc(), source_->desc(), row_slices, std::move(maybe_index_match)
    );
    if (update_and_insert(
                source_->opt_index_tensor(), source_->field_tensors(), source_->desc(), row_slices, matched
        )) {
        std::vector<EntityId> res;
        for (size_t row_slice_idx = 0; row_slice_idx < row_slices.size(); ++row_slice_idx) {
            ProcessingUnit& row_slice = row_slices[row_slice_idx];
            const size_t entity_count = row_slice.segments_->size();
            const size_t inserted_rows_count = matched.num_unmatched_rows(row_slice_idx);
            std::vector<EntityId> entts = component_manager_->add_entities(
                    std::move(*row_slice.segments_),
                    std::move(*row_slice.row_ranges_),
                    std::move(*row_slice.col_ranges_),
                    std::vector<EntityFetchCount>(entity_count, 1),
                    std::vector(entity_count, MergeUpdateInsertedRowsEntity{inserted_rows_count})
            );
            res.insert(res.end(), std::make_move_iterator(entts.begin()), std::make_move_iterator(entts.end()));
        }
        return res;
    }

    return {};
}

MergeUpdateClause::MatchRechord MergeUpdateClause::initialize_rows_to_update_for_row_range_indexed_data(
        std::span<ProcessingUnit> row_slices, const StreamDescriptor& source_descriptor
) const {
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            !on_.empty(),
            "MergeUpdate requires at least one column in the \"on\" parameter when the DataFrame is not a "
            "timeseries"
    );
    MatchRechord result(row_slices, get_source_start_end_for_time_slice(row_slices));
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
            using SourceRawType = typename SourceTDT::DataTypeTag::raw_type;
            const ColumnWithStrings target_column = std::get<ColumnWithStrings>(proc.get(ColumnName{column_name}));
            details::visit_type(target_column.column_->type().data_type(), [&](auto target_field_dt) {
                using TargetTDT = ScalarTagType<decltype(target_field_dt)>;
                if constexpr (std::same_as<std::decay_t<SourceTDT>, std::decay_t<TargetTDT>>) {
                    using SourceType = std::
                            conditional_t<is_sequence_type(SourceTDT::data_type()), PyObject* const, SourceRawType>;
                    auto target_values_to_rows = map_column_values_to_rows<TargetTDT>(target_column);
                    const size_t column_position_in_source_tensors =
                            source_field_position - source_descriptor.index().field_count();
                    std::span source_data(
                            static_cast<const SourceType*>(
                                    source_->field_tensors()[column_position_in_source_tensors].data()
                            ),
                            source_->num_rows
                    );
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
                                                    row_slice_idx,
                                                    source_row_idx,
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

std::pair<size_t, size_t> MergeUpdateClause::get_source_start_end(const ProcessingUnit& proc) const {
    if (source_->has_index()) {
        const std::span<const std::shared_ptr<AtomKey>> atom_keys = *proc.atom_keys_;
        const auto source_row_range_it = source_start_end_for_row_range_.find(atom_keys.front()->time_range());
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                source_row_range_it != source_start_end_for_row_range_.end(),
                "Missing mapping between AtomKey timerange {} {} and source row start/end",
                atom_keys.front()->time_range().first,
                atom_keys.front()->time_range().second
        );
        return source_row_range_it->second;
    } else {
        return std::pair{0, source_->num_rows};
    }
}

bool MergeUpdateClause::update_and_insert(
        const std::optional<NativeTensor>& index_tensor, const std::span<const NativeTensor> source_tensors,
        const StreamDescriptor& source_descriptor, std::span<const ProcessingUnit> row_slices,
        const MatchRechord& match_record
) const {
    bool time_slice_changed = false;
    for (size_t i = 0; i < row_slices.size(); ++i) {
        const size_t num_matched_rows = match_record.num_matched_rows(i);

        if (strategy_.update_only() && num_matched_rows == 0) {
            continue;
        }

        const size_t num_rows_to_insert = match_record.num_unmatched_rows(i) * strategy_.insert();
        if (strategy_.insert_only() && !num_rows_to_insert) {
            continue;
        }

        if (strategy_.update() && num_matched_rows > 0) {
            match_record.validate_rows_to_update();
        }
        const ProcessingUnit& proc = row_slices[i];
        const std::pair<size_t, size_t> source_range = get_source_start_end(proc);
        const std::span<const std::shared_ptr<SegmentInMemory>> target_segments = *proc.segments_;
        const bool is_timeseries = source_->desc().index().type() == IndexDescriptor::Type::TIMESTAMP;
        std::optional<Column> new_index = [&] {
            std::optional<Column> result;
            if (num_rows_to_insert > 0 && is_timeseries) {
                using IndexType = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
                const std::span source_data(
                        static_cast<const timestamp*>(index_tensor->data()),
                        index_tensor->nbytes() / index_tensor->elsize()
                );
                result.emplace(merge<IndexType>(
                        target_segments[0]->column(0),
                        source_data,
                        target_segments[0]->column(0),
                        source_data,
                        source_range,
                        match_record,
                        num_rows_to_insert,
                        MergeStrategy{.not_matched_by_target = MergeAction::INSERT}
                ));
            }
            return result;
        }();

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
                    // For pandas input the index NativeTensor is stored separately from the field NativeTensors.
                    // Subtract the index fields to get the correct position in the source descriptor
                    const size_t column_position_in_source = column_position_in_ts_descriptor - index_fields;
                    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                            util::is_cstyle_array<RawType>(source_tensors[column_position_in_source]),
                            "Fortran-style arrays are not supported by merge update yet. Column \"{}\" has data type "
                            "{} of size {} bytes but the stride is {} bytes",
                            target_field.name(),
                            target_field.type(),
                            sizeof(RawType),
                            source_tensors[column_position_in_source].strides()[0]
                    );
                    Column& target_column = target_segment.column(column_index_in_slice);
                    const NativeTensor& source_tensor = source_tensors[column_position_in_source];
                    using SourceType =
                            std::conditional_t<is_sequence_type(DataTypeTag::data_type), PyObject* const, RawType>;
                    const std::span source_data(
                            static_cast<const SourceType*>(source_tensor.data()),
                            source_tensor.nbytes() / source_tensor.elsize()
                    );
                    std::span<const std::vector<size_t>> rows_to_update = match_record.matched_rows(i);
                    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                            !rows_to_update.empty(),
                            "There must be at least one source row inside the target row slice."
                    );
                    // String columns are always recreated from scratch regardless if an update or insert is happening.
                    // This is because the string pool must be updated. With data read from disk, the map_ member of the
                    // pool is not populated. Which means that the mapping between a string and offset in the pool is
                    // missing. To get it, we need to rebuild the pool anyway.
                    segment_contains_string_column = is_sequence_type(DataTypeTag::data_type);
                    if (strategy_.update_only() || num_rows_to_insert == 0) {
                        time_slice_changed |= update_column<ScalarType>(
                                *source_,
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
                    } else {
                        internal::check<ErrorCode::E_NOT_IMPLEMENTED>(
                                is_timeseries, "Merge update with insertion is implemented only for timeseries"
                        );
                        const std::span source_index(
                                static_cast<const timestamp*>(index_tensor->data()), source_tensor.size()
                        );
                        const Column& target_index = target_segment.column(0);
                        target_segment.column(column_index_in_slice) = merge<ScalarType>(
                                target_index,
                                source_index,
                                target_column,
                                source_data,
                                source_range,
                                match_record,
                                num_rows_to_insert,
                                strategy_
                        );
                        time_slice_changed = true;
                    }
                });
            }
            if (num_rows_to_insert > 0) {
                if (new_index) {
                    const bool is_last_segment = segment_idx == target_segments.size() - 1;
                    target_segment.column(0) = is_last_segment ? std::move(*new_index) : new_index->clone();
                }
                target_segment.set_row_data(target_segment.row_count() + num_rows_to_insert - 1);
            }
            if (segment_contains_string_column) {
                target_segment.set_string_pool(std::make_shared<StringPool>(std::move(new_string_pool)));
            }
        }
    }

    return time_slice_changed;
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
MergeUpdateClause::MatchRechord MergeUpdateClause::filter_index_match(
        const std::span<const timestamp> source_index, std::span<ProcessingUnit> row_slices
) const {
    using IndexType = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
    MatchRechord result(row_slices, get_source_start_end_for_time_slice(row_slices));
    for (size_t i = 0; i < row_slices.size(); ++i) {
        const ProcessingUnit& proc = row_slices[i];
        const Column& target_index = proc.segments_->front()->column(0);
        ColumnData target_index_column_data = target_index.data();
        const auto [source_row_start, source_row_end] = get_source_start_end(proc);
        const auto target_index_accessor = random_accessor<IndexType>(&target_index_column_data);
        const size_t last_target_row_to_consider = [&] {
            auto row_indexes = ranges::iota_view{position_t{0}, target_index.row_count()};
            const auto upper_bound = ranges::upper_bound(
                    row_indexes,
                    source_index.back(),
                    [&](const timestamp value, const int64_t row_idx) {
                        return value < target_index_accessor.at(row_idx);
                    }
            );
            return upper_bound == row_indexes.end() ? target_index.row_count() : *upper_bound;
        }();
        size_t source_row = source_row_start;
        size_t target_row = 0;
        // This loop can be inverted so that if source_row_end - source_row_start is > len(target) the complexity
        // becomes O(n * log_2(m)) where: m is the count of source rows in the bounds of the segment, n is the number of
        // target rows in the segment.
        while (target_row < last_target_row_to_consider && source_row < source_row_end) {
            const timestamp source_ts = source_index[source_row];
            // TODO: Profile performance and try different optimizations. See Monday 10655963947
            auto row_index_view = std::ranges::iota_view{target_row, last_target_row_to_consider};
            auto target_match_it =
                    ranges::lower_bound(row_index_view, source_ts, [&](const size_t row_idx, const timestamp source) {
                        return target_index_accessor.at(row_idx) < source;
                    });
            if (target_match_it == row_index_view.end()) {
                break;
            }
            target_row = *target_match_it;
            while (target_row < last_target_row_to_consider && target_index_accessor.at(target_row) == source_ts) {
                result.add_match(source_row, i, target_row);
                ++target_row;
            }
            // Optimizes the case of repeated index values. All matched rows corresponding to a particular index value
            // must be the same, so just copy the matched rows in case index values are repeated.
            while (++source_row < source_row_end && source_index[source_row] == source_ts) {
                result.clone_source_match(source_row - 1, source_row);
            }
        }
    }

    return result;
}

/// Complexity: $$O(c * n * m)$$ m is the count of source rows in the bounds of the segment, n is the  number of target
/// rows in the segment. c is the number of columns in MergeUpdateClause::on_. It can be reached if the data in source
/// and target is the same up to the very last column in MergeUpdateClause::on_.
MergeUpdateClause::MatchRechord MergeUpdateClause::filter_on_additional_columns_match(
        const StreamDescriptor& source_descriptor, const StreamDescriptor& target_descriptor,
        std::span<ProcessingUnit> row_slices, std::optional<MatchRechord>&& index_match
) const {
    ranges::subrange on = on_;
    MatchRechord matched_rows = [&] {
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

    for (const std::string_view column_name : on) {
        const size_t source_field_position = field_index_for_matching_on_column(column_name, source_descriptor);
        const Field& source_field = source_descriptor.field(source_field_position);
        const size_t column_position_in_source_tensors =
                source_field_position - source_descriptor.index().field_count();
        // TODO: For dynamic schema the two fields might have different indexes
        const size_t target_field_position = source_field_position;
        const Field& target_field = target_descriptor.field(target_field_position);
        matched_rows.filter_matching_rows(
                target_field.name(),
                source_field.type().data_type(),
                target_field.type().data_type(),
                source_->field_tensors()[column_position_in_source_tensors],
                source_->opt_index_tensor()
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

std::pair<size_t, size_t> MergeUpdateClause::get_source_start_end_for_time_slice(
        std::span<const ProcessingUnit> row_slices
) const {
    return {get_source_start_end(row_slices.front()).first, get_source_start_end(row_slices.back()).second};
}

MergeUpdateClause::MatchRechord::MatchRechord(
        std::span<ProcessingUnit> row_slices, const std::pair<size_t, size_t>& source_range
) :
    matched_target_rows_(row_slices.size(), std::vector<std::vector<size_t>>(source_range.second - source_range.first)),
    matched_count_(row_slices.size(), 0),
    unmatched_count_(row_slices.size(), source_range.second - source_range.first),
    source_range_for_times_slice_(source_range),
    row_slices_(row_slices) {
    if (is_timeseries()) {
        const Column& index = row_slices.front().segments_->front()->column(0);
        running_index_value_ = *index.scalar_at<timestamp>(index.row_count() - 1);
    }
}

void MergeUpdateClause::MatchRechord::add_match(size_t source_row, size_t target_row_slice, size_t target_row) {
    const size_t relative_source_row = source_row - source_range_for_times_slice_.first;
    const bool is_unmatched = matched_target_rows_[target_row_slice][relative_source_row].empty();
    matched_count_[target_row_slice] += is_unmatched;
    unmatched_count_[target_row_slice] -= is_unmatched;
    matched_target_rows_[target_row_slice][relative_source_row].push_back(target_row);
}
void MergeUpdateClause::MatchRechord::add_match(
        size_t source_row, size_t target_row_slice, std::span<size_t> target_rows
) {
    const size_t relative_source_row = source_row - source_range_for_times_slice_.first;
    const bool is_unmatched = matched_target_rows_[target_row_slice][relative_source_row].empty();
    matched_count_[target_row_slice] += is_unmatched;
    unmatched_count_[target_row_slice] -= is_unmatched;
    const auto end = matched_target_rows_[target_row_slice][relative_source_row].end();
    matched_target_rows_[target_row_slice][source_row - source_range_for_times_slice_.first].insert(
            end, target_rows.begin(), target_rows.end()
    );
}

void MergeUpdateClause::MatchRechord::filter_matching_rows(
        std::string_view column_name, DataType source_type, DataType target_type, const NativeTensor& source_tensor,
        const std::optional<NativeTensor>& source_index_tensor
) {
    // GIL will be acquired if there is a string that is not pure ASCII/UTF-8
    // In this case a PyObject will be allocated by convert::py_unicode_to_buffer
    // If such a string is encountered in a column, then the GIL will be held until that whole column has
    // been processed, on the assumption that if a column has one such string it will probably have many.
    std::optional<ScopedGILLock> scoped_gil_lock;
    details::visit_type(source_type, [&]<typename SourceDataTypeTag>(SourceDataTypeTag) {
        using SourceTDT = ScalarTagType<SourceDataTypeTag>;
        using SourceType = std::conditional_t<
                is_sequence_type(SourceDataTypeTag::data_type),
                PyObject* const*,
                const typename SourceDataTypeTag::raw_type*>;
        std::span source_data(
                static_cast<SourceType>(source_tensor.data()) + source_range_for_times_slice_.first,
                source_range_for_times_slice_.second - source_range_for_times_slice_.first
        );
        details::visit_type(target_type, [&]<typename TargetDataTypeTag>(TargetDataTypeTag) {
            using TargetTDT = ScalarTagType<TargetDataTypeTag>;
            using TargetRawType = TargetDataTypeTag::raw_type;
            // TODO: Relax for dynamic schema
            if constexpr (std::same_as<std::decay_t<SourceDataTypeTag>, std::decay_t<TargetDataTypeTag>>) {
                for (size_t row_slice_idx = 0; row_slice_idx < matched_target_rows_.size(); ++row_slice_idx) {
                    ProcessingUnit& row_slice = row_slices_[row_slice_idx];
                    ColumnWithStrings target_column =
                            std::get<ColumnWithStrings>(row_slice.get(ColumnName{column_name}));
                    ColumnData col_data = target_column.column_->data();
                    auto target_column_accessor = random_accessor<TargetTDT>(&col_data);
                    std::vector<std::vector<size_t>>& matched_rows = matched_target_rows_[row_slice_idx];
                    for (size_t source_row_idx = 0; source_row_idx < matched_rows.size(); ++source_row_idx) {
                        const size_t initial_target_rows_to_update_count = matched_rows[source_row_idx].size();
                        const size_t discarded_matches =
                                std::erase_if(matched_rows[source_row_idx], [&](const size_t target_row) {
                                    const TargetRawType target_value = target_column_accessor[target_row];
                                    const auto& source_value = source_data[source_row_idx];
                                    auto are_values_equal = are_merge_values_matching<SourceTDT, TargetTDT>(
                                            source_value, target_value, *target_column.string_pool_, scoped_gil_lock
                                    );
                                    if constexpr (is_sequence_type(TargetDataTypeTag::data_type)) {
                                        return util::variant_match(
                                                are_values_equal,
                                                [](const bool equal) { return !equal; },
                                                [&](convert::StringEncodingError& err) -> bool {
                                                    err.row_index_in_slice_ = source_row_idx;
                                                    err.raise(column_name, source_range_for_times_slice_.first);
                                                }
                                        );
                                    } else {
                                        return !std::get<bool>(are_values_equal);
                                    }
                                });
                        const bool initially_matched = initial_target_rows_to_update_count;
                        const bool becomes_unmatched =
                                initially_matched && initial_target_rows_to_update_count == discarded_matches;
                        matched_count_[row_slice_idx] -= becomes_unmatched;
                        bool unmatched_across_all_row_slices = becomes_unmatched;
                        if (becomes_unmatched && is_timeseries() && row_slices_.size() > 1) {
                            ARCTICDB_DEBUG_CHECK(
                                    ErrorCode::E_ASSERTION_FAILURE,
                                    source_index_tensor,
                                    "When an index value is contained in more than one segment filtering on column "
                                    "other than the index must also use the source index to decide in which target "
                                    "rows lice the source will be inserted"
                            );
                            const size_t source_row = source_range_for_times_slice_.first + source_row_idx;

                            const bool is_last_row_slice = row_slice_idx == row_slices_.size() - 1;
                            const bool source_index_value_crosses_segments =
                                    static_cast<const timestamp*>(source_index_tensor->data())[source_row] ==
                                    running_index_value_;
                            unmatched_across_all_row_slices =
                                    !source_index_value_crosses_segments ||
                                    (is_last_row_slice &&
                                     std::ranges::all_of(
                                             matched_target_rows_,
                                             [&](const std::span<const std::vector<size_t>>& matched) {
                                                 return matched[source_row_idx].empty();
                                             }
                                     ));
                        }
                        unmatched_count_[row_slice_idx] += unmatched_across_all_row_slices;
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

void MergeUpdateClause::MatchRechord::clone_source_match(size_t source_row_src, size_t source_row_dst) {
    const size_t relative_src = source_row_src - source_range_for_times_slice_.first;
    const size_t relative_dst = source_row_dst - source_range_for_times_slice_.first;
    for (size_t row_slice = 0; row_slice < matched_target_rows_.size(); ++row_slice) {
        const bool is_new_match = !matched_target_rows_[row_slice][relative_src].empty() &&
                                  matched_target_rows_[row_slice][relative_dst].empty();
        matched_count_[row_slice] += is_new_match;
        unmatched_count_[row_slice] -= is_new_match;
        matched_target_rows_[row_slice][relative_dst] = matched_target_rows_[row_slice][relative_src];
    }
}

size_t MergeUpdateClause::MatchRechord::num_matched_rows(size_t row_slice) const { return matched_count_[row_slice]; }
size_t MergeUpdateClause::MatchRechord::num_unmatched_rows(size_t row_slice) const {
    return unmatched_count_[row_slice];
}
void MergeUpdateClause::MatchRechord::validate_rows_to_update() const {
    // TODO: This can be inlined in the loop iterating over all columns to avoid iterating the source one more
    // time. The loop structure makes it not intuitive. The performance cost must be evaluated. Monday:
    // 10655963947
    ankerl::unordered_dense::set<std::pair<size_t, size_t>> matched_target_rows;
    for (size_t row_slice = 0; row_slice < matched_target_rows_.size(); ++row_slice) {
        for (size_t source_row = 0; source_row < matched_target_rows_[row_slice].size(); ++source_row) {
            for (size_t target_row : matched_target_rows_[row_slice][source_row]) {
                auto [_, inserted] = matched_target_rows.insert({target_row, row_slice});
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                        inserted,
                        "Multiple source rows match the same target row. This is not allowed as it's not clear "
                        "which source row should be used for the update. Source row index is: {}",
                        source_range_for_times_slice_.first + source_row
                );
            }
        }
    }
}

std::span<const std::vector<size_t>> MergeUpdateClause::MatchRechord::matched_rows(size_t target_row_slice) const {
    return matched_target_rows_[target_row_slice];
}

[[nodiscard]] bool MergeUpdateClause::MatchRechord::should_insert(
        size_t source_row, size_t target_row_slice, timestamp source_index
) const {
    const size_t relative_source_row = source_row - source_range_for_times_slice_.first;
    if (source_index != running_index_value_) {
        return matched_target_rows_[target_row_slice][relative_source_row].empty();
    }
    return target_row_slice == row_slices_.size() - 1 &&
           std::ranges::all_of(matched_target_rows_, [&](const std::span<const std::vector<size_t>> matched) {
               return matched[relative_source_row].empty();
           });
}

bool MergeUpdateClause::MatchRechord::is_timeseries() const {
    return row_slices_.front().segments_->front()->descriptor().index().type() == IndexDescriptor::Type::TIMESTAMP;
}

} // namespace arcticdb
