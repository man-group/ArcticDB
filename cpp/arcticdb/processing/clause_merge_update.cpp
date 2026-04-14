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
#include <arcticdb/pipeline/frame_utils_python.hpp>
#include <arcticdb/version/schema_checks.hpp>
#include <arcticdb/pipeline/slicing.hpp>
#include <ankerl/unordered_dense.h>

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

template<typename TDT>
requires util::instantiation_of<TDT, TypeDescriptorTag>
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

template<typename TDT>
requires util::instantiation_of<TDT, TypeDescriptorTag>
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

/// When merge update is performed on a string column the string pool is rebuild from scratch, thus we must iterate over
/// all rows in the column. When the column is a part of a timeseries all target rows stored in rows_to_update will be
/// ordered (i.e. sorted(flatten(rows_to_update)) = true). This means we can iterate over the target column row and
/// at the same time forward iterate on the source rows. It's a nested loop but the complexity is O(n + m + k)
/// where n is the total number of rows in the target, m are the rows in the source, and k are the total matches between
/// source and target (i.e. sum(rows_to_update[i] for i in range(len(rows_to_update))))
template<typename TDT>
requires util::instantiation_of<TDT, TypeDescriptorTag>
bool merge_update_string_column_timeseries(
        Column& target_column, const std::span<const std::vector<size_t>> rows_to_update, const Field& target_field,
        const RowRange& row_range, const StringPool& target_string_pool,
        const std::span<PyObject* const> source_column_view, StringPool& new_string_pool
) {
    bool is_column_changed = false;
    if constexpr (is_fixed_string_type(TDT::data_type())) {
        user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                "Fixed string sequences are not supported for merge update"
        );
    } else if constexpr (is_dynamic_string_type(TDT::data_type())) {
        // GIL will be acquired if there is a string that is not pure ASCII/UTF-8
        // In this case a PyObject will be allocated by convert::py_unicode_to_buffer
        // If such a string is encountered in a column, then the GIL will be held until that whole column has
        // been processed, on the assumption that if a column has one such string it will probably have many.
        std::optional<ScopedGILLock> gil_lock;
        // TODO: Create new column in case of insertion
        auto source_row_it = rows_to_update.begin();
        auto next_target_row_to_update = source_row_it->begin();
        // TODO: Handle sparse target columns Monday 10756321294
        arcticdb::for_each_enumerated<TDT>(target_column, [&](auto row) {
            source_row_it = std::find_if(
                    source_row_it,
                    rows_to_update.end(),
                    [&, new_row = false](const std::vector<size_t>& vec) mutable {
                        // TODO: Handle inserts. Empty vec means insert.
                        next_target_row_to_update = std::find_if(
                                new_row ? vec.begin() : next_target_row_to_update,
                                vec.end(),
                                [&](const int64_t target_row_to_update) { return target_row_to_update >= row.idx(); }
                        );
                        if (next_target_row_to_update == vec.end()) {
                            new_row = true;
                        }
                        return next_target_row_to_update != vec.end();
                    }
            );
            const bool current_target_row_is_matched = source_row_it != rows_to_update.end() &&
                                                       next_target_row_to_update != source_row_it->end() &&
                                                       static_cast<int64_t>(*next_target_row_to_update) == row.idx();
            if (current_target_row_is_matched) {
                is_column_changed = true;
                const auto py_string_object = source_column_view[source_row_it - rows_to_update.begin()];
                row.value() = write_py_string_to_pool_or_throw<TDT>(
                        py_string_object, row.idx(), row_range, gil_lock, new_string_pool, target_field.name()
                );
            } else {
                if (is_a_string(row.value())) {
                    const std::string_view string_in_target = target_string_pool.get_const_view(row.value());
                    const OffsetString offset_in_new_pool = new_string_pool.get(string_in_target);
                    row.value() = offset_in_new_pool.offset();
                }
            }
        });
    }
    return is_column_changed;
}

/// When merge update is performed on a string column the string pool is rebuild from scratch, thus we must iterate over
/// all rows in the column. When the column is *not* part of a timeseries the rows in rows_to_update are in random
/// order. To avoid having quadratic complexity (i.e. O(n * k) where n are the rows in the target and k are all matches
/// i.e. sum(rows_to_update[i] for i in range(len(rows_to_update)))) we invert the loops and iterate over all
/// rows_to_update perform the update and then add additional loop over all rows in target to rebuild the string pool.
/// Asymptotically this will yield the same complexity as the timeseries case but practically is a bit slower.
template<typename TDT>
requires util::instantiation_of<TDT, TypeDescriptorTag>
bool merge_update_string_column_rowrange(
        Column& target_column, const std::span<const std::vector<size_t>> rows_to_update, const Field& target_field,
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
                        py_string_object, target_row, row_range, gil_lock, new_string_pool, target_field.name()
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

template<typename TDT>
requires util::instantiation_of<TDT, TypeDescriptorTag>
bool merge_update_string_column(
        Column& target_column, const MergeUpdateStringColumnFlags& flags,
        const std::span<const std::vector<size_t>> rows_to_update, const Field& target_field, const RowRange& row_range,
        const StringPool& target_string_pool, const std::span<PyObject* const> source_column_view,
        StringPool& new_string_pool
) {
    if (flags.data_not_changed) {
        rebuild_sequence_column_in_new_pool<TDT>(target_column, target_string_pool, new_string_pool);
        return false;
    } else {
        if (flags.is_timeseries) {
            return merge_update_string_column_timeseries<TDT>(
                    target_column,
                    rows_to_update,
                    target_field,
                    row_range,
                    target_string_pool,
                    source_column_view,
                    new_string_pool
            );
        } else {
            return merge_update_string_column_rowrange<TDT>(
                    target_column,
                    rows_to_update,
                    target_field,
                    row_range,
                    target_string_pool,
                    source_column_view,
                    new_string_pool
            );
        }
    }
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
        typename SourceTDT, typename TargetTDT, typename SourceValueRawType = TargetTDT::DataTypeTag::raw_type,
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

template<typename TDT>
requires util::instantiation_of<TDT, TypeDescriptorTag>
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
                target_values[column.string_at_offset(row.value())].push_back(row.idx());
            } else {
                target_values[std::nullopt].push_back(row.idx());
            }
        } else {
            target_values[row.value()].push_back(row.idx());
        }
    });
    return target_values;
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
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            strategy_.not_matched_by_target == MergeAction::DO_NOTHING, "Merge cannot perform insertion at the moment"
    );
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
    std::vector<std::vector<size_t>> offsets = structure_by_row_slice(ranges_and_keys);
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
    std::span index(static_cast<const IndexType::DataTypeTag::raw_type*>(index_tensor.data()), source_->num_rows);
    for (size_t row_slice_idx = 0; row_slice_idx < offsets.size(); ++row_slice_idx) {
        // TODO: Add logic for insertion.
        const TimestampRange& time_range = ranges_and_keys[offsets[row_slice_idx].front()].key_.time_range();
        if (source_start_end_for_row_range_.contains(time_range)) {
            row_slices_to_keep.push_back(row_slice_idx);
            continue;
        }
        const auto source_range_start = std::ranges::lower_bound(index, time_range.first);
        if (source_range_start == index.end()) {
            break;
        }
        if (*source_range_start < time_range.second) {
            auto source_range_end = std::upper_bound(source_range_start, index.end(), time_range.second - 1);
            const std::pair<size_t, size_t> source_row_range = {
                    source_range_start - index.begin(), source_range_end - index.begin()
            };
            source_start_end_for_row_range_.insert({time_range, source_row_range});
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
/// len(source) where each element is a vector of row-indexes in target that match the corresponding source index value.
/// The produced vector is passed to the next step
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
    // TODO: Add exception handling two source rows matching the same target row. This should be done in the
    // function
    //  handling the "on" parameter matching multiple columns. Monday 10655943156
    auto matched = [&]() -> std::optional<std::vector<std::vector<size_t>>> {
        std::optional<std::vector<std::vector<size_t>>> result;
        if (source_->has_index()) {
            using IndexType = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
            const TypedTensor<IndexType::DataTypeTag::raw_type> index_tensor(source_->opt_index_tensor().value());
            result.emplace(filter_index_match(
                    proc.segments_->front()->column(0), std::span(index_tensor.data(), source_->num_rows), proc
            ));
        }
        return result;
    }();
    // TODO: Source and target descriptor can differ for dynamic schema
    matched = filter_on_additional_columns_match(
            source_->desc(), source_->desc(), source_->field_tensors(), proc, std::move(matched)
    );
    if (source_->has_segment()) {
        user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Arrow format is not supported as input for merge update"
        );
    } else if (source_->has_tensors()) {
        if (update_and_insert(source_->field_tensors(), source_->desc(), proc, *matched)) {
            return push_entities(*component_manager_, std::move(proc));
        }
    } else {
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Input frame does not contain neither a segment nor tensors");
    }
    return {};
}

std::vector<std::vector<size_t>> MergeUpdateClause::initialize_rows_to_update_for_rowrange_indexed_data(
        ProcessingUnit& proc, const StreamDescriptor& source_descriptor
) const {
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            !on_.empty(),
            "MergeUpdate requires at least one column in the \"on\" parameter when the DataFrame is not a "
            "timeseries"
    );
    // GIL will be acquired if there is a string that is not pure ASCII/UTF-8
    // In this case a PyObject will be allocated by convert::py_unicode_to_buffer
    // If such a string is encountered in a column, then the GIL will be held until that whole column has
    // been processed, on the assumption that if a column has one such string it will probably have many.
    std::optional<ScopedGILLock> scoped_gil_lock;
    const std::string_view column_name = on_.front();
    std::vector<std::vector<size_t>> result;
    result.resize(source_->num_rows);
    const std::optional<size_t> source_field_position = source_descriptor.find_field(column_name);
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            source_field_position,
            "Column '{}' specified in the 'on' parameter is not present in the source DataFrame",
            column_name
    );
    const Field& source_field = source_descriptor.field(*source_field_position);
    details::visit_type(source_field.type().data_type(), [&](auto source_field_dt) {
        using SourceTDT = ScalarTagType<std::decay_t<decltype(source_field_dt)>>;
        using SourceRawType = typename SourceTDT::DataTypeTag::raw_type;
        const ColumnWithStrings target_column = std::get<ColumnWithStrings>(proc.get(ColumnName{column_name}));
        details::visit_type(target_column.column_->type().data_type(), [&](auto target_field_dt) {
            using TargetTDT = ScalarTagType<decltype(target_field_dt)>;
            if constexpr (std::is_same_v<std::decay_t<SourceTDT>, std::decay_t<TargetTDT>>) {
                using SourceType =
                        std::conditional_t<is_sequence_type(SourceTDT::data_type()), PyObject* const, SourceRawType>;
                auto target_values_to_rows = map_column_values_to_rows<TargetTDT>(target_column);
                const size_t column_position_in_source_tensors =
                        *source_field_position - source_descriptor.index().field_count();
                std::span source_data(
                        static_cast<const SourceType*>(source_->field_tensors()[column_position_in_source_tensors].data(
                        )),
                        source_->num_rows
                );
                for (size_t source_row_idx = 0; source_row_idx < source_data.size(); ++source_row_idx) {
                    auto source_value = source_data[source_row_idx];
                    if constexpr (is_sequence_type(SourceTDT::data_type())) {
                        if (is_py_none(source_value) || is_py_nan(source_value)) {
                            result[source_row_idx] = target_values_to_rows[std::nullopt];
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
                                        result[source_row_idx] = target_values_to_rows[std::optional{wrapper_view}];
                                    }
                            );
                        }
                    } else {
                        result[source_row_idx] = target_values_to_rows[source_value];
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
        const std::span<const NativeTensor> source_tensors, const StreamDescriptor& source_descriptor,
        const ProcessingUnit& proc, const std::span<const std::vector<size_t>> rows_to_update
) const {
    const std::span<const std::shared_ptr<SegmentInMemory>> target_segments = *proc.segments_;
    const std::span<const std::shared_ptr<RowRange>> row_ranges = *proc.row_ranges_;
    const std::span<const std::shared_ptr<ColRange>> col_ranges = *proc.col_ranges_;
    const auto [source_row_start, source_row_end] = get_source_start_end(proc);
    bool row_slice_changed = false;
    const bool is_timeseries = source_->desc().index().type() == IndexDescriptor::Type::TIMESTAMP;
    util::BitSet matched_target_rows(row_ranges.front()->diff());
    // TODO: This can be inlined in the loop iterating over all columns to avoid iterating the source one more
    // time. The loop structure makes it not intuitive. The performance cost must be evaluated. Monday: 10655963947
    for (size_t source_row_idx = 0; source_row_idx < rows_to_update.size(); ++source_row_idx) {
        for (const size_t target_row : rows_to_update[source_row_idx]) {
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    !matched_target_rows[target_row],
                    "Multiple source rows match the same target row. This is not allowed as it's not clear which "
                    "source row should be used for the update. Target row is {}, the second source row to match it is "
                    "{}",
                    target_row,
                    source_row_idx + source_row_start
            );
            matched_target_rows[target_row] = true;
        }
    }
    // Update one column at a time to increase cache coherency and to avoid calling visit_field for each row
    // being updated
    for (size_t segment_idx = 0; segment_idx < target_segments.size(); ++segment_idx) {
        StringPool new_string_pool;
        bool segment_contains_string_column = false;
        SegmentInMemory& target_segment = *target_segments[segment_idx];
        const size_t slice_size = target_segment.num_columns();
        const int index_fields = source_descriptor.index().field_count();
        for (size_t column_index_in_slice = index_fields; column_index_in_slice < slice_size; ++column_index_in_slice) {
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
                        "{} of "
                        "size {} bytes but the stride is {} bytes",
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
                        static_cast<const SourceType*>(source_tensor.data()) + source_row_start,
                        source_row_end - source_row_start
                );
                internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                        rows_to_update.size() == source_data.size(), "Mismatched source row sizes"
                );
                internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                        !rows_to_update.empty(), "There must be at least one source row inside the target row slice."
                );
                if constexpr (is_sequence_type(DataTypeTag::data_type)) {
                    // String columns are always recreated from scratch regardless if an update or insert is
                    // happening. This is done because the string pool must be updated. With data read from disk,
                    // the map_ member of the pool is not populated. Which means that the mapping between a string
                    // and offset in the pool is missing. To get it, we need to rebuild the pool anyway.
                    segment_contains_string_column = true;
                    const MergeUpdateStringColumnFlags column_update_flags{
                            .is_timeseries = is_timeseries,
                            .data_not_changed = on_set_.contains(target_field.name()) && strategy_.update_only()
                    };
                    row_slice_changed |= merge_update_string_column<ScalarType>(
                            target_column,
                            column_update_flags,
                            rows_to_update,
                            target_field,
                            *row_ranges[segment_idx],
                            target_segment.string_pool(),
                            source_data,
                            new_string_pool
                    );
                } else {
                    if (is_update_only() && on_set_.contains(target_field.name())) {
                        return;
                    }
                    ColumnData target_column_data = target_column.data();
                    if (is_timeseries) {
                        auto target_row_to_update_it = target_column_data.begin<ScalarType>();
                        size_t target_row_to_update_idx = 0;
                        for (size_t source_row_idx = 0; source_row_idx < source_data.size(); ++source_row_idx) {
                            // TODO: Handle insert. Empty rows_to_update[source_row_idx] means that update must be done
                            row_slice_changed |= !rows_to_update[source_row_idx].empty();
                            for (const size_t target_row_idx : rows_to_update[source_row_idx]) {
                                const size_t rows_to_skip = target_row_idx - target_row_to_update_idx;
                                std::advance(target_row_to_update_it, rows_to_skip);
                                *target_row_to_update_it = source_data[source_row_idx];
                                target_row_to_update_idx = target_row_idx;
                            }
                        }
                    } else {
                        auto target = random_accessor<ScalarType>(&target_column_data);
                        for (size_t source_row_idx = 0; source_row_idx < source_data.size(); ++source_row_idx) {
                            // TODO: Handle insert. Empty rows_to_update[source_row_idx] means that update must be done
                            row_slice_changed |= !rows_to_update[source_row_idx].empty();
                            for (const size_t target_row_idx : rows_to_update[source_row_idx]) {
                                target[target_row_idx] = source_data[source_row_idx];
                            }
                        }
                    }
                }
            });
        }
        if (segment_contains_string_column) {
            target_segment.set_string_pool(std::make_shared<StringPool>(std::move(new_string_pool)));
        }
    }
    return row_slice_changed;
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
std::vector<std::vector<size_t>> MergeUpdateClause::filter_index_match(
        const Column& target_index, const std::span<const timestamp> source_index, const ProcessingUnit& proc
) const {
    using IndexType = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
    ColumnData target_index_column_data = target_index.data();
    const auto target_index_accessor = random_accessor<IndexType>(&target_index_column_data);
    const auto [source_row_start, source_row_end] = get_source_start_end(proc);
    const size_t last_target_row_to_consider = [&] {
        auto row_indexes = ranges::iota_view{position_t{0}, target_index.row_count()};
        const auto upper_bound = ranges::upper_bound(
                row_indexes,
                source_index.back(),
                [&](const timestamp value, const int64_t row_idx) { return value < target_index_accessor.at(row_idx); }
        );
        return upper_bound == row_indexes.end() ? target_index.row_count() : *upper_bound;
    }();
    size_t source_row = source_row_start;
    const size_t source_rows_in_row_slice = source_row_end - source_row;
    std::vector<std::vector<size_t>> matched_rows(source_rows_in_row_slice);
    size_t target_row = 0;
    // This loop can be inverted so that if source_row_end - source_row_start is > len(target) the complexity becomes
    // O(n * log_2(m)) where: m is the count of source rows in the bounds of the segment, n is the number of target rows
    // in the segment.
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
            matched_rows[source_row - source_row_start].push_back(target_row);
            ++target_row;
        }
        // Optimizes the case of repeated index values. All matched rows corresponding to a particular index value must
        // be the same, so just copy the matched rows in case index values are repeated.
        while (++source_row < source_row_end && source_index[source_row] == source_ts) {
            const size_t source_row_offset = source_row - source_row_start;
            matched_rows[source_row_offset] = matched_rows[source_row_offset - 1];
        }
    }
    return matched_rows;
}

/// Complexity: $$O(c * n * m)$$ m is the count of source rows in the bounds of the segment, n is the  number of target
/// rows in the segment. c is the number of columns in MergeUpdateClause::on_. It can be reached if the data in source
/// and target is the same up to the very last column in MergeUpdateClause::on_.
std::vector<std::vector<size_t>> MergeUpdateClause::filter_on_additional_columns_match(
        const StreamDescriptor& source_descriptor, const StreamDescriptor& target_descriptor,
        const std::span<const NativeTensor> source_tensors, ProcessingUnit& proc,
        std::optional<std::vector<std::vector<size_t>>>&& index_match
) const {
    ranges::subrange on = on_;
    std::vector<std::vector<size_t>> matched_rows;
    const IndexDescriptor::Type source_index_type = source_descriptor.index().type();
    const IndexDescriptor::Type target_index_type = target_descriptor.index().type();
    if (index_match) {
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                (source_index_type == target_index_type) && (source_index_type == IndexDescriptor::Type::TIMESTAMP),
                "Source and target index types must both be TIMESTAMP. Source: {}, target: {}",
                source_index_type,
                target_index_type
        );
        matched_rows = std::move(*index_match);
    } else {
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                (source_index_type == target_index_type) && (source_index_type == IndexDescriptor::Type::ROWCOUNT),
                "Source and target index types to be both ROWCOUNT. Source: {}, target: {}",
                source_index_type,
                target_index_type
        );
        matched_rows = initialize_rows_to_update_for_rowrange_indexed_data(proc, source_descriptor);
        on = on.next();
    }
    if (on.empty()) {
        return matched_rows;
    }
    const auto [source_row_start, source_row_end] = get_source_start_end(proc);
    // GIL will be acquired if there is a string that is not pure ASCII/UTF-8
    // In this case a PyObject will be allocated by convert::py_unicode_to_buffer
    // If such a string is encountered in a column, then the GIL will be held until that whole column has
    // been processed, on the assumption that if a column has one such string it will probably have many.
    std::optional<ScopedGILLock> scoped_gil_lock;
    for (std::string_view column_name : on) {
        const std::optional<size_t> source_field_position = source_descriptor.find_field(column_name);
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                source_field_position,
                "Column '{}' specified in the 'on' parameter is not present in the source DataFrame",
                column_name
        );
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                target_descriptor.index().type() != IndexDescriptor::Type::TIMESTAMP ||
                        target_descriptor.field(0).name() != column_name,
                "Column '{}' specified in the 'on' parameter cannot have the same name as the timestamp index column"
        );
        const Field& source_field = source_descriptor.field(*source_field_position);
        details::visit_type(source_field.type().data_type(), [&]<typename SourceDataTypeTag>(SourceDataTypeTag) {
            using SourceTDT = ScalarTagType<SourceDataTypeTag>;
            using SourceType = std::conditional_t<
                    is_sequence_type(SourceDataTypeTag::data_type),
                    PyObject* const*,
                    const typename SourceDataTypeTag::raw_type*>;
            const size_t column_position_in_source_tensors =
                    *source_field_position - source_descriptor.index().field_count();
            std::span source_data(
                    static_cast<SourceType>(source_tensors[column_position_in_source_tensors].data()) +
                            source_row_start,
                    source_row_end - source_row_start
            );
            const ColumnWithStrings target_column = std::get<ColumnWithStrings>(proc.get(ColumnName{column_name}));
            ColumnData target_data = target_column.column_->data();
            details::visit_type(
                    target_column.column_->type().data_type(),
                    [&]<typename TargetDataTypeTag>(TargetDataTypeTag) {
                        using TargetTDT = ScalarTagType<TargetDataTypeTag>;
                        using TargetRawType = TargetDataTypeTag::raw_type;
                        // TODO: Relax for dynamic schema
                        if constexpr (std::same_as<std::decay_t<SourceDataTypeTag>, std::decay_t<TargetDataTypeTag>>) {
                            auto target_accessor = random_accessor<TargetTDT>(&target_data);
                            for (size_t source_row_idx = 0; source_row_idx < matched_rows.size(); ++source_row_idx) {
                                std::erase_if(matched_rows[source_row_idx], [&](const size_t target_row_idx) {
                                    const TargetRawType target_value = target_accessor.at(target_row_idx);
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
                                                    err.raise(column_name, source_row_start);
                                                }
                                        );
                                    } else {
                                        return !std::get<bool>(are_values_equal);
                                    }
                                });
                            }
                        } else {
                            internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                                    "Target column \"{}\" has unexpected type {}. Source type: {}",
                                    column_name,
                                    TargetDataTypeTag::data_type,
                                    SourceDataTypeTag::data_type
                            );
                        }
                    }
            );
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

} // namespace arcticdb
