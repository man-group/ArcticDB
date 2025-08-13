#include <arcticdb/version/schema_checks.hpp>
#include <arcticdb/pipeline/index_segment_reader.hpp>

namespace arcticdb {

std::string_view normalization_operation_str(NormalizationOperation operation) {
    switch (operation) {
        case APPEND:
            return "APPEND";
        case UPDATE:
            return "UPDATE";
        default:
            util::raise_rte("Unknown operation type {}", static_cast<uint8_t>(operation));
    }
}

IndexDescriptor::Type get_common_index_type(const IndexDescriptor::Type& left, const IndexDescriptor::Type& right) {
    if (left == right) {
        return left;
    }
    if (left == IndexDescriptor::Type::EMPTY) {
        return right;
    }
    if (right == IndexDescriptor::Type::EMPTY) {
        return left;
    }
    return IndexDescriptor::Type::UNKNOWN;
}

void check_normalization_index_match(
    NormalizationOperation operation,
    const StreamDescriptor& old_descriptor,
    const pipelines::InputTensorFrame& frame,
    bool empty_types
) {
    const IndexDescriptor::Type old_idx_kind = old_descriptor.index().type();
    const IndexDescriptor::Type new_idx_kind = frame.desc.index().type();
    if (operation == UPDATE) {
        const bool new_is_timeseries = std::holds_alternative<arcticdb::stream::TimeseriesIndex>(frame.index);
        util::check_rte(
            (old_idx_kind == IndexDescriptor::Type::TIMESTAMP || old_idx_kind == IndexDescriptor::Type::EMPTY) && new_is_timeseries,
            "Update will not work as expected with a non-timeseries index"
        );
    } else {
        const IndexDescriptor::Type common_index_type = get_common_index_type(old_idx_kind, new_idx_kind);
        if (empty_types) {
            normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
                common_index_type != IndexDescriptor::Type::UNKNOWN,
                "Cannot append {} index to {} index",
                index_type_to_str(new_idx_kind),
                index_type_to_str(old_idx_kind)
            );
        } else {
            // (old_idx_kind == IndexDescriptor::Type::TIMESTAMP && new_idx_kind == IndexDescriptor::Type::ROWCOUNT) is left to preserve
            // pre-empty index behavior with pandas 2, see test_empty_writes.py::test_append_empty_series. Empty pd.Series
            // have Rowrange index, but due to: https://github.com/man-group/ArcticDB/blob/bd1776291fe402d8b18af9fea865324ebd7705f1/python/arcticdb/version_store/_normalization.py#L545
            // it gets converted to DatetimeIndex (all empty indexes except categorical and multiindex are converted to datetime index
            // in pandas 2 if empty index type is disabled), however we still want to be able to append pd.Series to empty pd.Series.
            // Having this will not allow appending RowCont indexed pd.DataFrames to DateTime indexed pd.DataFrames because they would
            // have different field size (the rowcount index is not stored as a field). This logic is bug prone and will become better
            // after we enable the empty index.
            const bool input_frame_is_series = frame.norm_meta.has_series();
            normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
                common_index_type != IndexDescriptor::Type::UNKNOWN ||
                    (input_frame_is_series && old_idx_kind == IndexDescriptor::Type::TIMESTAMP && new_idx_kind == IndexDescriptor::Type::ROWCOUNT),
                "Cannot append {} index to {} index",
                index_type_to_str(new_idx_kind),
                index_type_to_str(old_idx_kind)
            );
        }
    }
}

bool index_names_match(
    const StreamDescriptor& df_in_store_descriptor,
    const StreamDescriptor& new_df_descriptor
) {
    auto df_in_store_index_field_count = df_in_store_descriptor.index().field_count();
    auto new_df_field_index_count = new_df_descriptor.index().field_count();

    // If either index is empty, we consider them to match
    if (df_in_store_index_field_count == 0 || new_df_field_index_count == 0) {
        return true;
    }

    if (df_in_store_index_field_count != new_df_field_index_count) {
        return false;
    }

    for (auto i = 0; i < int(df_in_store_index_field_count); ++i) {
        if (df_in_store_descriptor.fields(i).name() != new_df_descriptor.fields(i).name()) {
            return false;
        }
    }

    return true;
}

/// @param convert_int_to_float If this is true it will consider all pairs of integer types (both signed and unsigned)
///   as identical. If a field in df_in_store_descriptor is FLOAT64 and the corresponding field in new_df_descriptor
///   is of any integer type they will be considered identical. Note that this makes the function unsymmetrical. If a
///   field in new_df_descriptor is FLOAT64 and the corresponding field in df_in_store_descriptor is of integer type
///   the types won't be considered identical. This is supposed to be used only from compact_incomplete.B
bool columns_match(
    const StreamDescriptor& df_in_store_descriptor,
    const StreamDescriptor& new_df_descriptor,
    const bool convert_int_to_float
) {
    const int index_field_size =
        df_in_store_descriptor.index().type() == IndexDescriptor::Type::EMPTY ? new_df_descriptor.index().field_count() : 0;
    // The empty index is compatible with all other index types. Differences in the index fields in this case is
    // allowed. The index fields are always the first in the list.
    if (df_in_store_descriptor.fields().size() + index_field_size != new_df_descriptor.fields().size()) {
        return false;
    }
    // In case the left index is empty index we want to skip name/type checking of the index fields which are always
    // the first fields.
    for (auto i = 0; i < int(df_in_store_descriptor.fields().size()); ++i) {
        if (df_in_store_descriptor.fields(i).name() != new_df_descriptor.fields(i + index_field_size).name())
            return false;

        const TypeDescriptor& left_type = df_in_store_descriptor.fields(i).type();
        const TypeDescriptor& right_type = new_df_descriptor.fields(i + index_field_size).type();

        if (!trivially_compatible_types(left_type, right_type) &&
            !(is_empty_type(left_type.data_type()) || is_empty_type(right_type.data_type()))) {
            if (convert_int_to_float) {
                const bool both_are_int = is_integer_type(left_type.data_type()) && is_integer_type(right_type.data_type());
                if (!(both_are_int || (left_type.data_type() == DataType::FLOAT64 && is_integer_type(right_type.data_type())))) {
                    return false;
                }
            } else {
                return false;
            }
        }

    }
    return true;
}

void fix_descriptor_mismatch_or_throw(
    NormalizationOperation operation,
    bool dynamic_schema,
    const pipelines::index::IndexSegmentReader &existing_isr,
    const pipelines::InputTensorFrame &new_frame,
    bool empty_types) {
    const auto &old_sd = existing_isr.tsd().as_stream_descriptor();
    check_normalization_index_match(operation, old_sd, new_frame, empty_types);

    fix_normalization_or_throw(operation == APPEND, existing_isr, new_frame);

    // We need to check that the index names match regardless of the dynamic schema setting
    if(!index_names_match(old_sd, new_frame.desc)) {
        throw StreamDescriptorMismatch(
            "The index names in the argument are not identical to that of the existing version",
            new_frame.desc.id(),
            old_sd,
            new_frame.desc,
            operation);
    }

    if (!dynamic_schema && !columns_match(old_sd, new_frame.desc)) {
        throw StreamDescriptorMismatch(
            "The columns (names and types) in the argument are not identical to that of the existing version",
            new_frame.desc.id(),
            old_sd,
            new_frame.desc,
            operation);
    }
    if (dynamic_schema && new_frame.norm_meta.has_series() && existing_isr.tsd().normalization().has_series()) {
        // Series have one column. If there's a date time index or a Multiindex, the first fields will be for the index.
        // The last field will always be the data column.
        const size_t new_column_index = new_frame.desc.fields().size() - 1;
        const size_t existing_column_index = existing_isr.tsd().fields().size() - 1;
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
            new_frame.desc.fields()[new_column_index].name() == existing_isr.tsd().fields()[existing_column_index].name(),
            "Series are not allowed to have columns with different names for append and update even for dynamic schema. Existing name: {}, new name: {}",
            existing_isr.tsd().as_stream_descriptor().fields()[existing_column_index].name(),
            new_frame.desc.fields()[new_column_index].name());
    }
}
} // namespace arcticdb
