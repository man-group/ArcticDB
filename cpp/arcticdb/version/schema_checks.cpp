#include <arcticdb/version/schema_checks.hpp>
#include <arcticdb/pipeline/index_segment_reader.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/processing/schema_combine.hpp>

namespace {
using namespace arcticdb;

// A symbol with no rows contributes no columns, so its schema is discarded in favour of the new frame's rather than
// combined with it. Its index type is still a constraint though: appending a RangeIndexed frame to a symbol written
// as a zero-row timeseries has never been allowed, and the schema combine never sees the pair to reject it.
void check_rowless_index_types_combinable(
        const SchemaCombineOptions& options, const TimeseriesDescriptor& existing_tsd,
        const pipelines::InputFrame& frame
) {
    const IndexDescriptor::Type old_idx_kind = existing_tsd.as_stream_descriptor().index().type();
    const IndexDescriptor::Type new_idx_kind = frame.desc().index().type();
    // A Series written empty lands as a timeseries even though pandas gives it a RangeIndex, so appending a Series
    // to an empty Series has to keep working. See test_empty_writes.py::test_append_empty_series and
    // _normalization.py, which converts every empty index except categorical and multi-index to a DatetimeIndex.
    if (frame.norm_meta.has_series() && old_idx_kind == IndexDescriptor::Type::TIMESTAMP &&
        new_idx_kind == IndexDescriptor::Type::ROWCOUNT) {
        return;
    }
    check_index_types_combinable(old_idx_kind, new_idx_kind, options);
}

// A RangeIndex has to continue where the existing one stopped, which is the only part of the merge that needs the
// existing row count and so the only part combine_schema cannot do. Rewrites the new frame's start so that it spans
// both, and so has to run before the schemas are combined.
void align_rowrange_norm_for_append(const TimeseriesDescriptor& existing_tsd, const pipelines::InputFrame& new_frame) {
    if (existing_tsd.index().type() != IndexDescriptor::Type::ROWCOUNT ||
        new_frame.desc().index().type() != IndexDescriptor::Type::ROWCOUNT) {
        return;
    }
    // We need to update only for pandas rowrange.
    const auto* existing_pandas = pandas_common(existing_tsd.normalization());
    const auto* new_pandas = pandas_common(new_frame.norm_meta);
    if (existing_pandas == nullptr || new_pandas == nullptr || !existing_pandas->has_index() ||
        !new_pandas->has_index()) {
        return;
    }
    update_rowrange_norm_for_append(existing_tsd.normalization(), new_frame.norm_meta, existing_tsd.total_rows());
}

} // namespace

namespace arcticdb {

bool index_names_match(const StreamDescriptor& df_in_store_descriptor, const StreamDescriptor& new_df_descriptor) {
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
        const StreamDescriptor& df_in_store_descriptor, const StreamDescriptor& new_df_descriptor,
        const bool convert_int_to_float
) {
    const int index_field_size = df_in_store_descriptor.index().type() == IndexDescriptor::Type::EMPTY
                                         ? new_df_descriptor.index().field_count()
                                         : 0;
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
                const bool both_are_int =
                        is_integer_type(left_type.data_type()) && is_integer_type(right_type.data_type());
                if (!(both_are_int ||
                      (left_type.data_type() == DataType::FLOAT64 && is_integer_type(right_type.data_type())))) {
                    return false;
                }
            } else {
                return false;
            }
        }
    }
    return true;
}

entity::OutputSchema combine_existing_tsd_with_frame(
        NormalizationOperation operation, bool dynamic_schema, const TimeseriesDescriptor& existing_tsd,
        const pipelines::InputFrame& new_frame
) {
    const auto options = append_or_update_options(dynamic_schema, operation, new_frame.desc().id());
    if (operation == NormalizationOperation::APPEND) {
        align_rowrange_norm_for_append(existing_tsd, new_frame);
    }
    if (existing_tsd.total_rows() == 0) {
        check_rowless_index_types_combinable(options, existing_tsd, new_frame);
        return schema_from_input_frame(new_frame);
    }
    const std::array schemas{schema_from_tsd(existing_tsd), schema_from_input_frame(new_frame)};
    return combine_schema(schemas, options);
}
} // namespace arcticdb
