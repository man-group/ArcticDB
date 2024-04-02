#pragma once

#include <arcticdb/pipeline/input_tensor_frame.hpp>
#include <arcticdb/python/normalization_checks.hpp>
#include <arcticdb/entity/type_utils.hpp>

namespace arcticdb {

enum NormalizationOperation : uint8_t {
    APPEND,
    UPDATE,
};

inline std::string_view normalization_operation_str(NormalizationOperation operation) {
    switch (operation) {
        case APPEND:
            return "APPEND";
        case UPDATE:
            return "UPDATE";
        default:
            util::raise_rte("Unknown operation type {}", static_cast<uint8_t>(operation));
    }
}


struct StreamDescriptorMismatch : ArcticSpecificException<ErrorCode::E_DESCRIPTOR_MISMATCH>  {
    StreamDescriptorMismatch(const char* preamble, const StreamDescriptor& existing, const StreamDescriptor& new_val, NormalizationOperation operation) :
    ArcticSpecificException(fmt::format("{}: {} \nexisting={}\n new_val={}", preamble, normalization_operation_str(operation),
                                        fmt::join(existing.fields(), ", "), fmt::join(new_val.fields(), ", "))) {}
};

inline IndexDescriptor::Type get_common_index_type(const IndexDescriptor::Type& left, const IndexDescriptor::Type& right) {
    if (left == right) {
        return left;
    }
    if (left == IndexDescriptor::EMPTY) {
        return right;
    }
    if (right == IndexDescriptor::EMPTY) {
        return left;
    }
    return IndexDescriptor::UNKNOWN;
}

inline void check_normalization_index_match(NormalizationOperation operation,
                                     const StreamDescriptor &old_descriptor,
                                     const pipelines::InputTensorFrame &frame) {
    const IndexDescriptor::Type old_idx_kind = old_descriptor.index().type();
    const IndexDescriptor::Type new_idx_kind = frame.desc.index().type();
    if (operation == UPDATE) {
        const bool new_is_timeseries = std::holds_alternative<TimeseriesIndex>(frame.index);
        util::check_rte(
            (old_idx_kind == IndexDescriptor::TIMESTAMP || old_idx_kind == IndexDescriptor::EMPTY) && new_is_timeseries,
                        "Update will not work as expected with a non-timeseries index");
    } else {
        const IndexDescriptor::Type common_index_type = get_common_index_type(old_idx_kind, new_idx_kind);
        normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
            common_index_type != IndexDescriptor::UNKNOWN,
            "Cannot append {} index to {} index",
            index_type_to_str(new_idx_kind),
            index_type_to_str(old_idx_kind)
        );
    }
}

inline bool columns_match(const StreamDescriptor& df_in_store_descriptor, const StreamDescriptor& new_df_descriptor) {
    const int right_fields_offset = df_in_store_descriptor.index().type() == IndexDescriptor::EMPTY ? new_df_descriptor.index().field_count() : 0;
    // The empty index is compatible with all other index types. Differences in the index fields in this case is
    // allowed. The index fields are always the first in the list.
    if (df_in_store_descriptor.fields().size() + right_fields_offset != new_df_descriptor.fields().size()) {
        return false;
    }
    // In case the left index is empty index we want to skip name/type checking of the index fields which are always
    // the first fields.
    for (auto i = 0; i < int(df_in_store_descriptor.fields().size()); ++i) {
        if (df_in_store_descriptor.fields(i).name() != new_df_descriptor.fields(i + right_fields_offset).name())
            return false;

        const TypeDescriptor& left_type = df_in_store_descriptor.fields(i).type();
        const TypeDescriptor& right_type = new_df_descriptor.fields(i + right_fields_offset).type();

        if (!trivially_compatible_types(left_type, right_type) &&
            !(is_empty_type(left_type.data_type()) || is_empty_type(right_type.data_type())))
            return false;
    }
    return true;
}

inline void fix_descriptor_mismatch_or_throw(
    NormalizationOperation operation,
    bool dynamic_schema,
    const pipelines::index::IndexSegmentReader &existing_isr,
    const pipelines::InputTensorFrame &new_frame) {
    const auto &old_sd = existing_isr.tsd().as_stream_descriptor();
    check_normalization_index_match(operation, old_sd, new_frame);

    if (dynamic_schema)
        return; // TODO: dynamic schema may need some of the checks as below

    fix_normalization_or_throw(operation == APPEND, existing_isr, new_frame);

    if (!columns_match(old_sd, new_frame.desc)) {
        throw StreamDescriptorMismatch(
            "The columns (names and types) in the argument are not identical to that of the existing version",
            StreamDescriptor{old_sd},
            new_frame.desc,
            operation);
    }
}
} // namespace arcticdb
