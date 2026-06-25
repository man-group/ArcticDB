#pragma once

#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/python/normalization_utils.hpp>
#include <arcticdb/entity/timeseries_descriptor.hpp>

namespace arcticdb {

enum NormalizationOperation : uint8_t {
    APPEND,
    UPDATE,
};

struct StreamDescriptorMismatch : ArcticSpecificException<ErrorCode::E_DESCRIPTOR_MISMATCH> {
    StreamDescriptorMismatch(
            const char* preamble, const StreamId& stream_id, const StreamDescriptor& existing,
            const StreamDescriptor& new_val, NormalizationOperation operation
    );
};

bool index_names_match(const StreamDescriptor& df_in_store_descriptor, const StreamDescriptor& new_df_descriptor);

bool columns_match(
        const StreamDescriptor& df_in_store_descriptor, const StreamDescriptor& new_df_descriptor,
        const bool convert_int_to_float = false
);

void fix_descriptor_mismatch_or_throw(
        NormalizationOperation operation, bool dynamic_schema, const TimeseriesDescriptor& existing_tsd,
        const pipelines::InputFrame& new_frame, bool empty_types
);
} // namespace arcticdb
