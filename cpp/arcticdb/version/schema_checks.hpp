#pragma once

#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/processing/schema_combine.hpp>
#include <arcticdb/python/normalization_utils.hpp>
#include <arcticdb/entity/timeseries_descriptor.hpp>

namespace arcticdb {

// TODO (monday ref 12821228270): Replace both `index_names_match` and `columns_match` with combine_schema.
// Currently used for reading and compacting incomplete segments, merge-update and defrag.
bool index_names_match(const StreamDescriptor& df_in_store_descriptor, const StreamDescriptor& new_df_descriptor);

bool columns_match(
        const StreamDescriptor& df_in_store_descriptor, const StreamDescriptor& new_df_descriptor,
        const bool convert_int_to_float = false
);

entity::OutputSchema combine_existing_tsd_with_frame(
        NormalizationOperation operation, bool dynamic_schema, const TimeseriesDescriptor& existing_tsd,
        const pipelines::InputFrame& new_frame
);
} // namespace arcticdb
