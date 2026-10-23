#pragma once

#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/processing/schema_combine.hpp>
#include <arcticdb/entity/normalization_utils.hpp>
#include <arcticdb/entity/timeseries_descriptor.hpp>
#include <arcticdb/stream/incompletes.hpp>

namespace arcticdb {

entity::OutputSchema combine_existing_tsd_with_frame(
        NormalizationOperation operation, bool dynamic_schema, const TimeseriesDescriptor& existing_tsd,
        pipelines::InputFrame& new_frame
);

struct IncompleteSchemas {
    entity::OutputSchema staged_;   // the incomplete segments combined with each other
    entity::OutputSchema combined_; // `staged_` combined with what is already in the symbol
};

// Combines the schema of the data already in a symbol with the schemas of all the incomplete segments going into it.
// `existing` is nullopt only when the symbol holds nothing but staged data.
IncompleteSchemas combine_incomplete_schemas(
        const std::optional<entity::OutputSchema>& existing, size_t existing_total_rows,
        std::span<const AppendMapEntry> incompletes, const ReadIncompletesFlags& flags, const StreamId& stream_id
);
} // namespace arcticdb
