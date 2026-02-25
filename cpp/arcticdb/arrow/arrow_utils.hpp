/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <sparrow/c_interface.hpp>

#include <arcticdb/entity/types.hpp>

// Anything that transitively includes sparrow.array.hpp takes ages to build the (unused by us) std::format impl
// So avoid including sparrow in headers where possible until this is resolved
namespace sparrow {
class array;
class record_batch;
} // namespace sparrow

namespace arcticdb {

class SegmentInMemory;
class Column;
struct RecordBatchData;

std::vector<sparrow::array> arrow_arrays_from_column(const Column& column, std::string_view name);

std::shared_ptr<std::vector<sparrow::record_batch>> segment_to_arrow_data(SegmentInMemory& segment);

// It would be cleaner if the index column position finding happened in the Python layer. However, finding a column by
// name is O(n), and we have to iterate through the columns here anyway
std::pair<SegmentInMemory, std::optional<size_t>> arrow_data_to_segment(
        const std::vector<sparrow::record_batch>& record_batches,
        const std::optional<std::string>& index_name = std::nullopt
);

// Horizontally merge two RecordBatchData objects (column-slice merging).
// Takes children (column arrays) from both batches, deduplicates by column name
// (index columns appear in every slice), and returns a merged RecordBatchData.
// Zero-copy: child buffer pointers are transferred, not copied.
// The input batches are consumed (moved from) and their release callbacks are
// managed by the merged output's release callback.
RecordBatchData horizontal_merge_arrow_batches(RecordBatchData&& batch_a, RecordBatchData&& batch_b);

// Target field for schema padding. Describes a single column in the target schema.
// The arrow_format and is_dictionary fields are resolved eagerly from the descriptor
// and ReadOptions at iterator construction time.
struct TargetField {
    std::string name;
    // Arrow C Data Interface format string (e.g. "l" for int64, "g" for float64).
    // Empty until resolved from an actual batch.
    std::string arrow_format;
    // True if the column is dictionary-encoded (arrow_format is the key type).
    bool is_dictionary = false;
    // True once arrow_format has been captured from an actual batch.
    bool format_resolved = false;
};

// Map an ArcticDB DataType to a default Arrow format string.
// Used as fallback when no actual batch has been seen for this column.
std::string default_arrow_format_for_type(entity::DataType data_type);

// Resolve unresolved target fields using the schema from an actual batch.
// For each child in batch_schema, if a matching TargetField exists and is unresolved,
// captures the arrow_format and is_dictionary flag.
void resolve_target_fields_from_batch(std::vector<TargetField>& target_fields, const ArrowSchema& batch_schema);

// Pad a RecordBatchData to match a target schema.
// Adds null-filled columns for fields missing from the batch, removes columns
// not in the target, and reorders columns to match target field order.
// Returns the batch unchanged if it already matches.
RecordBatchData pad_batch_to_schema(RecordBatchData&& batch, const std::vector<TargetField>& target_fields);

} // namespace arcticdb