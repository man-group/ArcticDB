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

#include <ankerl/unordered_dense.h>

// Anything that transitively includes sparrow.array.hpp takes ages to build the (unused by us) std::format impl
// So avoid including sparrow in headers where possible until this is resolved
namespace sparrow {
class array;
class record_batch;
} // namespace sparrow

namespace arcticdb {

namespace entity {
struct StreamDescriptor;
}

class SegmentInMemory;
class Column;
struct ArrowOutputConfig;
struct RecordBatchData;

std::vector<sparrow::array> arrow_arrays_from_column(const Column& column, std::string_view name);

std::shared_ptr<std::vector<sparrow::record_batch>> segment_to_arrow_data(SegmentInMemory& segment);

// It would be cleaner if the index column position finding happened in the Python layer. However, finding a column by
// name is O(n), and we have to iterate through the columns here anyway
std::pair<SegmentInMemory, std::optional<size_t>> arrow_data_to_segment(
        const std::vector<sparrow::record_batch>& record_batches,
        const std::optional<std::string>& index_name = std::nullopt
);

RecordBatchData arrow_schema_from_descriptor(
        const entity::StreamDescriptor& stream_desc, const ArrowOutputConfig& arrow_output_config,
        const std::optional<ankerl::unordered_dense::set<std::string_view>>& columns
);

} // namespace arcticdb