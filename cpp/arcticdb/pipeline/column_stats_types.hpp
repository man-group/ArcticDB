/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <column_stats.pb.h>

#include <string>
#include <vector>

namespace arcticdb {

using ColumnStatTypeInternal = arcticc::pb2::column_stats_pb2::ColumnStatsType;

struct ColumnStatValue {
    // to_segment_column_name(<name of the column at data_col_offset>, type), e.g. "v1_MIN(col)"
    std::string segment_column_name;
    ColumnStatTypeInternal type;
    size_t data_col_offset;
    Value value;
};

/// One per row slice. Added to the ComponentManager as a standalone entity by
/// ColumnStatsGenerationClause::process and read back by create_column_stats_impl via
/// process_entities
struct ColumnStatsRow {
    uint64_t start_row;
    uint64_t end_row;
    std::vector<ColumnStatValue> stats;
};

} // namespace arcticdb
