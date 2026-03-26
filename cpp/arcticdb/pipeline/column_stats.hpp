#pragma once

#include <arcticdb/processing/clause.hpp>
#include <ankerl/unordered_dense.h>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace arcticdb {

SegmentInMemory merge_column_stats_segments(const std::vector<SegmentInMemory>& segments);

// User facing types - eg users are only allowed to create min and max together, not one or the other
enum class ColumnStatType { MINMAX };
// Total universe of column stats we support - min and max are treated separately here
// TODO aseaton just use the protobuf type
enum class ColumnStatTypeInternal { MIN, MAX };

struct ColumnStatMetadata {
    ColumnStatType type;
    // Offsets for these statistics in to the segment in which they are saved on disk
    // Empty if the stats have not been serialized yet.
    std::vector<size_t> stats_seg_offsets;
};

static const char* const start_index_column_name = "start_index";
static const char* const end_index_column_name = "end_index";

class ColumnStats {
  public:
    explicit ColumnStats(const std::unordered_map<std::string, std::unordered_set<std::string>>& column_stats);
    explicit ColumnStats(
        const arcticc::pb2::descriptors_pb2::ColumnStatsHeader& header, const FieldCollection& data_fields
    );

    // Returns dropped offsets in to the segment's fields
    std::vector<size_t> drop(const ColumnStats& to_drop, bool warn_if_missing = true);
    ankerl::unordered_dense::set<std::string> segment_column_names() const;

    std::unordered_map<std::string, std::unordered_set<std::string>> to_map() const;
    std::optional<Clause> clause(const FieldCollection& all_fields) const;
    bool empty() const;

    bool operator==(const ColumnStats& right) const;

  private:
    // Use ordered map/set here for consistent ordering in the resulting stats objects
    std::map<std::string, std::set<ColumnStatType>> column_stats_;

    // eg, "col_a" -> "MINMAX" -> [4,5] to indicate that min max stats for "col_a" are in offsets
    // 4 and 5 of the column stats segment
    std::unordered_map<std::string, std::unordered_map<ColumnStatType, std::vector<size_t>>> offsets_;
};

} // namespace arcticdb