#pragma once

#include <arcticdb/pipeline/column_name_resolution.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/pipeline/index_fields.hpp>
#include <column_stats.pb.h>
#include <ankerl/unordered_dense.h>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace arcticdb {
SegmentInMemory merge_column_stats_segments(const std::vector<SegmentInMemory>& segments);

// User facing types - eg users are only allowed to create min and max together, not one or the other
enum class ColumnStatType { MINMAX };
// Total universe of column stats we support - min and max are treated separately here
using ColumnStatTypeInternal = arcticc::pb2::column_stats_pb2::ColumnStatsType;

static const char* const start_row_column_name = "start_row";
static constexpr size_t start_row_column_offset = 0;
static constexpr size_t first_stat_column_offset = 1;

struct NameAndStats {
    std::string mangled_name;
    std::set<ColumnStatType> column_stats;

    bool operator==(const NameAndStats& right) const {
        return mangled_name == right.mangled_name && column_stats == right.column_stats;
    }
};

void validate_column_stats_header_version(const arcticc::pb2::column_stats_pb2::ColumnStatsHeader& header);

// Produce the segment column name for column and its stat type.
// Example: stat MIN for the column "price" -> "v1_MIN(price)"
std::string column_and_stat_to_segment_name(const std::string& column, ColumnStatTypeInternal type);

class ColumnStats {
  public:
    explicit ColumnStats(const std::unordered_map<std::string, std::unordered_set<std::string>>& column_stats);
    explicit ColumnStats(
            const arcticc::pb2::column_stats_pb2::ColumnStatsHeader& header, const TimeseriesDescriptor& tsd
    );

    // Returns the segment column names of the dropped stats (e.g. "v1_MIN(col)", "v1_MAX(col)")
    std::vector<std::string> drop(const ColumnStats& to_drop, bool warn_if_missing = true);

    // Calculate the fields to which the column stats refer.
    void calculate_offsets(
            const TimeseriesDescriptor& tsd,
            utils::MissingColumnsBehavior missing_columns = utils::MissingColumnsBehavior::RAISE
    );

    std::unordered_map<std::string, std::unordered_set<std::string>> to_map() const;
    std::optional<Clause> clause() const;
    bool empty() const;

    bool operator==(const ColumnStats& right) const;

  private:
    void map_stats_to_column_name(const std::string& column_name, const std::unordered_set<std::string>& stats_names);

    std::map<std::string, std::set<ColumnStatType>> input_column_name_to_stats_;
    std::unordered_map<size_t, NameAndStats> offset_to_input_column_and_stats_;
    bool offset_to_input_column_and_stats_calculated_{false};
};

} // namespace arcticdb
