#pragma once

#include <arcticdb/pipeline/column_name_resolution.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/pipeline/index_fields.hpp>
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
using ColumnStatTypeInternal = arcticc::pb2::descriptors_pb2::ColumnStatsType;

static const char* const start_index_column_name = "start_index";
static constexpr size_t start_index_column_offset = 0;
static const char* const end_index_column_name = "end_index";
static constexpr size_t end_index_column_offset = 1;

struct NameAndStatTypes {
    std::string mangled_name;
    std::set<ColumnStatType> column_stats;

    bool operator==(const NameAndStatTypes& right) const {
        return mangled_name == right.mangled_name && column_stats == right.column_stats;
    }
};

class ColumnStats {
  public:
    explicit ColumnStats(const std::unordered_map<std::string, std::unordered_set<std::string>>& column_stats);
    explicit ColumnStats(
            const arcticc::pb2::descriptors_pb2::ColumnStatsHeader& header, const TimeseriesDescriptor& tsd
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
    // Use ordered map/set here for consistent ordering in the resulting stats objects
    std::map<std::string, std::set<ColumnStatType>> user_specified_column_stats_;
    std::unordered_map<size_t, NameAndStatTypes> offset_to_stat_info_;
    bool offset_to_stat_info_set_{false};
};

} // namespace arcticdb
